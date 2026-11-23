package search

import (
	"context"
	"fmt"
	"strings"

	"github.com/nacfson/nacfson-memory/internal/storage"
)

const minHitsBeforeORFallback = 5

// LexicalSearch executes stock PostgreSQL 17 full-text (tsvector) + trigram (pg_trgm) search.
type LexicalSearch struct{}

// NewLexicalSearch returns a new LexicalSearch helper.
func NewLexicalSearch() *LexicalSearch {
	return &LexicalSearch{}
}

// SearchParams configures a single-scope or workspace-inherited lexical recall pass.
type SearchParams struct {
	WorkspaceID    string
	InheritGeneral string
	Query          string
	Limit          int
}

// LexicalHit represents one scored lexical search result.
type LexicalHit struct {
	Memory  storage.Memory `json:"memory"`
	Score   float64        `json:"score"`
	TSRank  float64        `json:"ts_rank"`
	TrgmSim float64        `json:"trgm_sim"`
}

// Search executes GIN-indexed tsvector + pg_trgm search across allowed non-constitution
// memories, falling back to an OR-joined lexeme query inside db when < 5 hits match
// websearch_to_tsquery, and applying project-over-'general' topic_key overrides.
func (s *LexicalSearch) Search(ctx context.Context, db storage.DBTX, params SearchParams) ([]LexicalHit, error) {
	limit := params.Limit
	if limit <= 0 {
		limit = 20
	}

	projectHits, err := s.searchScope(ctx, db, params.WorkspaceID, false, params.Query, limit)
	if err != nil {
		return nil, err
	}

	if params.WorkspaceID == storage.WorkspaceGeneral || params.InheritGeneral != storage.InheritConstitutionAndDurable {
		return projectHits, nil
	}

	generalHits, err := s.searchScope(ctx, db, storage.WorkspaceGeneral, true, params.Query, limit)
	if err != nil {
		return nil, err
	}

	seenTopics := make(map[string]struct{}, len(projectHits))
	merged := make([]LexicalHit, 0, len(projectHits)+len(generalHits))
	for _, h := range projectHits {
		seenTopics[h.Memory.TopicKey] = struct{}{}
		merged = append(merged, h)
	}
	for _, h := range generalHits {
		if _, overridden := seenTopics[h.Memory.TopicKey]; overridden {
			continue
		}
		merged = append(merged, h)
	}
	return merged, nil
}

func (s *LexicalSearch) searchScope(ctx context.Context, db storage.DBTX, workspaceID string, durableOnly bool, rawQuery string, limit int) ([]LexicalHit, error) {
	durFilter := "m.durability <> 'constitution'"
	if durableOnly {
		durFilter = "m.durability = 'durable'"
	}

	trimmed := strings.TrimSpace(rawQuery)
	if trimmed == "" {
		return queryRecentScope(ctx, db, workspaceID, durFilter, limit)
	}

	// Pass 1: high-precision websearch_to_tsquery + topic_key trigram match.
	andHits, err := queryTSQueryScope(ctx, db, workspaceID, durFilter, trimmed, limit, false)
	if err != nil {
		return nil, err
	}
	if len(andHits) >= minHitsBeforeORFallback {
		return andHits, nil
	}

	// Pass 2: broad OR-joined lexeme fallback within the same DBTX when < 5 hits matched.
	orHits, err := queryTSQueryScope(ctx, db, workspaceID, durFilter, trimmed, limit, true)
	if err != nil {
		return nil, err
	}

	seenIDs := make(map[string]struct{}, len(andHits))
	combined := make([]LexicalHit, 0, limit)
	for _, h := range andHits {
		seenIDs[h.Memory.MemoryID.String()] = struct{}{}
		combined = append(combined, h)
	}
	for _, h := range orHits {
		if len(combined) >= limit {
			break
		}
		idKey := h.Memory.MemoryID.String()
		if _, exists := seenIDs[idKey]; exists {
			continue
		}
		seenIDs[idKey] = struct{}{}
		combined = append(combined, h)
	}
	return combined, nil
}

func queryRecentScope(ctx context.Context, db storage.DBTX, workspaceID, durFilter string, limit int) ([]LexicalHit, error) {
	sql := fmt.Sprintf(`
		SELECT
			m.memory_id, m.workspace_id, m.durability, m.topic_key, m.status, m.curation_state,
			m.current_version, m.summary_stub, m.body, m.token_count,
			COALESCE(m.embedding_space_id, ''), m.supersedes_memory_id, m.superseded_by_memory_id,
			m.last_reverted_version, false AS premise_drifted, m.created_at, m.updated_at,
			0::float8 AS ts_rank, 0::float8 AS trgm_sim, 0::float8 AS score
		FROM memory_manifest m
		WHERE m.workspace_id = $1
		  AND m.status = 'active'
		  AND %s
		ORDER BY m.updated_at DESC, m.memory_id DESC
		LIMIT $2
	`, durFilter)

	rows, err := db.Query(ctx, sql, workspaceID, limit)
	if err != nil {
		return nil, fmt.Errorf("recent lexical scan in %q: %w", workspaceID, err)
	}
	defer rows.Close()

	return scanLexicalHits(rows)
}

func queryTSQueryScope(ctx context.Context, db storage.DBTX, workspaceID, durFilter, q string, limit int, orFallback bool) ([]LexicalHit, error) {
	tsqCTE := `SELECT websearch_to_tsquery('english', $2) AS tsq`
	if orFallback {
		tsqCTE = `
			SELECT CASE
				WHEN numnode(plainto_tsquery('english', $2)) > 0
				THEN replace(plainto_tsquery('english', $2)::text, ' & ', ' | ')::tsquery
				ELSE ''::tsquery
			END AS tsq`
	}

	sql := fmt.Sprintf(`
		WITH q AS (%s)
		SELECT
			m.memory_id, m.workspace_id, m.durability, m.topic_key, m.status, m.curation_state,
			m.current_version, m.summary_stub, m.body, m.token_count,
			COALESCE(m.embedding_space_id, ''), m.supersedes_memory_id, m.superseded_by_memory_id,
			m.last_reverted_version, false AS premise_drifted, m.created_at, m.updated_at,
			ts_rank_cd(m.search_tsv, q.tsq)::float8 AS ts_rank,
			similarity(m.topic_key, $2)::float8 AS trgm_sim,
			(0.7 * ts_rank_cd(m.search_tsv, q.tsq) + 0.3 * similarity(m.topic_key, $2))::float8 AS score
		FROM memory_manifest m
		CROSS JOIN q
		WHERE m.workspace_id = $1
		  AND m.status = 'active'
		  AND %s
		  AND (m.search_tsv @@ q.tsq OR m.topic_key %% $2)
		ORDER BY score DESC, m.updated_at DESC, m.memory_id DESC
		LIMIT $3
	`, tsqCTE, durFilter)

	rows, err := db.Query(ctx, sql, workspaceID, q, limit)
	if err != nil {
		return nil, fmt.Errorf("lexical search (orFallback=%v) in %q: %w", orFallback, workspaceID, err)
	}
	defer rows.Close()

	return scanLexicalHits(rows)
}

func scanLexicalHits(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}) ([]LexicalHit, error) {
	var hits []LexicalHit
	for rows.Next() {
		var h LexicalHit
		if err := rows.Scan(
			&h.Memory.MemoryID,
			&h.Memory.WorkspaceID,
			&h.Memory.Durability,
			&h.Memory.TopicKey,
			&h.Memory.Status,
			&h.Memory.CurationState,
			&h.Memory.CurrentVersion,
			&h.Memory.SummaryStub,
			&h.Memory.Body,
			&h.Memory.TokenCount,
			&h.Memory.EmbeddingSpaceID,
			&h.Memory.SupersedesMemoryID,
			&h.Memory.SupersededByMemoryID,
			&h.Memory.LastRevertedVersion,
			&h.Memory.PremiseDrifted,
			&h.Memory.CreatedAt,
			&h.Memory.UpdatedAt,
			&h.TSRank,
			&h.TrgmSim,
			&h.Score,
		); err != nil {
			return nil, fmt.Errorf("scan lexical hit: %w", err)
		}
		hits = append(hits, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate lexical hits: %w", err)
	}
	return hits, nil
}
