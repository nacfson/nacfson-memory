package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pgvector/pgvector-go"
)

const (
	DurabilityConstitution = "constitution"
	DurabilityDurable      = "durable"
	DurabilityWorking      = "working"

	StatusActive     = "active"
	StatusSuperseded = "superseded"
	StatusRetired    = "retired"

	CurationCuratorVerified      = "curator_verified"
	CurationProvisionalHeuristic = "provisional_heuristic"
	CurationUserExplicit         = "user_explicit"
)

// Memory models a row in memory_manifest together with computed read-time drift status.
type Memory struct {
	MemoryID             uuid.UUID  `json:"memory_id"`
	WorkspaceID          string     `json:"workspace_id"`
	Durability           string     `json:"durability"`
	TopicKey             string     `json:"topic_key"`
	Status               string     `json:"status"`
	CurationState        string     `json:"curation_state"`
	CurrentVersion       int        `json:"current_version"`
	SummaryStub          string     `json:"summary_stub"`
	Body                 string     `json:"body"`
	TokenCount           int        `json:"token_count"`
	Embedding            []float32  `json:"-"`
	EmbeddingSpaceID     string     `json:"embedding_space_id,omitempty"`
	SupersedesMemoryID   *uuid.UUID `json:"supersedes_memory_id,omitempty"`
	SupersededByMemoryID *uuid.UUID `json:"superseded_by_memory_id,omitempty"`
	LastRevertedVersion  int        `json:"last_reverted_version"`
	PremiseDrifted       bool       `json:"premise_drifted,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

// VectorHit represents one vector similarity search match from SearchVectorTopK.
type VectorHit struct {
	Memory         Memory  `json:"memory"`
	CosineDistance float64 `json:"cosine_distance"`
	Similarity     float64 `json:"similarity"`
}

// CitationInput supplies contributor provenance attached to a memory revision.
type CitationInput struct {
	InboxID uuid.UUID
	AgentID string
}

// MemoryCitation models a row in memory_citations.
type MemoryCitation struct {
	MemoryID  uuid.UUID `json:"memory_id"`
	Version   int       `json:"version"`
	InboxID   uuid.UUID `json:"inbox_id"`
	AgentID   string    `json:"agent_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// MemoryRevision models a row in memory_revisions with optional joined citations.
type MemoryRevision struct {
	MemoryID            uuid.UUID        `json:"memory_id"`
	Version             int              `json:"version"`
	WorkspaceID         string           `json:"workspace_id"`
	Durability          string           `json:"durability"`
	SummaryStub         string           `json:"summary_stub"`
	Body                *string          `json:"body"`
	BodyPruned          bool             `json:"body_pruned"`
	RestoredFromVersion *int             `json:"restored_from_version,omitempty"`
	AgentID             string           `json:"agent_id,omitempty"`
	SourceAuthority     string           `json:"source_authority"`
	CurationState       string           `json:"curation_state"`
	ChangeReason        string           `json:"change_reason"`
	CreatedAt           time.Time        `json:"created_at"`
	Citations           []MemoryCitation `json:"citations,omitempty"`
}

// MemoryListItem is returned by ListAllMemoriesKeyset.
type MemoryListItem struct {
	Memory
	Revisions []MemoryRevision `json:"revisions,omitempty"`
}

// ListMemoriesParams configures keyset-paginated operator listing on curationPool.
type ListMemoriesParams struct {
	WorkspaceID      string
	Durability       string
	Status           string
	CurationState    string
	PremiseDrifted   *bool
	IncludeRevisions bool
	Cursor           string
	Limit            int
}

// ListMemoriesPage is a single keyset-paginated page of MemoryListItem rows.
type ListMemoriesPage struct {
	Items      []MemoryListItem `json:"items"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

// CreateMemoryParams configures CreateMemoryTx.
type CreateMemoryParams struct {
	MemoryID           uuid.UUID
	WorkspaceID        string
	Durability         string
	TopicKey           string
	CurationState      string
	SummaryStub        string
	Body               string
	TokenCount         int
	Embedding          []float32
	EmbeddingSpaceID   string
	SupersedesMemoryID *uuid.UUID
	AgentID            string
	SourceAuthority    string
	ChangeReason       string
	Citations          []CitationInput
}

// ReviseMemoryParams configures ReviseCoupledTopicTx.
type ReviseMemoryParams struct {
	MemoryID         uuid.UUID
	WorkspaceID      string
	ExpectedVersion  int // optional CAS check when > 0
	Durability       string
	CurationState    string
	SummaryStub      string
	Body             string
	TokenCount       int
	Embedding        []float32
	EmbeddingSpaceID string
	AgentID          string
	SourceAuthority  string
	ChangeReason     string
	Citations        []CitationInput
}

// EditMemoryParams configures partial manual edits via EditMemoryTx.
type EditMemoryParams struct {
	MemoryID                 uuid.UUID
	WorkspaceID              string // optional check if non-empty
	ExpectedVersion          int
	RequireCurrentDurability string // optional TOCTOU guard if non-empty
	Durability               *string
	SummaryStub              *string
	Body                     *string
	TokenCount               *int
	CurationState            string
	Embedding                []float32
	EmbeddingSpaceID         string
	AgentID                  string
	SourceAuthority          string
	ChangeReason             string
}

// SupersedeMemoryParams configures SupersedeMemoryTx.
type SupersedeMemoryParams struct {
	WorkspaceID      string
	OldMemoryID      uuid.UUID
	ExpectedVersion  int // optional CAS check when > 0
	NewMemoryID      uuid.UUID
	Durability       string
	TopicKey         string
	CurationState    string
	SummaryStub      string
	Body             string
	TokenCount       int
	Embedding        []float32
	EmbeddingSpaceID string
	AgentID          string
	SourceAuthority  string
	ChangeReason     string
	Citations        []CitationInput
}

// SoftRetireParams configures SoftRetireTx.
type SoftRetireParams struct {
	MemoryID           uuid.UUID
	WorkspaceID        string
	ExpectedVersion    int // optional when > 0
	ForbidConstitution bool
}

// RevertMemoryParams configures RevertToVersionTx.
type RevertMemoryParams struct {
	MemoryID         uuid.UUID
	WorkspaceID      string // optional check when non-empty
	ExpectedVersion  int
	TargetVersion    int
	TokenCount       int
	Embedding        []float32
	EmbeddingSpaceID string
	Reason           string
}

// HardPurgeResult summarizes HardPurgeTx cleanup counts.
type HardPurgeResult struct {
	MemoryID                uuid.UUID          `json:"memory_id"`
	WorkspaceID             string             `json:"workspace_id"`
	Durability              string             `json:"durability"`
	InheritGeneral          string             `json:"inherit_general,omitempty"`
	GeneralManifestRevision int64              `json:"general_manifest_revision,omitempty"`
	DeletedInboxCount       int64              `json:"deleted_inbox_count"`
	Revisions               WorkspaceRevisions `json:"revisions"`
}

// CurationDecision models a row in curation_decisions.
type CurationDecision struct {
	DecisionID       uuid.UUID   `json:"decision_id"`
	WorkspaceID      string      `json:"workspace_id"`
	InboxIDs         []uuid.UUID `json:"inbox_ids"`
	Action           string      `json:"action"`
	TargetMemoryID   *uuid.UUID  `json:"target_memory_id,omitempty"`
	TargetVersion    *int        `json:"target_version,omitempty"`
	ProviderName     string      `json:"provider_name,omitempty"`
	ModelName        string      `json:"model_name,omitempty"`
	PromptTokens     int         `json:"prompt_tokens"`
	CompletionTokens int         `json:"completion_tokens"`
	Rationale        string      `json:"rationale"`
	CreatedAt        time.Time   `json:"created_at"`
}

// MemoryLineage returns relational provenance returned by ExplainLineage.
type MemoryLineage struct {
	MemoryID       uuid.UUID          `json:"memory_id"`
	CurrentVersion int                `json:"current_version"`
	InboxSources   []InboxItem        `json:"inbox_sources"`
	Decisions      []CurationDecision `json:"decisions"`
}

// ListDecisionsParams configures cursor-paginated curation_decisions queries.
type ListDecisionsParams struct {
	WorkspaceID string
	Action      string
	Since       *time.Time
	Cursor      string
	Limit       int
}

// ListDecisionsPage holds one page of curation decisions.
type ListDecisionsPage struct {
	Decisions  []CurationDecision `json:"decisions"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

const premiseDriftedExpr = `EXISTS (
	SELECT 1
	FROM memory_edges e
	JOIN memory_manifest tgt ON tgt.memory_id = e.to_memory_id
	WHERE e.workspace_id = m.workspace_id
	  AND e.from_memory_id = m.memory_id
	  AND e.status = 'active'
	  AND (tgt.status <> 'active' OR e.expected_to_rev < tgt.last_reverted_version)
)`

func nullableHalfVector(vec []float32) *pgvector.HalfVector {
	if len(vec) == 0 {
		return nil
	}
	hv := pgvector.NewHalfVector(vec)
	return &hv
}

func nullableStr(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return &s
}

func GenerateSummaryStub(body string) string {
	collapsed := strings.Join(strings.Fields(body), " ")
	return truncateRunes(collapsed, 177)
}

func scanMemoryRow(row pgx.Row) (Memory, error) {
	var m Memory
	err := row.Scan(
		&m.MemoryID,
		&m.WorkspaceID,
		&m.Durability,
		&m.TopicKey,
		&m.Status,
		&m.CurationState,
		&m.CurrentVersion,
		&m.SummaryStub,
		&m.Body,
		&m.TokenCount,
		&m.EmbeddingSpaceID,
		&m.SupersedesMemoryID,
		&m.SupersededByMemoryID,
		&m.LastRevertedVersion,
		&m.PremiseDrifted,
		&m.CreatedAt,
		&m.UpdatedAt,
	)
	return m, err
}

// GetConstitution returns active constitution rows for workspaceID merged with 'general'
// according to inheritGeneral, with project entries overriding 'general' on topic_key.
func GetConstitution(ctx context.Context, db DBTX, workspaceID, inheritGeneral string) ([]Memory, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if workspaceID == WorkspaceGeneral || inheritGeneral == InheritNone {
		rows, err = db.Query(ctx, `
			SELECT
				m.memory_id, m.workspace_id, m.durability, m.topic_key, m.status, m.curation_state,
				m.current_version, m.summary_stub, m.body, m.token_count,
				COALESCE(m.embedding_space_id, ''), m.supersedes_memory_id, m.superseded_by_memory_id,
				m.last_reverted_version, false AS premise_drifted, m.created_at, m.updated_at
			FROM memory_manifest m
			WHERE m.workspace_id = $1
			  AND m.durability = 'constitution'
			  AND m.status = 'active'
			ORDER BY m.topic_key ASC
		`, workspaceID)
	} else {
		rows, err = db.Query(ctx, `
			SELECT DISTINCT ON (m.topic_key)
				m.memory_id, m.workspace_id, m.durability, m.topic_key, m.status, m.curation_state,
				m.current_version, m.summary_stub, m.body, m.token_count,
				COALESCE(m.embedding_space_id, ''), m.supersedes_memory_id, m.superseded_by_memory_id,
				m.last_reverted_version, false AS premise_drifted, m.created_at, m.updated_at
			FROM memory_manifest m
			WHERE m.workspace_id IN ($1, 'general')
			  AND m.durability = 'constitution'
			  AND m.status = 'active'
			ORDER BY m.topic_key ASC, (m.workspace_id = $1) DESC
		`, workspaceID)
	}
	if err != nil {
		return nil, fmt.Errorf("query constitution for %q: %w", workspaceID, err)
	}
	defer rows.Close()

	var result []Memory
	for rows.Next() {
		m, err := scanMemoryRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan constitution row: %w", err)
		}
		result = append(result, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate constitution rows: %w", err)
	}
	return result, nil
}

// SearchVectorTopK executes active-only halfvec cosine search per allowed scope
// (fetching 'general' durable rows separately when inheritGeneral == 'constitution_and_durable')
// with project entries overriding 'general' on matching topic_key.
func SearchVectorTopK(ctx context.Context, db DBTX, workspaceID, inheritGeneral, spaceID string, queryVec []float32, limit int) ([]VectorHit, error) {
	if len(queryVec) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}

	if _, err := db.Exec(ctx, `SET LOCAL hnsw.iterative_scan = relaxed_order`); err != nil {
		return nil, fmt.Errorf("enable hnsw iterative_scan: %w", err)
	}

	qv := pgvector.NewHalfVector(queryVec)

	projectHits, err := queryVectorScope(ctx, db, workspaceID, false, spaceID, qv, limit)
	if err != nil {
		return nil, err
	}

	if workspaceID == WorkspaceGeneral || inheritGeneral != InheritConstitutionAndDurable {
		return projectHits, nil
	}

	generalHits, err := queryVectorScope(ctx, db, WorkspaceGeneral, true, spaceID, qv, limit)
	if err != nil {
		return nil, err
	}

	seenTopics := make(map[string]struct{}, len(projectHits))
	merged := make([]VectorHit, 0, len(projectHits)+len(generalHits))
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

func queryVectorScope(ctx context.Context, db DBTX, workspaceID string, durableOnly bool, spaceID string, qv pgvector.HalfVector, limit int) ([]VectorHit, error) {
	durFilter := "m.durability <> 'constitution'"
	if durableOnly {
		durFilter = "m.durability = 'durable'"
	}
	query := fmt.Sprintf(`
		SELECT
			m.memory_id, m.workspace_id, m.durability, m.topic_key, m.status, m.curation_state,
			m.current_version, m.summary_stub, m.body, m.token_count,
			COALESCE(m.embedding_space_id, ''), m.supersedes_memory_id, m.superseded_by_memory_id,
			m.last_reverted_version, false AS premise_drifted, m.created_at, m.updated_at,
			(m.embedding <=> $2)::float8 AS distance
		FROM memory_manifest m
		WHERE m.workspace_id = $1
		  AND m.status = 'active'
		  AND %s
		  AND m.embedding IS NOT NULL
		  AND m.embedding_space_id = $3
		ORDER BY m.embedding <=> $2 ASC
		LIMIT $4
	`, durFilter)

	rows, err := db.Query(ctx, query, workspaceID, qv, spaceID, limit)
	if err != nil {
		return nil, fmt.Errorf("vector search in %q: %w", workspaceID, err)
	}
	defer rows.Close()

	var hits []VectorHit
	for rows.Next() {
		var h VectorHit
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
			&h.CosineDistance,
		); err != nil {
			return nil, fmt.Errorf("scan vector hit: %w", err)
		}
		h.Similarity = 1.0 - h.CosineDistance
		hits = append(hits, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate vector hits: %w", err)
	}
	return hits, nil
}

// CreateMemoryTx inserts a new active memory_manifest entry plus v1 revision and citations.
func CreateMemoryTx(ctx context.Context, tx pgx.Tx, params CreateMemoryParams) (Memory, error) {
	memID := params.MemoryID
	if memID == uuid.Nil {
		var err error
		memID, err = uuid.NewV7()
		if err != nil {
			return Memory{}, fmt.Errorf("generate memory_id UUIDv7: %w", err)
		}
	}
	sourceAuth := params.SourceAuthority
	if sourceAuth == "" {
		sourceAuth = "agent_observed"
	}
	reason := params.ChangeReason
	if reason == "" {
		reason = "create"
	}
	summaryStub := params.SummaryStub
	if summaryStub == "" {
		summaryStub = GenerateSummaryStub(params.Body)
	}

	var m Memory
	err := tx.QueryRow(ctx, `
		INSERT INTO memory_manifest (
			memory_id, workspace_id, durability, topic_key, status, curation_state,
			current_version, summary_stub, body, token_count, embedding, embedding_space_id,
			supersedes_memory_id
		)
		VALUES ($1, $2, $3, $4, 'active', $5, 1, $6, $7, $8, $9, $10, $11)
		RETURNING
			memory_id, workspace_id, durability, topic_key, status, curation_state,
			current_version, summary_stub, body, token_count,
			COALESCE(embedding_space_id, ''), supersedes_memory_id, superseded_by_memory_id,
			last_reverted_version, false AS premise_drifted, created_at, updated_at
	`,
		memID,
		params.WorkspaceID,
		params.Durability,
		params.TopicKey,
		params.CurationState,
		summaryStub,
		params.Body,
		params.TokenCount,
		nullableHalfVector(params.Embedding),
		nullableStr(params.EmbeddingSpaceID),
		params.SupersedesMemoryID,
	).Scan(
		&m.MemoryID, &m.WorkspaceID, &m.Durability, &m.TopicKey, &m.Status, &m.CurationState,
		&m.CurrentVersion, &m.SummaryStub, &m.Body, &m.TokenCount,
		&m.EmbeddingSpaceID, &m.SupersedesMemoryID, &m.SupersededByMemoryID,
		&m.LastRevertedVersion, &m.PremiseDrifted, &m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return Memory{}, fmt.Errorf("topic_key %q in workspace %q: %w", params.TopicKey, params.WorkspaceID, ErrTopicConflict)
		}
		if isForeignKeyViolation(err) {
			return Memory{}, fmt.Errorf("workspace %q %w", params.WorkspaceID, ErrNotFound)
		}
		return Memory{}, fmt.Errorf("insert memory_manifest: %w", err)
	}

	if err := insertRevisionTx(ctx, tx, m, params.AgentID, sourceAuth, reason, nil); err != nil {
		return Memory{}, err
	}
	if err := insertCitationsTx(ctx, tx, m.MemoryID, m.CurrentVersion, params.Citations); err != nil {
		return Memory{}, err
	}
	return m, nil
}

// ReviseCoupledTopicTx updates an existing active topic memory in place to current_version + 1.
func ReviseCoupledTopicTx(ctx context.Context, tx pgx.Tx, params ReviseMemoryParams) (Memory, error) {
	cur, err := lockManifestRowTx(ctx, tx, params.MemoryID, params.WorkspaceID)
	if err != nil {
		return Memory{}, err
	}
	if cur.Status != StatusActive {
		return Memory{}, fmt.Errorf("cannot revise %s memory %s: %w", cur.Status, params.MemoryID, ErrInactiveMemory)
	}
	if params.ExpectedVersion > 0 && cur.CurrentVersion != params.ExpectedVersion {
		return Memory{}, fmt.Errorf("expected version %d, found %d: %w", params.ExpectedVersion, cur.CurrentVersion, ErrVersionConflict)
	}

	dur := cur.Durability
	if params.Durability != "" {
		dur = params.Durability
	}
	curState := cur.CurationState
	if params.CurationState != "" {
		curState = params.CurationState
	}
	stub := params.SummaryStub
	if stub == "" {
		stub = GenerateSummaryStub(params.Body)
	}
	sourceAuth := params.SourceAuthority
	if sourceAuth == "" {
		sourceAuth = "agent_observed"
	}
	reason := params.ChangeReason
	if reason == "" {
		reason = "revise"
	}

	var m Memory
	err = tx.QueryRow(ctx, `
		UPDATE memory_manifest
		SET durability = $3,
		    curation_state = $4,
		    current_version = current_version + 1,
		    summary_stub = $5,
		    body = $6,
		    token_count = $7,
		    embedding = $8,
		    embedding_space_id = $9,
		    updated_at = now()
		WHERE memory_id = $1 AND workspace_id = $2
		RETURNING
			memory_id, workspace_id, durability, topic_key, status, curation_state,
			current_version, summary_stub, body, token_count,
			COALESCE(embedding_space_id, ''), supersedes_memory_id, superseded_by_memory_id,
			last_reverted_version, false AS premise_drifted, created_at, updated_at
	`,
		params.MemoryID,
		params.WorkspaceID,
		dur,
		curState,
		stub,
		params.Body,
		params.TokenCount,
		nullableHalfVector(params.Embedding),
		nullableStr(params.EmbeddingSpaceID),
	).Scan(
		&m.MemoryID, &m.WorkspaceID, &m.Durability, &m.TopicKey, &m.Status, &m.CurationState,
		&m.CurrentVersion, &m.SummaryStub, &m.Body, &m.TokenCount,
		&m.EmbeddingSpaceID, &m.SupersedesMemoryID, &m.SupersededByMemoryID,
		&m.LastRevertedVersion, &m.PremiseDrifted, &m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		return Memory{}, fmt.Errorf("revise memory_manifest: %w", err)
	}

	if err := insertRevisionTx(ctx, tx, m, params.AgentID, sourceAuth, reason, nil); err != nil {
		return Memory{}, err
	}
	if err := insertCitationsTx(ctx, tx, m.MemoryID, m.CurrentVersion, params.Citations); err != nil {
		return Memory{}, err
	}
	return m, nil
}

// EditMemoryTx performs a CAS-guarded partial update on an active memory entry.
func EditMemoryTx(ctx context.Context, tx pgx.Tx, params EditMemoryParams) (Memory, error) {
	cur, err := lockManifestRowTx(ctx, tx, params.MemoryID, params.WorkspaceID)
	if err != nil {
		return Memory{}, err
	}
	if cur.Status != StatusActive {
		return Memory{}, fmt.Errorf("cannot edit %s memory %s: %w", cur.Status, params.MemoryID, ErrInactiveMemory)
	}
	if cur.CurrentVersion != params.ExpectedVersion {
		return Memory{}, fmt.Errorf("expected version %d, found %d: %w", params.ExpectedVersion, cur.CurrentVersion, ErrVersionConflict)
	}
	if params.RequireCurrentDurability != "" && cur.Durability != params.RequireCurrentDurability {
		return Memory{}, fmt.Errorf("invalid_action: expected durability %q, found %q: %w", params.RequireCurrentDurability, cur.Durability, ErrInvalidInput)
	}

	dur := cur.Durability
	if params.Durability != nil && *params.Durability != "" {
		dur = *params.Durability
	}
	body := cur.Body
	bodyChanged := false
	if params.Body != nil {
		if *params.Body != cur.Body {
			bodyChanged = true
		}
		body = *params.Body
	}
	tokenCount := cur.TokenCount
	if params.TokenCount != nil {
		tokenCount = *params.TokenCount
	}
	stub := cur.SummaryStub
	if params.SummaryStub != nil {
		stub = *params.SummaryStub
	} else if bodyChanged {
		stub = GenerateSummaryStub(body)
	}
	curState := cur.CurationState
	if params.CurationState != "" {
		curState = params.CurationState
	}
	sourceAuth := params.SourceAuthority
	if sourceAuth == "" {
		sourceAuth = "user_explicit"
	}
	reason := params.ChangeReason
	if reason == "" {
		reason = "manual_edit"
	}

	var m Memory
	err = tx.QueryRow(ctx, `
		UPDATE memory_manifest
		SET durability = $2,
		    curation_state = $3,
		    current_version = current_version + 1,
		    summary_stub = $4,
		    body = $5,
		    token_count = $6,
		    embedding = $7,
		    embedding_space_id = $8,
		    updated_at = now()
		WHERE memory_id = $1
		RETURNING
			memory_id, workspace_id, durability, topic_key, status, curation_state,
			current_version, summary_stub, body, token_count,
			COALESCE(embedding_space_id, ''), supersedes_memory_id, superseded_by_memory_id,
			last_reverted_version, false AS premise_drifted, created_at, updated_at
	`,
		params.MemoryID,
		dur,
		curState,
		stub,
		body,
		tokenCount,
		nullableHalfVector(params.Embedding),
		nullableStr(params.EmbeddingSpaceID),
	).Scan(
		&m.MemoryID, &m.WorkspaceID, &m.Durability, &m.TopicKey, &m.Status, &m.CurationState,
		&m.CurrentVersion, &m.SummaryStub, &m.Body, &m.TokenCount,
		&m.EmbeddingSpaceID, &m.SupersedesMemoryID, &m.SupersededByMemoryID,
		&m.LastRevertedVersion, &m.PremiseDrifted, &m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		return Memory{}, fmt.Errorf("edit memory_manifest: %w", err)
	}

	if err := insertRevisionTx(ctx, tx, m, params.AgentID, sourceAuth, reason, nil); err != nil {
		return Memory{}, err
	}
	return m, nil
}

// SupersedeMemoryTx retires the old memory and its outgoing edges BEFORE inserting
// the replacement memory so reusing the same topic_key does not trigger 23505.
func SupersedeMemoryTx(ctx context.Context, tx pgx.Tx, params SupersedeMemoryParams) (Memory, error) {
	oldMem, err := lockManifestRowTx(ctx, tx, params.OldMemoryID, params.WorkspaceID)
	if err != nil {
		return Memory{}, err
	}
	if oldMem.Status != StatusActive {
		return Memory{}, fmt.Errorf("cannot supersede %s memory %s: %w", oldMem.Status, params.OldMemoryID, ErrInactiveMemory)
	}
	if params.ExpectedVersion > 0 && oldMem.CurrentVersion != params.ExpectedVersion {
		return Memory{}, fmt.Errorf("expected version %d, found %d: %w", params.ExpectedVersion, oldMem.CurrentVersion, ErrVersionConflict)
	}

	// Step 1: Retire prior row and its active outgoing edges BEFORE inserting replacement.
	if _, err := tx.Exec(ctx, `
		UPDATE memory_manifest
		SET status = 'superseded', updated_at = now()
		WHERE memory_id = $1
	`, params.OldMemoryID); err != nil {
		return Memory{}, fmt.Errorf("mark old memory superseded: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE memory_edges
		SET status = 'retired', updated_at = now()
		WHERE workspace_id = $1 AND from_memory_id = $2 AND status = 'active'
	`, oldMem.WorkspaceID, params.OldMemoryID); err != nil {
		return Memory{}, fmt.Errorf("retire superseded outgoing edges: %w", err)
	}

	topicKey := params.TopicKey
	if topicKey == "" {
		topicKey = oldMem.TopicKey
	}
	reason := params.ChangeReason
	if reason == "" {
		reason = "supersede"
	}

	// Step 2: Insert replacement active memory linked backward to OldMemoryID.
	newMem, err := CreateMemoryTx(ctx, tx, CreateMemoryParams{
		MemoryID:           params.NewMemoryID,
		WorkspaceID:        oldMem.WorkspaceID,
		Durability:         params.Durability,
		TopicKey:           topicKey,
		CurationState:      params.CurationState,
		SummaryStub:        params.SummaryStub,
		Body:               params.Body,
		TokenCount:         params.TokenCount,
		Embedding:          params.Embedding,
		EmbeddingSpaceID:   params.EmbeddingSpaceID,
		SupersedesMemoryID: &params.OldMemoryID,
		AgentID:            params.AgentID,
		SourceAuthority:    params.SourceAuthority,
		ChangeReason:       reason,
		Citations:          params.Citations,
	})
	if err != nil {
		return Memory{}, err
	}

	// Step 3: Link old row forward to replacement memory_id.
	if _, err := tx.Exec(ctx, `
		UPDATE memory_manifest
		SET superseded_by_memory_id = $2
		WHERE memory_id = $1
	`, params.OldMemoryID, newMem.MemoryID); err != nil {
		return Memory{}, fmt.Errorf("link superseded_by_memory_id: %w", err)
	}

	return newMem, nil
}

// SoftRetireTx marks an active memory 'retired' and retires its active outgoing edges.
func SoftRetireTx(ctx context.Context, tx pgx.Tx, params SoftRetireParams) (Memory, error) {
	cur, err := lockManifestRowTx(ctx, tx, params.MemoryID, params.WorkspaceID)
	if err != nil {
		return Memory{}, err
	}
	if cur.Status != StatusActive {
		return Memory{}, fmt.Errorf("cannot retire %s memory %s: %w", cur.Status, params.MemoryID, ErrInactiveMemory)
	}
	if params.ExpectedVersion > 0 && cur.CurrentVersion != params.ExpectedVersion {
		return Memory{}, fmt.Errorf("expected version %d, found %d: %w", params.ExpectedVersion, cur.CurrentVersion, ErrVersionConflict)
	}
	if params.ForbidConstitution && cur.Durability == DurabilityConstitution {
		return Memory{}, fmt.Errorf("invalid_action: cannot directly retire constitution memory %s: %w", params.MemoryID, ErrInvalidInput)
	}

	var m Memory
	err = tx.QueryRow(ctx, `
		UPDATE memory_manifest
		SET status = 'retired', updated_at = now()
		WHERE memory_id = $1
		RETURNING
			memory_id, workspace_id, durability, topic_key, status, curation_state,
			current_version, summary_stub, body, token_count,
			COALESCE(embedding_space_id, ''), supersedes_memory_id, superseded_by_memory_id,
			last_reverted_version, false AS premise_drifted, created_at, updated_at
	`, params.MemoryID).Scan(
		&m.MemoryID, &m.WorkspaceID, &m.Durability, &m.TopicKey, &m.Status, &m.CurationState,
		&m.CurrentVersion, &m.SummaryStub, &m.Body, &m.TokenCount,
		&m.EmbeddingSpaceID, &m.SupersedesMemoryID, &m.SupersededByMemoryID,
		&m.LastRevertedVersion, &m.PremiseDrifted, &m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		return Memory{}, fmt.Errorf("retire memory_manifest: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE memory_edges
		SET status = 'retired', updated_at = now()
		WHERE workspace_id = $1 AND from_memory_id = $2 AND status = 'active'
	`, m.WorkspaceID, m.MemoryID); err != nil {
		return Memory{}, fmt.Errorf("retire outgoing edges on soft_retire: %w", err)
	}

	return m, nil
}

// RevertToVersionTx appends a monotonic cherry-pick rollback revision restoring an earlier
// unpruned revision, reactivating superseded/retired rows and updating last_reverted_version.
func RevertToVersionTx(ctx context.Context, tx pgx.Tx, params RevertMemoryParams) (Memory, error) {
	cur, err := lockManifestRowTx(ctx, tx, params.MemoryID, params.WorkspaceID)
	if err != nil {
		return Memory{}, err
	}
	if cur.CurrentVersion != params.ExpectedVersion {
		return Memory{}, fmt.Errorf("expected version %d, found %d: %w", params.ExpectedVersion, cur.CurrentVersion, ErrVersionConflict)
	}

	var (
		revDurability string
		revStub       string
		revBody       *string
		bodyPruned    bool
	)
	err = tx.QueryRow(ctx, `
		SELECT durability, summary_stub, body, body_pruned
		FROM memory_revisions
		WHERE memory_id = $1 AND version = $2
	`, params.MemoryID, params.TargetVersion).Scan(&revDurability, &revStub, &revBody, &bodyPruned)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Memory{}, fmt.Errorf("target revision v%d for memory %s %w", params.TargetVersion, params.MemoryID, ErrNotFound)
		}
		return Memory{}, fmt.Errorf("read target revision: %w", err)
	}
	if bodyPruned || revBody == nil {
		return Memory{}, fmt.Errorf("revision v%d for memory %s: %w", params.TargetVersion, params.MemoryID, ErrRevisionPruned)
	}

	newVersion := cur.CurrentVersion + 1
	reason := params.Reason
	if reason == "" {
		reason = fmt.Sprintf("revert to version %d", params.TargetVersion)
	}

	var m Memory
	err = tx.QueryRow(ctx, `
		UPDATE memory_manifest
		SET durability = $2,
		    status = 'active',
		    curation_state = 'user_explicit',
		    current_version = $3,
		    summary_stub = $4,
		    body = $5,
		    token_count = $6,
		    embedding = $7,
		    embedding_space_id = $8,
		    superseded_by_memory_id = NULL,
		    last_reverted_version = $3,
		    updated_at = now()
		WHERE memory_id = $1
		RETURNING
			memory_id, workspace_id, durability, topic_key, status, curation_state,
			current_version, summary_stub, body, token_count,
			COALESCE(embedding_space_id, ''), supersedes_memory_id, superseded_by_memory_id,
			last_reverted_version, false AS premise_drifted, created_at, updated_at
	`,
		params.MemoryID,
		revDurability,
		newVersion,
		revStub,
		*revBody,
		params.TokenCount,
		nullableHalfVector(params.Embedding),
		nullableStr(params.EmbeddingSpaceID),
	).Scan(
		&m.MemoryID, &m.WorkspaceID, &m.Durability, &m.TopicKey, &m.Status, &m.CurationState,
		&m.CurrentVersion, &m.SummaryStub, &m.Body, &m.TokenCount,
		&m.EmbeddingSpaceID, &m.SupersedesMemoryID, &m.SupersededByMemoryID,
		&m.LastRevertedVersion, &m.PremiseDrifted, &m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return Memory{}, fmt.Errorf("active topic_key %q collision on revert: %w", cur.TopicKey, ErrTopicConflict)
		}
		return Memory{}, fmt.Errorf("revert memory_manifest: %w", err)
	}

	targetVer := params.TargetVersion
	if err := insertRevisionTx(ctx, tx, m, "", "user_explicit", reason, &targetVer); err != nil {
		return Memory{}, err
	}
	return m, nil
}

// HardPurgeTx atomically removes the memory, cascades revisions/citations/edges, cleans
// exclusively-cited raw_inbox rows (redacting shared ones), redacts curation_decisions
// rationales, writes a 'purge' decision, and bumps workspace revisions.
func HardPurgeTx(ctx context.Context, tx pgx.Tx, memoryID uuid.UUID, reason string) (HardPurgeResult, error) {
	var (
		workspaceID string
		durability  string
	)
	err := tx.QueryRow(ctx, `
		SELECT workspace_id, durability
		FROM memory_manifest
		WHERE memory_id = $1
	`, memoryID).Scan(&workspaceID, &durability)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return HardPurgeResult{}, fmt.Errorf("memory %s %w", memoryID, ErrNotFound)
		}
		return HardPurgeResult{}, fmt.Errorf("lookup memory for hard purge: %w", err)
	}

	if _, err := LockWorkspaceTx(ctx, tx, workspaceID); err != nil {
		return HardPurgeResult{}, err
	}
	var inheritGeneral string
	if err := tx.QueryRow(ctx, `SELECT inherit_general FROM workspaces WHERE workspace_id = $1`, workspaceID).Scan(&inheritGeneral); err != nil {
		return HardPurgeResult{}, fmt.Errorf("lookup workspace inherit_general on purge: %w", err)
	}
	var genManifestRev int64
	if workspaceID != WorkspaceGeneral {
		_ = tx.QueryRow(ctx, `SELECT manifest_revision FROM workspaces WHERE workspace_id = $1`, WorkspaceGeneral).Scan(&genManifestRev)
	}

	// Snapshot cited inbox_ids BEFORE deleting memory_manifest (which cascades memory_citations).
	rows, err := tx.Query(ctx, `SELECT DISTINCT inbox_id FROM memory_citations WHERE memory_id = $1`, memoryID)
	if err != nil {
		return HardPurgeResult{}, fmt.Errorf("snapshot cited inbox IDs: %w", err)
	}
	citedInboxIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return HardPurgeResult{}, fmt.Errorf("scan cited inbox_id: %w", err)
		}
		citedInboxIDs = append(citedInboxIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return HardPurgeResult{}, fmt.Errorf("iterate cited inbox IDs: %w", err)
	}
	rows.Close()

	if _, err := tx.Exec(ctx, `DELETE FROM memory_manifest WHERE memory_id = $1`, memoryID); err != nil {
		return HardPurgeResult{}, fmt.Errorf("delete memory_manifest: %w", err)
	}

	var deletedInboxCount int64
	if len(citedInboxIDs) > 0 {
		delTag, err := tx.Exec(ctx, `
			DELETE FROM raw_inbox r
			WHERE r.inbox_id = ANY($1)
			  AND NOT EXISTS (
			      SELECT 1 FROM memory_citations mc WHERE mc.inbox_id = r.inbox_id
			  )
		`, citedInboxIDs)
		if err != nil {
			return HardPurgeResult{}, fmt.Errorf("delete exclusively-cited raw_inbox rows: %w", err)
		}
		deletedInboxCount = delTag.RowsAffected()

		purgedSHA := sha256.Sum256([]byte("[PURGED]"))
		if _, err := tx.Exec(ctx, `
			UPDATE raw_inbox
			SET content = '[PURGED]', content_sha256 = $2
			WHERE inbox_id = ANY($1)
		`, citedInboxIDs, purgedSHA[:]); err != nil {
			return HardPurgeResult{}, fmt.Errorf("redact shared raw_inbox rows: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE curation_decisions
		SET rationale = '[PURGED]'
		WHERE target_memory_id = $1 OR inbox_ids && $2
	`, memoryID, citedInboxIDs); err != nil {
		return HardPurgeResult{}, fmt.Errorf("redact curation_decisions: %w", err)
	}

	purgeReason := reason
	if strings.TrimSpace(purgeReason) == "" {
		purgeReason = "manual_purge"
	}
	if _, err := InsertCurationDecisionTx(ctx, tx, CurationDecision{
		WorkspaceID:    workspaceID,
		InboxIDs:       []uuid.UUID{},
		Action:         "purge",
		TargetMemoryID: &memoryID,
		Rationale:      purgeReason,
	}); err != nil {
		return HardPurgeResult{}, fmt.Errorf("record purge decision: %w", err)
	}

	revs, err := BumpRevisionTx(ctx, tx, workspaceID, durability == DurabilityConstitution)
	if err != nil {
		return HardPurgeResult{}, err
	}

	return HardPurgeResult{
		MemoryID:                memoryID,
		WorkspaceID:             workspaceID,
		Durability:              durability,
		InheritGeneral:          inheritGeneral,
		GeneralManifestRevision: genManifestRev,
		DeletedInboxCount:       deletedInboxCount,
		Revisions:               revs,
	}, nil
}

// PruneRevisionBodiesTx clears old revision bodies beyond the newest retentionN revisions
// while retaining revision metadata until HardPurgeTx.
func PruneRevisionBodiesTx(ctx context.Context, tx pgx.Tx, memoryID uuid.UUID, retentionN int) (int64, error) {
	if retentionN <= 0 {
		return 0, nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE memory_revisions
		SET body = NULL, body_pruned = TRUE
		WHERE memory_id = $1
		  AND body_pruned = FALSE
		  AND version NOT IN (
		      SELECT version
		      FROM memory_revisions
		      WHERE memory_id = $1
		      ORDER BY version DESC
		      LIMIT $2
		  )
	`, memoryID, retentionN)
	if err != nil {
		return 0, fmt.Errorf("prune revision bodies: %w", err)
	}
	return tag.RowsAffected(), nil
}

// DeleteProvisionalManifestByInboxTx atomically removes provisional:<inbox_id> manifest rows
// upon successful curation commit.
func DeleteProvisionalManifestByInboxTx(ctx context.Context, tx pgx.Tx, workspaceID string, inboxIDs []uuid.UUID) (int64, error) {
	if len(inboxIDs) == 0 {
		return 0, nil
	}
	keys := make([]string, len(inboxIDs))
	for i, id := range inboxIDs {
		keys[i] = "provisional:" + id.String()
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM memory_manifest
		WHERE workspace_id = $1
		  AND curation_state = 'provisional_heuristic'
		  AND topic_key = ANY($2)
	`, workspaceID, keys)
	if err != nil {
		return 0, fmt.Errorf("delete provisional manifest rows: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ListAllMemoriesKeyset executes UUIDv7 keyset pagination across workspaces on curationPool.
func ListAllMemoriesKeyset(ctx context.Context, db DBTX, params ListMemoriesParams) (ListMemoriesPage, error) {
	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if params.IncludeRevisions && limit > 50 {
		limit = 50
	}

	var args []any
	var conds []string
	addArg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	if params.WorkspaceID != "" {
		conds = append(conds, "m.workspace_id = "+addArg(params.WorkspaceID))
	}
	if params.Durability != "" {
		conds = append(conds, "m.durability = "+addArg(params.Durability))
	}
	if params.Status != "" {
		conds = append(conds, "m.status = "+addArg(params.Status))
	}
	if params.CurationState != "" {
		conds = append(conds, "m.curation_state = "+addArg(params.CurationState))
	}
	if params.PremiseDrifted != nil {
		if *params.PremiseDrifted {
			conds = append(conds, premiseDriftedExpr)
		} else {
			conds = append(conds, "NOT "+premiseDriftedExpr)
		}
	}
	if params.Cursor != "" {
		cursorID, err := decodeUUIDCursor(params.Cursor)
		if err != nil {
			return ListMemoriesPage{}, fmt.Errorf("invalid cursor: %w", ErrInvalidInput)
		}
		conds = append(conds, "m.memory_id < "+addArg(cursorID))
	}

	whereClause := ""
	if len(conds) > 0 {
		whereClause = "WHERE " + strings.Join(conds, " AND ")
	}
	limitPlaceholder := addArg(limit + 1)

	query := fmt.Sprintf(`
		SELECT
			m.memory_id, m.workspace_id, m.durability, m.topic_key, m.status, m.curation_state,
			m.current_version, m.summary_stub, m.body, m.token_count,
			COALESCE(m.embedding_space_id, ''), m.supersedes_memory_id, m.superseded_by_memory_id,
			m.last_reverted_version, %s AS premise_drifted, m.created_at, m.updated_at
		FROM memory_manifest m
		%s
		ORDER BY m.memory_id DESC
		LIMIT %s
	`, premiseDriftedExpr, whereClause, limitPlaceholder)

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return ListMemoriesPage{}, fmt.Errorf("list memories keyset: %w", err)
	}
	defer rows.Close()

	items := make([]MemoryListItem, 0, limit)
	for rows.Next() {
		m, err := scanMemoryRow(rows)
		if err != nil {
			return ListMemoriesPage{}, fmt.Errorf("scan listed memory: %w", err)
		}
		items = append(items, MemoryListItem{Memory: m})
	}
	if err := rows.Err(); err != nil {
		return ListMemoriesPage{}, fmt.Errorf("iterate listed memories: %w", err)
	}

	var nextCursor string
	if len(items) > limit {
		items = items[:limit]
		nextCursor = encodeUUIDCursor(items[len(items)-1].MemoryID)
	}

	if params.IncludeRevisions && len(items) > 0 {
		if err := attachRevisionsBatch(ctx, db, items); err != nil {
			return ListMemoriesPage{}, err
		}
	}

	return ListMemoriesPage{
		Items:      items,
		NextCursor: nextCursor,
	}, nil
}

func attachRevisionsBatch(ctx context.Context, db DBTX, items []MemoryListItem) error {
	ids := make([]uuid.UUID, len(items))
	idxByID := make(map[uuid.UUID]int, len(items))
	for i := range items {
		ids[i] = items[i].MemoryID
		idxByID[items[i].MemoryID] = i
	}

	rows, err := db.Query(ctx, `
		SELECT
			memory_id, version, workspace_id, durability, summary_stub, body, body_pruned,
			restored_from_version, COALESCE(agent_id, ''), source_authority, curation_state,
			change_reason, created_at
		FROM memory_revisions
		WHERE memory_id = ANY($1)
		ORDER BY memory_id ASC, version DESC
	`, ids)
	if err != nil {
		return fmt.Errorf("batch load revisions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var rev MemoryRevision
		if err := rows.Scan(
			&rev.MemoryID,
			&rev.Version,
			&rev.WorkspaceID,
			&rev.Durability,
			&rev.SummaryStub,
			&rev.Body,
			&rev.BodyPruned,
			&rev.RestoredFromVersion,
			&rev.AgentID,
			&rev.SourceAuthority,
			&rev.CurationState,
			&rev.ChangeReason,
			&rev.CreatedAt,
		); err != nil {
			return fmt.Errorf("scan batch revision: %w", err)
		}
		if idx, ok := idxByID[rev.MemoryID]; ok {
			items[idx].Revisions = append(items[idx].Revisions, rev)
		}
	}
	return rows.Err()
}

// ListMemoriesForReindexKeyset scans active memories ascending by memory_id for batch re-embedding.
func ListMemoriesForReindexKeyset(ctx context.Context, db DBTX, afterMemoryID *uuid.UUID, batchSize int) ([]Memory, error) {
	if batchSize <= 0 {
		batchSize = 32
	}
	var (
		rows pgx.Rows
		err  error
	)
	if afterMemoryID == nil {
		rows, err = db.Query(ctx, `
			SELECT
				m.memory_id, m.workspace_id, m.durability, m.topic_key, m.status, m.curation_state,
				m.current_version, m.summary_stub, m.body, m.token_count,
				COALESCE(m.embedding_space_id, ''), m.supersedes_memory_id, m.superseded_by_memory_id,
				m.last_reverted_version, false AS premise_drifted, m.created_at, m.updated_at
			FROM memory_manifest m
			WHERE m.status = 'active'
			ORDER BY m.memory_id ASC
			LIMIT $1
		`, batchSize)
	} else {
		rows, err = db.Query(ctx, `
			SELECT
				m.memory_id, m.workspace_id, m.durability, m.topic_key, m.status, m.curation_state,
				m.current_version, m.summary_stub, m.body, m.token_count,
				COALESCE(m.embedding_space_id, ''), m.supersedes_memory_id, m.superseded_by_memory_id,
				m.last_reverted_version, false AS premise_drifted, m.created_at, m.updated_at
			FROM memory_manifest m
			WHERE m.status = 'active' AND m.memory_id > $1
			ORDER BY m.memory_id ASC
			LIMIT $2
		`, *afterMemoryID, batchSize)
	}
	if err != nil {
		return nil, fmt.Errorf("list memories for reindex: %w", err)
	}
	defer rows.Close()

	var result []Memory
	for rows.Next() {
		m, err := scanMemoryRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan reindex memory: %w", err)
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

// UpdateMemoryEmbeddingTx updates embedding and embedding_space_id in place without altering current_version.
func UpdateMemoryEmbeddingTx(ctx context.Context, db DBTX, memoryID uuid.UUID, embedding []float32, spaceID string) error {
	tag, err := db.Exec(ctx, `
		UPDATE memory_manifest
		SET embedding = $2,
		    embedding_space_id = $3,
		    updated_at = now()
		WHERE memory_id = $1
	`, memoryID, nullableHalfVector(embedding), nullableStr(spaceID))
	if err != nil {
		return fmt.Errorf("update memory embedding: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("memory %s %w", memoryID, ErrNotFound)
	}
	return nil
}

// UpdateMemoryEmbeddingVersionTx updates an active memory's embedding only when current_version
// still matches expectedVersion, preventing stale async embed jobs from overwriting newer versions.
func UpdateMemoryEmbeddingVersionTx(ctx context.Context, db DBTX, memoryID uuid.UUID, expectedVersion int, embedding []float32, spaceID string) (bool, error) {
	tag, err := db.Exec(ctx, `
		UPDATE memory_manifest
		SET embedding = $2,
		    embedding_space_id = $3,
		    updated_at = now()
		WHERE memory_id = $1
		  AND current_version = $4
		  AND status = 'active'
	`, memoryID, nullableHalfVector(embedding), nullableStr(spaceID), expectedVersion)
	if err != nil {
		return false, fmt.Errorf("update memory embedding for version %d: %w", expectedVersion, err)
	}
	return tag.RowsAffected() > 0, nil
}

// GetMemory loads one memory_manifest row together with computed premise_drifted status.
func GetMemory(ctx context.Context, db DBTX, memoryID uuid.UUID) (Memory, error) {
	m, err := scanMemoryRow(db.QueryRow(ctx, fmt.Sprintf(`
		SELECT
			m.memory_id, m.workspace_id, m.durability, m.topic_key, m.status, m.curation_state,
			m.current_version, m.summary_stub, m.body, m.token_count,
			COALESCE(m.embedding_space_id, ''), m.supersedes_memory_id, m.superseded_by_memory_id,
			m.last_reverted_version, %s AS premise_drifted, m.created_at, m.updated_at
		FROM memory_manifest m
		WHERE m.memory_id = $1
	`, premiseDriftedExpr), memoryID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Memory{}, fmt.Errorf("memory %s %w", memoryID, ErrNotFound)
		}
		return Memory{}, fmt.Errorf("get memory: %w", err)
	}
	return m, nil
}

// GetRevisions loads all versions and joined citations for memoryID ordered descending by version.
func GetRevisions(ctx context.Context, db DBTX, memoryID uuid.UUID) ([]MemoryRevision, error) {
	var exists bool
	if err := db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM memory_manifest WHERE memory_id = $1)`, memoryID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check memory existence: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("memory %s %w", memoryID, ErrNotFound)
	}

	rows, err := db.Query(ctx, `
		SELECT
			memory_id, version, workspace_id, durability, summary_stub, body, body_pruned,
			restored_from_version, COALESCE(agent_id, ''), source_authority, curation_state,
			change_reason, created_at
		FROM memory_revisions
		WHERE memory_id = $1
		ORDER BY version DESC
	`, memoryID)
	if err != nil {
		return nil, fmt.Errorf("query memory revisions: %w", err)
	}
	defer rows.Close()

	var revisions []MemoryRevision
	idxByVer := make(map[int]int)
	for rows.Next() {
		var rev MemoryRevision
		if err := rows.Scan(
			&rev.MemoryID, &rev.Version, &rev.WorkspaceID, &rev.Durability, &rev.SummaryStub,
			&rev.Body, &rev.BodyPruned, &rev.RestoredFromVersion, &rev.AgentID,
			&rev.SourceAuthority, &rev.CurationState, &rev.ChangeReason, &rev.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan memory revision: %w", err)
		}
		idxByVer[rev.Version] = len(revisions)
		revisions = append(revisions, rev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory revisions: %w", err)
	}

	cRows, err := db.Query(ctx, `
		SELECT memory_id, version, inbox_id, COALESCE(agent_id, ''), created_at
		FROM memory_citations
		WHERE memory_id = $1
		ORDER BY version DESC, created_at ASC
	`, memoryID)
	if err != nil {
		return nil, fmt.Errorf("query memory citations: %w", err)
	}
	defer cRows.Close()

	for cRows.Next() {
		var c MemoryCitation
		if err := cRows.Scan(&c.MemoryID, &c.Version, &c.InboxID, &c.AgentID, &c.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan memory citation: %w", err)
		}
		if idx, ok := idxByVer[c.Version]; ok {
			revisions[idx].Citations = append(revisions[idx].Citations, c)
		}
	}
	if err := cRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory citations: %w", err)
	}

	return revisions, nil
}

// ExplainLineage builds relational provenance from raw_inbox through memory_citations and
// curation_decisions to the current memory version.
func ExplainLineage(ctx context.Context, db DBTX, memoryID uuid.UUID) (MemoryLineage, error) {
	m, err := GetMemory(ctx, db, memoryID)
	if err != nil {
		return MemoryLineage{}, err
	}

	lineage := MemoryLineage{
		MemoryID:       m.MemoryID,
		CurrentVersion: m.CurrentVersion,
		InboxSources:   []InboxItem{},
		Decisions:      []CurationDecision{},
	}

	inRows, err := db.Query(ctx, `
		SELECT DISTINCT ON (r.inbox_id)
			r.inbox_id, r.workspace_id, COALESCE(r.agent_id, ''), r.source_authority,
			r.request_id, COALESCE(r.idempotency_key, ''), r.content, r.status,
			r.occurred_at, r.ingested_at
		FROM raw_inbox r
		JOIN memory_citations mc ON mc.inbox_id = r.inbox_id
		WHERE mc.memory_id = $1
		ORDER BY r.inbox_id ASC
	`, memoryID)
	if err != nil {
		return MemoryLineage{}, fmt.Errorf("query lineage inbox sources: %w", err)
	}
	defer inRows.Close()

	inboxIDs := make([]uuid.UUID, 0)
	for inRows.Next() {
		var item InboxItem
		if err := inRows.Scan(
			&item.InboxID, &item.WorkspaceID, &item.AgentID, &item.SourceAuthority,
			&item.RequestID, &item.IdempotencyKey, &item.Content, &item.Status,
			&item.OccurredAt, &item.IngestedAt,
		); err != nil {
			return MemoryLineage{}, fmt.Errorf("scan lineage inbox source: %w", err)
		}
		inboxIDs = append(inboxIDs, item.InboxID)
		lineage.InboxSources = append(lineage.InboxSources, item)
	}
	if err := inRows.Err(); err != nil {
		return MemoryLineage{}, fmt.Errorf("iterate lineage inbox sources: %w", err)
	}

	decRows, err := db.Query(ctx, `
		SELECT
			decision_id, workspace_id, inbox_ids, action, target_memory_id, target_version,
			COALESCE(provider_name, ''), COALESCE(model_name, ''), prompt_tokens,
			completion_tokens, rationale, created_at
		FROM curation_decisions
		WHERE target_memory_id = $1 OR inbox_ids && $2
		ORDER BY created_at ASC, decision_id ASC
	`, memoryID, inboxIDs)
	if err != nil {
		return MemoryLineage{}, fmt.Errorf("query lineage curation decisions: %w", err)
	}
	defer decRows.Close()

	for decRows.Next() {
		var d CurationDecision
		if err := decRows.Scan(
			&d.DecisionID, &d.WorkspaceID, &d.InboxIDs, &d.Action, &d.TargetMemoryID,
			&d.TargetVersion, &d.ProviderName, &d.ModelName, &d.PromptTokens,
			&d.CompletionTokens, &d.Rationale, &d.CreatedAt,
		); err != nil {
			return MemoryLineage{}, fmt.Errorf("scan lineage decision: %w", err)
		}
		lineage.Decisions = append(lineage.Decisions, d)
	}
	if err := decRows.Err(); err != nil {
		return MemoryLineage{}, fmt.Errorf("iterate lineage decisions: %w", err)
	}

	return lineage, nil
}

// InsertCurationDecisionTx records an audit row in curation_decisions.
func InsertCurationDecisionTx(ctx context.Context, tx pgx.Tx, d CurationDecision) (uuid.UUID, error) {
	decID := d.DecisionID
	if decID == uuid.Nil {
		var err error
		decID, err = uuid.NewV7()
		if err != nil {
			return uuid.Nil, fmt.Errorf("generate decision_id UUIDv7: %w", err)
		}
	}
	inboxIDs := d.InboxIDs
	if inboxIDs == nil {
		inboxIDs = []uuid.UUID{}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO curation_decisions (
			decision_id, workspace_id, inbox_ids, action, target_memory_id, target_version,
			provider_name, model_name, prompt_tokens, completion_tokens, rationale
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`,
		decID,
		d.WorkspaceID,
		inboxIDs,
		d.Action,
		d.TargetMemoryID,
		d.TargetVersion,
		nullableStr(d.ProviderName),
		nullableStr(d.ModelName),
		d.PromptTokens,
		d.CompletionTokens,
		d.Rationale,
	); err != nil {
		return uuid.Nil, fmt.Errorf("insert curation_decision: %w", err)
	}
	return decID, nil
}

// ListCurationDecisions executes keyset-paginated listing of curation_decisions.
func ListCurationDecisions(ctx context.Context, db DBTX, params ListDecisionsParams) (ListDecisionsPage, error) {
	limit := params.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	var args []any
	var conds []string
	addArg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	if params.WorkspaceID != "" {
		conds = append(conds, "workspace_id = "+addArg(params.WorkspaceID))
	}
	if params.Action != "" {
		conds = append(conds, "action = "+addArg(params.Action))
	}
	if params.Since != nil {
		conds = append(conds, "created_at >= "+addArg(*params.Since))
	}
	if params.Cursor != "" {
		ts, id, err := decodeDecisionCursor(params.Cursor)
		if err != nil {
			return ListDecisionsPage{}, fmt.Errorf("invalid decision cursor: %w", ErrInvalidInput)
		}
		tsArg := addArg(ts)
		idArg := addArg(id)
		conds = append(conds, fmt.Sprintf("(created_at, decision_id) < (%s, %s)", tsArg, idArg))
	}

	whereClause := ""
	if len(conds) > 0 {
		whereClause = "WHERE " + strings.Join(conds, " AND ")
	}
	limitArg := addArg(limit + 1)

	query := fmt.Sprintf(`
		SELECT
			decision_id, workspace_id, inbox_ids, action, target_memory_id, target_version,
			COALESCE(provider_name, ''), COALESCE(model_name, ''), prompt_tokens,
			completion_tokens, rationale, created_at
		FROM curation_decisions
		%s
		ORDER BY created_at DESC, decision_id DESC
		LIMIT %s
	`, whereClause, limitArg)

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return ListDecisionsPage{}, fmt.Errorf("list curation decisions: %w", err)
	}
	defer rows.Close()

	decisions := make([]CurationDecision, 0, limit)
	for rows.Next() {
		var d CurationDecision
		if err := rows.Scan(
			&d.DecisionID, &d.WorkspaceID, &d.InboxIDs, &d.Action, &d.TargetMemoryID,
			&d.TargetVersion, &d.ProviderName, &d.ModelName, &d.PromptTokens,
			&d.CompletionTokens, &d.Rationale, &d.CreatedAt,
		); err != nil {
			return ListDecisionsPage{}, fmt.Errorf("scan curation decision: %w", err)
		}
		decisions = append(decisions, d)
	}
	if err := rows.Err(); err != nil {
		return ListDecisionsPage{}, fmt.Errorf("iterate curation decisions: %w", err)
	}

	var nextCursor string
	if len(decisions) > limit {
		decisions = decisions[:limit]
		last := decisions[len(decisions)-1]
		nextCursor = encodeDecisionCursor(last.CreatedAt, last.DecisionID)
	}
	return ListDecisionsPage{
		Decisions:  decisions,
		NextCursor: nextCursor,
	}, nil
}

func lockManifestRowTx(ctx context.Context, tx pgx.Tx, memoryID uuid.UUID, workspaceID string) (Memory, error) {
	m, err := scanMemoryRow(tx.QueryRow(ctx, `
		SELECT
			m.memory_id, m.workspace_id, m.durability, m.topic_key, m.status, m.curation_state,
			m.current_version, m.summary_stub, m.body, m.token_count,
			COALESCE(m.embedding_space_id, ''), m.supersedes_memory_id, m.superseded_by_memory_id,
			m.last_reverted_version, false AS premise_drifted, m.created_at, m.updated_at
		FROM memory_manifest m
		WHERE m.memory_id = $1
		FOR UPDATE
	`, memoryID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Memory{}, fmt.Errorf("memory %s %w", memoryID, ErrNotFound)
		}
		return Memory{}, fmt.Errorf("lock memory_manifest row: %w", err)
	}
	if workspaceID != "" && m.WorkspaceID != workspaceID {
		return Memory{}, fmt.Errorf("memory %s not in workspace %q: %w", memoryID, workspaceID, ErrNotFound)
	}
	return m, nil
}

func insertRevisionTx(ctx context.Context, tx pgx.Tx, m Memory, agentID, sourceAuthority, reason string, restoredFrom *int) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO memory_revisions (
			memory_id, version, workspace_id, durability, summary_stub, body, body_pruned,
			restored_from_version, agent_id, source_authority, curation_state, change_reason
		)
		VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7, $8, $9, $10, $11)
	`,
		m.MemoryID,
		m.CurrentVersion,
		m.WorkspaceID,
		m.Durability,
		m.SummaryStub,
		m.Body,
		restoredFrom,
		nullableStr(agentID),
		sourceAuthority,
		m.CurationState,
		reason,
	)
	if err != nil {
		return fmt.Errorf("insert memory_revisions v%d: %w", m.CurrentVersion, err)
	}
	return nil
}

func insertCitationsTx(ctx context.Context, tx pgx.Tx, memoryID uuid.UUID, version int, citations []CitationInput) error {
	for _, c := range citations {
		if _, err := tx.Exec(ctx, `
			INSERT INTO memory_citations (memory_id, version, inbox_id, agent_id)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (memory_id, version, inbox_id) DO NOTHING
		`, memoryID, version, c.InboxID, nullableStr(c.AgentID)); err != nil {
			return fmt.Errorf("insert memory_citation (%s v%d <- %s): %w", memoryID, version, c.InboxID, err)
		}
	}
	return nil
}

func encodeUUIDCursor(id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString(id[:])
}

func decodeUUIDCursor(raw string) (uuid.UUID, error) {
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return uuid.Nil, err
	}
	if len(b) != 16 {
		return uuid.Nil, errors.New("invalid cursor byte length")
	}
	return uuid.FromBytes(b)
}

func encodeDecisionCursor(createdAt time.Time, id uuid.UUID) string {
	raw := createdAt.UTC().Format(time.RFC3339Nano) + "|" + id.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeDecisionCursor(raw string) (time.Time, uuid.UUID, error) {
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, errors.New("malformed cursor")
	}
	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}
	return ts, id, nil
}
