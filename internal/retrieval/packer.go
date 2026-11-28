package retrieval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/search"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

const (
	// MaxAgentTokens caps all serialized agent recall/expand JSON responses per spec.
	MaxAgentTokens   = 2000
	defaultRRFK      = 60.0
	candidatePoolTop = 20
)

// RecallRequest configures a read-only workspace recall query.
type RecallRequest struct {
	WorkspaceID string `json:"workspace_id"`
	Query       string `json:"query"`
	MaxTokens   int    `json:"max_tokens,omitempty"`
}

// MemoryDependencyView surfaces a 1-hop DEPENDS_ON edge plus read-time drift status.
type MemoryDependencyView struct {
	FromMemoryID           uuid.UUID  `json:"from_memory_id,omitempty"`
	TargetMemoryID         uuid.UUID  `json:"target_memory_id"`
	TargetTopicKey         string     `json:"target_topic_key"`
	TargetSummaryStub      string     `json:"target_summary_stub"`
	ExpectedToRev          int        `json:"expected_to_rev"`
	Rationale              string     `json:"rationale"`
	PremiseDrifted         bool       `json:"premise_drifted"`
	ReplacementMemoryID    *uuid.UUID `json:"replacement_memory_id,omitempty"`
	ReplacementTopicKey    string     `json:"replacement_topic_key,omitempty"`
	ReplacementSummaryStub string     `json:"replacement_summary_stub,omitempty"`
	ReplacementVersion     int        `json:"replacement_version,omitempty"`
}

// RecalledMemory is a full memory entry packed inside RecallResponse or ExpandResponse.
type RecalledMemory struct {
	MemoryID       uuid.UUID              `json:"memory_id"`
	WorkspaceID    string                 `json:"workspace_id"`
	Durability     string                 `json:"durability"`
	TopicKey       string                 `json:"topic_key"`
	CurationState  string                 `json:"curation_state"`
	Version        int                    `json:"version"`
	SummaryStub    string                 `json:"summary_stub"`
	Body           string                 `json:"body"`
	PremiseDrifted bool                   `json:"premise_drifted,omitempty"`
	Dependencies   []MemoryDependencyView `json:"dependencies,omitempty"`
}

// MemoryStub renders an overflow hit within the serialized token cap for follow-up expand.
type MemoryStub struct {
	MemoryID       uuid.UUID `json:"memory_id"`
	WorkspaceID    string    `json:"workspace_id"`
	Durability     string    `json:"durability"`
	TopicKey       string    `json:"topic_key"`
	CurationState  string    `json:"curation_state"`
	Version        int       `json:"version"`
	SummaryStub    string    `json:"summary_stub"`
	PremiseDrifted bool      `json:"premise_drifted,omitempty"`
	Expandable     bool      `json:"expandable"`
}

// RecallResponse is the structured output of RecallContext, guaranteed <= MaxTokens.
type RecallResponse struct {
	Memories          []RecalledMemory `json:"memories"`
	Stubs             []MemoryStub     `json:"stubs"`
	ManifestETag      string           `json:"manifest_etag"`
	WorkspaceRevision int64            `json:"workspace_revision"`
	Degraded          bool             `json:"degraded"`
	SerializedTokens  int              `json:"-"`
}

// ExpandRequest fetches full details for specific memory IDs visible to a workspace.
type ExpandRequest struct {
	WorkspaceID string      `json:"workspace_id"`
	MemoryIDs   []uuid.UUID `json:"memory_ids"`
	MaxTokens   int         `json:"max_tokens,omitempty"`
}

// ExpandResponse returns full memory bodies and dependency links within MaxTokens.
type ExpandResponse struct {
	Memories          []RecalledMemory       `json:"memories"`
	Edges             []MemoryDependencyView `json:"edges"`
	Truncated         bool                   `json:"truncated"`
	ManifestETag      string                 `json:"manifest_etag"`
	WorkspaceRevision int64                  `json:"workspace_revision"`
	SerializedTokens  int                    `json:"-"`
}

// RankedHit pairs a fused candidate memory with its RRF score and 1-hop dependency views.
type RankedHit struct {
	Memory       storage.Memory
	Score        float64
	Dependencies []MemoryDependencyView
}

// Packer performs pre-DB query embedding, 1-connection BEGIN READ ONLY recall,
// RRF fusion, drift resolution, and deterministic <= 2,000-token packing.
type Packer struct {
	store           *storage.Store
	embedder        embedding.Embedder
	embedderBreaker *curation.Breaker
	lexical         *search.LexicalSearch
	depGraph        *graph.DependencyGraph
	embedTimeout    time.Duration
	logger          *slog.Logger
}

// NewPacker builds a Packer.
func NewPacker(
	store *storage.Store,
	embedder embedding.Embedder,
	embedderBreaker *curation.Breaker,
	lexical *search.LexicalSearch,
	depGraph *graph.DependencyGraph,
	embedTimeout time.Duration,
) *Packer {
	if lexical == nil {
		lexical = search.NewLexicalSearch()
	}
	if depGraph == nil {
		depGraph = graph.NewDependencyGraph()
	}
	if embedderBreaker == nil {
		embedderBreaker = curation.NewBreaker(3, 60*time.Second)
	}
	if embedTimeout <= 0 {
		embedTimeout = 2 * time.Second
	}
	return &Packer{
		store:           store,
		embedder:        embedder,
		embedderBreaker: embedderBreaker,
		lexical:         lexical,
		depGraph:        depGraph,
		embedTimeout:    embedTimeout,
	}
}

// WithLogger sets the logger that receives metadata-only embed_call events for recall query
// embeddings and returns p. Without it the Packer emits no telemetry.
func (p *Packer) WithLogger(logger *slog.Logger) *Packer {
	p.logger = logger
	return p
}

// ClampMaxTokens enforces [1, 2000], defaulting <= 0 to 2000.
func ClampMaxTokens(maxTokens int) int {
	if maxTokens <= 0 || maxTokens > MaxAgentTokens {
		return MaxAgentTokens
	}
	return maxTokens
}

// RecallContext runs pre-DB embedding + single-connection BEGIN READ ONLY recall and packs
// the response strictly within ClampMaxTokens(req.MaxTokens) serialized cl100k_base tokens.
func (p *Packer) RecallContext(ctx context.Context, req RecallRequest) (RecallResponse, error) {
	if err := storage.ValidateWorkspaceID(req.WorkspaceID); err != nil {
		return RecallResponse{}, err
	}
	budget := ClampMaxTokens(req.MaxTokens)

	// Step 1: Pre-DB optional query embedding (0 DB connections held).
	degraded := false
	var queryVec []float32
	spaceID := ""
	trimmedQuery := strings.TrimSpace(req.Query)
	if trimmedQuery != "" && p.embedder != nil {
		spaceID = p.embedder.SpaceID()
		call := embedding.EmbedCall{Caller: "recall", WorkspaceID: req.WorkspaceID, SpaceID: spaceID}
		start := time.Now()
		if !p.embedderBreaker.Allow() {
			degraded = true
			call.Outcome = embedding.EmbedOutcomeBreakerOpen
		} else {
			call.InputChars = utf8.RuneCountInString(trimmedQuery)
			embCtx, cancel := context.WithTimeout(ctx, p.embedTimeout)
			vecs, err := p.embedder.EmbedBatch(embCtx, []string{trimmedQuery})
			cancel()
			if err != nil || len(vecs) == 0 {
				if err == nil {
					err = errors.New("empty query embedding response")
				}
				p.embedderBreaker.RecordFailure(err)
				degraded = true
				call.Outcome = embedding.EmbedOutcomeError
			} else {
				p.embedderBreaker.RecordSuccess()
				queryVec = vecs[0]
				call.Outcome = embedding.EmbedOutcomeOK
			}
		}
		call.Duration = time.Since(start)
		call.Log(p.logger)
	}

	// Step 2: Single-connection pipelined BEGIN READ ONLY recall on ServingPool.
	tx, err := p.store.ServingPool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return RecallResponse{Degraded: true}, fmt.Errorf("begin read-only recall tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ws, err := storage.GetWorkspace(ctx, tx, req.WorkspaceID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			_ = tx.Commit(ctx)
			resp := PackRecall(budget, nil, nil, storage.ComputeMetadataETag(0, 0, storage.InheritNone), 0, degraded)
			return resp, nil
		}
		return RecallResponse{Degraded: true}, err
	}

	var genRev int64
	if ws.WorkspaceID != storage.WorkspaceGeneral && ws.InheritGeneral != storage.InheritNone {
		if genWS, err := storage.GetWorkspace(ctx, tx, storage.WorkspaceGeneral); err == nil {
			genRev = genWS.ManifestRevision
		}
	}
	etag := storage.ComputeMetadataETag(ws.ManifestRevision, genRev, ws.InheritGeneral)

	// Query 1: Constitution rules.
	constitution, err := storage.GetConstitution(ctx, tx, ws.WorkspaceID, ws.InheritGeneral)
	if err != nil {
		return RecallResponse{Degraded: true}, err
	}

	// Query 2: Active-only halfvec cosine vector search (when embedding succeeded).
	var vecHits []storage.VectorHit
	if len(queryVec) > 0 && spaceID != "" {
		vecHits, err = storage.SearchVectorTopK(ctx, tx, ws.WorkspaceID, ws.InheritGeneral, spaceID, queryVec, candidatePoolTop)
		if err != nil {
			return RecallResponse{Degraded: true}, err
		}
	}

	// Query 3: GIN lexical search (+ < 5-hit OR-lexeme fallback).
	lexHits, err := p.lexical.Search(ctx, tx, search.SearchParams{
		WorkspaceID:    ws.WorkspaceID,
		InheritGeneral: ws.InheritGeneral,
		Query:          trimmedQuery,
		Limit:          candidatePoolTop,
	})
	if err != nil {
		return RecallResponse{Degraded: true}, err
	}

	// RRF fusion + project-over-'general' topic_key overrides.
	ranked := FuseCandidates(ws.WorkspaceID, vecHits, lexHits)

	// Query 4: 1-hop DEPENDS_ON edges and multi-hop replacement stubs on the SAME tx.
	if len(ranked) > 0 {
		idsByWS := make(map[string][]uuid.UUID)
		for _, r := range ranked {
			idsByWS[r.Memory.WorkspaceID] = append(idsByWS[r.Memory.WorkspaceID], r.Memory.MemoryID)
		}
		edgesByMem := make(map[uuid.UUID][]graph.OutgoingEdge)
		for edgeWS, memIDs := range idsByWS {
			out, err := p.depGraph.LookupOutgoing(ctx, tx, edgeWS, memIDs)
			if err != nil {
				return RecallResponse{Degraded: true}, err
			}
			for id, list := range out {
				edgesByMem[id] = list
			}
		}
		for i := range ranked {
			edges := edgesByMem[ranked[i].Memory.MemoryID]
			if len(edges) > 0 {
				views, drifted := toDependencyViews(edges)
				ranked[i].Dependencies = views
				if drifted {
					ranked[i].Memory.PremiseDrifted = true
				}
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return RecallResponse{Degraded: true}, fmt.Errorf("commit read-only recall tx: %w", err)
	}

	return PackRecall(budget, constitution, ranked, etag, ws.ManifestRevision, degraded), nil
}

// ExpandMemories fetches full memory bodies and dependency edges for requested IDs
// authorized in workspaceID (respecting inherit_general) strictly within MaxTokens.
func (p *Packer) ExpandMemories(ctx context.Context, req ExpandRequest) (ExpandResponse, error) {
	if err := storage.ValidateWorkspaceID(req.WorkspaceID); err != nil {
		return ExpandResponse{}, err
	}
	if len(req.MemoryIDs) == 0 {
		return ExpandResponse{}, fmt.Errorf("memory_ids must not be empty: %w", storage.ErrInvalidInput)
	}
	budget := ClampMaxTokens(req.MaxTokens)

	tx, err := p.store.ServingPool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return ExpandResponse{}, fmt.Errorf("begin read-only expand tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ws, err := storage.GetWorkspace(ctx, tx, req.WorkspaceID)
	if err != nil {
		return ExpandResponse{}, err
	}

	var genRev int64
	if ws.WorkspaceID != storage.WorkspaceGeneral && ws.InheritGeneral != storage.InheritNone {
		if genWS, err := storage.GetWorkspace(ctx, tx, storage.WorkspaceGeneral); err == nil {
			genRev = genWS.ManifestRevision
		}
	}
	etag := storage.ComputeMetadataETag(ws.ManifestRevision, genRev, ws.InheritGeneral)

	// Deduplicate requested IDs preserving input order.
	uniqueIDs := make([]uuid.UUID, 0, len(req.MemoryIDs))
	seen := make(map[uuid.UUID]struct{}, len(req.MemoryIDs))
	for _, id := range req.MemoryIDs {
		if id == uuid.Nil {
			return ExpandResponse{}, fmt.Errorf("memory_ids contains nil UUID: %w", storage.ErrInvalidInput)
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		uniqueIDs = append(uniqueIDs, id)
	}

	memsByID := make(map[uuid.UUID]storage.Memory, len(uniqueIDs))
	idsByWS := make(map[string][]uuid.UUID)
	for _, id := range uniqueIDs {
		m, err := storage.GetMemory(ctx, tx, id)
		if err != nil {
			return ExpandResponse{}, err
		}
		if !isVisibleInWorkspace(ws, m) {
			return ExpandResponse{}, fmt.Errorf("memory %s is not accessible in workspace %q: %w", id, ws.WorkspaceID, storage.ErrInvalidInput)
		}
		memsByID[id] = m
		idsByWS[m.WorkspaceID] = append(idsByWS[m.WorkspaceID], id)
	}

	edgesByMem := make(map[uuid.UUID][]graph.OutgoingEdge, len(uniqueIDs))
	for edgeWS, memIDs := range idsByWS {
		out, err := p.depGraph.LookupOutgoing(ctx, tx, edgeWS, memIDs)
		if err != nil {
			return ExpandResponse{}, err
		}
		for id, list := range out {
			edgesByMem[id] = list
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ExpandResponse{}, fmt.Errorf("commit read-only expand tx: %w", err)
	}

	ordered := make([]RankedHit, 0, len(uniqueIDs))
	for _, id := range uniqueIDs {
		m := memsByID[id]
		views, drifted := toDependencyViews(edgesByMem[id])
		if drifted {
			m.PremiseDrifted = true
		}
		ordered = append(ordered, RankedHit{
			Memory:       m,
			Dependencies: views,
		})
	}

	return PackExpand(budget, ordered, etag, ws.ManifestRevision), nil
}

func isVisibleInWorkspace(ws storage.Workspace, m storage.Memory) bool {
	if m.WorkspaceID == ws.WorkspaceID {
		return true
	}
	if m.WorkspaceID != storage.WorkspaceGeneral {
		return false
	}
	switch ws.InheritGeneral {
	case storage.InheritNone:
		return false
	case storage.InheritConstitutionOnly:
		return m.Durability == storage.DurabilityConstitution
	case storage.InheritConstitutionAndDurable:
		return m.Durability == storage.DurabilityConstitution || m.Durability == storage.DurabilityDurable
	default:
		return false
	}
}

// FuseCandidates merges vector and lexical hits with RRF scoring and project-over-'general' topic_key overrides.
func FuseCandidates(projectWorkspaceID string, vecHits []storage.VectorHit, lexHits []search.LexicalHit) []RankedHit {
	projectTopics := make(map[string]struct{})
	if projectWorkspaceID != storage.WorkspaceGeneral {
		for _, h := range vecHits {
			if h.Memory.WorkspaceID == projectWorkspaceID {
				projectTopics[h.Memory.TopicKey] = struct{}{}
			}
		}
		for _, h := range lexHits {
			if h.Memory.WorkspaceID == projectWorkspaceID {
				projectTopics[h.Memory.TopicKey] = struct{}{}
			}
		}
	}

	type acc struct {
		mem   storage.Memory
		score float64
	}
	byID := make(map[uuid.UUID]*acc, len(vecHits)+len(lexHits))
	vecRank := 0
	for _, h := range vecHits {
		if projectWorkspaceID != storage.WorkspaceGeneral && h.Memory.WorkspaceID == storage.WorkspaceGeneral {
			if _, overridden := projectTopics[h.Memory.TopicKey]; overridden {
				continue
			}
		}
		vecRank++
		entry, ok := byID[h.Memory.MemoryID]
		if !ok {
			entry = &acc{mem: h.Memory}
			byID[h.Memory.MemoryID] = entry
		}
		entry.score += 1.0 / (defaultRRFK + float64(vecRank))
	}

	lexRank := 0
	for _, h := range lexHits {
		if projectWorkspaceID != storage.WorkspaceGeneral && h.Memory.WorkspaceID == storage.WorkspaceGeneral {
			if _, overridden := projectTopics[h.Memory.TopicKey]; overridden {
				continue
			}
		}
		lexRank++
		entry, ok := byID[h.Memory.MemoryID]
		if !ok {
			entry = &acc{mem: h.Memory}
			byID[h.Memory.MemoryID] = entry
		}
		entry.score += 1.0 / (defaultRRFK + float64(lexRank))
	}

	ranked := make([]RankedHit, 0, len(byID))
	for _, entry := range byID {
		mult := weightMultiplier(entry.mem)
		if projectWorkspaceID != storage.WorkspaceGeneral && entry.mem.WorkspaceID == projectWorkspaceID {
			mult *= 1.1
		}
		ranked = append(ranked, RankedHit{
			Memory: entry.mem,
			Score:  entry.score * mult,
		})
	}

	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].Score != ranked[j].Score {
			return ranked[i].Score > ranked[j].Score
		}
		if !ranked[i].Memory.UpdatedAt.Equal(ranked[j].Memory.UpdatedAt) {
			return ranked[i].Memory.UpdatedAt.After(ranked[j].Memory.UpdatedAt)
		}
		return ranked[i].Memory.MemoryID.String() > ranked[j].Memory.MemoryID.String()
	})
	return ranked
}

func weightMultiplier(m storage.Memory) float64 {
	if m.CurationState == storage.CurationProvisionalHeuristic {
		return 0.45
	}
	if m.Durability == storage.DurabilityWorking {
		return 0.85
	}
	return 1.0
}

// PackRecall packs constitution rules + direct hits + 1-hop dependencies + overflow stubs
// strictly within budget serialized cl100k_base JSON tokens using the 4-tier eviction order.
func PackRecall(budget int, constitution []storage.Memory, ranked []RankedHit, etag string, wsRev int64, degraded bool) RecallResponse {
	budget = ClampMaxTokens(budget)
	constitutionCap := minInt(500, budget/2)
	stubHeadroom := minInt(200, budget/4)

	constMemories := make([]RecalledMemory, 0, len(constitution))
	constStubs := make([]MemoryStub, 0)
	constTokensUsed := 0

	for _, c := range constitution {
		rm := toRecalledMemory(c, nil)
		tok := estimateMemoryJSONTokens(c, 0)
		if len(constMemories) == 0 || constTokensUsed+tok <= constitutionCap {
			constMemories = append(constMemories, rm)
			constTokensUsed += tok
		} else {
			constStubs = append(constStubs, toMemoryStub(rm))
		}
	}

	directBudget := budget - stubHeadroom
	if directBudget < constTokensUsed {
		directBudget = constTokensUsed
	}
	hitMemories := make([]RecalledMemory, 0, len(ranked))
	hitStubs := make([]MemoryStub, 0)
	usedTokens := constTokensUsed

	for _, r := range ranked {
		rm := toRecalledMemory(r.Memory, r.Dependencies)
		tok := estimateMemoryJSONTokens(r.Memory, len(r.Dependencies))
		if usedTokens+tok <= directBudget {
			hitMemories = append(hitMemories, rm)
			usedTokens += tok
		} else {
			hitStubs = append(hitStubs, toMemoryStub(rm))
		}
	}

	constCount := len(constMemories)
	memories := make([]RecalledMemory, 0, len(constMemories)+len(hitMemories))
	memories = append(memories, constMemories...)
	memories = append(memories, hitMemories...)

	stubs := make([]MemoryStub, 0, len(hitStubs)+len(constStubs))
	stubs = append(stubs, hitStubs...)
	stubs = append(stubs, constStubs...)

	resp := RecallResponse{
		Memories:          memories,
		Stubs:             stubs,
		ManifestETag:      etag,
		WorkspaceRevision: wsRev,
		Degraded:          degraded,
	}

	// Exact serialized JSON eviction loop across 4 deterministic tiers.
	curTokens := countSerializedTokens(resp)
	for curTokens > budget {
		// Tier 1: Drop lowest-ranked non-drifted dependency details first.
		if dropLowestNonDriftedDep(&resp, constCount) {
			curTokens = countSerializedTokens(resp)
			continue
		}
		// Tier 2: Downgrade lowest-ranked non-constitution hit from full body to stub.
		if len(resp.Memories) > constCount {
			last := resp.Memories[len(resp.Memories)-1]
			resp.Memories = resp.Memories[:len(resp.Memories)-1]
			// Insert newly downgraded higher-ranked hit stub before lower-ranked hit stubs.
			resp.Stubs = append([]MemoryStub{toMemoryStub(last)}, resp.Stubs...)
			curTokens = countSerializedTokens(resp)
			continue
		}
		// Tier 3: Drop lowest-ranked overflow stub.
		if len(resp.Stubs) > 0 {
			resp.Stubs = resp.Stubs[:len(resp.Stubs)-1]
			curTokens = countSerializedTokens(resp)
			continue
		}
		// Tier 4: Convert excess constitution entry to stub (when budget permits stubs to fit).
		if constCount > 0 {
			lastConst := resp.Memories[constCount-1]
			resp.Memories = resp.Memories[:constCount-1]
			constCount--
			// Only emit stub if doing so leaves progress room under budget.
			candidateStub := toMemoryStub(lastConst)
			withStub := resp
			withStub.Stubs = append([]MemoryStub{candidateStub}, resp.Stubs...)
			if countSerializedTokens(withStub) <= budget || len(resp.Memories) > 0 {
				resp.Stubs = append([]MemoryStub{candidateStub}, resp.Stubs...)
			}
			curTokens = countSerializedTokens(resp)
			continue
		}
		// Ultra-small budget safety: cannot shrink items further; clear etag if non-empty.
		if resp.ManifestETag != "" {
			resp.ManifestETag = ""
			curTokens = countSerializedTokens(resp)
			continue
		}
		break
	}

	resp.SerializedTokens = curTokens
	return resp
}

func estimateMemoryJSONTokens(m storage.Memory, depCount int) int {
	bodyToks := m.TokenCount
	if bodyToks <= 0 {
		bodyToks = curation.CountTokens(m.Body)
	}
	stubToks := curation.CountTokens(m.SummaryStub)
	return bodyToks + stubToks + 52 + depCount*48
}

func dropLowestNonDriftedDep(resp *RecallResponse, constCount int) bool {
	for i := len(resp.Memories) - 1; i >= constCount; i-- {
		deps := resp.Memories[i].Dependencies
		for j := len(deps) - 1; j >= 0; j-- {
			if !deps[j].PremiseDrifted {
				resp.Memories[i].Dependencies = append(deps[:j], deps[j+1:]...)
				return true
			}
		}
	}
	return false
}

// PackExpand packs explicit memory expansions and edges strictly within budget tokens.
func PackExpand(budget int, ordered []RankedHit, etag string, wsRev int64) ExpandResponse {
	budget = ClampMaxTokens(budget)

	memories := make([]RecalledMemory, 0, len(ordered))
	edges := make([]MemoryDependencyView, 0)
	for _, r := range ordered {
		rm := toRecalledMemory(r.Memory, nil)
		memories = append(memories, rm)
		edges = append(edges, r.Dependencies...)
	}

	resp := ExpandResponse{
		Memories:          memories,
		Edges:             edges,
		ManifestETag:      etag,
		WorkspaceRevision: wsRev,
	}

	curTokens := countSerializedTokens(resp)
	for curTokens > budget {
		if dropExpandEdge(&resp) {
			resp.Truncated = true
			curTokens = countSerializedTokens(resp)
			continue
		}
		if len(resp.Memories) > 1 {
			resp.Memories = resp.Memories[:len(resp.Memories)-1]
			resp.Truncated = true
			curTokens = countSerializedTokens(resp)
			continue
		}
		if len(resp.Memories) == 1 && len(resp.Memories[0].Body) > 0 {
			toks := curation.CountTokens(resp.Memories[0].Body)
			if toks <= 4 {
				resp.Memories[0].Body = ""
			} else {
				trimmed, _ := curation.TruncateTokens(resp.Memories[0].Body, toks/2)
				resp.Memories[0].Body = trimmed
			}
			resp.Truncated = true
			curTokens = countSerializedTokens(resp)
			continue
		}
		break
	}

	resp.SerializedTokens = curTokens
	return resp
}

func dropExpandEdge(resp *ExpandResponse) bool {
	for j := len(resp.Edges) - 1; j >= 0; j-- {
		if !resp.Edges[j].PremiseDrifted {
			resp.Edges = append(resp.Edges[:j], resp.Edges[j+1:]...)
			return true
		}
	}
	if len(resp.Edges) > 0 {
		resp.Edges = resp.Edges[:len(resp.Edges)-1]
		return true
	}
	return false
}

func toRecalledMemory(m storage.Memory, deps []MemoryDependencyView) RecalledMemory {
	return RecalledMemory{
		MemoryID:       m.MemoryID,
		WorkspaceID:    m.WorkspaceID,
		Durability:     m.Durability,
		TopicKey:       m.TopicKey,
		CurationState:  m.CurationState,
		Version:        m.CurrentVersion,
		SummaryStub:    m.SummaryStub,
		Body:           m.Body,
		PremiseDrifted: m.PremiseDrifted,
		Dependencies:   deps,
	}
}

func toMemoryStub(m RecalledMemory) MemoryStub {
	return MemoryStub{
		MemoryID:       m.MemoryID,
		WorkspaceID:    m.WorkspaceID,
		Durability:     m.Durability,
		TopicKey:       m.TopicKey,
		CurationState:  m.CurationState,
		Version:        m.Version,
		SummaryStub:    m.SummaryStub,
		PremiseDrifted: m.PremiseDrifted,
		Expandable:     true,
	}
}

func toDependencyViews(edges []graph.OutgoingEdge) ([]MemoryDependencyView, bool) {
	if len(edges) == 0 {
		return nil, false
	}
	views := make([]MemoryDependencyView, len(edges))
	drifted := false
	for i, e := range edges {
		if e.PremiseDrifted {
			drifted = true
		}
		views[i] = MemoryDependencyView{
			FromMemoryID:           e.FromMemoryID,
			TargetMemoryID:         e.ToMemoryID,
			TargetTopicKey:         e.TargetTopicKey,
			TargetSummaryStub:      e.TargetSummaryStub,
			ExpectedToRev:          e.ExpectedToRev,
			Rationale:              e.Rationale,
			PremiseDrifted:         e.PremiseDrifted,
			ReplacementMemoryID:    e.ReplacementMemoryID,
			ReplacementTopicKey:    e.ReplacementTopicKey,
			ReplacementSummaryStub: e.ReplacementSummaryStub,
			ReplacementVersion:     e.ReplacementVersion,
		}
	}
	return views, drifted
}

func countSerializedTokens(v any) int {
	raw, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return curation.CountTokens(string(raw))
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
