package curation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/search"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

const (
	maxClaimItems          = 32
	maxClaimBytes          = 32768
	maxCandidateMemories   = 10
	defaultHistoryRetainN  = 20
	provisionalReplayLimit = 10
)

// WorkerDeps groups dependencies shared across River background curation workers.
type WorkerDeps struct {
	Store                *storage.Store
	Curator              Curator
	Embedder             embedding.Embedder
	Lexical              *search.LexicalSearch
	Graph                *graph.DependencyGraph
	CuratorBreaker       *Breaker
	EmbedderBreaker      *Breaker
	HistoryRetentionN    int
	CurationPaused       func() bool
	ForceProvisional     func() bool
	Logger               *slog.Logger
	SnoozeDuration       time.Duration
	PausedSnoozeDuration time.Duration
	ActiveWorkspaces     *sync.Map
}

func normalizeWorkerDeps(deps WorkerDeps) WorkerDeps {
	if deps.Lexical == nil {
		deps.Lexical = search.NewLexicalSearch()
	}
	if deps.Graph == nil {
		deps.Graph = graph.NewDependencyGraph()
	}
	if deps.CuratorBreaker == nil {
		deps.CuratorBreaker = NewBreaker(defaultBreakerThreshold, defaultBreakerCooldown)
	}
	if deps.EmbedderBreaker == nil {
		deps.EmbedderBreaker = NewBreaker(defaultBreakerThreshold, defaultBreakerCooldown)
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.SnoozeDuration <= 0 {
		deps.SnoozeDuration = 2 * time.Second
	}
	if deps.PausedSnoozeDuration <= 0 {
		deps.PausedSnoozeDuration = 30 * time.Second
	}
	if deps.ActiveWorkspaces == nil {
		deps.ActiveWorkspaces = &sync.Map{}
	}
	return deps
}

// RegisterWorkers binds all mem1 River workers to workers.
func RegisterWorkers(workers *river.Workers, deps WorkerDeps) {
	norm := normalizeWorkerDeps(deps)
	river.AddWorker(workers, NewCurateWorkspaceWorker(norm))
	river.AddWorker(workers, NewEmbedMemoryWorker(norm))
	river.AddWorker(workers, NewProvisionalReplayWorker(norm))
}

// PeriodicJobs returns River's scheduled background jobs (ProvisionalReplayJob every 60s).
func PeriodicJobs() []*river.PeriodicJob {
	return []*river.PeriodicJob{
		river.NewPeriodicJob(
			river.PeriodicInterval(60*time.Second),
			func() (river.JobArgs, *river.InsertOpts) {
				opts := storage.ProvisionalReplayJobArgs{}.InsertOpts()
				return storage.ProvisionalReplayJobArgs{}, &opts
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
	}
}

// CurateWorkspaceWorker runs serialized per-workspace background curation.
type CurateWorkspaceWorker struct {
	river.WorkerDefaults[storage.CurateWorkspaceJobArgs]
	deps WorkerDeps
}

// NewCurateWorkspaceWorker constructs a CurateWorkspaceWorker.
func NewCurateWorkspaceWorker(deps WorkerDeps) *CurateWorkspaceWorker {
	return &CurateWorkspaceWorker{deps: normalizeWorkerDeps(deps)}
}

type phaseASnapshot struct {
	workspace     storage.Workspace
	items         []storage.InboxItem
	candidates    []CandidateView
	candidateByID map[uuid.UUID]CandidateView
	existingEdges map[uuid.UUID][]uuid.UUID
}

func (w *CurateWorkspaceWorker) Work(ctx context.Context, job *river.Job[storage.CurateWorkspaceJobArgs]) error {
	wsID := job.Args.WorkspaceID
	if _, loaded := w.deps.ActiveWorkspaces.LoadOrStore(wsID, struct{}{}); loaded {
		return river.JobSnooze(w.deps.SnoozeDuration)
	}
	defer w.deps.ActiveWorkspaces.Delete(wsID)

	if w.deps.CurationPaused != nil && w.deps.CurationPaused() {
		return river.JobSnooze(w.deps.PausedSnoozeDuration)
	}

	// Phase A0: Pre-transaction query embedding with 0 DB connections held.
	queryVec := w.preTxEmbedPending(ctx, wsID)

	// Phase A: Short READ ONLY transaction claiming pending batch and snapshotting state.
	snap, err := w.loadPhaseASnapshot(ctx, wsID, queryVec)
	if err != nil {
		return err
	}
	if len(snap.items) == 0 {
		return nil
	}

	// Phase B / Fallback gate: forced provisional or open Curator circuit breaker.
	forceProv := w.deps.ForceProvisional != nil && w.deps.ForceProvisional()
	if forceProv || !w.deps.CuratorBreaker.Allow() {
		return w.runFallbackAndCheckRemaining(ctx, snap.workspace.WorkspaceID, snap.items, "provisional fallback active (breaker open or forced)")
	}

	return w.processBatch(ctx, job, snap, snap.items, queryVec)
}

func (w *CurateWorkspaceWorker) preTxEmbedPending(ctx context.Context, wsID string) []float32 {
	if w.deps.Embedder == nil || !w.deps.EmbedderBreaker.Allow() {
		return nil
	}
	pending, err := storage.ClaimPendingBatch(ctx, w.deps.Store.CurationPool, wsID, maxClaimItems, maxClaimBytes)
	if err != nil || len(pending) == 0 {
		return nil
	}
	var sb strings.Builder
	for i, it := range pending {
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(it.Content)
		if sb.Len() >= 2000 {
			break
		}
	}
	q := sb.String()
	if len(q) > 2000 {
		q = q[:2000]
	}
	vecs, err := w.deps.Embedder.EmbedBatch(ctx, []string{q})
	if err != nil {
		w.deps.EmbedderBreaker.RecordFailure(err)
		w.deps.Logger.Debug("pre-tx candidate embedding failed; using lexical fallback", "workspace_id", wsID, "err", err)
		return nil
	}
	w.deps.EmbedderBreaker.RecordSuccess()
	if len(vecs) == 0 {
		return nil
	}
	return vecs[0]
}

func (w *CurateWorkspaceWorker) loadPhaseASnapshot(ctx context.Context, wsID string, queryVec []float32) (phaseASnapshot, error) {
	tx, err := w.deps.Store.CurationPool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return phaseASnapshot{}, fmt.Errorf("begin Phase A read tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ws, err := storage.GetWorkspace(ctx, tx, wsID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return phaseASnapshot{}, nil
		}
		return phaseASnapshot{}, err
	}

	items, err := storage.ClaimPendingBatch(ctx, tx, wsID, maxClaimItems, maxClaimBytes)
	if err != nil {
		return phaseASnapshot{}, err
	}
	if len(items) == 0 {
		_ = tx.Commit(ctx)
		return phaseASnapshot{workspace: ws}, nil
	}

	candidates, candidateByID, err := w.loadCandidatesTx(ctx, tx, wsID, items, queryVec)
	if err != nil {
		return phaseASnapshot{}, err
	}

	existingEdges, err := loadActiveWorkspaceEdgesTx(ctx, tx, wsID)
	if err != nil {
		return phaseASnapshot{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return phaseASnapshot{}, fmt.Errorf("commit Phase A snapshot: %w", err)
	}

	return phaseASnapshot{
		workspace:     ws,
		items:         items,
		candidates:    candidates,
		candidateByID: candidateByID,
		existingEdges: existingEdges,
	}, nil
}

func (w *CurateWorkspaceWorker) loadCandidatesTx(ctx context.Context, tx pgx.Tx, wsID string, items []storage.InboxItem, queryVec []float32) ([]CandidateView, map[uuid.UUID]CandidateView, error) {
	seen := make(map[uuid.UUID]struct{}, maxCandidateMemories)
	ordered := make([]storage.Memory, 0, maxCandidateMemories)
	addMem := func(m storage.Memory) {
		if len(ordered) >= maxCandidateMemories {
			return
		}
		if m.Durability == storage.DurabilityConstitution || m.CurationState == storage.CurationProvisionalHeuristic {
			return
		}
		if _, ok := seen[m.MemoryID]; ok {
			return
		}
		seen[m.MemoryID] = struct{}{}
		ordered = append(ordered, m)
	}

	if len(queryVec) > 0 && w.deps.Embedder != nil {
		vhits, err := storage.SearchVectorTopK(ctx, tx, wsID, storage.InheritNone, w.deps.Embedder.SpaceID(), queryVec, maxCandidateMemories)
		if err == nil {
			for _, h := range vhits {
				addMem(h.Memory)
			}
		}
	}

	var qBuilder strings.Builder
	for i, it := range items {
		if i > 0 {
			qBuilder.WriteByte(' ')
		}
		qBuilder.WriteString(it.Content)
		if qBuilder.Len() >= 512 {
			break
		}
	}
	lhits, err := w.deps.Lexical.Search(ctx, tx, search.SearchParams{
		WorkspaceID:    wsID,
		InheritGeneral: storage.InheritNone,
		Query:          qBuilder.String(),
		Limit:          maxCandidateMemories,
	})
	if err == nil {
		for _, h := range lhits {
			addMem(h.Memory)
		}
	}

	if len(ordered) < maxCandidateMemories {
		rows, err := tx.Query(ctx, `
			SELECT
				memory_id, workspace_id, durability, topic_key, status, curation_state,
				current_version, summary_stub, body, token_count,
				COALESCE(embedding_space_id, ''), supersedes_memory_id, superseded_by_memory_id,
				last_reverted_version, created_at, updated_at
			FROM memory_manifest
			WHERE workspace_id = $1
			  AND status = 'active'
			  AND durability <> 'constitution'
			  AND curation_state <> 'provisional_heuristic'
			ORDER BY updated_at DESC, memory_id DESC
			LIMIT $2
		`, wsID, maxCandidateMemories)
		if err != nil {
			return nil, nil, fmt.Errorf("load recent curation candidates: %w", err)
		}
		for rows.Next() {
			var m storage.Memory
			if err := rows.Scan(
				&m.MemoryID, &m.WorkspaceID, &m.Durability, &m.TopicKey, &m.Status, &m.CurationState,
				&m.CurrentVersion, &m.SummaryStub, &m.Body, &m.TokenCount,
				&m.EmbeddingSpaceID, &m.SupersedesMemoryID, &m.SupersededByMemoryID,
				&m.LastRevertedVersion, &m.CreatedAt, &m.UpdatedAt,
			); err != nil {
				rows.Close()
				return nil, nil, fmt.Errorf("scan recent candidate: %w", err)
			}
			addMem(m)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("iterate recent candidates: %w", err)
		}
		rows.Close()
	}

	candidates := make([]CandidateView, 0, len(ordered))
	byID := make(map[uuid.UUID]CandidateView, len(ordered))
	for _, m := range ordered {
		cv := CandidateView{
			MemoryID:       m.MemoryID,
			Durability:     m.Durability,
			TopicKey:       m.TopicKey,
			CurrentVersion: m.CurrentVersion,
			SummaryStub:    m.SummaryStub,
			Body:           m.Body,
		}
		if m.CurrentVersion > 1 {
			var prevStub string
			var prevBody *string
			if err := tx.QueryRow(ctx, `
				SELECT summary_stub, body
				FROM memory_revisions
				WHERE memory_id = $1 AND version = $2 AND body_pruned = FALSE
			`, m.MemoryID, m.CurrentVersion-1).Scan(&prevStub, &prevBody); err == nil {
				cv.PrevSummaryStub = prevStub
				if prevBody != nil {
					cv.PrevBody = *prevBody
				}
			}
		}
		candidates = append(candidates, cv)
		byID[cv.MemoryID] = cv
	}
	return candidates, byID, nil
}

func loadActiveWorkspaceEdgesTx(ctx context.Context, db storage.DBTX, wsID string) (map[uuid.UUID][]uuid.UUID, error) {
	rows, err := db.Query(ctx, `
		SELECT from_memory_id, to_memory_id
		FROM memory_edges
		WHERE workspace_id = $1 AND status = 'active'
	`, wsID)
	if err != nil {
		return nil, fmt.Errorf("load active edges: %w", err)
	}
	defer rows.Close()

	edges := make(map[uuid.UUID][]uuid.UUID)
	for rows.Next() {
		var fromID, toID uuid.UUID
		if err := rows.Scan(&fromID, &toID); err != nil {
			return nil, fmt.Errorf("scan active edge: %w", err)
		}
		edges[fromID] = append(edges[fromID], toID)
	}
	return edges, rows.Err()
}

func (w *CurateWorkspaceWorker) processBatch(
	ctx context.Context,
	job *river.Job[storage.CurateWorkspaceJobArgs],
	snap phaseASnapshot,
	subItems []storage.InboxItem,
	queryVec []float32,
) error {
	if len(subItems) == 0 {
		return nil
	}

	req := CurateRequest{
		WorkspaceID:     snap.workspace.WorkspaceID,
		WorkspaceKind:   snap.workspace.Kind,
		SourceAuthority: "agent_observed",
		InboxItems:      toInboxItemViews(subItems),
		Candidates:      snap.candidates,
	}

	plan, err := w.deps.Curator.CurateBatch(ctx, req)
	if err != nil {
		if !errors.Is(err, ErrPlanValidation) {
			// Transient provider / HTTP / network error.
			openedNow := w.deps.CuratorBreaker.RecordFailure(err)
			if openedNow || job.Attempt >= job.MaxAttempts {
				return w.runFallbackAndCheckRemaining(ctx, snap.workspace.WorkspaceID, subItems, fmt.Sprintf("curator error fallback (attempt %d): %v", job.Attempt, err))
			}
			return err
		}
		// Malformed plan falls through to recursive bisection.
		return w.handleValidationFailure(ctx, job, snap, subItems, queryVec, err)
	}

	w.deps.CuratorBreaker.RecordSuccess()

	claimedIDs := make([]uuid.UUID, len(subItems))
	for i, it := range subItems {
		claimedIDs[i] = it.InboxID
	}
	vctx := ValidationContext{
		WorkspaceID:      snap.workspace.WorkspaceID,
		WorkspaceKind:    snap.workspace.Kind,
		SourceAuthority:  "agent_observed",
		ClaimedInboxIDs:  claimedIDs,
		ActiveCandidates: snap.candidateByID,
		ExistingEdges:    snap.existingEdges,
	}
	if err := ValidatePlan(vctx, plan); err != nil {
		return w.handleValidationFailure(ctx, job, snap, subItems, queryVec, err)
	}

	// Phase C: Serialized commit inside workspace FOR UPDATE lock.
	return w.commitPhaseC(ctx, snap, subItems, plan)
}

func (w *CurateWorkspaceWorker) handleValidationFailure(
	ctx context.Context,
	job *river.Job[storage.CurateWorkspaceJobArgs],
	snap phaseASnapshot,
	subItems []storage.InboxItem,
	queryVec []float32,
	valErr error,
) error {
	if len(subItems) > 1 {
		mid := len(subItems) / 2
		leftErr := w.processBatch(ctx, job, snap, subItems[:mid], queryVec)

		// Refresh candidate snapshot for right half so valid left-half commits are visible.
		refreshedSnap := snap
		if reloaded, err := w.loadPhaseASnapshot(ctx, snap.workspace.WorkspaceID, queryVec); err == nil && len(reloaded.items) > 0 {
			refreshedSnap = reloaded
		}
		rightErr := w.processBatch(ctx, job, refreshedSnap, subItems[mid:], queryVec)

		if leftErr != nil {
			return leftErr
		}
		return rightErr
	}

	// Isolated single poison-pill item.
	if job.Attempt >= job.MaxAttempts {
		item := subItems[0]
		if err := storage.MarkInboxDeadLetter(ctx, w.deps.Store.CurationPool, snap.workspace.WorkspaceID, item.InboxID, valErr.Error()); err != nil {
			return fmt.Errorf("dead-letter inbox %s after validation failure (%v): %w", item.InboxID, valErr, err)
		}
		w.deps.Logger.Warn("dead-lettered poison-pill inbox item",
			"workspace_id", snap.workspace.WorkspaceID,
			"inbox_id", item.InboxID,
			"attempt", job.Attempt,
			"err", valErr,
		)
		// Ensure subsequent pending items in the same workspace do not stall when River discards this job.
		_ = w.enqueueFollowupIfPending(ctx, w.deps.Store.CurationPool, snap.workspace.WorkspaceID, false)
	}
	return valErr
}

func (w *CurateWorkspaceWorker) runFallbackAndCheckRemaining(ctx context.Context, workspaceID string, items []storage.InboxItem, reason string) error {
	tx, err := w.deps.Store.CurationPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin provisional fallback tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := storage.LockWorkspaceTx(ctx, tx, workspaceID); err != nil {
		return err
	}
	if _, err := CommitProvisionalFallbackTx(ctx, tx, workspaceID, items, reason); err != nil {
		return err
	}
	if err := w.enqueueFollowupIfPendingTx(ctx, tx, workspaceID, false); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (w *CurateWorkspaceWorker) commitPhaseC(ctx context.Context, snap phaseASnapshot, subItems []storage.InboxItem, plan CuratePlan) error {
	wsID := snap.workspace.WorkspaceID
	subByID := make(map[uuid.UUID]storage.InboxItem, len(subItems))
	subIDList := make([]uuid.UUID, len(subItems))
	for i, it := range subItems {
		subByID[it.InboxID] = it
		subIDList[i] = it.InboxID
	}

	tx, err := w.deps.Store.CurationPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin Phase C tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := storage.LockWorkspaceTx(ctx, tx, wsID); err != nil {
		return err
	}

	// Re-verify which subItems remain 'pending' under workspace lock.
	pendingNow, err := filterCurrentlyPendingTx(ctx, tx, wsID, subIDList)
	if err != nil {
		return err
	}
	if len(pendingNow) == 0 {
		return tx.Commit(ctx)
	}

	var (
		committedInbox []uuid.UUID
		committedSet   = make(map[uuid.UUID]struct{}, len(subItems))
		manifestMut    bool
		hasConflict    bool
	)

	for _, mut := range plan.Mutations {
		// Filter citations down to still-pending items in this mutation.
		var mutInboxIDs []uuid.UUID
		var citations []storage.CitationInput
		primaryAgent := ""
		for _, id := range mut.InboxIDs {
			if _, ok := pendingNow[id]; !ok {
				continue
			}
			it := subByID[id]
			mutInboxIDs = append(mutInboxIDs, id)
			citations = append(citations, storage.CitationInput{
				InboxID: id,
				AgentID: it.AgentID,
			})
			if primaryAgent == "" && it.AgentID != "" {
				primaryAgent = it.AgentID
			}
		}
		if len(mutInboxIDs) == 0 {
			continue
		}

		sp, err := tx.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin mutation savepoint: %w", err)
		}

		var (
			targetMemID *uuid.UUID
			targetVer   *int
			changedMem  *storage.Memory
		)

		var mutErr error
		switch mut.Action {
		case ActionNoop:
			_, mutErr = storage.InsertCurationDecisionTx(ctx, sp, storage.CurationDecision{
				WorkspaceID:      wsID,
				InboxIDs:         mutInboxIDs,
				Action:           "noop",
				ProviderName:     plan.ProviderName,
				ModelName:        plan.ModelName,
				PromptTokens:     plan.PromptTokens,
				CompletionTokens: plan.CompletionTokens,
				Rationale:        mut.Rationale,
			})

		case ActionCreate:
			var mem storage.Memory
			mem, mutErr = storage.CreateMemoryTx(ctx, sp, storage.CreateMemoryParams{
				WorkspaceID:     wsID,
				Durability:      mut.Durability,
				TopicKey:        mut.TopicKey,
				CurationState:   storage.CurationCuratorVerified,
				SummaryStub:     mut.SummaryStub,
				Body:            mut.Body,
				TokenCount:      CountTokens(mut.Body),
				AgentID:         primaryAgent,
				SourceAuthority: "agent_observed",
				ChangeReason:    mut.Rationale,
				Citations:       citations,
			})
			if mutErr == nil {
				mutErr = w.deps.Graph.UpsertEdges(ctx, sp, wsID, mem.MemoryID, mem.CurrentVersion, mut.DependsOn)
			}
			if mutErr == nil {
				v := mem.CurrentVersion
				targetMemID = &mem.MemoryID
				targetVer = &v
				changedMem = &mem
				_, mutErr = storage.InsertCurationDecisionTx(ctx, sp, storage.CurationDecision{
					WorkspaceID:      wsID,
					InboxIDs:         mutInboxIDs,
					Action:           "create",
					TargetMemoryID:   targetMemID,
					TargetVersion:    targetVer,
					ProviderName:     plan.ProviderName,
					ModelName:        plan.ModelName,
					PromptTokens:     plan.PromptTokens,
					CompletionTokens: plan.CompletionTokens,
					Rationale:        mut.Rationale,
				})
			}

		case ActionRevise:
			expectedVer := snap.candidateByID[*mut.TargetMemoryID].CurrentVersion
			var mem storage.Memory
			mem, mutErr = storage.ReviseCoupledTopicTx(ctx, sp, storage.ReviseMemoryParams{
				MemoryID:        *mut.TargetMemoryID,
				WorkspaceID:     wsID,
				ExpectedVersion: expectedVer,
				Durability:      mut.Durability,
				CurationState:   storage.CurationCuratorVerified,
				SummaryStub:     mut.SummaryStub,
				Body:            mut.Body,
				TokenCount:      CountTokens(mut.Body),
				AgentID:         primaryAgent,
				SourceAuthority: "agent_observed",
				ChangeReason:    mut.Rationale,
				Citations:       citations,
			})
			if mutErr == nil {
				mutErr = w.deps.Graph.UpsertEdges(ctx, sp, wsID, mem.MemoryID, mem.CurrentVersion, mut.DependsOn)
			}
			if mutErr == nil && w.deps.HistoryRetentionN > 0 {
				_, mutErr = storage.PruneRevisionBodiesTx(ctx, sp, mem.MemoryID, w.deps.HistoryRetentionN)
			}
			if mutErr == nil {
				v := mem.CurrentVersion
				targetMemID = &mem.MemoryID
				targetVer = &v
				changedMem = &mem
				_, mutErr = storage.InsertCurationDecisionTx(ctx, sp, storage.CurationDecision{
					WorkspaceID:      wsID,
					InboxIDs:         mutInboxIDs,
					Action:           "revise",
					TargetMemoryID:   targetMemID,
					TargetVersion:    targetVer,
					ProviderName:     plan.ProviderName,
					ModelName:        plan.ModelName,
					PromptTokens:     plan.PromptTokens,
					CompletionTokens: plan.CompletionTokens,
					Rationale:        mut.Rationale,
				})
			}

		case ActionSupersede:
			expectedVer := snap.candidateByID[*mut.TargetMemoryID].CurrentVersion
			var mem storage.Memory
			mem, mutErr = storage.SupersedeMemoryTx(ctx, sp, storage.SupersedeMemoryParams{
				WorkspaceID:     wsID,
				OldMemoryID:     *mut.TargetMemoryID,
				ExpectedVersion: expectedVer,
				Durability:      mut.Durability,
				TopicKey:        mut.TopicKey,
				CurationState:   storage.CurationCuratorVerified,
				SummaryStub:     mut.SummaryStub,
				Body:            mut.Body,
				TokenCount:      CountTokens(mut.Body),
				AgentID:         primaryAgent,
				SourceAuthority: "agent_observed",
				ChangeReason:    mut.Rationale,
				Citations:       citations,
			})
			if mutErr == nil {
				mutErr = w.deps.Graph.UpsertEdges(ctx, sp, wsID, mem.MemoryID, mem.CurrentVersion, mut.DependsOn)
			}
			if mutErr == nil {
				v := mem.CurrentVersion
				targetMemID = &mem.MemoryID
				targetVer = &v
				changedMem = &mem
				_, mutErr = storage.InsertCurationDecisionTx(ctx, sp, storage.CurationDecision{
					WorkspaceID:      wsID,
					InboxIDs:         mutInboxIDs,
					Action:           "supersede",
					TargetMemoryID:   targetMemID,
					TargetVersion:    targetVer,
					ProviderName:     plan.ProviderName,
					ModelName:        plan.ModelName,
					PromptTokens:     plan.PromptTokens,
					CompletionTokens: plan.CompletionTokens,
					Rationale:        mut.Rationale,
				})
			}

		case ActionRetire:
			expectedVer := snap.candidateByID[*mut.TargetMemoryID].CurrentVersion
			var mem storage.Memory
			mem, mutErr = storage.SoftRetireTx(ctx, sp, storage.SoftRetireParams{
				MemoryID:        *mut.TargetMemoryID,
				WorkspaceID:     wsID,
				ExpectedVersion: expectedVer,
			})
			if mutErr == nil {
				v := mem.CurrentVersion
				targetMemID = &mem.MemoryID
				targetVer = &v
				_, mutErr = storage.InsertCurationDecisionTx(ctx, sp, storage.CurationDecision{
					WorkspaceID:      wsID,
					InboxIDs:         mutInboxIDs,
					Action:           "retire",
					TargetMemoryID:   targetMemID,
					TargetVersion:    targetVer,
					ProviderName:     plan.ProviderName,
					ModelName:        plan.ModelName,
					PromptTokens:     plan.PromptTokens,
					CompletionTokens: plan.CompletionTokens,
					Rationale:        mut.Rationale,
				})
			}
		}

		if mutErr == nil && changedMem != nil {
			_, mutErr = w.deps.Store.RiverInsert.InsertTx(ctx, sp, storage.EmbedMemoryJobArgs{
				WorkspaceID: wsID,
				MemoryID:    changedMem.MemoryID,
				Version:     changedMem.CurrentVersion,
			}, nil)
		}

		if mutErr != nil {
			_ = sp.Rollback(ctx)
			if errors.Is(mutErr, storage.ErrConflict) || errors.Is(mutErr, storage.ErrNotFound) {
				// Concurrent manual edit or topic_key race mid-flight: discard only this mutation cleanly.
				hasConflict = true
				w.deps.Logger.Info("discarded conflicted curation mutation mid-flight",
					"workspace_id", wsID,
					"action", mut.Action,
					"err", mutErr,
				)
				continue
			}
			return mutErr
		}

		if err := sp.Commit(ctx); err != nil {
			return fmt.Errorf("commit mutation savepoint: %w", err)
		}

		if mut.Action != ActionNoop {
			manifestMut = true
		}
		for _, id := range mutInboxIDs {
			if _, seen := committedSet[id]; !seen {
				committedSet[id] = struct{}{}
				committedInbox = append(committedInbox, id)
			}
		}
	}

	if len(committedInbox) > 0 {
		delProv, err := storage.DeleteProvisionalManifestByInboxTx(ctx, tx, wsID, committedInbox)
		if err != nil {
			return err
		}
		if delProv > 0 {
			manifestMut = true
		}
		if _, err := storage.MarkInboxCurated(ctx, tx, wsID, committedInbox); err != nil {
			return err
		}
	}

	if manifestMut {
		if _, err := storage.BumpRevisionTx(ctx, tx, wsID, false); err != nil {
			return err
		}
	}

	if err := w.enqueueFollowupIfPendingTx(ctx, tx, wsID, hasConflict); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (w *CurateWorkspaceWorker) enqueueFollowupIfPending(ctx context.Context, db storage.DBTX, wsID string, forceEnqueue bool) error {
	hasPending := forceEnqueue
	if !hasPending {
		if err := db.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM raw_inbox WHERE workspace_id = $1 AND status = 'pending'
			)
		`, wsID).Scan(&hasPending); err != nil {
			return fmt.Errorf("check remaining pending inbox items: %w", err)
		}
	}
	if !hasPending {
		return nil
	}
	_, err := w.deps.Store.EnqueueCurateWorkspace(ctx, wsID)
	return err
}

func (w *CurateWorkspaceWorker) enqueueFollowupIfPendingTx(ctx context.Context, tx pgx.Tx, wsID string, forceEnqueue bool) error {
	hasPending := forceEnqueue
	if !hasPending {
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM raw_inbox WHERE workspace_id = $1 AND status = 'pending'
			)
		`, wsID).Scan(&hasPending); err != nil {
			return fmt.Errorf("check remaining pending inbox items in tx: %w", err)
		}
	}
	if !hasPending {
		return nil
	}
	_, err := w.deps.Store.EnqueueCurateWorkspaceTx(ctx, tx, wsID)
	return err
}

func filterCurrentlyPendingTx(ctx context.Context, tx pgx.Tx, wsID string, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	rows, err := tx.Query(ctx, `
		SELECT inbox_id
		FROM raw_inbox
		WHERE workspace_id = $1
		  AND status = 'pending'
		  AND inbox_id = ANY($2)
	`, wsID, ids)
	if err != nil {
		return nil, fmt.Errorf("re-verify pending inbox items: %w", err)
	}
	defer rows.Close()

	out := make(map[uuid.UUID]struct{}, len(ids))
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan verified pending inbox_id: %w", err)
		}
		out[id] = struct{}{}
	}
	return out, rows.Err()
}

func toInboxItemViews(items []storage.InboxItem) []InboxItemView {
	views := make([]InboxItemView, len(items))
	for i, it := range items {
		views[i] = InboxItemView{
			InboxID:    it.InboxID,
			AgentID:    it.AgentID,
			OccurredAt: it.OccurredAt,
			Content:    it.Content,
		}
	}
	return views
}

// EmbedMemoryWorker generates and updates inline halfvec(1536) embeddings for committed memories.
type EmbedMemoryWorker struct {
	river.WorkerDefaults[storage.EmbedMemoryJobArgs]
	deps WorkerDeps
}

// NewEmbedMemoryWorker constructs an EmbedMemoryWorker.
func NewEmbedMemoryWorker(deps WorkerDeps) *EmbedMemoryWorker {
	return &EmbedMemoryWorker{deps: normalizeWorkerDeps(deps)}
}

func (w *EmbedMemoryWorker) Work(ctx context.Context, job *river.Job[storage.EmbedMemoryJobArgs]) error {
	if w.deps.Embedder == nil {
		return nil
	}
	if !w.deps.EmbedderBreaker.Allow() {
		return river.JobSnooze(15 * time.Second)
	}

	mem, err := storage.GetMemory(ctx, w.deps.Store.CurationPool, job.Args.MemoryID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		return err
	}
	if mem.Status != storage.StatusActive || mem.CurrentVersion > job.Args.Version {
		return nil
	}

	text := embedding.FormatMemoryText(mem.TopicKey, mem.SummaryStub, mem.Body)
	vecs, err := w.deps.Embedder.EmbedBatch(ctx, []string{text})
	if err != nil {
		w.deps.EmbedderBreaker.RecordFailure(err)
		return err
	}
	w.deps.EmbedderBreaker.RecordSuccess()
	if len(vecs) == 0 {
		return errors.New("embedder returned empty vector slice")
	}

	_, err = storage.UpdateMemoryEmbeddingVersionTx(
		ctx,
		w.deps.Store.CurationPool,
		job.Args.MemoryID,
		job.Args.Version,
		vecs[0],
		w.deps.Embedder.SpaceID(),
	)
	return err
}

// ProvisionalReplayWorker periodically resets up to 10 active provisional items to 'pending'
// once the Curator circuit breaker permits traffic again.
type ProvisionalReplayWorker struct {
	river.WorkerDefaults[storage.ProvisionalReplayJobArgs]
	deps WorkerDeps
}

// NewProvisionalReplayWorker constructs a ProvisionalReplayWorker.
func NewProvisionalReplayWorker(deps WorkerDeps) *ProvisionalReplayWorker {
	return &ProvisionalReplayWorker{deps: normalizeWorkerDeps(deps)}
}

func (w *ProvisionalReplayWorker) Work(ctx context.Context, _ *river.Job[storage.ProvisionalReplayJobArgs]) error {
	if w.deps.CurationPaused != nil && w.deps.CurationPaused() {
		return nil
	}
	if w.deps.ForceProvisional != nil && w.deps.ForceProvisional() {
		return nil
	}
	if !w.deps.CuratorBreaker.Allow() {
		return nil
	}

	tx, err := w.deps.Store.CurationPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin provisional replay tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	workspaces, count, err := storage.ResetProvisionalToPending(ctx, tx, provisionalReplayLimit)
	if err != nil {
		return err
	}
	for _, wsID := range workspaces {
		if _, err := w.deps.Store.EnqueueCurateWorkspaceTx(ctx, tx, wsID); err != nil {
			return fmt.Errorf("enqueue replay curate_workspace for %q: %w", wsID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit provisional replay tx: %w", err)
	}
	if count > 0 {
		w.deps.Logger.Info("re-queued provisional inbox entries for full curation", "count", count, "workspaces", workspaces)
	}
	return nil
}
