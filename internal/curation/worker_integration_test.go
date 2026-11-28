//go:build integration

package curation_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"

	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/storage"
	"github.com/nacfson/nacfson-memory/internal/testutil"
)

type scriptCurator struct {
	mu   sync.Mutex
	fn   func(ctx context.Context, req curation.CurateRequest) (curation.CuratePlan, error)
	hits atomic.Int32
}

func (s *scriptCurator) CurateBatch(ctx context.Context, req curation.CurateRequest) (curation.CuratePlan, error) {
	s.hits.Add(1)
	s.mu.Lock()
	fn := s.fn
	s.mu.Unlock()
	return fn(ctx, req)
}

type fastRetryPolicy struct{}

func (fastRetryPolicy) NextRetry(*rivertype.JobRow) time.Time {
	return time.Now().Add(20 * time.Millisecond)
}

func startRiverWorkers(t *testing.T, store *storage.Store, deps curation.WorkerDeps) {
	t.Helper()
	workers := river.NewWorkers()
	deps.Store = store
	if deps.SnoozeDuration <= 0 {
		deps.SnoozeDuration = 100 * time.Millisecond
	}
	curation.RegisterWorkers(workers, deps)

	client, err := river.NewClient(riverpgxv5.New(store.CurationPool), &river.Config{
		Queues:            storage.RiverQueues(),
		Workers:           workers,
		RetryPolicy:       fastRetryPolicy{},
		FetchCooldown:     20 * time.Millisecond,
		FetchPollInterval: 50 * time.Millisecond,
		TestOnly:          true,
	})
	if err != nil {
		t.Fatalf("new river test client: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start river test client: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.StopAndCancel(stopCtx)
	})
}

func waitUntil(t *testing.T, timeout time.Duration, desc string, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok, err := cond()
		if err == nil && ok {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

func TestCurationLifecycleCreateReviseSupersedeRetireAndEmbed(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)
	startRiverWorkers(t, store, curation.WorkerDeps{
		Curator:           curation.NewMockCurator(),
		Embedder:          embedding.NewMockEmbedder("mock/embed:1536"),
		HistoryRetentionN: 20,
	})

	// 1. Coupled two-agent create on same topic.
	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "payments", AgentID: "claude-code",
		Content: "topic:retry-backoff Initial exponential backoff starts at 100ms.",
	}); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "payments", AgentID: "cursor",
		Content: "topic:retry-backoff Cap backoff jitter at 30s.",
	}); err != nil {
		t.Fatalf("write 2: %v", err)
	}

	waitUntil(t, 10*time.Second, "initial batch curated and embedded", func() (bool, error) {
		var embedded int
		err := store.CurationPool.QueryRow(ctx, `
			SELECT count(*) FROM memory_manifest
			WHERE workspace_id = 'payments' AND topic_key = 'retry-backoff'
			  AND status = 'active' AND embedding IS NOT NULL AND embedding_space_id = 'mock/embed:1536'
		`).Scan(&embedded)
		return embedded == 1, err
	})

	// 2. Additive revise without bumping extra versions during embedding.
	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "payments", AgentID: "codex-cli",
		Content: "topic:retry-backoff Maximum retry budget is 5 attempts.",
	}); err != nil {
		t.Fatalf("write 3: %v", err)
	}
	waitUntil(t, 10*time.Second, "coupled revise committed and embedded", func() (bool, error) {
		var ver int
		var space, body string
		err := store.CurationPool.QueryRow(ctx, `
			SELECT current_version, COALESCE(embedding_space_id, ''), body
			FROM memory_manifest
			WHERE workspace_id = 'payments' AND topic_key = 'retry-backoff' AND status = 'active'
		`).Scan(&ver, &space, &body)
		return ver >= 2 && strings.Contains(body, "Maximum retry budget") && space == "mock/embed:1536", err
	})

	// 3. Contradiction -> supersede (old row becomes superseded; new active row created).
	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "payments", AgentID: "claude-code",
		Content: "topic:retry-backoff Switched strategy: fixed 500ms retry delay only.",
	}); err != nil {
		t.Fatalf("write 4: %v", err)
	}
	waitUntil(t, 10*time.Second, "supersede committed", func() (bool, error) {
		var supCount, activeCount int
		_ = store.CurationPool.QueryRow(ctx, `SELECT count(*) FROM memory_manifest WHERE workspace_id = 'payments' AND topic_key = 'retry-backoff' AND status = 'superseded'`).Scan(&supCount)
		err := store.CurationPool.QueryRow(ctx, `SELECT count(*) FROM memory_manifest WHERE workspace_id = 'payments' AND topic_key = 'retry-backoff' AND status = 'active' AND body LIKE '%500ms%'`).Scan(&activeCount)
		return supCount == 1 && activeCount == 1, err
	})

	// 4. Explicit retire.
	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "payments", AgentID: "claude-code",
		Content: "topic:retry-backoff Retire this task note, retry backoff checklist finished.",
	}); err != nil {
		t.Fatalf("write 5: %v", err)
	}
	waitUntil(t, 10*time.Second, "retire committed", func() (bool, error) {
		var retired int
		err := store.CurationPool.QueryRow(ctx, `
			SELECT count(*) FROM memory_manifest
			WHERE workspace_id = 'payments' AND topic_key = 'retry-backoff' AND status = 'retired'
		`).Scan(&retired)
		return retired == 1, err
	})
}

func TestCuratorOutageFallbackAndProvisionalReplayCleanup(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)

	now := time.Date(2026, 11, 23, 14, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	breaker := curation.NewBreakerWithClock(3, 60*time.Second, func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	})

	var failMode atomic.Bool
	failMode.Store(true)
	sc := &scriptCurator{
		fn: func(ctx context.Context, req curation.CurateRequest) (curation.CuratePlan, error) {
			if failMode.Load() {
				return curation.CuratePlan{}, errors.New("429 rate limited")
			}
			return curation.NewMockCurator().CurateBatch(ctx, req)
		},
	}

	deps := curation.WorkerDeps{
		Curator:        sc,
		Embedder:       embedding.NewMockEmbedder("mock/embed:1536"),
		CuratorBreaker: breaker,
	}
	startRiverWorkers(t, store, deps)

	inRes, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "outagews", Content: "wip: deploy payment worker cleanly",
	})
	if err != nil {
		t.Fatalf("insert inbox: %v", err)
	}

	// Wait until Attempts 1..3 fail -> 3rd opens breaker & commits provisional:<inbox_id> immediately!
	waitUntil(t, 15*time.Second, "provisional fallback committed on Attempt 3", func() (bool, error) {
		var provManifests int
		err := store.CurationPool.QueryRow(ctx, `
			SELECT count(*) FROM memory_manifest
			WHERE workspace_id = 'outagews'
			  AND topic_key = $1
			  AND durability = 'working'
			  AND curation_state = 'provisional_heuristic'
			  AND status = 'active'
		`, "provisional:"+inRes.InboxID.String()).Scan(&provManifests)
		return provManifests == 1 && breaker.IsOpen(), err
	})

	// Recover Curator, advance breaker clock past 60s, and run ProvisionalReplayWorker directly.
	failMode.Store(false)
	mu.Lock()
	now = now.Add(61 * time.Second)
	mu.Unlock()

	deps.Store = store
	replayWorker := curation.NewProvisionalReplayWorker(deps)
	if err := replayWorker.Work(ctx, &river.Job[storage.ProvisionalReplayJobArgs]{}); err != nil {
		t.Fatalf("replay worker failed: %v", err)
	}

	// Provisional row must be atomically deleted and inbox marked 'curated'.
	waitUntil(t, 10*time.Second, "provisional replay replaced and cleaned up", func() (bool, error) {
		var provLeft int
		var inboxStatus string
		_ = store.CurationPool.QueryRow(ctx, `SELECT count(*) FROM memory_manifest WHERE workspace_id = 'outagews' AND curation_state = 'provisional_heuristic'`).Scan(&provLeft)
		err := store.CurationPool.QueryRow(ctx, `SELECT status FROM raw_inbox WHERE inbox_id = $1`, inRes.InboxID).Scan(&inboxStatus)
		return provLeft == 0 && inboxStatus == "curated", err
	})
}

func TestPoisonPillBisectionDeadLetterAndMidFlightConflict(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)

	// Scripted curator fails validation ONLY for inbox items containing "POISON",
	// succeeding cleanly for healthy items.
	sc := &scriptCurator{
		fn: func(ctx context.Context, req curation.CurateRequest) (curation.CuratePlan, error) {
			for _, it := range req.InboxItems {
				if strings.Contains(it.Content, "POISON") {
					return curation.CuratePlan{
						Mutations: []curation.CurateMutation{{
							InboxIDs:    []uuid.UUID{it.InboxID},
							Action:      curation.ActionCreate,
							Category:    curation.CategoryProjectTechnical,
							Durability:  storage.DurabilityConstitution, // invalid! triggers bisection -> dead_letter
							TopicKey:    "bad-poison",
							SummaryStub: "Bad",
							Body:        "Forbidden constitution write",
							Rationale:   "poison",
						}},
					}, nil
				}
			}
			return curation.NewMockCurator().CurateBatch(ctx, req)
		},
	}

	startRiverWorkers(t, store, curation.WorkerDeps{
		Curator:  sc,
		Embedder: embedding.NewMockEmbedder("mock/embed:1536"),
	})

	goodRes, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "bisectws", Content: "topic:good-note Always log request_id cleanly.",
	})
	if err != nil {
		t.Fatalf("insert good: %v", err)
	}
	poisonRes, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "bisectws", Content: "POISON bad input",
	})
	if err != nil {
		t.Fatalf("insert poison: %v", err)
	}

	// Wait until good item is curated and poison item transitions to dead_letter on Attempt 3.
	waitUntil(t, 20*time.Second, "bisection curated good item and dead-lettered poison pill", func() (bool, error) {
		var goodStatus, poisonStatus string
		_ = store.CurationPool.QueryRow(ctx, `SELECT status FROM raw_inbox WHERE inbox_id = $1`, goodRes.InboxID).Scan(&goodStatus)
		err := store.CurationPool.QueryRow(ctx, `SELECT status FROM raw_inbox WHERE inbox_id = $1`, poisonRes.InboxID).Scan(&poisonStatus)
		return goodStatus == "curated" && poisonStatus == "dead_letter", err
	})

	// Subsequent good write in the same workspace is not blocked!
	afterRes, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "bisectws", Content: "topic:after-note Workspace queue keeps moving.",
	})
	if err != nil {
		t.Fatalf("insert after: %v", err)
	}
	waitUntil(t, 10*time.Second, "post-poison item curated", func() (bool, error) {
		var st string
		err := store.CurationPool.QueryRow(ctx, `SELECT status FROM raw_inbox WHERE inbox_id = $1`, afterRes.InboxID).Scan(&st)
		return st == "curated", err
	})

	// Test Workspace Exclusion snooze directly via ActiveWorkspaces sync.Map.
	var active sync.Map
	active.Store("busyws", struct{}{})
	worker := curation.NewCurateWorkspaceWorker(curation.WorkerDeps{
		Store:            store,
		Curator:          curation.NewMockCurator(),
		ActiveWorkspaces: &active,
		SnoozeDuration:   2 * time.Second,
	})
	snoozeErr := worker.Work(ctx, &river.Job[storage.CurateWorkspaceJobArgs]{
		Args: storage.CurateWorkspaceJobArgs{WorkspaceID: "busyws"},
	})
	var snoozeTarget *river.JobSnoozeError
	if !errors.As(snoozeErr, &snoozeTarget) {
		t.Fatalf("expected *river.JobSnoozeError when workspace active, got %v", snoozeErr)
	}

	// Verify CurationPaused snooze and 'general' provisional fallback path.
	pausedWorker := curation.NewCurateWorkspaceWorker(curation.WorkerDeps{
		Store:          store,
		Curator:        curation.NewMockCurator(),
		CurationPaused: func() bool { return true },
	})
	pausedErr := pausedWorker.Work(ctx, &river.Job[storage.CurateWorkspaceJobArgs]{
		Args: storage.CurateWorkspaceJobArgs{WorkspaceID: "pausedws"},
	})
	if !errors.As(pausedErr, &snoozeTarget) {
		t.Fatalf("expected *river.JobSnoozeError when paused, got %v", pausedErr)
	}

	genIn, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: storage.WorkspaceGeneral, Content: "general note while breaker open",
	})
	if err != nil {
		t.Fatalf("insert general inbox item: %v", err)
	}
	gtx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin gtx: %v", err)
	}
	claimedGen, err := storage.ClaimPendingBatch(ctx, gtx, storage.WorkspaceGeneral, 10, 32768)
	if err != nil || len(claimedGen) == 0 {
		_ = gtx.Rollback(ctx)
		t.Fatalf("claim general pending: %v", err)
	}
	if _, err := storage.LockWorkspaceTx(ctx, gtx, storage.WorkspaceGeneral); err != nil {
		_ = gtx.Rollback(ctx)
		t.Fatalf("lock general: %v", err)
	}
	nProvGen, err := curation.CommitProvisionalFallbackTx(ctx, gtx, storage.WorkspaceGeneral, claimedGen, "general fallback")
	if err != nil || nProvGen < 1 {
		_ = gtx.Rollback(ctx)
		t.Fatalf("CommitProvisionalFallbackTx on general failed: n=%d err=%v", nProvGen, err)
	}
	if err := gtx.Commit(ctx); err != nil {
		t.Fatalf("commit gtx: %v", err)
	}
	var genStatus string
	if err := store.CurationPool.QueryRow(ctx, `SELECT status FROM raw_inbox WHERE inbox_id = $1`, genIn.InboxID).Scan(&genStatus); err != nil || genStatus != "provisional" {
		t.Fatalf("expected general inbox status=provisional without working manifest, got %q err=%v", genStatus, err)
	}

	// Verify EmbedMemoryWorker snoozes cleanly when EmbedderBreaker is open.
	openEmbBreaker := curation.NewBreaker(1, time.Minute)
	openEmbBreaker.RecordFailure(errors.New("embed down"))
	embWorker := curation.NewEmbedMemoryWorker(curation.WorkerDeps{
		Store:           store,
		Embedder:        embedding.NewMockEmbedder(""),
		EmbedderBreaker: openEmbBreaker,
	})
	embSnoozeErr := embWorker.Work(ctx, &river.Job[storage.EmbedMemoryJobArgs]{
		Args: storage.EmbedMemoryJobArgs{WorkspaceID: "bisectws", MemoryID: uuid.New(), Version: 1},
	})
	if !errors.As(embSnoozeErr, &snoozeTarget) {
		t.Fatalf("expected *river.JobSnoozeError when embed breaker open, got %v", embSnoozeErr)
	}
}

func TestMidFlightConflictSavepointIsolation(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)

	if err := storage.EnsureWorkspace(ctx, store.CurationPool, "conflictws"); err != nil {
		t.Fatalf("ensure conflictws: %v", err)
	}
	seedTx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	mem, err := storage.CreateMemoryTx(ctx, seedTx, storage.CreateMemoryParams{
		WorkspaceID: "conflictws", Durability: storage.DurabilityDurable, TopicKey: "api-timeout",
		CurationState: storage.CurationUserExplicit, SummaryStub: "v1 timeout", Body: "Timeout 10s", TokenCount: 3,
	})
	if err != nil {
		t.Fatalf("seed memory: %v", err)
	}
	if err := seedTx.Commit(ctx); err != nil {
		t.Fatalf("commit seed tx: %v", err)
	}

	inPhaseB := make(chan struct{}, 1)
	releasePhaseB := make(chan struct{})
	var callNum atomic.Int32
	var releaseOnce sync.Once

	sc := &scriptCurator{
		fn: func(ctx context.Context, req curation.CurateRequest) (curation.CuratePlan, error) {
			n := callNum.Add(1)
			if n == 1 {
				// Signal that Phase A has snapshotted current_version=1, then wait for concurrent manual edit.
				select {
				case inPhaseB <- struct{}{}:
				default:
				}
				<-releasePhaseB
				// Return a batch with a stale revise target (expecting v1) alongside a valid create.
				muts := make([]curation.CurateMutation, 0, len(req.InboxItems))
				for _, it := range req.InboxItems {
					if strings.Contains(it.Content, "conflicted") {
						targetID := mem.MemoryID
						muts = append(muts, curation.CurateMutation{
							InboxIDs:       []uuid.UUID{it.InboxID},
							Action:         curation.ActionRevise,
							Category:       curation.CategoryProjectTechnical,
							TargetMemoryID: &targetID,
							Durability:     storage.DurabilityDurable,
							TopicKey:       "api-timeout",
							SummaryStub:    "Stale update",
							Body:           "Timeout 20s from agent",
							Rationale:      "stale v1 revise",
						})
					} else {
						muts = append(muts, curation.CurateMutation{
							InboxIDs:    []uuid.UUID{it.InboxID},
							Action:      curation.ActionCreate,
							Category:    curation.CategoryProjectTechnical,
							Durability:  storage.DurabilityDurable,
							TopicKey:    "sibling-note",
							SummaryStub: "Sibling note ok",
							Body:        "Sibling mutation commits independently",
							Rationale:   "healthy sibling",
						})
					}
				}
				return curation.CuratePlan{CurateUsage: curation.CurateUsage{ProviderName: "mock", ModelName: "script"}, Mutations: muts}, nil
			}
			// On follow-up after conflict re-enqueue, emit noop so inbox finishes cleanly.
			ids := make([]uuid.UUID, len(req.InboxItems))
			for i, it := range req.InboxItems {
				ids[i] = it.InboxID
			}
			return curation.CuratePlan{
				CurateUsage: curation.CurateUsage{ProviderName: "mock", ModelName: "script"},
				Mutations:   []curation.CurateMutation{{InboxIDs: ids, Action: curation.ActionNoop, Rationale: "reconciled after manual edit"}},
			}, nil
		},
	}

	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "conflictws", Content: "conflicted timeout update",
	}); err != nil {
		t.Fatalf("insert conflicted turn: %v", err)
	}
	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "conflictws", Content: "healthy sibling turn",
	}); err != nil {
		t.Fatalf("insert sibling turn: %v", err)
	}

	startRiverWorkers(t, store, curation.WorkerDeps{
		Curator:  sc,
		Embedder: embedding.NewMockEmbedder("mock/embed:1536"),
	})

	// Wait for worker to hold Phase A v1 snapshot in Phase B.
	select {
	case <-inPhaseB:
	case <-time.After(10 * time.Second):
		releaseOnce.Do(func() { close(releasePhaseB) })
		t.Fatalf("timed out waiting for worker Phase B")
	}

	// Simulate concurrent manual edit bumping v1 -> v2 mid-flight.
	etx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		releaseOnce.Do(func() { close(releasePhaseB) })
		t.Fatalf("begin manual edit tx: %v", err)
	}
	newBody := "Manual operator override: Timeout 5s"
	if _, err := storage.EditMemoryTx(ctx, etx, storage.EditMemoryParams{
		MemoryID: mem.MemoryID, WorkspaceID: "conflictws", ExpectedVersion: 1, Body: &newBody,
	}); err != nil {
		_ = etx.Rollback(ctx)
		releaseOnce.Do(func() { close(releasePhaseB) })
		t.Fatalf("manual edit tx: %v", err)
	}
	if err := etx.Commit(ctx); err != nil {
		releaseOnce.Do(func() { close(releasePhaseB) })
		t.Fatalf("commit manual edit tx: %v", err)
	}
	releaseOnce.Do(func() { close(releasePhaseB) })

	// Sibling commits cleanly AND manual v2 body stays intact (not overwritten by stale v1 mutation)!
	waitUntil(t, 10*time.Second, "sibling committed and conflicted item drained cleanly", func() (bool, error) {
		var siblingCount, pending int
		_ = store.CurationPool.QueryRow(ctx, `SELECT count(*) FROM memory_manifest WHERE workspace_id = 'conflictws' AND topic_key = 'sibling-note' AND status = 'active'`).Scan(&siblingCount)
		err := store.CurationPool.QueryRow(ctx, `SELECT count(*) FROM raw_inbox WHERE workspace_id = 'conflictws' AND status = 'pending'`).Scan(&pending)
		return siblingCount == 1 && pending == 0 && callNum.Load() >= 2, err
	})

	gotMem, err := storage.GetMemory(ctx, store.CurationPool, mem.MemoryID)
	if err != nil || gotMem.CurrentVersion != 2 || gotMem.Body != newBody {
		t.Fatalf("expected manual v2 preserved untouched, got %+v err=%v", gotMem, err)
	}
}
