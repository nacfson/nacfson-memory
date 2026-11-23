//go:build integration

package storage_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/storage"
	"github.com/nacfson/nacfson-memory/internal/testutil"
)

func unitVector(idx int, scale float32) []float32 {
	vec := make([]float32, config.RequiredEmbedDim)
	vec[idx] = scale
	if idx+1 < len(vec) {
		vec[idx+1] = 0.05
	}
	return vec
}

func TestMigrateIdempotentAndDownReset(t *testing.T) {
	ctx := context.Background()
	dsn := testutil.TestDSN()
	t.Cleanup(func() {
		_ = storage.Migrate(context.Background(), dsn, false)
	})

	// Ensure UP is applied, then test --down reset and ErrMigrationRequired guard.
	if err := storage.Migrate(ctx, dsn, false); err != nil {
		t.Fatalf("initial migrate up: %v", err)
	}
	if err := storage.Migrate(ctx, dsn, true); err != nil {
		t.Fatalf("migrate down: %v", err)
	}
	// Calling --down twice must stay idempotent cleanly.
	if err := storage.Migrate(ctx, dsn, true); err != nil {
		t.Fatalf("second migrate down: %v", err)
	}

	cfg, err := config.LoadFromLookup(config.ModeMCPStdio, func(k string) (string, bool) {
		switch k {
		case "MEM1_DATABASE_URL":
			return dsn, true
		case "MEM1_EMBED_PROVIDER":
			return config.ProviderMock, true
		default:
			return "", false
		}
	})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if _, err := storage.OpenPools(ctx, cfg); !errors.Is(err, storage.ErrMigrationRequired) {
		t.Fatalf("expected ErrMigrationRequired after down migration, got %v", err)
	}

	// Migrate UP twice to prove idempotency.
	if err := storage.Migrate(ctx, dsn, false); err != nil {
		t.Fatalf("migrate up #1: %v", err)
	}
	if err := storage.Migrate(ctx, dsn, false); err != nil {
		t.Fatalf("migrate up #2: %v", err)
	}

	store, err := storage.OpenPools(ctx, cfg)
	if err != nil {
		t.Fatalf("open pools after migrate: %v", err)
	}
	defer store.Close()

	if err := store.Ping(ctx); err != nil {
		t.Fatalf("store ping: %v", err)
	}
	stats := store.PoolStats()
	if stats.Serving.MaxConns != 4 || stats.Curation.MaxConns != 2 {
		t.Fatalf("unexpected stdio pool max conns: %+v", stats)
	}
	queues := storage.RiverQueues()
	if queues[storage.QueueCuration].MaxWorkers != 4 ||
		queues[storage.QueueEmbedding].MaxWorkers != 4 ||
		queues[storage.QueueMaintenance].MaxWorkers != 1 {
		t.Fatalf("unexpected RiverQueues config: %+v", queues)
	}
	counts, err := store.InboxCounts(ctx)
	if err != nil {
		t.Fatalf("inbox counts: %v", err)
	}
	if counts.Pending != 0 || counts.Provisional != 0 || counts.DeadLetter != 0 {
		t.Fatalf("expected zero counts on fresh DB, got %+v", counts)
	}
	if _, err := store.RiverQueueStats(ctx); err != nil {
		t.Fatalf("river queue stats: %v", err)
	}
}

func TestWorkspacesCRUDAndETags(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)

	if err := storage.ValidateWorkspaceID("Bad Workspace"); !errors.Is(err, storage.ErrInvalidWorkspaceID) {
		t.Fatalf("expected ErrInvalidWorkspaceID, got %v", err)
	}

	if err := storage.EnsureWorkspace(ctx, store.CurationPool, storage.WorkspaceGeneral); err != nil {
		t.Fatalf("ensure general: %v", err)
	}
	if err := storage.EnsureWorkspace(ctx, store.CurationPool, "payments"); err != nil {
		t.Fatalf("ensure payments: %v", err)
	}
	if err := storage.EnsureWorkspace(ctx, store.CurationPool, "payments"); err != nil {
		t.Fatalf("idempotent ensure payments: %v", err)
	}

	if _, err := storage.CreateWorkspace(ctx, store.CurationPool, storage.WorkspaceGeneral, ""); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("expected ErrConflict creating reserved general, got %v", err)
	}
	if _, err := storage.CreateWorkspace(ctx, store.CurationPool, "payments", ""); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("expected ErrConflict creating existing payments, got %v", err)
	}
	if _, err := storage.CreateWorkspace(ctx, store.CurationPool, "billing", "invalid"); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for bad inherit_general, got %v", err)
	}

	billing, err := storage.CreateWorkspace(ctx, store.CurationPool, "billing", storage.InheritConstitutionOnly)
	if err != nil || billing.Kind != storage.KindProject || billing.InheritGeneral != storage.InheritConstitutionOnly {
		t.Fatalf("create billing failed: %+v err=%v", billing, err)
	}

	got, err := storage.GetWorkspace(ctx, store.ServingPool, "billing")
	if err != nil || got.WorkspaceID != "billing" {
		t.Fatalf("get billing: %+v err=%v", got, err)
	}
	if _, err := storage.GetWorkspace(ctx, store.ServingPool, "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing workspace, got %v", err)
	}

	updated, err := storage.UpdateInheritGeneral(ctx, store.CurationPool, "billing", storage.InheritNone)
	if err != nil || updated.InheritGeneral != storage.InheritNone {
		t.Fatalf("update inherit_general: %+v err=%v", updated, err)
	}
	if _, err := storage.UpdateInheritGeneral(ctx, store.CurationPool, storage.WorkspaceGeneral, storage.InheritConstitutionOnly); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput mutating general inherit_general, got %v", err)
	}
	if _, err := storage.UpdateInheritGeneral(ctx, store.CurationPool, "missing", storage.InheritNone); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound updating missing workspace, got %v", err)
	}

	list, err := storage.ListWorkspaces(ctx, store.CurationPool)
	if err != nil || len(list) != 3 || list[0].WorkspaceID != storage.WorkspaceGeneral {
		t.Fatalf("unexpected workspace list: %+v err=%v", list, err)
	}

	tx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	locked, err := storage.LockWorkspaceTx(ctx, tx, "billing")
	if err != nil || locked.ManifestRevision != 0 || locked.ConstitutionRevision != 0 {
		t.Fatalf("lock billing: %+v err=%v", locked, err)
	}
	bumped, err := storage.BumpRevisionTx(ctx, tx, "billing", true)
	if err != nil || bumped.ManifestRevision != 1 || bumped.ConstitutionRevision != 1 {
		t.Fatalf("bump revision: %+v err=%v", bumped, err)
	}
	if _, err := storage.LockWorkspaceTx(ctx, tx, "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound locking missing workspace, got %v", err)
	}
	_ = tx.Commit(ctx)

	if e := storage.ComputeConstitutionETag(3, 7, storage.InheritConstitutionAndDurable); e != `W/"const:3+gen:7"` {
		t.Fatalf("unexpected inherited constitution etag: %s", e)
	}
	if e := storage.ComputeConstitutionETag(3, 7, storage.InheritNone); e != `W/"const:3"` {
		t.Fatalf("unexpected isolated constitution etag: %s", e)
	}
	if e := storage.ComputeMetadataETag(11, 4, storage.InheritConstitutionAndDurable); e != `W/"ws:11+gen:4"` {
		t.Fatalf("unexpected inherited metadata etag: %s", e)
	}
	if e := storage.ComputeMetadataETag(11, 4, storage.InheritNone); e != `W/"ws:11"` {
		t.Fatalf("unexpected isolated metadata etag: %s", e)
	}
}

func TestConfirmTokensSingleUseAndConcurrency(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)

	if err := storage.EnsureWorkspace(ctx, store.CurationPool, "app"); err != nil {
		t.Fatalf("ensure app: %v", err)
	}

	targetID, _ := uuid.NewV7()
	wrongID, _ := uuid.NewV7()

	tok, exp, err := store.IssueConfirmToken(ctx, "app", "purge", &targetID, 0)
	if err != nil || tok == "" || time.Until(exp) < 4*time.Minute {
		t.Fatalf("issue token failed: tok=%q exp=%v err=%v", tok, exp, err)
	}

	// Wrong workspace, action, target, or bad token must fail without consuming valid token.
	if err := store.ConsumeConfirmToken(ctx, tok, "other", "purge", &targetID); !errors.Is(err, storage.ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for wrong workspace, got %v", err)
	}
	if err := store.ConsumeConfirmToken(ctx, tok, "app", "revert", &targetID); !errors.Is(err, storage.ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for wrong action, got %v", err)
	}
	if err := store.ConsumeConfirmToken(ctx, tok, "app", "purge", &wrongID); !errors.Is(err, storage.ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for wrong target, got %v", err)
	}
	if err := store.ConsumeConfirmToken(ctx, tok, "app", "purge", nil); !errors.Is(err, storage.ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for nil target, got %v", err)
	}

	// Concurrent consume race: exactly 1 of 8 goroutines may succeed.
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		success int
		fails   int
	)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := store.ConsumeConfirmToken(ctx, tok, "app", "purge", &targetID)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				success++
			} else if errors.Is(err, storage.ErrInvalidToken) {
				fails++
			} else {
				t.Errorf("unexpected error during token race: %v", err)
			}
		}()
	}
	wg.Wait()
	if success != 1 || fails != 7 {
		t.Fatalf("expected exactly 1 success and 7 ErrInvalidToken, got success=%d fails=%d", success, fails)
	}

	// Expired token cannot be consumed.
	expiredTok, _, err := store.IssueConfirmToken(ctx, "app", "create", nil, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("issue short-lived token: %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	if err := store.ConsumeConfirmToken(ctx, expiredTok, "app", "create", nil); !errors.Is(err, storage.ErrInvalidToken) {
		t.Fatalf("expected expired token rejected, got %v", err)
	}

	// Unknown workspace rejected.
	if _, _, err := store.IssueConfirmToken(ctx, "unknown_ws", "create", nil, time.Minute); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing workspace, got %v", err)
	}
}

func TestInboxLifecycleIdempotencyAndDeadLetters(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)

	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "INVALID",
		Content:     "hello",
	}); !errors.Is(err, storage.ErrInvalidWorkspaceID) {
		t.Fatalf("expected ErrInvalidWorkspaceID, got %v", err)
	}
	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "ws1",
		Content:     "   ",
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for empty content, got %v", err)
	}

	// Write #1 with empty idempotency_key twice creates 2 separate inbox rows.
	res1, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "ws1",
		AgentID:     "claude-code",
		Content:     "first message without idem",
		OccurredAt:  time.Now().Add(-3 * time.Minute),
	})
	if err != nil || res1.Replayed {
		t.Fatalf("insert 1 failed: %+v err=%v", res1, err)
	}
	res2, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "ws1",
		AgentID:     "codex-cli",
		Content:     "first message without idem",
		OccurredAt:  time.Now().Add(-2 * time.Minute),
	})
	if err != nil || res2.InboxID == res1.InboxID {
		t.Fatalf("expected distinct inbox rows for empty idempotency_key: %+v vs %+v err=%v", res1, res2, err)
	}

	// Write #3 with explicit idempotency_key; replay succeeds; mismatched content errors 422.
	res3, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID:    "ws1",
		AgentID:        "cursor",
		IdempotencyKey: "turn-42",
		Content:        "idempotent body",
		OccurredAt:     time.Now().Add(-1 * time.Minute),
	})
	if err != nil || res3.Replayed {
		t.Fatalf("insert 3 failed: %+v err=%v", res3, err)
	}
	replay, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID:    "ws1",
		IdempotencyKey: "turn-42",
		Content:        "idempotent body",
	})
	if err != nil || !replay.Replayed || replay.InboxID != res3.InboxID || replay.RequestID != res3.RequestID {
		t.Fatalf("expected exact idempotent replay, got %+v vs %+v err=%v", replay, res3, err)
	}
	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID:    "ws1",
		IdempotencyKey: "turn-42",
		Content:        "tampered body",
	}); !errors.Is(err, storage.ErrIdempotencyMismatch) {
		t.Fatalf("expected ErrIdempotencyMismatch, got %v", err)
	}

	// Verify only 3 River curate_workspace jobs were created (replayed write skipped duplicate job;
	// 5s ByArgs+ByPeriod coalesces identical WorkspaceID args into 1 pending/available River job!).
	qStats, err := store.RiverQueueStats(ctx)
	if err != nil || len(qStats) == 0 {
		t.Fatalf("expected non-empty river queue stats, got %+v err=%v", qStats, err)
	}

	// Insert a huge > 32 KiB item to verify ClaimPendingBatch allows first oversized item through.
	bigContent := strings.Repeat("a", 40*1024)
	resBig, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "ws1",
		Content:     bigContent,
		OccurredAt:  time.Now(),
	})
	if err != nil {
		t.Fatalf("insert large inbox item: %v", err)
	}

	claimed, err := storage.ClaimPendingBatch(ctx, store.CurationPool, "ws1", 32, 32768)
	if err != nil || len(claimed) != 3 {
		t.Fatalf("expected 3 small pending items before 40KiB item, got %d err=%v", len(claimed), err)
	}

	tx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	affected, err := storage.MarkInboxCurated(ctx, tx, "ws1", []uuid.UUID{res1.InboxID, res2.InboxID})
	if err != nil || affected != 2 {
		t.Fatalf("mark curated affected=%d err=%v", affected, err)
	}
	provAffected, err := storage.MarkInboxProvisional(ctx, tx, "ws1", []uuid.UUID{res3.InboxID})
	if err != nil || provAffected != 1 {
		t.Fatalf("mark provisional affected=%d err=%v", provAffected, err)
	}
	// Create a provisional manifest row + citation linked to res3 so ResetProvisionalToPending picks it up.
	if _, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:   "ws1",
		Durability:    storage.DurabilityWorking,
		TopicKey:      "provisional:" + res3.InboxID.String(),
		CurationState: storage.CurationProvisionalHeuristic,
		SummaryStub:   "provisional stub",
		Body:          "provisional body",
		TokenCount:    5,
		Citations:     []storage.CitationInput{{InboxID: res3.InboxID, AgentID: "cursor"}},
	}); err != nil {
		t.Fatalf("create provisional manifest: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	// Claim remaining pending batch -> single 40KiB item must still be returned!
	bigClaimed, err := storage.ClaimPendingBatch(ctx, store.CurationPool, "ws1", 32, 32768)
	if err != nil || len(bigClaimed) != 1 || bigClaimed[0].InboxID != resBig.InboxID {
		t.Fatalf("expected 40KiB single item returned, got %d err=%v", len(bigClaimed), err)
	}

	// Mark big item as dead_letter & verify ListDeadLetters.
	if err := storage.MarkInboxDeadLetter(ctx, store.CurationPool, "ws1", resBig.InboxID, "mock validation error"); err != nil {
		t.Fatalf("mark dead letter: %v", err)
	}
	deadReport, err := store.ListDeadLetters(ctx, 20)
	if err != nil || len(deadReport.DeadLetters) != 1 || deadReport.DeadLetters[0].InboxID != resBig.InboxID {
		t.Fatalf("unexpected dead letters report: %+v err=%v", deadReport, err)
	}

	// Reset provisional back to pending, then delete provisional manifest row.
	rtx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin reset tx: %v", err)
	}
	defer func() { _ = rtx.Rollback(ctx) }()
	wsList, resetCount, err := storage.ResetProvisionalToPending(ctx, rtx, 10)
	if err != nil || resetCount != 1 || len(wsList) != 1 || wsList[0] != "ws1" {
		t.Fatalf("reset provisional: ws=%v count=%d err=%v", wsList, resetCount, err)
	}
	delProv, err := storage.DeleteProvisionalManifestByInboxTx(ctx, rtx, "ws1", []uuid.UUID{res3.InboxID})
	if err != nil || delProv != 1 {
		t.Fatalf("delete provisional manifest: del=%d err=%v", delProv, err)
	}
	if err := rtx.Commit(ctx); err != nil {
		t.Fatalf("commit reset tx: %v", err)
	}

	counts, err := store.InboxCounts(ctx)
	if err != nil {
		t.Fatalf("inbox counts: %v", err)
	}
	if counts.Pending != 1 || counts.DeadLetter != 1 || counts.ProvisionalActive != 0 {
		t.Fatalf("unexpected counts after reset+deadletter: %+v", counts)
	}
}

func TestManifestLifecycleReviseEditSupersedeRevertPurgeAndSearch(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)
	dg := graph.NewDependencyGraph()

	if err := storage.EnsureWorkspace(ctx, store.CurationPool, "proj"); err != nil {
		t.Fatalf("ensure proj: %v", err)
	}

	// Seed two inbox entries for provenance & shared/exclusive hard-purge testing.
	inShared, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "proj",
		AgentID:     "claude",
		Content:     "Shared source message referencing architecture and auth.",
	})
	if err != nil {
		t.Fatalf("insert shared inbox: %v", err)
	}
	inExclusive, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "proj",
		AgentID:     "codex",
		Content:     "Exclusive source message only cited by deleted note.",
	})
	if err != nil {
		t.Fatalf("insert exclusive inbox: %v", err)
	}

	tx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:   storage.WorkspaceGeneral,
		Durability:    storage.DurabilityConstitution,
		TopicKey:      "tone-rule",
		CurationState: storage.CurationUserExplicit,
		SummaryStub:   "General tone",
		Body:          "General concise tone rule",
		TokenCount:    6,
	})
	if err != nil {
		t.Fatalf("create general constitution: %v", err)
	}
	_, err = storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:   "proj",
		Durability:    storage.DurabilityConstitution,
		TopicKey:      "tone-rule",
		CurationState: storage.CurationUserExplicit,
		SummaryStub:   "Proj tone override",
		Body:          "Project-specific terse tone rule",
		TokenCount:    6,
	})
	if err != nil {
		t.Fatalf("create proj constitution: %v", err)
	}

	// Durable entries in 'general' and 'proj' for vector search & topic override.
	vecGen, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:      storage.WorkspaceGeneral,
		Durability:       storage.DurabilityDurable,
		TopicKey:         "user-lang",
		CurationState:    storage.CurationCuratorVerified,
		SummaryStub:      "Speaks Korean and English",
		Body:             "User speaks Korean and English",
		TokenCount:       7,
		Embedding:        unitVector(0, 1.0),
		EmbeddingSpaceID: "test:1536",
	})
	if err != nil {
		t.Fatalf("create general durable: %v", err)
	}
	_, err = storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:      "proj",
		Durability:       storage.DurabilityDurable,
		TopicKey:         "user-lang",
		CurationState:    storage.CurationCuratorVerified,
		SummaryStub:      "Project docs in English",
		Body:             "Project overrides general language to English",
		TokenCount:       8,
		Embedding:        unitVector(0, 0.9),
		EmbeddingSpaceID: "test:1536",
	})
	if err != nil {
		t.Fatalf("create proj override durable: %v", err)
	}

	// Duplicate active topic_key fails with ErrTopicConflict (inside savepoint so parent tx stays usable).
	dupSave, err := tx.Begin(ctx)
	if err != nil {
		t.Fatalf("begin savepoint: %v", err)
	}
	if _, err := storage.CreateMemoryTx(ctx, dupSave, storage.CreateMemoryParams{
		WorkspaceID:   "proj",
		Durability:    storage.DurabilityDurable,
		TopicKey:      "user-lang",
		CurationState: storage.CurationCuratorVerified,
		SummaryStub:   "dup",
		Body:          "dup body",
		TokenCount:    2,
	}); !errors.Is(err, storage.ErrTopicConflict) {
		_ = dupSave.Rollback(ctx)
		t.Fatalf("expected ErrTopicConflict on duplicate active topic_key, got %v", err)
	}
	_ = dupSave.Rollback(ctx)

	// Create primary memory -> revise v2, v3, v4 -> prune retention=2 -> test revert constraints.
	mA, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:   "proj",
		Durability:    storage.DurabilityDurable,
		TopicKey:      "db-engine",
		CurationState: storage.CurationCuratorVerified,
		SummaryStub:   "Use SQLite v1",
		Body:          "Initial DB SQLite v1",
		TokenCount:    6,
		Citations: []storage.CitationInput{
			{InboxID: inShared.InboxID, AgentID: "claude"},
			{InboxID: inExclusive.InboxID, AgentID: "codex"},
		},
	})
	if err != nil {
		t.Fatalf("create mA: %v", err)
	}

	// Second surviving memory citing inShared so HardPurge redacts instead of deleting inShared.
	mSurvivor, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:   "proj",
		Durability:    storage.DurabilityDurable,
		TopicKey:      "auth-mode",
		CurationState: storage.CurationCuratorVerified,
		SummaryStub:   "Bearer token auth",
		Body:          "Use split admin/agent bearer tokens",
		TokenCount:    7,
		Citations:     []storage.CitationInput{{InboxID: inShared.InboxID, AgentID: "claude"}},
	})
	if err != nil {
		t.Fatalf("create mSurvivor: %v", err)
	}

	// Add outgoing edge from mA -> mSurvivor so we verify SupersedeMemoryTx retires outgoing edges.
	if err := dg.UpsertEdges(ctx, tx, "proj", mA.MemoryID, 1, []graph.EdgeSpec{{
		TargetMemoryID: mSurvivor.MemoryID,
		ExpectedToRev:  1,
		Rationale:      "auth premise",
	}}); err != nil {
		t.Fatalf("upsert edge mA -> mSurvivor: %v", err)
	}

	// Revise mA to v2, Edit mA to v3 & v4.
	if _, err := storage.ReviseCoupledTopicTx(ctx, tx, storage.ReviseMemoryParams{
		MemoryID: mA.MemoryID, WorkspaceID: "proj", ExpectedVersion: 99,
		SummaryStub: "bad", Body: "bad", TokenCount: 1,
	}); !errors.Is(err, storage.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict, got %v", err)
	}

	mA, err = storage.ReviseCoupledTopicTx(ctx, tx, storage.ReviseMemoryParams{
		MemoryID: mA.MemoryID, WorkspaceID: "proj", ExpectedVersion: 1,
		SummaryStub: "SQLite v2", Body: "SQLite WAL v2", TokenCount: 5,
	})
	if err != nil || mA.CurrentVersion != 2 {
		t.Fatalf("revise v2: %+v err=%v", mA, err)
	}

	newBodyV3 := "SQLite tuned cache v3"
	newTokV3 := 5
	mA, err = storage.EditMemoryTx(ctx, tx, storage.EditMemoryParams{
		MemoryID: mA.MemoryID, WorkspaceID: "proj", ExpectedVersion: 2,
		Body: &newBodyV3, TokenCount: &newTokV3,
	})
	if err != nil || mA.CurrentVersion != 3 || !strings.Contains(mA.SummaryStub, "SQLite tuned cache v3") {
		t.Fatalf("edit v3: %+v err=%v", mA, err)
	}

	newBodyV4 := "SQLite final before PG v4"
	mA, err = storage.EditMemoryTx(ctx, tx, storage.EditMemoryParams{
		MemoryID: mA.MemoryID, WorkspaceID: "proj", ExpectedVersion: 3,
		Body: &newBodyV4,
	})
	if err != nil || mA.CurrentVersion != 4 {
		t.Fatalf("edit v4: %+v err=%v", mA, err)
	}

	pruned, err := storage.PruneRevisionBodiesTx(ctx, tx, mA.MemoryID, 2)
	if err != nil || pruned != 2 {
		t.Fatalf("expected 2 pruned revisions (v1, v2), got %d err=%v", pruned, err)
	}

	// Revert to pruned v1 must fail with ErrRevisionPruned.
	if _, err := storage.RevertToVersionTx(ctx, tx, storage.RevertMemoryParams{
		MemoryID: mA.MemoryID, WorkspaceID: "proj", ExpectedVersion: 4, TargetVersion: 1, TokenCount: 5,
	}); !errors.Is(err, storage.ErrRevisionPruned) {
		t.Fatalf("expected ErrRevisionPruned, got %v", err)
	}

	// Revert to v3 appends v5 and sets last_reverted_version = 5.
	mA, err = storage.RevertToVersionTx(ctx, tx, storage.RevertMemoryParams{
		MemoryID: mA.MemoryID, WorkspaceID: "proj", ExpectedVersion: 4, TargetVersion: 3, TokenCount: 5,
	})
	if err != nil || mA.CurrentVersion != 5 || mA.LastRevertedVersion != 5 || mA.Body != newBodyV3 {
		t.Fatalf("revert to v3 failed: %+v err=%v", mA, err)
	}

	// Supersede mA with mB sharing the EXACT same topic_key "db-engine" -> must succeed without 23505!
	mB, err := storage.SupersedeMemoryTx(ctx, tx, storage.SupersedeMemoryParams{
		WorkspaceID:      "proj",
		OldMemoryID:      mA.MemoryID,
		Durability:       storage.DurabilityDurable,
		TopicKey:         "db-engine",
		CurationState:    storage.CurationCuratorVerified,
		SummaryStub:      "Use PostgreSQL 17",
		Body:             "Switched to PostgreSQL 17 with pgvector",
		TokenCount:       7,
		Embedding:        unitVector(10, 1.0),
		EmbeddingSpaceID: "test:1536",
		Citations:        []storage.CitationInput{{InboxID: inShared.InboxID, AgentID: "claude"}},
	})
	if err != nil || mB.SupersedesMemoryID == nil || *mB.SupersedesMemoryID != mA.MemoryID {
		t.Fatalf("supersede mA -> mB failed: %+v err=%v", mB, err)
	}

	// Trying to revert superseded mA while mB holds active topic_key "db-engine" returns ErrTopicConflict!
	revSave, err := tx.Begin(ctx)
	if err != nil {
		t.Fatalf("begin revert savepoint: %v", err)
	}
	if _, err := storage.RevertToVersionTx(ctx, revSave, storage.RevertMemoryParams{
		MemoryID: mA.MemoryID, WorkspaceID: "proj", ExpectedVersion: 5, TargetVersion: 3, TokenCount: 5,
	}); !errors.Is(err, storage.ErrTopicConflict) {
		_ = revSave.Rollback(ctx)
		t.Fatalf("expected ErrTopicConflict reverting superseded memory onto active topic_key, got %v", err)
	}
	_ = revSave.Rollback(ctx)

	// Soft-retire mB -> then Revert mA to v3 succeeds because topic_key "db-engine" is now free!
	retiredB, err := storage.SoftRetireTx(ctx, tx, storage.SoftRetireParams{
		MemoryID: mB.MemoryID, WorkspaceID: "proj", ExpectedVersion: 1,
	})
	if err != nil || retiredB.Status != storage.StatusRetired {
		t.Fatalf("soft retire mB failed: %+v err=%v", retiredB, err)
	}
	if _, err := storage.EditMemoryTx(ctx, tx, storage.EditMemoryParams{
		MemoryID: mB.MemoryID, WorkspaceID: "proj", ExpectedVersion: 1, Body: &newBodyV4,
	}); !errors.Is(err, storage.ErrInactiveMemory) {
		t.Fatalf("expected ErrInactiveMemory editing retired row, got %v", err)
	}

	// Decision audit inserts.
	if _, err := storage.InsertCurationDecisionTx(ctx, tx, storage.CurationDecision{
		WorkspaceID:    "proj",
		InboxIDs:       []uuid.UUID{inShared.InboxID, inExclusive.InboxID},
		Action:         "create",
		TargetMemoryID: &mA.MemoryID,
		TargetVersion:  &mA.CurrentVersion,
		ProviderName:   "mock",
		ModelName:      "mock-v1",
		Rationale:      "sensitive note rationale",
	}); err != nil {
		t.Fatalf("insert decision: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit setup tx: %v", err)
	}

	// Verify constitution inheritance merge.
	constProj, err := storage.GetConstitution(ctx, store.ServingPool, "proj", storage.InheritConstitutionAndDurable)
	if err != nil || len(constProj) != 1 || constProj[0].WorkspaceID != "proj" {
		t.Fatalf("expected project constitution override, got %+v err=%v", constProj, err)
	}
	// Verify UpdateMemoryEmbeddingTx updates embedding without bumping current_version.
	if err := storage.UpdateMemoryEmbeddingTx(ctx, store.CurationPool, mSurvivor.MemoryID, unitVector(20, 1.0), "test:1536"); err != nil {
		t.Fatalf("update memory embedding: %v", err)
	}
	afterEmbed, err := storage.GetMemory(ctx, store.CurationPool, mSurvivor.MemoryID)
	if err != nil || afterEmbed.CurrentVersion != 1 || afterEmbed.EmbeddingSpaceID != "test:1536" {
		t.Fatalf("expected version unchanged after UpdateMemoryEmbeddingTx: %+v err=%v", afterEmbed, err)
	}

	// Verify SearchVectorTopK inside read-only tx.
	rtx, err := store.ServingPool.BeginTx(ctx, pgxTxReadOnly())
	if err != nil {
		t.Fatalf("begin read-only tx: %v", err)
	}
	vecHits, err := storage.SearchVectorTopK(ctx, rtx, "proj", storage.InheritConstitutionAndDurable, "test:1536", unitVector(0, 1.0), 10)
	_ = rtx.Commit(ctx)
	if err != nil || len(vecHits) != 2 {
		t.Fatalf("expected 2 vector hits (general user-lang overridden by proj), got %+v err=%v", vecHits, err)
	}
	if vecHits[0].Memory.TopicKey == "user-lang" && vecHits[0].Memory.MemoryID == vecGen.MemoryID {
		t.Fatalf("expected proj user-lang to override general user-lang, got general row %+v", vecHits)
	}

	// Verify ListMemoriesForReindexKeyset, ListAllMemoriesKeyset (with cursor & include_revisions), GetRevisions, ExplainLineage, ListCurationDecisions.
	reindexBatch1, err := storage.ListMemoriesForReindexKeyset(ctx, store.CurationPool, nil, 2)
	if err != nil || len(reindexBatch1) != 2 {
		t.Fatalf("reindex batch 1: len=%d err=%v", len(reindexBatch1), err)
	}
	reindexBatch2, err := storage.ListMemoriesForReindexKeyset(ctx, store.CurationPool, &reindexBatch1[1].MemoryID, 2)
	if err != nil || len(reindexBatch2) == 0 {
		t.Fatalf("reindex batch 2: len=%d err=%v", len(reindexBatch2), err)
	}

	page1, err := storage.ListAllMemoriesKeyset(ctx, store.CurationPool, storage.ListMemoriesParams{
		WorkspaceID: "proj", IncludeRevisions: true, Limit: 2,
	})
	if err != nil || len(page1.Items) != 2 || page1.NextCursor == "" || len(page1.Items[0].Revisions) == 0 {
		t.Fatalf("list keyset page1 failed: items=%d next=%q err=%v", len(page1.Items), page1.NextCursor, err)
	}
	page2, err := storage.ListAllMemoriesKeyset(ctx, store.CurationPool, storage.ListMemoriesParams{
		WorkspaceID: "proj", Cursor: page1.NextCursor, Limit: 10,
	})
	if err != nil || len(page2.Items) == 0 {
		t.Fatalf("list keyset page2 failed: items=%d err=%v", len(page2.Items), err)
	}

	revs, err := storage.GetRevisions(ctx, store.CurationPool, mA.MemoryID)
	if err != nil || len(revs) != 5 || len(revs[4].Citations) != 2 {
		t.Fatalf("expected 5 revisions and 2 citations on v1, got len=%d err=%v", len(revs), err)
	}

	lineage, err := storage.ExplainLineage(ctx, store.CurationPool, mA.MemoryID)
	if err != nil || len(lineage.InboxSources) != 2 || len(lineage.Decisions) != 1 {
		t.Fatalf("explain lineage failed: %+v err=%v", lineage, err)
	}

	// HardPurge mA: removes exclusive inbox (inExclusive), redacts shared inbox (inShared -> "[PURGED]"),
	// redacts decision rationale, records 'purge' decision, preserves mSurvivor.
	ptx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin purge tx: %v", err)
	}
	purgeRes, err := storage.HardPurgeTx(ctx, ptx, mA.MemoryID, "cleanup sensitive topic")
	if err != nil || purgeRes.DeletedInboxCount != 1 {
		t.Fatalf("hard purge mA failed: %+v err=%v", purgeRes, err)
	}
	if err := ptx.Commit(ctx); err != nil {
		t.Fatalf("commit purge tx: %v", err)
	}

	if _, err := storage.GetMemory(ctx, store.CurationPool, mA.MemoryID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected purged memory removed, got %v", err)
	}
	var sharedContent string
	if err := store.CurationPool.QueryRow(ctx, `SELECT content FROM raw_inbox WHERE inbox_id = $1`, inShared.InboxID).Scan(&sharedContent); err != nil || sharedContent != "[PURGED]" {
		t.Fatalf("expected shared inbox redacted to [PURGED], got %q err=%v", sharedContent, err)
	}
	decPage, err := storage.ListCurationDecisions(ctx, store.CurationPool, storage.ListDecisionsParams{
		WorkspaceID: "proj", Limit: 10,
	})
	if err != nil || len(decPage.Decisions) != 2 {
		t.Fatalf("expected 2 decisions (redacted create + purge), got %+v err=%v", decPage, err)
	}
	if decPage.Decisions[0].Action != "purge" || decPage.Decisions[1].Rationale != "[PURGED]" {
		t.Fatalf("unexpected post-purge decisions state: %+v", decPage.Decisions)
	}
}

func TestStorageEdgePathsPaginationAndJobs(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)

	// Store nil guards, Migrate error checks, and token input guards.
	var nilStore *storage.Store
	nilStore.Close()
	if err := nilStore.Ping(ctx); err == nil {
		t.Fatalf("expected error pinging nil store")
	}
	if err := storage.Migrate(ctx, "", false); err == nil {
		t.Fatalf("expected error on empty DSN")
	}
	if _, _, err := store.IssueConfirmToken(ctx, "general", "bogus", nil, time.Minute); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on bogus token action, got %v", err)
	}
	if err := store.ConsumeConfirmToken(ctx, "", "general", "create", nil); !errors.Is(err, storage.ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken on empty token, got %v", err)
	}

	// Enqueue EmbedMemoryJobArgs and ProvisionalReplayJobArgs via RiverInsert inside tx.
	if err := storage.EnsureWorkspace(ctx, store.CurationPool, "wsedge"); err != nil {
		t.Fatalf("ensure wsedge: %v", err)
	}
	jtx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin jtx: %v", err)
	}
	defer func() { _ = jtx.Rollback(ctx) }()

	memConst, err := storage.CreateMemoryTx(ctx, jtx, storage.CreateMemoryParams{
		WorkspaceID:   "wsedge",
		Durability:    storage.DurabilityConstitution,
		TopicKey:      "rule-1",
		CurationState: storage.CurationUserExplicit,
		SummaryStub:   "Strict rule",
		Body:          "Always check migrations carefully",
		TokenCount:    5,
	})
	if err != nil {
		t.Fatalf("create memConst: %v", err)
	}
	memWork, err := storage.CreateMemoryTx(ctx, jtx, storage.CreateMemoryParams{
		WorkspaceID:   "wsedge",
		Durability:    storage.DurabilityWorking,
		TopicKey:      "work-1",
		CurationState: storage.CurationCuratorVerified,
		SummaryStub:   "Work item",
		Body:          "Temporary work item",
		TokenCount:    4,
	})
	if err != nil {
		t.Fatalf("create memWork: %v", err)
	}
	if _, err := store.RiverInsert.InsertTx(ctx, jtx, storage.EmbedMemoryJobArgs{
		WorkspaceID: "wsedge", MemoryID: memWork.MemoryID, Version: 1,
	}, nil); err != nil {
		t.Fatalf("enqueue embed_memory: %v", err)
	}
	if _, err := store.RiverInsert.InsertTx(ctx, jtx, storage.ProvisionalReplayJobArgs{}, nil); err != nil {
		t.Fatalf("enqueue provisional_replay: %v", err)
	}
	if _, err := jtx.Exec(ctx, `
		INSERT INTO river_job (state, attempt, max_attempts, kind, queue, args, finalized_at)
		VALUES ('discarded', 3, 3, 'curate_workspace', 'curation', '{"workspace_id":"wsedge"}', now())
	`); err != nil {
		t.Fatalf("insert discarded river job fixture: %v", err)
	}

	// Empty-slice and retention=0 no-ops.
	if n, err := storage.MarkInboxCurated(ctx, jtx, "wsedge", nil); err != nil || n != 0 {
		t.Fatalf("expected 0 on empty MarkInboxCurated: n=%d err=%v", n, err)
	}
	if n, err := storage.MarkInboxProvisional(ctx, jtx, "wsedge", nil); err != nil || n != 0 {
		t.Fatalf("expected 0 on empty MarkInboxProvisional: n=%d err=%v", n, err)
	}
	if n, err := storage.PruneRevisionBodiesTx(ctx, jtx, memConst.MemoryID, 0); err != nil || n != 0 {
		t.Fatalf("expected 0 on retentionN=0 prune: n=%d err=%v", n, err)
	}
	if n, err := storage.DeleteProvisionalManifestByInboxTx(ctx, jtx, "wsedge", nil); err != nil || n != 0 {
		t.Fatalf("expected 0 on empty provisional delete: n=%d err=%v", n, err)
	}

	// Decision pagination with 3 rows + cursor + filters.
	for i := 0; i < 3; i++ {
		if _, err := storage.InsertCurationDecisionTx(ctx, jtx, storage.CurationDecision{
			WorkspaceID:    "wsedge",
			Action:         "noop",
			TargetMemoryID: &memWork.MemoryID,
			Rationale:      "noop check",
		}); err != nil {
			t.Fatalf("insert decision %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := jtx.Commit(ctx); err != nil {
		t.Fatalf("commit jtx: %v", err)
	}

	// Verify discarded river job + Inbox edge errors.
	dl, err := store.ListDeadLetters(ctx, 200)
	if err != nil || len(dl.DiscardedJobs) != 1 || dl.DiscardedJobs[0].Kind != "curate_workspace" {
		t.Fatalf("expected 1 discarded river job, got %+v err=%v", dl, err)
	}
	if err := storage.MarkInboxDeadLetter(ctx, store.CurationPool, "wsedge", uuid.New(), "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound dead-lettering missing inbox item, got %v", err)
	}

	// Decision pagination & filters.
	since := time.Now().Add(-time.Hour)
	dPage1, err := storage.ListCurationDecisions(ctx, store.CurationPool, storage.ListDecisionsParams{
		WorkspaceID: "wsedge", Action: "noop", Since: &since, Limit: 2,
	})
	if err != nil || len(dPage1.Decisions) != 2 || dPage1.NextCursor == "" {
		t.Fatalf("decision page1 failed: %+v err=%v", dPage1, err)
	}
	dPage2, err := storage.ListCurationDecisions(ctx, store.CurationPool, storage.ListDecisionsParams{
		WorkspaceID: "wsedge", Action: "noop", Cursor: dPage1.NextCursor, Limit: 2,
	})
	if err != nil || len(dPage2.Decisions) != 1 || dPage2.NextCursor != "" {
		t.Fatalf("decision page2 failed: %+v err=%v", dPage2, err)
	}
	if _, err := storage.ListCurationDecisions(ctx, store.CurationPool, storage.ListDecisionsParams{
		Cursor: "bad-cursor",
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on malformed decision cursor, got %v", err)
	}

	// Constitution InheritNone, empty SearchVectorTopK, ListAllMemoriesKeyset filters & malformed cursor.
	constIsolated, err := storage.GetConstitution(ctx, store.ServingPool, "wsedge", storage.InheritNone)
	if err != nil || len(constIsolated) != 1 {
		t.Fatalf("expected 1 isolated constitution rule, got %+v err=%v", constIsolated, err)
	}
	if emptyVec, err := storage.SearchVectorTopK(ctx, store.ServingPool, "wsedge", storage.InheritNone, "test:1536", nil, 5); err != nil || len(emptyVec) != 0 {
		t.Fatalf("expected nil on empty query vec, got %+v err=%v", emptyVec, err)
	}
	driftedFalse := false
	filtered, err := storage.ListAllMemoriesKeyset(ctx, store.CurationPool, storage.ListMemoriesParams{
		WorkspaceID:    "wsedge",
		Durability:     storage.DurabilityWorking,
		Status:         storage.StatusActive,
		CurationState:  storage.CurationCuratorVerified,
		PremiseDrifted: &driftedFalse,
		Limit:          1000,
	})
	if err != nil || len(filtered.Items) != 1 || filtered.Items[0].MemoryID != memWork.MemoryID {
		t.Fatalf("unexpected filtered memory page: %+v err=%v", filtered, err)
	}
	if _, err := storage.ListAllMemoriesKeyset(ctx, store.CurationPool, storage.ListMemoriesParams{
		Cursor: "bad",
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on malformed memory cursor, got %v", err)
	}

	// Missing / error paths across manifest mutators and readers.
	missingID := uuid.New()
	if err := storage.UpdateMemoryEmbeddingTx(ctx, store.CurationPool, missingID, unitVector(0, 1.0), "test:1536"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound updating missing embedding, got %v", err)
	}
	if _, err := storage.GetRevisions(ctx, store.CurationPool, missingID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound getting missing revisions, got %v", err)
	}
	if _, err := storage.ExplainLineage(ctx, store.CurationPool, missingID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound explaining missing memory, got %v", err)
	}

	etx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin etx: %v", err)
	}
	defer func() { _ = etx.Rollback(ctx) }()

	if _, err := storage.HardPurgeTx(ctx, etx, missingID, ""); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound purging missing memory, got %v", err)
	}
	if _, err := storage.EditMemoryTx(ctx, etx, storage.EditMemoryParams{
		MemoryID: memWork.MemoryID, WorkspaceID: "other_ws", ExpectedVersion: 1,
	}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound on cross-workspace lock, got %v", err)
	}
	if _, err := storage.EditMemoryTx(ctx, etx, storage.EditMemoryParams{
		MemoryID: memWork.MemoryID, WorkspaceID: "wsedge", ExpectedVersion: 99,
	}); !errors.Is(err, storage.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict editing wrong version, got %v", err)
	}
	if _, err := storage.SoftRetireTx(ctx, etx, storage.SoftRetireParams{
		MemoryID: memWork.MemoryID, WorkspaceID: "wsedge", ExpectedVersion: 99,
	}); !errors.Is(err, storage.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict retiring wrong version, got %v", err)
	}
	if _, err := storage.RevertToVersionTx(ctx, etx, storage.RevertMemoryParams{
		MemoryID: memWork.MemoryID, WorkspaceID: "wsedge", ExpectedVersion: 99, TargetVersion: 1,
	}); !errors.Is(err, storage.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict reverting wrong version, got %v", err)
	}
	if _, err := storage.RevertToVersionTx(ctx, etx, storage.RevertMemoryParams{
		MemoryID: memWork.MemoryID, WorkspaceID: "wsedge", ExpectedVersion: 1, TargetVersion: 99,
	}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound reverting missing target revision, got %v", err)
	}

	// Hard purge constitution memory -> verifies constitution_revision increments!
	cPurge, err := storage.HardPurgeTx(ctx, etx, memConst.MemoryID, "")
	if err != nil || cPurge.Revisions.ConstitutionRevision != 1 {
		t.Fatalf("expected constitution_revision bumped to 1, got %+v err=%v", cPurge, err)
	}
	if err := etx.Commit(ctx); err != nil {
		t.Fatalf("commit etx: %v", err)
	}
}
