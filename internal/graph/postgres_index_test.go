//go:build integration

package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/storage"
	"github.com/nacfson/nacfson-memory/internal/testutil"
)

func TestDependencyGraphLifecycleAndDrift(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)
	dg := graph.NewDependencyGraph()

	if err := storage.EnsureWorkspace(ctx, store.CurationPool, "proj"); err != nil {
		t.Fatalf("ensure proj: %v", err)
	}
	if err := storage.EnsureWorkspace(ctx, store.CurationPool, "other"); err != nil {
		t.Fatalf("ensure other: %v", err)
	}

	tx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	memA, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: "proj", Durability: storage.DurabilityDurable, TopicKey: "primary-db",
		CurationState: storage.CurationCuratorVerified, SummaryStub: "Use SQLite v1", Body: "Use SQLite", TokenCount: 3,
	})
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	memB, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: "proj", Durability: storage.DurabilityWorking, TopicKey: "wal-pragma",
		CurationState: storage.CurationCuratorVerified, SummaryStub: "Set SQLite WAL", Body: "PRAGMA journal_mode=WAL", TokenCount: 5,
	})
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	memC, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: "proj", Durability: storage.DurabilityWorking, TopicKey: "backup-cron",
		CurationState: storage.CurationCuratorVerified, SummaryStub: "Backup sqlite db", Body: "Copy .db nightly", TokenCount: 4,
	})
	if err != nil {
		t.Fatalf("create C: %v", err)
	}
	memOther, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: "other", Durability: storage.DurabilityDurable, TopicKey: "foreign-note",
		CurationState: storage.CurationCuratorVerified, SummaryStub: "Other WS", Body: "Foreign", TokenCount: 2,
	})
	if err != nil {
		t.Fatalf("create other: %v", err)
	}

	// Upsert validations.
	if err := dg.UpsertEdges(ctx, tx, "proj", memB.MemoryID, 1, []graph.EdgeSpec{{
		TargetMemoryID: memB.MemoryID, ExpectedToRev: 1, Rationale: "self",
	}}); !errors.Is(err, graph.ErrSelfEdge) {
		t.Fatalf("expected ErrSelfEdge, got %v", err)
	}
	if err := dg.UpsertEdges(ctx, tx, "proj", memB.MemoryID, 1, []graph.EdgeSpec{{
		TargetMemoryID: memA.MemoryID, ExpectedToRev: 1, Rationale: "   ",
	}}); !errors.Is(err, graph.ErrEmptyRationale) {
		t.Fatalf("expected ErrEmptyRationale, got %v", err)
	}
	if err := dg.UpsertEdges(ctx, tx, "proj", memB.MemoryID, 1, []graph.EdgeSpec{{
		TargetMemoryID: memOther.MemoryID, ExpectedToRev: 1, Rationale: "cross",
	}}); !errors.Is(err, graph.ErrCrossWorkspaceEdge) {
		t.Fatalf("expected ErrCrossWorkspaceEdge, got %v", err)
	}
	if err := dg.UpsertEdges(ctx, tx, "proj", memB.MemoryID, 1, []graph.EdgeSpec{
		{TargetMemoryID: memA.MemoryID, ExpectedToRev: 1, Rationale: "r1"},
		{TargetMemoryID: memA.MemoryID, ExpectedToRev: 1, Rationale: "r2"},
	}); !errors.Is(err, graph.ErrDuplicateEdge) {
		t.Fatalf("expected ErrDuplicateEdge, got %v", err)
	}
	tooMany := make([]graph.EdgeSpec, 6)
	for i := range tooMany {
		id, _ := uuid.NewV7()
		tooMany[i] = graph.EdgeSpec{TargetMemoryID: id, ExpectedToRev: 1, Rationale: "r"}
	}
	if err := dg.UpsertEdges(ctx, tx, "proj", memB.MemoryID, 1, tooMany); !errors.Is(err, graph.ErrTooManyEdges) {
		t.Fatalf("expected ErrTooManyEdges, got %v", err)
	}

	// Valid DAG: C -> B -> A.
	if err := dg.UpsertEdges(ctx, tx, "proj", memB.MemoryID, 1, []graph.EdgeSpec{{
		TargetMemoryID: memA.MemoryID, ExpectedToRev: 1, Rationale: "depends on sqlite choice",
	}}); err != nil {
		t.Fatalf("upsert B -> A: %v", err)
	}
	if err := dg.UpsertEdges(ctx, tx, "proj", memC.MemoryID, 1, []graph.EdgeSpec{{
		TargetMemoryID: memB.MemoryID, ExpectedToRev: 1, Rationale: "depends on wal",
	}}); err != nil {
		t.Fatalf("upsert C -> B: %v", err)
	}

	// Cycle check: trying A -> C closes 3-hop cycle C -> B -> A -> C!
	if err := dg.UpsertEdges(ctx, tx, "proj", memA.MemoryID, 1, []graph.EdgeSpec{{
		TargetMemoryID: memC.MemoryID, ExpectedToRev: 1, Rationale: "cycle",
	}}); !errors.Is(err, graph.ErrEdgeCycle) {
		t.Fatalf("expected ErrEdgeCycle, got %v", err)
	}

	// Preserve existing edges when dependsOn == nil.
	if err := dg.UpsertEdges(ctx, tx, "proj", memB.MemoryID, 1, nil); err != nil {
		t.Fatalf("nil preserve failed: %v", err)
	}

	// Idempotent re-upsert at SAME from_rev=1 succeeds via ON CONFLICT DO UPDATE.
	if err := dg.UpsertEdges(ctx, tx, "proj", memB.MemoryID, 1, []graph.EdgeSpec{{
		TargetMemoryID: memA.MemoryID, ExpectedToRev: 1, Rationale: "depends on sqlite choice updated",
	}}); err != nil {
		t.Fatalf("re-upsert same rev failed: %v", err)
	}

	// Healthy edge & harmless additive revision v1 -> v2 do NOT trigger premise_drifted.
	memA, err = storage.ReviseCoupledTopicTx(ctx, tx, storage.ReviseMemoryParams{
		MemoryID: memA.MemoryID, WorkspaceID: "proj", ExpectedVersion: 1,
		SummaryStub: "Use SQLite v2", Body: "SQLite 3.45 additive detail", TokenCount: 6,
	})
	if err != nil {
		t.Fatalf("revise A to v2: %v", err)
	}
	outHealthy, err := dg.LookupOutgoing(ctx, tx, "proj", []uuid.UUID{memB.MemoryID})
	if err != nil || len(outHealthy[memB.MemoryID]) != 1 || outHealthy[memB.MemoryID][0].PremiseDrifted {
		t.Fatalf("expected non-drifted edge after additive revision, got %+v err=%v", outHealthy, err)
	}

	// Reverse dependents on A includes B.
	revDeps, err := dg.LookupReverseDependents(ctx, tx, "proj", memA.MemoryID)
	if err != nil || len(revDeps) != 1 || revDeps[0].FromMemoryID != memB.MemoryID || revDeps[0].PremiseDrifted {
		t.Fatalf("unexpected reverse dependents: %+v err=%v", revDeps, err)
	}

	// Multi-hop supersession: A -> A' -> A'' (active) resolves A'' stub automatically!
	memA1, err := storage.SupersedeMemoryTx(ctx, tx, storage.SupersedeMemoryParams{
		WorkspaceID: "proj", OldMemoryID: memA.MemoryID, Durability: storage.DurabilityDurable,
		TopicKey: "primary-db", CurationState: storage.CurationCuratorVerified,
		SummaryStub: "Use PG16", Body: "PostgreSQL 16", TokenCount: 3,
	})
	if err != nil {
		t.Fatalf("supersede A -> A': %v", err)
	}
	memA2, err := storage.SupersedeMemoryTx(ctx, tx, storage.SupersedeMemoryParams{
		WorkspaceID: "proj", OldMemoryID: memA1.MemoryID, Durability: storage.DurabilityDurable,
		TopicKey: "primary-db", CurationState: storage.CurationCuratorVerified,
		SummaryStub: "Use PG17", Body: "PostgreSQL 17 with pgvector", TokenCount: 5,
	})
	if err != nil {
		t.Fatalf("supersede A' -> A'': %v", err)
	}

	outDrifted, err := dg.LookupOutgoing(ctx, tx, "proj", []uuid.UUID{memB.MemoryID})
	if err != nil || len(outDrifted[memB.MemoryID]) != 1 {
		t.Fatalf("lookup drifted failed: %+v err=%v", outDrifted, err)
	}
	e := outDrifted[memB.MemoryID][0]
	if !e.PremiseDrifted || e.ReplacementMemoryID == nil || *e.ReplacementMemoryID != memA2.MemoryID || e.ReplacementSummaryStub != "Use PG17" {
		t.Fatalf("expected 2-hop active replacement A'', got %+v", e)
	}

	// SoftRetire target triggers premise_drifted with nil replacement;
	// Revert target triggers premise_drifted when expected_to_rev < last_reverted_version!
	memTargetRev, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: "proj", Durability: storage.DurabilityDurable, TopicKey: "cache-policy",
		CurationState: storage.CurationCuratorVerified, SummaryStub: "Cache v1", Body: "Cache TTL 60s", TokenCount: 4,
	})
	if err != nil {
		t.Fatalf("create cache target: %v", err)
	}
	memTargetRev, err = storage.ReviseCoupledTopicTx(ctx, tx, storage.ReviseMemoryParams{
		MemoryID: memTargetRev.MemoryID, WorkspaceID: "proj", ExpectedVersion: 1,
		SummaryStub: "Cache v2", Body: "Cache TTL 120s", TokenCount: 4,
	})
	if err != nil {
		t.Fatalf("revise cache target to v2: %v", err)
	}
	memDepRev, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: "proj", Durability: storage.DurabilityWorking, TopicKey: "dep-cache",
		CurationState: storage.CurationCuratorVerified, SummaryStub: "Depends on cache v2", Body: "Assume 120s TTL", TokenCount: 5,
	})
	if err != nil {
		t.Fatalf("create dep-cache: %v", err)
	}
	if err := dg.UpsertEdges(ctx, tx, "proj", memDepRev.MemoryID, 1, []graph.EdgeSpec{{
		TargetMemoryID: memTargetRev.MemoryID, ExpectedToRev: 2, Rationale: "relies on v2 cache",
	}}); err != nil {
		t.Fatalf("upsert edge to cache v2: %v", err)
	}

	// Revert memTargetRev back to v1 -> current_version becomes 3, last_reverted_version = 3 > expected_to_rev (2)!
	if _, err := storage.RevertToVersionTx(ctx, tx, storage.RevertMemoryParams{
		MemoryID: memTargetRev.MemoryID, WorkspaceID: "proj", ExpectedVersion: 2, TargetVersion: 1, TokenCount: 4,
	}); err != nil {
		t.Fatalf("revert cache target: %v", err)
	}
	outReverted, err := dg.LookupOutgoing(ctx, tx, "proj", []uuid.UUID{memDepRev.MemoryID})
	if err != nil || !outReverted[memDepRev.MemoryID][0].PremiseDrifted {
		t.Fatalf("expected premise_drifted=true after target revert, got %+v err=%v", outReverted, err)
	}

	// Clearing edges with empty non-nil slice []EdgeSpec{} retires active edges.
	if err := dg.UpsertEdges(ctx, tx, "proj", memC.MemoryID, 2, []graph.EdgeSpec{}); err != nil {
		t.Fatalf("clear outgoing edges with empty slice: %v", err)
	}
	cleared, err := dg.LookupOutgoing(ctx, tx, "proj", []uuid.UUID{memC.MemoryID})
	if err != nil || len(cleared[memC.MemoryID]) != 0 {
		t.Fatalf("expected 0 outgoing edges after clearing, got %+v err=%v", cleared, err)
	}

	// ValidateBatchDAG direct self-edge and intra-batch cycle checks.
	idX, _ := uuid.NewV7()
	idY, _ := uuid.NewV7()
	if err := graph.ValidateBatchDAG(nil, map[uuid.UUID][]uuid.UUID{
		idX: {idY},
		idY: {idX},
	}); !errors.Is(err, graph.ErrEdgeCycle) {
		t.Fatalf("expected ErrEdgeCycle from ValidateBatchDAG, got %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit graph tx: %v", err)
	}
}
