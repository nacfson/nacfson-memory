//go:build integration

package retrieval_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/retrieval"
	"github.com/nacfson/nacfson-memory/internal/search"
	"github.com/nacfson/nacfson-memory/internal/storage"
	"github.com/nacfson/nacfson-memory/internal/testutil"
)

type failingEmbedder struct {
	err error
}

func (f failingEmbedder) SpaceID() string { return "mock/embed:1536" }
func (f failingEmbedder) Dimensions() int { return 1536 }
func (f failingEmbedder) EmbedBatch(context.Context, []string) ([][]float32, error) {
	return nil, f.err
}

func TestRecallAndExpandIntegration(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)
	emb := embedding.NewMockEmbedder("mock/embed:1536")
	lx := search.NewLexicalSearch()
	dg := graph.NewDependencyGraph()

	if err := storage.EnsureWorkspace(ctx, store.CurationPool, "payments"); err != nil {
		t.Fatalf("ensure payments: %v", err)
	}
	if err := storage.EnsureWorkspace(ctx, store.CurationPool, "other"); err != nil {
		t.Fatalf("ensure other: %v", err)
	}

	tx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	embedText := func(topic, stub, body string) []float32 {
		vecs, err := emb.EmbedBatch(ctx, []string{embedding.FormatMemoryText(topic, stub, body)})
		if err != nil {
			t.Fatalf("mock embed: %v", err)
		}
		return vecs[0]
	}

	// General constitution + general durable + general overridden durable.
	if _, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: storage.WorkspaceGeneral, Durability: storage.DurabilityConstitution,
		TopicKey: "reply-style", CurationState: storage.CurationUserExplicit,
		SummaryStub: "Respond concisely", Body: "Respond in concise English.", TokenCount: 5,
	}); err != nil {
		t.Fatalf("seed general constitution: %v", err)
	}
	genTZ, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: storage.WorkspaceGeneral, Durability: storage.DurabilityDurable,
		TopicKey: "home-timezone", CurationState: storage.CurationCuratorVerified,
		SummaryStub: "Seoul timezone", Body: "User lives in Asia/Seoul timezone.", TokenCount: 8,
		Embedding:        embedText("home-timezone", "Seoul timezone", "User lives in Asia/Seoul timezone."),
		EmbeddingSpaceID: emb.SpaceID(),
	})
	if err != nil {
		t.Fatalf("seed general durable: %v", err)
	}
	if _, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: storage.WorkspaceGeneral, Durability: storage.DurabilityDurable,
		TopicKey: "primary-db", CurationState: storage.CurationCuratorVerified,
		SummaryStub: "General Postgres default", Body: "User likes SQLite for small personal scripts.", TokenCount: 8,
		Embedding:        embedText("primary-db", "General Postgres default", "User likes SQLite for small personal scripts."),
		EmbeddingSpaceID: emb.SpaceID(),
	}); err != nil {
		t.Fatalf("seed general override target: %v", err)
	}

	// Project old DB superseded by PG17, with dependent WAL memory triggering premise_drifted.
	oldDB, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: "payments", Durability: storage.DurabilityDurable,
		TopicKey: "primary-db", CurationState: storage.CurationCuratorVerified,
		SummaryStub: "Use SQLite v1", Body: "Use SQLite local prototype.", TokenCount: 5,
	})
	if err != nil {
		t.Fatalf("seed oldDB: %v", err)
	}
	walDep, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: "payments", Durability: storage.DurabilityWorking,
		TopicKey: "wal-pragma", CurationState: storage.CurationCuratorVerified,
		SummaryStub: "SQLite WAL mode pragma", Body: "Configure SQLite journal_mode=WAL checkpoint pragma.", TokenCount: 9,
		Embedding:        embedText("wal-pragma", "SQLite WAL mode pragma", "Configure SQLite journal_mode=WAL checkpoint pragma."),
		EmbeddingSpaceID: emb.SpaceID(),
	})
	if err != nil {
		t.Fatalf("seed walDep: %v", err)
	}
	if err := dg.UpsertEdges(ctx, tx, "payments", walDep.MemoryID, 1, []graph.EdgeSpec{{
		TargetMemoryID: oldDB.MemoryID, ExpectedToRev: 1, Rationale: "WAL pragma assumes SQLite storage",
	}}); err != nil {
		t.Fatalf("link walDep -> oldDB: %v", err)
	}
	newDB, err := storage.SupersedeMemoryTx(ctx, tx, storage.SupersedeMemoryParams{
		WorkspaceID: "payments", OldMemoryID: oldDB.MemoryID, Durability: storage.DurabilityDurable,
		TopicKey: "primary-db", CurationState: storage.CurationCuratorVerified,
		SummaryStub: "Use PostgreSQL 17 with pgvector", Body: "Primary database migrated to PostgreSQL 17.", TokenCount: 8,
		Embedding:        embedText("primary-db", "Use PostgreSQL 17 with pgvector", "Primary database migrated to PostgreSQL 17."),
		EmbeddingSpaceID: emb.SpaceID(),
	})
	if err != nil {
		t.Fatalf("supersede oldDB -> newDB: %v", err)
	}
	foreignMem, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID: "other", Durability: storage.DurabilityDurable,
		TopicKey: "foreign", CurationState: storage.CurationCuratorVerified,
		SummaryStub: "Other project secret", Body: "Belongs to other workspace", TokenCount: 4,
	})
	if err != nil {
		t.Fatalf("seed foreign: %v", err)
	}
	if _, err := storage.BumpRevisionTx(ctx, tx, "payments", false); err != nil {
		t.Fatalf("bump payments rev: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit seed tx: %v", err)
	}

	packer := retrieval.NewPacker(store, emb, curation.NewBreaker(3, time.Minute), lx, dg, 2*time.Second)

	// 1. Recall combines general constitution + project hits (with drifted WAL dep) + non-overridden general durable.
	resp, err := packer.RecallContext(ctx, retrieval.RecallRequest{
		WorkspaceID: "payments",
		Query:       "SQLite WAL PostgreSQL 17 Seoul timezone",
		MaxTokens:   2000,
	})
	if err != nil {
		t.Fatalf("RecallContext failed: %v", err)
	}
	if resp.Degraded || resp.SerializedTokens > 2000 {
		t.Fatalf("unexpected degraded or over-budget recall: degraded=%v toks=%d", resp.Degraded, resp.SerializedTokens)
	}
	var sawConst, sawWALDrifted, sawGeneralTZ, sawGeneralOverride bool
	for _, m := range resp.Memories {
		if m.Durability == storage.DurabilityConstitution && m.TopicKey == "reply-style" {
			sawConst = true
		}
		if m.MemoryID == walDep.MemoryID && m.PremiseDrifted && len(m.Dependencies) == 1 && m.Dependencies[0].ReplacementMemoryID != nil && *m.Dependencies[0].ReplacementMemoryID == newDB.MemoryID {
			sawWALDrifted = true
		}
		if m.MemoryID == genTZ.MemoryID {
			sawGeneralTZ = true
		}
		if m.WorkspaceID == storage.WorkspaceGeneral && m.TopicKey == "primary-db" {
			sawGeneralOverride = true
		}
	}
	if !sawConst || !sawWALDrifted || !sawGeneralTZ || sawGeneralOverride {
		t.Fatalf("unexpected recall composition: const=%v walDrifted=%v genTZ=%v genOverride=%v resp=%+v", sawConst, sawWALDrifted, sawGeneralTZ, sawGeneralOverride, resp)
	}

	// 2. Unknown workspace returns empty non-degraded result cleanly.
	unknownResp, err := packer.RecallContext(ctx, retrieval.RecallRequest{
		WorkspaceID: "never-created-ws", Query: "anything", MaxTokens: 500,
	})
	if err != nil || unknownResp.Degraded || len(unknownResp.Memories) != 0 {
		t.Fatalf("unexpected unknown workspace response: %+v err=%v", unknownResp, err)
	}

	// 3. Embedder outage triggers degraded=true with lexical + constitution results.
	degradedPacker := retrieval.NewPacker(store, failingEmbedder{err: errors.New("embed down")}, curation.NewBreaker(3, time.Minute), lx, dg, time.Second)
	degResp, err := degradedPacker.RecallContext(ctx, retrieval.RecallRequest{
		WorkspaceID: "payments", Query: "PostgreSQL", MaxTokens: 1000,
	})
	if err != nil || !degResp.Degraded || len(degResp.Memories) == 0 {
		t.Fatalf("expected degraded lexical fallback recall, got %+v err=%v", degResp, err)
	}

	// 4. ExpandMemories authorizes project + general, rejects foreign workspace ID.
	expResp, err := packer.ExpandMemories(ctx, retrieval.ExpandRequest{
		WorkspaceID: "payments",
		MemoryIDs:   []uuid.UUID{walDep.MemoryID, genTZ.MemoryID},
		MaxTokens:   2000,
	})
	if err != nil || len(expResp.Memories) != 2 || len(expResp.Edges) != 1 || !expResp.Edges[0].PremiseDrifted {
		t.Fatalf("unexpected ExpandMemories response: %+v err=%v", expResp, err)
	}
	if _, err := packer.ExpandMemories(ctx, retrieval.ExpandRequest{
		WorkspaceID: "payments",
		MemoryIDs:   []uuid.UUID{foreignMem.MemoryID},
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput expanding foreign workspace memory, got %v", err)
	}

	// Switch payments inherit_general='none' -> general IDs must now be rejected.
	if _, err := storage.UpdateInheritGeneral(ctx, store.CurationPool, "payments", storage.InheritNone); err != nil {
		t.Fatalf("update inherit_general: %v", err)
	}
	if _, err := packer.ExpandMemories(ctx, retrieval.ExpandRequest{
		WorkspaceID: "payments",
		MemoryIDs:   []uuid.UUID{genTZ.MemoryID},
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput when inherit_general='none', got %v", err)
	}
	_ = strings.TrimSpace
}
