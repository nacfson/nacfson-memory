//go:build integration

package search_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/nacfson/nacfson-memory/internal/search"
	"github.com/nacfson/nacfson-memory/internal/storage"
	"github.com/nacfson/nacfson-memory/internal/testutil"
)

func TestLexicalSearchRankingORFallbackAndInheritance(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)
	lx := search.NewLexicalSearch()

	if err := storage.EnsureWorkspace(ctx, store.CurationPool, "app"); err != nil {
		t.Fatalf("ensure app: %v", err)
	}

	tx, err := store.CurationPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}

	// Constitution must NEVER appear in lexical recall.
	if _, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:   "app",
		Durability:    storage.DurabilityConstitution,
		TopicKey:      "database-constitution",
		CurationState: storage.CurationUserExplicit,
		SummaryStub:   "Always validate database queries",
		Body:          "Always validate PostgreSQL database migrations carefully",
		TokenCount:    7,
	}); err != nil {
		t.Fatalf("seed constitution: %v", err)
	}

	// Project memories: strong match vs partial match (for OR fallback) vs trigram topic match.
	if _, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:   "app",
		Durability:    storage.DurabilityDurable,
		TopicKey:      "database-choice",
		CurationState: storage.CurationCuratorVerified,
		SummaryStub:   "PostgreSQL 17 chosen for storage",
		Body:          "Primary backend database engine is PostgreSQL 17 with pgvector HNSW.",
		TokenCount:    12,
	}); err != nil {
		t.Fatalf("seed db-choice: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	if _, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:   "app",
		Durability:    storage.DurabilityWorking,
		TopicKey:      "backup-schedule",
		CurationState: storage.CurationCuratorVerified,
		SummaryStub:   "Nightly snapshot schedule",
		Body:          "Run pg_dump backup nightly at 03:00 UTC.",
		TokenCount:    9,
	}); err != nil {
		t.Fatalf("seed backup: %v", err)
	}

	// General durable + general working (working in general is invalid per design, durable allowed).
	if _, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:   storage.WorkspaceGeneral,
		Durability:    storage.DurabilityDurable,
		TopicKey:      "home-timezone",
		CurationState: storage.CurationCuratorVerified,
		SummaryStub:   "User timezone Asia/Seoul",
		Body:          "User lives in Seoul timezone Asia/Seoul.",
		TokenCount:    7,
	}); err != nil {
		t.Fatalf("seed general durable: %v", err)
	}
	// Shared topic_key in general overridden by project 'app'.
	if _, err := storage.CreateMemoryTx(ctx, tx, storage.CreateMemoryParams{
		WorkspaceID:   storage.WorkspaceGeneral,
		Durability:    storage.DurabilityDurable,
		TopicKey:      "database-choice",
		CurationState: storage.CurationCuratorVerified,
		SummaryStub:   "General prefers postgres",
		Body:          "User prefers PostgreSQL personally",
		TokenCount:    5,
	}); err != nil {
		t.Fatalf("seed general database-choice: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit seed tx: %v", err)
	}

	rtx, err := store.ServingPool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("begin read-only tx: %v", err)
	}
	defer func() { _ = rtx.Commit(ctx) }()

	// 1. Multi-word natural language query (< 5 AND hits) must trigger OR-lexeme fallback
	// and exclude constitution while returning project database-choice ahead of overridden general row.
	hits, err := lx.Search(ctx, rtx, search.SearchParams{
		WorkspaceID:    "app",
		InheritGeneral: storage.InheritConstitutionAndDurable,
		Query:          "Which PostgreSQL database or snapshot timezone schedule did we configure?",
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("lexical search natural language: %v", err)
	}
	if len(hits) != 3 {
		t.Fatalf("expected 3 unique hits (database-choice, backup-schedule, home-timezone), got %d: %+v", len(hits), hits)
	}
	for _, h := range hits {
		if h.Memory.Durability == storage.DurabilityConstitution {
			t.Fatalf("constitution leaked into lexical search: %+v", h.Memory)
		}
		if h.Memory.TopicKey == "database-choice" && h.Memory.WorkspaceID != "app" {
			t.Fatalf("expected project database-choice to override general")
		}
	}

	// 2. Empty query returns active non-constitution memories ordered by updated_at DESC.
	recent, err := lx.Search(ctx, rtx, search.SearchParams{
		WorkspaceID:    "app",
		InheritGeneral: storage.InheritNone,
		Query:          "",
		Limit:          10,
	})
	if err != nil || len(recent) != 2 || recent[0].Memory.TopicKey != "backup-schedule" {
		t.Fatalf("expected 2 app memories ordered most-recent first, got %+v err=%v", recent, err)
	}

	// 3. Constitution_only excludes general durable rows.
	noGeneralDurable, err := lx.Search(ctx, rtx, search.SearchParams{
		WorkspaceID:    "app",
		InheritGeneral: storage.InheritConstitutionOnly,
		Query:          "Seoul timezone",
		Limit:          10,
	})
	if err != nil || len(noGeneralDurable) != 0 {
		t.Fatalf("expected constitution_only to exclude general durable rows, got %+v err=%v", noGeneralDurable, err)
	}

	// 4. Stopword-only & adversarial punctuation queries never cause tsquery syntax errors.
	for _, q := range []string{"the and or of to", `postgresql & (v17)! 'unbalanced "quote`} {
		if _, err := lx.Search(ctx, rtx, search.SearchParams{
			WorkspaceID: "app", InheritGeneral: storage.InheritNone, Query: q,
		}); err != nil {
			t.Fatalf("adversarial query %q caused error: %v", q, err)
		}
	}
}
