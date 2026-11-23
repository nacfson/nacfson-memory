package testutil

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

const defaultTestDSN = "postgres://mem1:mem1@127.0.0.1:5432/mem1?sslmode=disable"

var migrateOnce sync.Once

// TestDSN resolves the PostgreSQL connection string for integration tests.
func TestDSN() string {
	if v := os.Getenv("MEM1_TEST_DATABASE_URL"); v != "" {
		return v
	}
	if v := os.Getenv("MEM1_DATABASE_URL"); v != "" {
		return v
	}
	return defaultTestDSN
}

// OpenTestDB ensures schema migrations are applied once, resets all domain and River
// tables cleanly, re-seeds 'general', and returns an initialized *storage.Store.
func OpenTestDB(t *testing.T) *storage.Store {
	t.Helper()

	dsn := TestDSN()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var migrateErr error
	migrateOnce.Do(func() {
		migrateErr = storage.Migrate(ctx, dsn, false)
	})
	if migrateErr != nil {
		t.Fatalf("migrate test database: %v", migrateErr)
	}

	cfg, err := config.LoadFromLookup(config.ModeMCPStdio, func(k string) (string, bool) {
		switch k {
		case "MEM1_DATABASE_URL":
			return dsn, true
		case "MEM1_EMBED_PROVIDER":
			return config.ProviderMock, true
		case "MEM1_STDIO_SERVING_MAX_CONNS":
			return "8", true
		case "MEM1_STDIO_CURATION_MAX_CONNS":
			return "4", true
		default:
			return "", false
		}
	})
	if err != nil {
		t.Fatalf("load test config: %v", err)
	}

	store, err := storage.OpenPools(ctx, cfg)
	if err != nil {
		t.Fatalf("open test pools: %v", err)
	}
	t.Cleanup(store.Close)

	if _, err := store.CurationPool.Exec(ctx, `
		TRUNCATE TABLE
			confirmation_tokens,
			curation_decisions,
			memory_edges,
			memory_citations,
			memory_revisions,
			memory_manifest,
			raw_inbox,
			workspaces,
			river_job,
			river_leader,
			river_queue
		RESTART IDENTITY CASCADE;
		INSERT INTO workspaces (workspace_id, kind, inherit_general, manifest_revision, constitution_revision)
		VALUES ('general', 'general', 'none', 0, 0)
		ON CONFLICT (workspace_id) DO NOTHING;
	`); err != nil {
		t.Fatalf("truncate test database tables: %v", err)
	}

	return store
}
