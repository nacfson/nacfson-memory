//go:build integration

package curation_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/storage"
	"github.com/nacfson/nacfson-memory/internal/testutil"
)

func TestEmbedCallEventsFromCurationCandidatesAndEmbedWorker(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)

	content := "topic:retry-policy " + sentinelContent + " Charge retries back off 3 times with jitter."
	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{WorkspaceID: "embed-telemetry", Content: content}); err != nil {
		t.Fatalf("insert inbox: %v", err)
	}

	logs := &lockedBuffer{}
	startRiverWorkers(t, store, curation.WorkerDeps{
		Curator:  curation.NewMockCurator(),
		Embedder: embedding.NewMockEmbedder("mock/embed:1536"),
		Logger:   slog.New(slog.NewJSONHandler(logs, nil)),
	})

	var topicKey, stub, body string
	waitUntil(t, 15*time.Second, "memory curated and embedded", func() (bool, error) {
		err := store.CurationPool.QueryRow(ctx, `
			SELECT topic_key, summary_stub, body FROM memory_manifest
			WHERE workspace_id = 'embed-telemetry' AND embedding IS NOT NULL
		`).Scan(&topicKey, &stub, &body)
		return err == nil, nil
	})

	want := map[string]float64{
		"curation_candidates": float64(utf8.RuneCountInString(content)),
		"worker":              float64(utf8.RuneCountInString(embedding.FormatMemoryText(topicKey, stub, body))),
	}
	seen := make(map[string]bool)
	allowed := map[string]bool{"time": true, "level": true, "msg": true, "caller": true, "workspace_id": true, "space_id": true, "input_chars": true, "outcome": true, "duration_ms": true}
	for _, rec := range logs.records(t) {
		if rec["msg"] != "embed_call" {
			continue
		}
		for k := range rec {
			if !allowed[k] {
				t.Fatalf("embed_call carries non-allowlisted key %q: %v", k, rec)
			}
		}
		caller, _ := rec["caller"].(string)
		chars, tracked := want[caller]
		if !tracked {
			t.Fatalf("unexpected embed_call caller: %v", rec)
		}
		if rec["workspace_id"] != "embed-telemetry" || rec["space_id"] != "mock/embed:1536" || rec["outcome"] != "ok" || rec["input_chars"] != chars {
			t.Fatalf("%s embed_call = %v, want ok with input_chars %v", caller, rec, chars)
		}
		seen[caller] = true
	}
	for caller := range want {
		if !seen[caller] {
			t.Fatalf("missing embed_call event for caller %q; logs:\n%s", caller, logs.String())
		}
	}
	if raw := logs.String(); strings.Contains(raw, sentinelContent) {
		t.Fatalf("captured logs leak memory text:\n%s", raw)
	}
}
