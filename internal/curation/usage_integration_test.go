//go:build integration

package curation_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/storage"
	"github.com/nacfson/nacfson-memory/internal/testutil"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) records(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(b.buf.Bytes()))
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("decode log line %q: %v", sc.Text(), err)
		}
		out = append(out, rec)
	}
	return out
}

func decisionTokenTotals(ctx context.Context, store *storage.Store, wsID string) (rows, prompt, completion int, err error) {
	err = store.CurationPool.QueryRow(ctx, `
		SELECT count(*), COALESCE(sum(prompt_tokens), 0), COALESCE(sum(completion_tokens), 0)
		FROM curation_decisions WHERE workspace_id = $1
	`, wsID).Scan(&rows, &prompt, &completion)
	return rows, prompt, completion, err
}

func TestCuratorTokenUsageBilledOncePerBatchAndFailedAttemptsLogged(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)

	var failWSCalls, transientWSCalls atomic.Int32
	sc := &scriptCurator{fn: func(ctx context.Context, req curation.CurateRequest) (curation.CuratePlan, error) {
		if req.WorkspaceID == "tokens-transient" && transientWSCalls.Add(1) == 1 {
			return curation.CuratePlan{}, &curation.CurateUsageError{
				Usage: curation.CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", PromptTokens: 640, CompletionTokens: 90, APICalls: 2},
				Err:   errors.New("curator endpoint returned HTTP 503: repair call overloaded"),
			}
		}
		if req.WorkspaceID == "tokens-fail" && failWSCalls.Add(1) == 1 {
			return curation.CuratePlan{}, &curation.CurateUsageError{
				Usage: curation.CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", PromptTokens: 612, CompletionTokens: 143, APICalls: 2},
				Err:   fmt.Errorf("%w: field mutations.0.depends_on.0.expected_to_rev must be an integer, got bool", curation.ErrMalformedPlan),
			}
		}
		plan, err := curation.NewMockCurator().CurateBatch(ctx, req)
		plan.ProviderName, plan.ModelName = "deepseek", "deepseek-chat"
		plan.PromptTokens, plan.CompletionTokens = 790, 256
		return plan, err
	}}

	for _, content := range []string{"topic:billing-stack Billing API runs Go 1.27 on PostgreSQL 17.", "thanks"} {
		if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{WorkspaceID: "tokens-batch", Content: content}); err != nil {
			t.Fatalf("insert tokens-batch inbox: %v", err)
		}
	}
	for _, wsID := range []string{"tokens-fail", "tokens-transient"} {
		if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{WorkspaceID: wsID, Content: "topic:charge-bench WIP TODO: benchmark /v1/charge retries."}); err != nil {
			t.Fatalf("insert %s inbox: %v", wsID, err)
		}
	}

	logs := &lockedBuffer{}
	startRiverWorkers(t, store, curation.WorkerDeps{
		Curator:  sc,
		Embedder: embedding.NewMockEmbedder("mock/embed:1536"),
		Logger:   slog.New(slog.NewJSONHandler(logs, nil)),
	})

	waitUntil(t, 15*time.Second, "all workspaces curated", func() (bool, error) {
		var pending int
		err := store.CurationPool.QueryRow(ctx, `
			SELECT count(*) FROM raw_inbox
			WHERE workspace_id IN ('tokens-batch', 'tokens-fail', 'tokens-transient') AND status <> 'curated'
		`).Scan(&pending)
		return pending == 0, err
	})

	rows, prompt, completion, err := decisionTokenTotals(ctx, store, "tokens-batch")
	if err != nil {
		t.Fatalf("sum tokens-batch decisions: %v", err)
	}
	if rows != 2 || prompt != 790 || completion != 256 {
		t.Fatalf("2-mutation batch: rows=%d prompt=%d completion=%d, want 2 rows summing to one API call 790/256", rows, prompt, completion)
	}

	_, prompt, completion, err = decisionTokenTotals(ctx, store, "tokens-fail")
	if err != nil {
		t.Fatalf("sum tokens-fail decisions: %v", err)
	}
	if prompt != 790 || completion != 256 {
		t.Fatalf("retried workspace decisions bill %d/%d, want only the successful call 790/256", prompt, completion)
	}

	want := map[string][2]float64{"tokens-fail": {612, 143}, "tokens-transient": {640, 90}}
	for _, rec := range logs.records(t) {
		wsID, _ := rec["workspace_id"].(string)
		usage, tracked := want[wsID]
		if rec["msg"] != "curator tokens not billed to curation_decisions" || !tracked {
			continue
		}
		if rec["prompt_tokens"] != usage[0] || rec["completion_tokens"] != usage[1] || rec["attempt"] != float64(1) || rec["model"] != "deepseek-chat" {
			t.Fatalf("failed attempt log for %s has wrong usage fields: %v", wsID, rec)
		}
		delete(want, wsID)
	}
	if len(want) > 0 {
		t.Fatalf("no failed-attempt usage log for %v; logs:\n%v", want, logs.records(t))
	}
}
