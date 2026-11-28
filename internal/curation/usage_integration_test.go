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
	"strings"
	"sync"
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

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
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

const (
	sentinelContent   = "SENTINEL-CONTENT-a71c"
	sentinelTopic     = "sentinel-topic-a71c"
	sentinelBody      = "SENTINEL-BODY-a71c"
	sentinelStub      = "SENTINEL-STUB-a71c"
	sentinelRationale = "SENTINEL-RATIONALE-a71c"
)

// llmCallKeys is the complete key set of a curation_llm_call record: slog's own keys plus
// the metadata-only fields the v1.1 telemetry contract allows.
var llmCallKeys = []string{
	"time", "level", "msg",
	"workspace_id", "provider", "model", "attempt", "batch_size", "mutations",
	"prompt_tokens", "completion_tokens", "api_calls", "outcome", "duration_ms",
}

// llmCallEvents returns curation_llm_call records grouped by workspace in emission order,
// failing the test if any record carries a key outside llmCallKeys.
func llmCallEvents(t *testing.T, logs *lockedBuffer) map[string][]map[string]any {
	t.Helper()
	allowed := make(map[string]bool, len(llmCallKeys))
	for _, k := range llmCallKeys {
		allowed[k] = true
	}
	byWS := make(map[string][]map[string]any)
	for _, rec := range logs.records(t) {
		if rec["msg"] != "curation_llm_call" {
			continue
		}
		for k := range rec {
			if !allowed[k] {
				t.Fatalf("curation_llm_call carries non-allowlisted key %q: %v", k, rec)
			}
		}
		if len(rec) != len(llmCallKeys) {
			t.Fatalf("curation_llm_call has %d keys, want %d: %v", len(rec), len(llmCallKeys), rec)
		}
		wsID, _ := rec["workspace_id"].(string)
		byWS[wsID] = append(byWS[wsID], rec)
	}
	return byWS
}

type llmCallWant struct {
	outcome                   string
	attempt, batch, mutations float64
	prompt, completion, calls float64
}

func assertLLMCall(t *testing.T, wsID string, rec map[string]any, want llmCallWant) {
	t.Helper()
	if rec["outcome"] != want.outcome || rec["attempt"] != want.attempt || rec["batch_size"] != want.batch ||
		rec["mutations"] != want.mutations || rec["prompt_tokens"] != want.prompt ||
		rec["completion_tokens"] != want.completion || rec["api_calls"] != want.calls ||
		rec["provider"] != "deepseek" || rec["model"] != "deepseek-chat" || rec["level"] != "INFO" {
		t.Fatalf("%s curation_llm_call = %v, want %+v", wsID, rec, want)
	}
	if d, ok := rec["duration_ms"].(float64); !ok || d < 0 {
		t.Fatalf("%s curation_llm_call duration_ms = %v", wsID, rec["duration_ms"])
	}
}

func TestCuratorTokenUsageBilledOncePerBatchAndFailedAttemptsLogged(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)

	var callsMu sync.Mutex
	calls := make(map[string]int)
	sc := &scriptCurator{fn: func(ctx context.Context, req curation.CurateRequest) (curation.CuratePlan, error) {
		callsMu.Lock()
		calls[req.WorkspaceID]++
		n := calls[req.WorkspaceID]
		callsMu.Unlock()
		if req.WorkspaceID == "tokens-transient" && n == 1 {
			return curation.CuratePlan{}, &curation.CurateUsageError{
				Usage: curation.CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", PromptTokens: 640, CompletionTokens: 90, APICalls: 2},
				Err:   errors.New("curator endpoint returned HTTP 503: repair call overloaded"),
			}
		}
		if req.WorkspaceID == "tokens-fail" && n == 1 {
			return curation.CuratePlan{}, &curation.CurateUsageError{
				Usage: curation.CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", PromptTokens: 612, CompletionTokens: 143, APICalls: 2},
				Err:   fmt.Errorf("%w: field mutations.0.depends_on.0.expected_to_rev must be an integer, got bool", curation.ErrMalformedPlan),
			}
		}
		plan, err := curation.NewMockCurator().CurateBatch(ctx, req)
		plan.CurateUsage = curation.CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", PromptTokens: 790, CompletionTokens: 256, APICalls: 1}
		for i := range plan.Mutations {
			plan.Mutations[i].Rationale = sentinelRationale
			if plan.Mutations[i].Action == curation.ActionCreate {
				plan.Mutations[i].Body = sentinelBody
				plan.Mutations[i].SummaryStub = sentinelStub
			}
		}
		return plan, err
	}}

	for _, content := range []string{"topic:" + sentinelTopic + " " + sentinelContent + " Billing API runs Go 1.27 on PostgreSQL 17.", "thanks"} {
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

	events := llmCallEvents(t, logs)
	callsMu.Lock()
	for wsID, n := range calls {
		if len(events[wsID]) != n {
			t.Fatalf("%s: %d CurateBatch calls but %d curation_llm_call events: %v", wsID, n, len(events[wsID]), events[wsID])
		}
	}
	callsMu.Unlock()
	ok := func(attempt, batch, mutations float64) llmCallWant {
		return llmCallWant{outcome: "ok", attempt: attempt, batch: batch, mutations: mutations, prompt: 790, completion: 256, calls: 1}
	}
	assertLLMCall(t, "tokens-batch", events["tokens-batch"][0], ok(1, 2, 2))
	assertLLMCall(t, "tokens-fail", events["tokens-fail"][0], llmCallWant{outcome: "validation_failed", attempt: 1, batch: 1, prompt: 612, completion: 143, calls: 2})
	assertLLMCall(t, "tokens-fail", events["tokens-fail"][1], ok(2, 1, 1))
	assertLLMCall(t, "tokens-transient", events["tokens-transient"][0], llmCallWant{outcome: "provider_error", attempt: 1, batch: 1, prompt: 640, completion: 90, calls: 2})
	assertLLMCall(t, "tokens-transient", events["tokens-transient"][1], ok(2, 1, 1))

	raw := logs.String()
	for _, sentinel := range []string{sentinelContent, sentinelTopic, sentinelBody, sentinelStub, sentinelRationale} {
		if strings.Contains(raw, sentinel) {
			t.Fatalf("captured logs leak curation payload %q:\n%s", sentinel, raw)
		}
	}
}

func TestCurationLLMCallReportsProvisionalFallback(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)
	sc := &scriptCurator{fn: func(context.Context, curation.CurateRequest) (curation.CuratePlan, error) {
		return curation.CuratePlan{}, &curation.CurateUsageError{
			Usage: curation.CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", PromptTokens: 500, CompletionTokens: 60, APICalls: 2},
			Err:   errors.New("curator endpoint returned HTTP 503: repair call overloaded"),
		}
	}}
	if _, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{WorkspaceID: "tokens-outage", Content: "topic:charge-bench WIP TODO: benchmark /v1/charge retries."}); err != nil {
		t.Fatalf("insert tokens-outage inbox: %v", err)
	}

	logs := &lockedBuffer{}
	startRiverWorkers(t, store, curation.WorkerDeps{
		Curator:        sc,
		CuratorBreaker: curation.NewBreaker(1, time.Minute),
		Logger:         slog.New(slog.NewJSONHandler(logs, nil)),
	})

	waitUntil(t, 15*time.Second, "provisional fallback", func() (bool, error) {
		var status string
		err := store.CurationPool.QueryRow(ctx, `SELECT status FROM raw_inbox WHERE workspace_id = 'tokens-outage'`).Scan(&status)
		return status == "provisional", err
	})

	events := llmCallEvents(t, logs)["tokens-outage"]
	if len(events) != 1 || int(sc.hits.Load()) != 1 {
		t.Fatalf("want one curation_llm_call for one CurateBatch call, got %d events for %d calls: %v", len(events), sc.hits.Load(), events)
	}
	assertLLMCall(t, "tokens-outage", events[0], llmCallWant{outcome: "provisional_fallback", attempt: 1, batch: 1, prompt: 500, completion: 60, calls: 2})
}
