//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"

	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/httpapi"
	"github.com/nacfson/nacfson-memory/internal/mcpserver"
	"github.com/nacfson/nacfson-memory/internal/retrieval"
	"github.com/nacfson/nacfson-memory/internal/search"
	"github.com/nacfson/nacfson-memory/internal/security"
	"github.com/nacfson/nacfson-memory/internal/service"
	"github.com/nacfson/nacfson-memory/internal/storage"
	"github.com/nacfson/nacfson-memory/internal/testutil"
)

const forcedJobFailure = "forced failure for dead-letter inspection"

type alwaysFailArgs struct{}

func (alwaysFailArgs) Kind() string { return "test_always_fail" }

type alwaysFailWorker struct {
	river.WorkerDefaults[alwaysFailArgs]
}

func (alwaysFailWorker) Work(context.Context, *river.Job[alwaysFailArgs]) error {
	return errors.New(forcedJobFailure)
}

type immediateRetry struct{}

func (immediateRetry) NextRetry(*rivertype.JobRow) time.Time { return time.Now() }

// discardRiverJob drives a real River job through two failed attempts so River itself
// writes the discarded state and its jsonb[] errors column.
func discardRiverJob(t *testing.T, store *storage.Store) int64 {
	t.Helper()
	ctx := context.Background()
	workers := river.NewWorkers()
	river.AddWorker(workers, alwaysFailWorker{})
	client, err := river.NewClient(riverpgxv5.New(store.CurationPool), &river.Config{
		Queues:            map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}},
		Workers:           workers,
		RetryPolicy:       immediateRetry{},
		FetchCooldown:     20 * time.Millisecond,
		FetchPollInterval: 50 * time.Millisecond,
		TestOnly:          true,
	})
	if err != nil {
		t.Fatalf("new river client: %v", err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatalf("start river client: %v", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.StopAndCancel(stopCtx)
	}()

	res, err := client.Insert(ctx, alwaysFailArgs{}, &river.InsertOpts{MaxAttempts: 2})
	if err != nil {
		t.Fatalf("insert always-failing job: %v", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		var state string
		if err := store.CurationPool.QueryRow(ctx, `SELECT state FROM river_job WHERE id = $1`, res.Job.ID).Scan(&state); err != nil {
			t.Fatalf("read river job state: %v", err)
		}
		if state == string(rivertype.JobStateDiscarded) {
			return res.Job.ID
		}
		if time.Now().After(deadline) {
			t.Fatalf("river job %d still %q after deadline", res.Job.ID, state)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

type deadLettersWire struct {
	DiscardedJobs []struct {
		ID      int64  `json:"id"`
		Kind    string `json:"kind"`
		Attempt int    `json:"attempt"`
		Errors  []struct {
			Attempt int    `json:"attempt"`
			Error   string `json:"error"`
		} `json:"errors"`
	} `json:"discarded_jobs"`
}

func assertDiscardedJobListed(t *testing.T, source string, body []byte, jobID int64) {
	t.Helper()
	var rep deadLettersWire
	if err := json.Unmarshal(body, &rep); err != nil {
		t.Fatalf("%s: decode dead-letters JSON: %v\n%s", source, err, body)
	}
	for _, job := range rep.DiscardedJobs {
		if job.ID != jobID {
			continue
		}
		if job.Kind != "test_always_fail" || job.Attempt != 2 || len(job.Errors) != 2 {
			t.Fatalf("%s: discarded job %d has kind=%q attempt=%d errors=%d, want test_always_fail/2/2\n%s", source, jobID, job.Kind, job.Attempt, len(job.Errors), body)
		}
		for i, e := range job.Errors {
			if e.Attempt != i+1 || !strings.Contains(e.Error, forcedJobFailure) {
				t.Fatalf("%s: errors[%d] = %+v, want attempt %d with %q", source, i, e, i+1, forcedJobFailure)
			}
		}
		return
	}
	t.Fatalf("%s: discarded job %d missing from dead-letters report\n%s", source, jobID, body)
}

// insertDiscardedJob writes a discarded river_job row whose errors column is errorsSQL
// (for example NULL or '{}'::jsonb[]) and returns its id.
func insertDiscardedJob(t *testing.T, store *storage.Store, errorsSQL string) int64 {
	t.Helper()
	var id int64
	if err := store.CurationPool.QueryRow(context.Background(), `
		INSERT INTO river_job (state, attempt, max_attempts, kind, queue, args, errors, finalized_at)
		VALUES ('discarded', 1, 1, 'test_no_errors', 'default', '{}', `+errorsSQL+`, now())
		RETURNING id
	`).Scan(&id); err != nil {
		t.Fatalf("insert discarded job with errors=%s: %v", errorsSQL, err)
	}
	return id
}

// assertEmptyErrorsArray checks that job jobID serializes "errors" as a present, empty JSON array.
func assertEmptyErrorsArray(t *testing.T, source string, body []byte, jobID int64) {
	t.Helper()
	var rep struct {
		DiscardedJobs []map[string]json.RawMessage `json:"discarded_jobs"`
	}
	if err := json.Unmarshal(body, &rep); err != nil {
		t.Fatalf("%s: decode dead-letters JSON: %v\n%s", source, err, body)
	}
	for _, job := range rep.DiscardedJobs {
		var id int64
		if err := json.Unmarshal(job["id"], &id); err != nil || id != jobID {
			continue
		}
		raw, present := job["errors"]
		if !present || string(raw) != "[]" {
			t.Fatalf("%s: discarded job %d errors = %q (present=%v), want []\n%s", source, jobID, raw, present, body)
		}
		return
	}
	t.Fatalf("%s: discarded job %d missing from dead-letters report\n%s", source, jobID, body)
}

func TestDeadLettersListsDiscardedRiverJobOverHTTPAndCLI(t *testing.T) {
	store := testutil.OpenTestDB(t)
	jobID := discardRiverJob(t, store)
	emptyErrorsJob := insertDiscardedJob(t, store, `'{}'::jsonb[]`)
	nullErrorsJob := insertDiscardedJob(t, store, `NULL`)

	cfg := config.Config{
		AllowedHosts:      []string{"127.0.0.1", "localhost", "[::1]"},
		AdminToken:        "admin-dead-letters-token",
		AgentToken:        "agent-dead-letters-token",
		MaxInflight:       64,
		MaxWriteBytes:     65536,
		HistoryRetentionN: 20,
		EmbedTimeout:      2 * time.Second,
	}
	emb := embedding.NewMockEmbedder("mock:1536")
	lx := search.NewLexicalSearch()
	dg := graph.NewDependencyGraph()
	curBreaker := curation.NewBreaker(3, time.Minute)
	embBreaker := curation.NewBreaker(3, time.Minute)
	packer := retrieval.NewPacker(store, emb, embBreaker, lx, dg, cfg.EmbedTimeout)
	svc := service.New(cfg, store, security.NewSecretScrubber(), emb, packer, dg, curBreaker, embBreaker)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(httpapi.NewHandler(cfg, svc, mcpserver.NewStreamableHTTPHandler(svc, logger), logger))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/ops/dead-letters?limit=10", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+cfg.AdminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/ops/dead-letters: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/ops/dead-letters status=%d body=%s", resp.StatusCode, body)
	}
	assertDiscardedJobListed(t, "GET /v1/ops/dead-letters", body, jobID)
	assertEmptyErrorsArray(t, "GET /v1/ops/dead-letters", body, emptyErrorsJob)
	assertEmptyErrorsArray(t, "GET /v1/ops/dead-letters", body, nullErrorsJob)

	t.Setenv("MEM1_ADMIN_TOKEN", cfg.AdminToken)
	t.Setenv("MEM1_HTTP_ADDR", srv.URL)
	var out, errBuf bytes.Buffer
	if code := run([]string{"dead-letters", "--limit=10"}, &out, &errBuf); code != 0 {
		t.Fatalf("mem1 dead-letters exit=%d stderr=%s", code, errBuf.String())
	}
	assertDiscardedJobListed(t, "mem1 dead-letters", out.Bytes(), jobID)
	assertEmptyErrorsArray(t, "mem1 dead-letters", out.Bytes(), emptyErrorsJob)
	assertEmptyErrorsArray(t, "mem1 dead-letters", out.Bytes(), nullErrorsJob)
}
