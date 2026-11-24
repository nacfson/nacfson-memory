package eval

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadSamplesAndMockFixtures(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, SuiteLongMemEval+".jsonl")
	raw := "{\"sample_id\":\"s1\",\"sessions\":[{\"session_id\":\"1\",\"occurred_at\":\"2026-10-01T00:00:00Z\",\"turns\":[{\"role\":\"user\",\"content\":\"hello\"}]}],\"question\":\"q1\",\"expected_answer\":\"a1\"}\n\n" +
		"{\"sample_id\":\"s2\",\"sessions\":[],\"question\":\"q2\",\"expected_answer\":\"a2\"}\n"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}

	all, err := LoadSamples(dir, SuiteLongMemEval, 0)
	if err != nil || len(all) != 2 || all[0].Dataset != SuiteLongMemEval {
		t.Fatalf("expected 2 samples, got %+v err=%v", all, err)
	}
	limited, err := LoadSamples(dir, SuiteLongMemEval, 1)
	if err != nil || len(limited) != 1 || limited[0].SampleID != "s1" {
		t.Fatalf("expected 1 limited sample, got %+v err=%v", limited, err)
	}

	if _, err := LoadSamples(dir, "missing", 0); err == nil {
		t.Fatalf("expected error on missing dataset file")
	}

	fixtures := MockFixtures()
	if len(fixtures) != 4 {
		t.Fatalf("expected 4 mock fixtures, got %d", len(fixtures))
	}
}

func TestWaitForQueueDrain(t *testing.T) {
	ctx := context.Background()
	var calls atomic.Int32

	err := WaitForQueueDrain(ctx, 10*time.Millisecond, time.Second, func(context.Context) (int64, int64, error) {
		n := calls.Add(1)
		if n < 3 {
			return 1, 1, nil
		}
		return 0, 0, nil
	})
	if err != nil || calls.Load() != 3 {
		t.Fatalf("expected drain after 3 polls, calls=%d err=%v", calls.Load(), err)
	}

	errTimeout := WaitForQueueDrain(ctx, 10*time.Millisecond, 35*time.Millisecond, func(context.Context) (int64, int64, error) {
		return 2, 0, nil
	})
	if errTimeout == nil {
		t.Fatalf("expected timeout error when queues stay non-empty")
	}
}

type scriptedJudge struct {
	outcomes map[int64]bool
	errAt    int64
}

func (s scriptedJudge) Grade(_ context.Context, _, _, _ string, seed int64) (bool, string, error) {
	if s.errAt == seed {
		return false, "", errors.New("judge failed")
	}
	return s.outcomes[seed], "ok", nil
}

func TestEvaluateMajorityThreeSeedVoting(t *testing.T) {
	ctx := context.Background()

	passed, votes, err := EvaluateMajority(ctx, scriptedJudge{
		outcomes: map[int64]bool{1: true, 2: false, 3: true},
	}, "q", "exp", "pred")
	if err != nil || !passed || votes != 2 {
		t.Fatalf("expected 2/3 majority pass, passed=%v votes=%d err=%v", passed, votes, err)
	}

	passedLow, votesLow, err := EvaluateMajority(ctx, scriptedJudge{
		outcomes: map[int64]bool{1: false, 2: true, 3: false},
	}, "q", "exp", "pred")
	if err != nil || passedLow || votesLow != 1 {
		t.Fatalf("expected 1/3 fail, passed=%v votes=%d err=%v", passedLow, votesLow, err)
	}

	if _, _, err := EvaluateMajority(ctx, scriptedJudge{errAt: 2}, "q", "exp", "pred"); err == nil {
		t.Fatalf("expected error propagation from judge")
	}
}

func TestOpenAIReaderJudgeAndMockReaderJudge(t *testing.T) {
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Seed *int64 `json:"seed"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		content := "PostgreSQL 17"
		if body.Seed != nil {
			content = `{"verdict":"correct","rationale":"matches"}`
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": content}}},
		})
	}))
	defer srv.Close()

	reader, err := NewOpenAIReader(OpenAIChatConfig{BaseURL: srv.URL, Model: "gpt-4.1-mini", APIKey: "k"})
	if err != nil {
		t.Fatalf("NewOpenAIReader: %v", err)
	}
	ans, err := reader.Answer(ctx, "What DB?", `{"memories":[{"body":"PostgreSQL 17"}]}`)
	if err != nil || ans != "PostgreSQL 17" {
		t.Fatalf("unexpected reader answer: %q err=%v", ans, err)
	}

	judge, err := NewOpenAIJudge(OpenAIChatConfig{BaseURL: srv.URL, Model: "gpt-4.1", APIKey: "k"})
	if err != nil {
		t.Fatalf("NewOpenAIJudge: %v", err)
	}
	passed, votes, err := EvaluateMajority(ctx, judge, "What DB?", "PostgreSQL 17", ans)
	if err != nil || !passed || votes != 3 {
		t.Fatalf("expected 3/3 OpenAIJudge pass, passed=%v votes=%d err=%v", passed, votes, err)
	}

	// MockReader & MockJudge offline checks.
	mr := MockReader{}
	mj := MockJudge{}
	mockAns, err := mr.Answer(ctx, "What time does the backup run?", `{"memories":[{"body":"Nightly PostgreSQL backup runs at 02:30 UTC."}]}`)
	if err != nil {
		t.Fatalf("MockReader: %v", err)
	}
	ok, _, err := mj.Grade(ctx, "What time?", "02:30 UTC", mockAns, 1)
	if err != nil || !ok {
		t.Fatalf("MockJudge failed on mock answer %q: ok=%v err=%v", mockAns, ok, err)
	}
}

func TestFinalizeReportGates(t *testing.T) {
	passReport := EvalReport{
		Results: []SampleResult{
			{Dataset: SuiteLongMemEval, Passed: true},
			{Dataset: SuiteLongMemEval, Passed: true},
			{Dataset: SuiteLongMemEval, Passed: true},
			{Dataset: SuiteLongMemEval, Passed: false},
			{Dataset: SuiteMemoryAgentBenchConfl, Passed: true},
		},
		Suites: make(map[string]SuiteSummary),
	}
	if !finalizeReport(&passReport) {
		t.Fatalf("expected report meeting 75%% and 100%% gates to pass: %+v", passReport)
	}

	failCap := passReport
	failCap.TokenCapViolations = 1
	failCap.Suites = make(map[string]SuiteSummary)
	if finalizeReport(&failCap) {
		t.Fatalf("expected token cap violation to fail report")
	}

	failBoundary := passReport
	failBoundary.BoundaryViolations = 1
	failBoundary.Suites = make(map[string]SuiteSummary)
	if finalizeReport(&failBoundary) {
		t.Fatalf("expected boundary violation to fail report")
	}
}
