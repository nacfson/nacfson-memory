package curation

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkoukk/tiktoken-go"
	tiktoken_loader "github.com/pkoukk/tiktoken-go-loader"

	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

func TestTokenCountingTruncationAndSpecialTokens(t *testing.T) {
	// Special tokens like <|endoftext|> must never fail or panic under EncodeOrdinary.
	special := "alpha beta <|endoftext|> gamma delta " + strings.Repeat("word ", 200)
	n := CountTokens(special)
	if n <= 150 {
		t.Fatalf("expected > 150 tokens, got %d", n)
	}
	if CountTokens("") != 0 {
		t.Fatalf("expected 0 tokens for empty string")
	}

	trunc, didTrunc := TruncateTokens(special, 25)
	if !didTrunc || CountTokens(trunc) > 25 {
		t.Fatalf("expected truncated <= 25 tokens, got didTrunc=%v tokens=%d", didTrunc, CountTokens(trunc))
	}
	if _, didZero := TruncateTokens("hello", 0); !didZero {
		t.Fatalf("expected maxTokens=0 on non-empty string to report truncated")
	}

	// Concurrent safety under -race.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if c := CountTokens("PostgreSQL 17 halfvec index <|endoftext|>"); c == 0 {
					t.Errorf("unexpected zero count")
				}
			}
		}()
	}
	wg.Wait()
}

func TestTruncateProvisionalBodyAndSummary(t *testing.T) {
	short, n := TruncateProvisionalBody("  short note  ")
	if short != "short note" || n != CountTokens("short note") {
		t.Fatalf("unexpected short provisional body: %q (%d)", short, n)
	}

	huge := strings.Repeat("postgres pgvector curation backoff retry configuration detail ", 350)
	trunc, tn := TruncateProvisionalBody(huge)
	if !strings.HasSuffix(trunc, ProvisionalTruncatedSuffix) {
		t.Fatalf("expected truncated suffix, got tail %q", trunc[len(trunc)-50:])
	}
	if tn > MaxBodyTokens || CountTokens(trunc) > MaxBodyTokens {
		t.Fatalf("provisional body exceeded %d tokens: %d", MaxBodyTokens, tn)
	}

	stub := ExtractProvisionalSummary("line one\n\nline two\r\n  " + strings.Repeat("x", 200))
	if !IsValidSummaryStub(stub) {
		t.Fatalf("expected valid single-line <= 180 rune stub, got %q", stub)
	}
	if ExtractProvisionalSummary("   ") != "provisional note" {
		t.Fatalf("expected default stub for whitespace")
	}
}

func TestBreakerTransitionsWithMockClock(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	b := NewBreakerWithClock(3, 60*time.Second, func() time.Time { return now })

	if !b.Allow() || b.IsOpen() {
		t.Fatalf("new breaker should allow calls")
	}
	if b.RecordFailure(errors.New("err1")) || b.IsOpen() {
		t.Fatalf("1st failure must not open breaker")
	}
	if b.RecordFailure(errors.New("err2")) || b.IsOpen() {
		t.Fatalf("2nd failure must not open breaker")
	}
	if !b.RecordFailure(errors.New("err3")) || !b.IsOpen() || b.Allow() {
		t.Fatalf("3rd failure must open breaker and block calls")
	}
	if st := b.Status(); st.State != BreakerOpen || st.CooldownRemainingMs <= 0 {
		t.Fatalf("unexpected open status: %+v", st)
	}

	// Advance past cooldown -> Allow transitions to half_open.
	now = now.Add(61 * time.Second)
	if b.IsOpen() || !b.Allow() {
		t.Fatalf("breaker should transition to half_open after cooldown")
	}
	if st := b.Status(); st.State != BreakerHalfOpen {
		t.Fatalf("expected half_open state, got %+v", st)
	}

	// Failure in half_open immediately re-opens breaker.
	if !b.RecordFailure(errors.New("probe fail")) || !b.IsOpen() {
		t.Fatalf("failure in half_open should re-open breaker")
	}

	// Recover -> closed.
	now = now.Add(61 * time.Second)
	if !b.Allow() {
		t.Fatalf("expected half_open allow after 2nd cooldown")
	}
	b.RecordSuccess()
	if st := b.Status(); st.State != BreakerClosed || st.ConsecutiveFailures != 0 {
		t.Fatalf("expected closed reset status, got %+v", st)
	}
}

func TestValidatePlanAllRules(t *testing.T) {
	in1 := uuid.New()
	in2 := uuid.New()
	memA := uuid.New()
	memB := uuid.New()
	memConst := uuid.New()

	baseProjectVctx := ValidationContext{
		WorkspaceID:     "payments",
		WorkspaceKind:   storage.KindProject,
		SourceAuthority: "agent_observed",
		ClaimedInboxIDs: []uuid.UUID{in1, in2},
		ActiveCandidates: map[uuid.UUID]CandidateView{
			memA: {
				MemoryID: memA, Durability: storage.DurabilityDurable, TopicKey: "primary-db", CurrentVersion: 2,
			},
			memB: {
				MemoryID: memB, Durability: storage.DurabilityWorking, TopicKey: "wal-pragma", CurrentVersion: 1,
			},
			memConst: {
				MemoryID: memConst, Durability: storage.DurabilityConstitution, TopicKey: "rule-const", CurrentVersion: 1,
			},
		},
		ExistingEdges: map[uuid.UUID][]uuid.UUID{
			memB: {memA}, // B -> A
		},
	}

	validPlan := CuratePlan{
		Mutations: []CurateMutation{
			{
				InboxIDs:       []uuid.UUID{in1},
				Action:         ActionRevise,
				Category:       CategoryProjectTechnical,
				TargetMemoryID: &memA,
				Durability:     storage.DurabilityDurable,
				TopicKey:       "primary-db",
				SummaryStub:    "Primary DB updated details",
				Body:           "PostgreSQL 17 with streaming replication",
				Rationale:      "additive update",
			},
			{
				InboxIDs:  []uuid.UUID{in2},
				Action:    ActionNoop,
				Category:  CategoryWorkflowNote,
				Rationale: "filler skipped",
			},
		},
	}
	if err := ValidatePlan(baseProjectVctx, validPlan); err != nil {
		t.Fatalf("expected valid plan to pass, got %v", err)
	}

	invalidCases := []struct {
		name string
		vctx ValidationContext
		plan CuratePlan
		want string
	}{
		{
			name: "missing inbox coverage",
			vctx: baseProjectVctx,
			plan: CuratePlan{Mutations: validPlan.Mutations[:1]},
			want: "covered 1 of 2",
		},
		{
			name: "unclaimed inbox id",
			vctx: baseProjectVctx,
			plan: CuratePlan{Mutations: []CurateMutation{
				{InboxIDs: []uuid.UUID{in1, in2, uuid.New()}, Action: ActionNoop, Rationale: "extra"},
			}},
			want: "unclaimed inbox_id",
		},
		{
			name: "constitution write forbidden",
			vctx: baseProjectVctx,
			plan: CuratePlan{Mutations: []CurateMutation{
				{
					InboxIDs: []uuid.UUID{in1, in2}, Action: ActionCreate, Category: CategoryProjectTechnical,
					Durability: storage.DurabilityConstitution, TopicKey: "forbidden", SummaryStub: "S", Body: "B", Rationale: "R",
				},
			}},
			want: "constitution",
		},
		{
			name: "cannot mutate constitution candidate",
			vctx: baseProjectVctx,
			plan: CuratePlan{Mutations: []CurateMutation{
				{
					InboxIDs: []uuid.UUID{in1, in2}, Action: ActionRevise, Category: CategoryProjectTechnical,
					TargetMemoryID: &memConst, Durability: storage.DurabilityDurable, TopicKey: "rule-const", SummaryStub: "S", Body: "B", Rationale: "R",
				},
			}},
			want: "cannot modify constitution",
		},
		{
			name: "general workspace rejects project_technical",
			vctx: ValidationContext{
				WorkspaceID:     storage.WorkspaceGeneral,
				WorkspaceKind:   storage.KindGeneral,
				ClaimedInboxIDs: []uuid.UUID{in1},
			},
			plan: CuratePlan{Mutations: []CurateMutation{
				{
					InboxIDs: []uuid.UUID{in1}, Action: ActionCreate, Category: CategoryProjectTechnical,
					Durability: storage.DurabilityDurable, TopicKey: "bad-general", SummaryStub: "S", Body: "B", Rationale: "R",
				},
			}},
			want: "personal_profile or personal_preference",
		},
		{
			name: "general workspace rejects working durability",
			vctx: ValidationContext{
				WorkspaceID:     storage.WorkspaceGeneral,
				WorkspaceKind:   storage.KindGeneral,
				ClaimedInboxIDs: []uuid.UUID{in1},
			},
			plan: CuratePlan{Mutations: []CurateMutation{
				{
					InboxIDs: []uuid.UUID{in1}, Action: ActionCreate, Category: CategoryPersonalProfile,
					Durability: storage.DurabilityWorking, TopicKey: "user-loc", SummaryStub: "S", Body: "B", Rationale: "R",
				},
			}},
			want: "working durability",
		},
		{
			name: "bad topic_key regex",
			vctx: baseProjectVctx,
			plan: CuratePlan{Mutations: []CurateMutation{
				{
					InboxIDs: []uuid.UUID{in1, in2}, Action: ActionCreate, Category: CategoryProjectTechnical,
					Durability: storage.DurabilityDurable, TopicKey: "UPPER_CASE", SummaryStub: "S", Body: "B", Rationale: "R",
				},
			}},
			want: "topic_key",
		},
		{
			name: "multiline summary_stub rejected",
			vctx: baseProjectVctx,
			plan: CuratePlan{Mutations: []CurateMutation{
				{
					InboxIDs: []uuid.UUID{in1, in2}, Action: ActionCreate, Category: CategoryProjectTechnical,
					Durability: storage.DurabilityDurable, TopicKey: "good-slug", SummaryStub: "line1\nline2", Body: "B", Rationale: "R",
				},
			}},
			want: "summary_stub",
		},
		{
			name: "oversized body > 1500 tokens",
			vctx: baseProjectVctx,
			plan: CuratePlan{Mutations: []CurateMutation{
				{
					InboxIDs: []uuid.UUID{in1, in2}, Action: ActionCreate, Category: CategoryProjectTechnical,
					Durability: storage.DurabilityDurable, TopicKey: "big-body", SummaryStub: "Stub",
					Body: strings.Repeat("token ", 1600), Rationale: "R",
				},
			}},
			want: "1500",
		},
		{
			name: "duplicate create topic_key in batch",
			vctx: baseProjectVctx,
			plan: CuratePlan{Mutations: []CurateMutation{
				{
					InboxIDs: []uuid.UUID{in1}, Action: ActionCreate, Category: CategoryProjectTechnical,
					Durability: storage.DurabilityDurable, TopicKey: "same-topic", SummaryStub: "S1", Body: "B1", Rationale: "R1",
				},
				{
					InboxIDs: []uuid.UUID{in2}, Action: ActionCreate, Category: CategoryProjectTechnical,
					Durability: storage.DurabilityDurable, TopicKey: "same-topic", SummaryStub: "S2", Body: "B2", Rationale: "R2",
				},
			}},
			want: "duplicate create topic_key",
		},
		{
			name: "dag cycle detected via existing edge B->A + proposed A->B",
			vctx: baseProjectVctx,
			plan: CuratePlan{Mutations: []CurateMutation{
				{
					InboxIDs: []uuid.UUID{in1, in2}, Action: ActionRevise, Category: CategoryProjectTechnical,
					TargetMemoryID: &memA, Durability: storage.DurabilityDurable, TopicKey: "primary-db", SummaryStub: "A", Body: "A",
					DependsOn: []EdgeSpec{{TargetMemoryID: memB, ExpectedToRev: 1, Rationale: "closes cycle"}},
					Rationale: "R",
				},
			}},
			want: "cycle",
		},
		{
			name: "exceeds 5 depends_on edges",
			vctx: baseProjectVctx,
			plan: CuratePlan{Mutations: []CurateMutation{
				{
					InboxIDs: []uuid.UUID{in1, in2}, Action: ActionCreate, Category: CategoryProjectTechnical,
					Durability: storage.DurabilityDurable, TopicKey: "many-edges", SummaryStub: "S", Body: "B",
					DependsOn: make([]EdgeSpec, graph.MaxActiveOutgoingEdges+1),
					Rationale: "R",
				},
			}},
			want: "depends_on",
		},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePlan(tc.vctx, tc.plan)
			if !errors.Is(err, ErrPlanValidation) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got err=%v, want ErrPlanValidation containing %q", err, tc.want)
			}
		})
	}
}

func TestOpenAICuratorAndMockCurator(t *testing.T) {
	ctx := context.Background()
	in1 := uuid.New()
	in2 := uuid.New()
	candID := uuid.New()

	// Valid structured completion server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer sk-cur" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		planJSON, _ := json.Marshal(map[string]any{
			"mutations": []map[string]any{
				{
					"inbox_ids":        []string{in1.String()},
					"action":           "create",
					"category":         "project_technical",
					"target_memory_id": "",
					"durability":       "durable",
					"topic_key":        "queue-engine",
					"summary_stub":     "Use River on PostgreSQL 17",
					"body":             "River handles curation and embedding queues.",
					"depends_on":       nil,
					"rationale":        "durable architecture choice",
				},
			},
		})
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]any{"content": "```json\n" + string(planJSON) + "\n```"}},
			},
			"usage": map[string]int{"prompt_tokens": 120, "completion_tokens": 45},
		})
	}))
	defer srv.Close()

	cur, err := NewOpenAICurator(OpenAICuratorConfig{
		BaseURL:      srv.URL + "/v1",
		APIKey:       "sk-cur",
		Model:        "gpt-4.1-mini",
		ProviderName: "openai",
		Timeout:      2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new curator: %v", err)
	}
	plan, err := cur.CurateBatch(ctx, CurateRequest{
		WorkspaceID:     "app",
		WorkspaceKind:   storage.KindProject,
		SourceAuthority: "agent_observed",
		InboxItems:      []InboxItemView{{InboxID: in1, Content: "Use River on Postgres 17"}},
	})
	if err != nil {
		t.Fatalf("CurateBatch failed: %v", err)
	}
	if plan.PromptTokens != 120 || plan.CompletionTokens != 45 || len(plan.Mutations) != 1 || plan.Mutations[0].TargetMemoryID != nil {
		t.Fatalf("unexpected decoded plan: %+v", plan)
	}

	// Malformed JSON -> ErrMalformedPlan (wraps ErrPlanValidation).
	malSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": "{bad"}}},
		})
	}))
	defer malSrv.Close()
	malCur, _ := NewOpenAICurator(OpenAICuratorConfig{BaseURL: malSrv.URL, Model: "m"})
	if _, err := malCur.CurateBatch(ctx, CurateRequest{}); !errors.Is(err, ErrPlanValidation) {
		t.Fatalf("expected ErrPlanValidation on malformed output, got %v", err)
	}

	// HTTP 500 -> transient error (NOT ErrPlanValidation).
	errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream overloaded", http.StatusServiceUnavailable)
	}))
	defer errSrv.Close()
	errCur, _ := NewOpenAICurator(OpenAICuratorConfig{BaseURL: errSrv.URL, Model: "m"})
	if _, err := errCur.CurateBatch(ctx, CurateRequest{}); err == nil || errors.Is(err, ErrPlanValidation) {
		t.Fatalf("expected non-validation transient error on HTTP 503, got %v", err)
	}

	// MockCurator: 'general' boundary drops technical item to noop, preserves personal fact,
	// and project workspace emits supersede / retire / create cleanly.
	mc := NewMockCurator()
	genPlan, err := mc.CurateBatch(ctx, CurateRequest{
		WorkspaceID:   storage.WorkspaceGeneral,
		WorkspaceKind: storage.KindGeneral,
		InboxItems: []InboxItemView{
			{InboxID: in1, Content: "User timezone is Asia/Seoul"},
			{InboxID: in2, Content: "Deploy Postgres 17 API container in Kubernetes"},
		},
	})
	if err != nil {
		t.Fatalf("MockCurator general failed: %v", err)
	}
	if err := ValidatePlan(ValidationContext{
		WorkspaceID: storage.WorkspaceGeneral, WorkspaceKind: storage.KindGeneral, ClaimedInboxIDs: []uuid.UUID{in1, in2},
	}, genPlan); err != nil {
		t.Fatalf("MockCurator general plan failed ValidatePlan: %v", err)
	}

	projPlan, err := mc.CurateBatch(ctx, CurateRequest{
		WorkspaceID:   "app",
		WorkspaceKind: storage.KindProject,
		InboxItems: []InboxItemView{
			{InboxID: in1, Content: "topic:primary-db Switched primary database from SQLite to PostgreSQL 17"},
			{InboxID: in2, Content: "[NOOP] ok sounds good"},
		},
		Candidates: []CandidateView{
			{MemoryID: candID, Durability: storage.DurabilityDurable, TopicKey: "primary-db", CurrentVersion: 1, SummaryStub: "SQLite", Body: "Use SQLite"},
		},
	})
	if err != nil {
		t.Fatalf("MockCurator project failed: %v", err)
	}
	if projPlan.Mutations[0].Action != ActionSupersede || projPlan.Mutations[1].Action != ActionNoop {
		t.Fatalf("unexpected mock project mutations: %+v", projPlan.Mutations)
	}

	// Verify PeriodicJobs constructor & interval.
	pJobs := PeriodicJobs()
	if len(pJobs) != 1 {
		t.Fatalf("expected 1 periodic job, got %d", len(pJobs))
	}

	// OpenAICurator validation errors on config & bad UUID payloads.
	if _, err := NewOpenAICurator(OpenAICuratorConfig{Model: "m"}); err == nil {
		t.Fatalf("expected error on missing curator baseURL")
	}
	if _, err := NewOpenAICurator(OpenAICuratorConfig{BaseURL: "https://api.openai.com/v1"}); err == nil {
		t.Fatalf("expected error on missing curator model")
	}
	badUUIDSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]any{"content": `{"mutations":[{"inbox_ids":["not-a-uuid"],"action":"noop","rationale":"x"}]}`}},
			},
		})
	}))
	defer badUUIDSrv.Close()
	badCur, _ := NewOpenAICurator(OpenAICuratorConfig{BaseURL: badUUIDSrv.URL, Model: "m"})
	if _, err := badCur.CurateBatch(ctx, CurateRequest{}); !errors.Is(err, ErrMalformedPlan) {
		t.Fatalf("expected ErrMalformedPlan on invalid inbox_id UUID, got %v", err)
	}

	// json_schema unsupported HTTP 400 automatically falls back to json_object and remembers setting.
	var seenFormats []string
	fallbackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ResponseFormat struct {
				Type string `json:"type"`
			} `json:"response_format"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		seenFormats = append(seenFormats, body.ResponseFormat.Type)
		if body.ResponseFormat.Type == "json_schema" {
			http.Error(w, `{"error":{"message":"response_format json_schema is unavailable"}}`, http.StatusBadRequest)
			return
		}
		planJSON, _ := json.Marshal(map[string]any{
			"mutations": []map[string]any{
				{
					"inbox_ids":  []string{in1.String()},
					"action":     "noop",
					"category":   "project_technical",
					"rationale":  "fallback ok",
					"depends_on": []any{},
				},
			},
		})
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": string(planJSON)}}},
		})
	}))
	defer fallbackSrv.Close()

	fbCur, _ := NewOpenAICurator(OpenAICuratorConfig{BaseURL: fallbackSrv.URL, Model: "deepseek-chat"})
	if plan, err := fbCur.CurateBatch(ctx, CurateRequest{InboxItems: []InboxItemView{{InboxID: in1, Content: "ok"}}}); err != nil || len(plan.Mutations) != 1 {
		t.Fatalf("expected json_object fallback success, got plan=%+v err=%v", plan, err)
	}
	if _, err := fbCur.CurateBatch(ctx, CurateRequest{InboxItems: []InboxItemView{{InboxID: in1, Content: "ok"}}}); err != nil {
		t.Fatalf("second call with cached json_object failed: %v", err)
	}
	if len(seenFormats) != 3 || seenFormats[0] != "json_schema" || seenFormats[1] != "json_object" || seenFormats[2] != "json_object" {
		t.Fatalf("unexpected response_format sequence: %v", seenFormats)
	}
}

func TestCountTokensMatchesTiktokenExact(t *testing.T) {
	tiktoken.SetBpeLoader(tiktoken_loader.NewOfflineLoader())
	enc, err := tiktoken.GetEncoding("cl100k_base")
	if err != nil {
		t.Fatalf("load cl100k_base reference encoding: %v", err)
	}

	samples := []string{
		"",
		"a",
		"'",
		"''",
		"'''",
		"'s",
		"'S",
		"'t",
		"'T",
		"'re",
		"'RE",
		"'Re",
		"'ve",
		"'VE",
		"'m",
		"'M",
		"'ll",
		"'LL",
		"'d",
		"'D",
		"'sfoo",
		"'REALLY",
		"'a",
		"'1",
		"I'M",
		"wouldn't've",
		"hello world",
		"I'll verify it's working; they're testing what we've built and shouldn't skip edge-cases.",
		`{"memories":[{"memory_id":"019a0000-0000-7000-8000-000000000111","workspace_id":"ws-1","durability":"durable","topic_key":"primary-db","summary_stub":"PG 17","body":"Primary DB is PostgreSQL 17 with pgvector HALFVEC(1536)."}],"stubs":[],"manifest_etag":"W/\"ws:1+gen:0\"","workspace_revision":1,"degraded":false}`,
		" ",
		"  ",
		"   ",
		" 1",
		"  1",
		"   123",
		"   1234",
		"\t",
		"\t1",
		"\t\t 1",
		"\t  foo",
		"   foo",
		"\u00a0foo",
		"\u3000\u30001",
		"\r\n",
		"\n\r",
		"  \r\n",
		" \r\n  ",
		"  \r\n  foo",
		" \n \n  foo",
		"!",
		" !",
		"  !",
		"!foo",
		" !foo",
		"!?\r\n\n  bar",
		" !\r\n\r",
		"1234567890 42 3.1415926535 100ms 1536 HALFVEC(1536) ²³⁴",
		"서울특별시 PostgreSQL 17 메모리 테스트 🚀✨ — em-dash & non-ASCII naïve résumé",
		"alpha <|endoftext|> beta \r\n\t   gamma   \n\n\ndelta",
		strings.Repeat("PostgreSQL 17 HNSW vector search & GIN lexical retrieval ", 40),
		strings.Repeat("a", 1000),
		strings.Repeat("ab", 350),
		strings.Repeat("한글", 200),
	}

	rng := rand.New(rand.NewSource(42))
	frags := []string{
		"a", "Foo", "BAR", "0", "42", "9999", " ", "  ", "\t", "\r", "\n", "\r\n",
		"'s", "'S", "'RE", "'ve", "'ll", "'m", "'d", "'", "!", "!?", ",", ":", "{", "}", `"`,
		"한글", "🚀", "\u00a0", "\u3000", "memory_id", "_", "-", "\n  ", "  1",
	}
	for i := 0; i < 300; i++ {
		var b strings.Builder
		parts := 1 + rng.Intn(14)
		for j := 0; j < parts; j++ {
			b.WriteString(frags[rng.Intn(len(frags))])
		}
		samples = append(samples, b.String())
	}

	for i, s := range samples {
		want := len(enc.EncodeOrdinary(s))
		got := CountTokens(s)
		if got != want {
			t.Fatalf("sample[%d] token mismatch: CountTokens=%d want=%d text=%q", i, got, want, s)
		}
	}

	// Verify lock-free concurrent execution under -race.
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				s := samples[(id+j)%len(samples)]
				want := len(enc.EncodeOrdinary(s))
				if got := CountTokens(s); got != want {
					t.Errorf("concurrent mismatch (%d,%d): got=%d want=%d", id, j, got, want)
				}
			}
		}(g)
	}
	wg.Wait()

	// Verify O(N log N) long unbroken piece performance and pure-Go TruncateTokens invariants.
	start := time.Now()
	longToks := CountTokens(strings.Repeat("a", 65536))
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond || longToks == 0 {
		t.Fatalf("unexpected 64KiB single-piece BPE performance: toks=%d elapsed=%s", longToks, elapsed)
	}
	for _, tc := range []struct {
		text      string
		maxTokens int
	}{
		{"hello 🚀 world 🎉 and more tokens", 4},
		{strings.Repeat("한글테스트", 60), 20},
		{strings.Repeat("a", 500), 10},
	} {
		trunc, did := TruncateTokens(tc.text, tc.maxTokens)
		if !did || CountTokens(trunc) > tc.maxTokens || CountTokens(trunc) != len(enc.EncodeOrdinary(trunc)) {
			t.Fatalf("TruncateTokens(%d) invalid: did=%v got=%d enc=%d", tc.maxTokens, did, CountTokens(trunc), len(enc.EncodeOrdinary(trunc)))
		}
	}
}
