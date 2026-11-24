package curation

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/storage"
)

func TestAliasTableInjectionAndResolution(t *testing.T) {
	in1 := uuid.New()
	in2 := uuid.New()
	mem1 := uuid.New()
	mem2 := uuid.New()

	req := CurateRequest{
		WorkspaceID:     "payments",
		WorkspaceKind:   storage.KindProject,
		SourceAuthority: "agent_observed",
		InboxItems: []InboxItemView{
			{InboxID: in1, AgentID: "claude", OccurredAt: time.Now().UTC(), Content: "Turn one"},
			{InboxID: in2, AgentID: "cursor", OccurredAt: time.Now().UTC(), Content: "Turn two"},
		},
		Candidates: []CandidateView{
			{MemoryID: mem1, Durability: storage.DurabilityDurable, TopicKey: "primary-db", CurrentVersion: 2, SummaryStub: "Postgres", Body: "Use PostgreSQL 17"},
			{MemoryID: mem2, Durability: storage.DurabilityWorking, TopicKey: "webhook-port", CurrentVersion: 1, SummaryStub: "Port 8080", Body: "Listens on 8080"},
		},
	}

	aliases := BuildAliasTable(req)
	payloadJSON, err := json.Marshal(aliases.BuildPromptPayload())
	if err != nil {
		t.Fatalf("marshal prompt payload: %v", err)
	}
	payloadStr := string(payloadJSON)

	if !strings.Contains(payloadStr, `"I1"`) || !strings.Contains(payloadStr, `"I2"`) || !strings.Contains(payloadStr, `"M1"`) || !strings.Contains(payloadStr, `"M2"`) {
		t.Fatalf("expected short aliases in prompt payload, got %s", payloadStr)
	}
	for _, rawID := range []uuid.UUID{in1, in2, mem1, mem2} {
		if strings.Contains(payloadStr, rawID.String()) {
			t.Fatalf("prompt payload leaked raw UUID %s: %s", rawID, payloadStr)
		}
	}

	// Short case-insensitive refs and full UUIDs resolve identically.
	if got, err := aliases.ResolveInboxRef("i1"); err != nil || got != in1 {
		t.Fatalf("ResolveInboxRef(i1) = %v, %v; want %v", got, err, in1)
	}
	if got, err := aliases.ResolveInboxRef(in2.String()); err != nil || got != in2 {
		t.Fatalf("ResolveInboxRef(UUID) = %v, %v; want %v", got, err, in2)
	}
	if got, err := aliases.ResolveMemoryRef("m2"); err != nil || got != mem2 {
		t.Fatalf("ResolveMemoryRef(m2) = %v, %v; want %v", got, err, mem2)
	}
	if got, err := aliases.ResolveMemoryRef(mem1.String()); err != nil || got != mem1 {
		t.Fatalf("ResolveMemoryRef(UUID) = %v, %v; want %v", got, err, mem1)
	}
	if got, err := aliases.ResolveMemoryRef(""); err != nil || got != uuid.Nil {
		t.Fatalf("ResolveMemoryRef empty = %v, %v; want Nil", got, err)
	}

	for _, bad := range []string{"I99", "not-a-uuid", uuid.New().String()} {
		if _, err := aliases.ResolveInboxRef(bad); !errors.Is(err, ErrMalformedPlan) {
			t.Fatalf("ResolveInboxRef(%q) err=%v; want ErrMalformedPlan", bad, err)
		}
	}
	for _, bad := range []string{"M99", "not-a-uuid", uuid.New().String()} {
		if _, err := aliases.ResolveMemoryRef(bad); !errors.Is(err, ErrMalformedPlan) {
			t.Fatalf("ResolveMemoryRef(%q) err=%v; want ErrMalformedPlan", bad, err)
		}
	}
}

func TestCompileRawPlanNormalizationAndShorthand(t *testing.T) {
	in1 := uuid.New()
	in2 := uuid.New()
	in3 := uuid.New()
	mem1 := uuid.New()
	mem2 := uuid.New()

	req := CurateRequest{
		WorkspaceID:     "app",
		WorkspaceKind:   storage.KindProject,
		SourceAuthority: "agent_observed",
		InboxItems: []InboxItemView{
			{InboxID: in1, Content: "Switch DB"},
			{InboxID: in2, Content: "Extra note on billing"},
			{InboxID: in3, Content: "thanks!"},
		},
		Candidates: []CandidateView{
			{MemoryID: mem1, Durability: storage.DurabilityDurable, TopicKey: "primary-db", CurrentVersion: 3, SummaryStub: "Postgres", Body: "Postgres 17"},
			{MemoryID: mem2, Durability: storage.DurabilityDurable, TopicKey: "auth-gateway", CurrentVersion: 2, SummaryStub: "JWT", Body: "JWT auth"},
		},
	}
	aliases := BuildAliasTable(req)

	longStub := "line one\nline two   " + strings.Repeat("a", 210)
	rawJSON := "```json\n" + `{
	  "mutations": [
	    {
	      "refs": ["I1", "i1"],
	      "op": "REVISE",
	      "target": "m1",
	      "durability": "DURABLE",
	      "topic_key": "",
	      "summary": ` + jsonString(longStub) + `,
	      "text": "PostgreSQL 17 with pgvector",
	      "depends_on": [
	        {"target": "M2", "expected_to_rev": 0, "rationale": ""},
	        {"target_memory_id": "M2", "expected_to_rev": 2, "rationale": "dup edge"}
	      ],
	      "rationale": ""
	    },
	    {
	      "inbox_ids": ["I2"],
	      "action": "create",
	      "category": "",
	      "target_memory_id": null,
	      "durability": "durable",
	      "topic_key": "  Billing_Service__API! ",
	      "summary_stub": "",
	      "body": "Billing API uses Go stdlib ServeMux.\nHandles checkout webhooks.",
	      "depends_on": [],
	      "rationale": "new service note"
	    },
	    {
	      "inbox_ids": ["I3"],
	      "action": "noop",
	      "target_memory_id": "M1",
	      "durability": "working",
	      "topic_key": "stray-key",
	      "summary_stub": "stray stub",
	      "body": "stray body",
	      "depends_on": [{"target_memory_id": "M2", "expected_to_rev": 2, "rationale": "stray"}],
	      "rationale": ""
	    }
	  ]
	}` + "\n```"

	muts, err := CompileRawPlan(aliases, rawJSON)
	if err != nil {
		t.Fatalf("CompileRawPlan failed: %v", err)
	}
	if len(muts) != 3 {
		t.Fatalf("expected 3 mutations, got %d", len(muts))
	}

	// Mutation 0: deduplicated I1, resolved M1, inherited topic_key, single-line <= 180 rune stub, filled edge rev.
	m0 := muts[0]
	if len(m0.InboxIDs) != 1 || m0.InboxIDs[0] != in1 {
		t.Fatalf("expected deduplicated [in1], got %v", m0.InboxIDs)
	}
	if m0.Action != ActionRevise || m0.TargetMemoryID == nil || *m0.TargetMemoryID != mem1 {
		t.Fatalf("unexpected m0 target/action: %+v", m0)
	}
	if m0.TopicKey != "primary-db" {
		t.Fatalf("expected inherited topic_key primary-db, got %q", m0.TopicKey)
	}
	if !IsValidSummaryStub(m0.SummaryStub) || utf8.RuneCountInString(m0.SummaryStub) > MaxSummaryStubRunes {
		t.Fatalf("invalid normalized stub (%d runes): %q", utf8.RuneCountInString(m0.SummaryStub), m0.SummaryStub)
	}
	if m0.Category != CategoryProjectTechnical {
		t.Fatalf("expected default project_technical category, got %q", m0.Category)
	}
	if len(m0.DependsOn) != 1 || m0.DependsOn[0].TargetMemoryID != mem2 || m0.DependsOn[0].ExpectedToRev != 2 || m0.DependsOn[0].Rationale == "" {
		t.Fatalf("unexpected compiled depends_on: %+v", m0.DependsOn)
	}

	// Mutation 1: slugified topic_key + derived summary_stub from body.
	m1 := muts[1]
	if m1.TopicKey != "billing-service-api" {
		t.Fatalf("expected slugified topic_key billing-service-api, got %q", m1.TopicKey)
	}
	if !strings.HasPrefix(m1.SummaryStub, "Billing API uses Go stdlib ServeMux.") || strings.Contains(m1.SummaryStub, "\n") {
		t.Fatalf("expected single-line stub derived from body, got %q", m1.SummaryStub)
	}

	// Mutation 2: noop clears stray fields.
	m2 := muts[2]
	if m2.TargetMemoryID != nil || m2.Durability != "" || m2.TopicKey != "" || m2.SummaryStub != "" || m2.Body != "" || len(m2.DependsOn) != 0 || m2.Rationale == "" {
		t.Fatalf("expected noop stray fields cleared, got %+v", m2)
	}

	// Compiled plan passes ValidatePlan cleanly.
	vctx := ValidationContext{
		WorkspaceID:      "app",
		WorkspaceKind:    storage.KindProject,
		SourceAuthority:  "agent_observed",
		ClaimedInboxIDs:  []uuid.UUID{in1, in2, in3},
		ActiveCandidates: map[uuid.UUID]CandidateView{mem1: req.Candidates[0], mem2: req.Candidates[1]},
	}
	if err := ValidatePlan(vctx, CuratePlan{Mutations: muts}); err != nil {
		t.Fatalf("ValidatePlan rejected compiled plan: %v", err)
	}
}

func TestCompileRawPlanPreservesSecurityInvariants(t *testing.T) {
	in1 := uuid.New()

	cases := []struct {
		name      string
		wsID      string
		wsKind    string
		rawJSON   string
		wantError string
	}{
		{
			name:   "constitution durability never masked",
			wsID:   "app",
			wsKind: storage.KindProject,
			rawJSON: `{"mutations":[{"inbox_ids":["I1"],"action":"create","category":"project_technical","target_memory_id":null,"durability":"constitution","topic_key":"bad-const","summary_stub":"Rule","body":"Cannot write constitution","depends_on":[],"rationale":"test"}]}`,
			wantError: "constitution",
		},
		{
			name:   "project_technical in general never masked",
			wsID:   storage.WorkspaceGeneral,
			wsKind: storage.KindGeneral,
			rawJSON: `{"mutations":[{"inbox_ids":["I1"],"action":"create","category":"project_technical","target_memory_id":null,"durability":"durable","topic_key":"k8s-port","summary_stub":"Deploy k8s","body":"Deploy redis on k8s","depends_on":[],"rationale":"test"}]}`,
			wantError: "personal_profile or personal_preference",
		},
		{
			name:   "working in general never masked",
			wsID:   storage.WorkspaceGeneral,
			wsKind: storage.KindGeneral,
			rawJSON: `{"mutations":[{"inbox_ids":["I1"],"action":"create","category":"personal_profile","target_memory_id":null,"durability":"working","topic_key":"user-loc","summary_stub":"Seoul","body":"Lives in Seoul","depends_on":[],"rationale":"test"}]}`,
			wantError: "working durability",
		},
		{
			name:   "oversized body > 1500 tokens never silently truncated",
			wsID:   "app",
			wsKind: storage.KindProject,
			rawJSON: `{"mutations":[{"inbox_ids":["I1"],"action":"create","category":"project_technical","target_memory_id":null,"durability":"durable","topic_key":"huge","summary_stub":"Huge","body":` + jsonString(strings.Repeat("token ", 1600)) + `,"depends_on":[],"rationale":"test"}]}`,
			wantError: "1500",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := CurateRequest{
				WorkspaceID:     tc.wsID,
				WorkspaceKind:   tc.wsKind,
				SourceAuthority: "agent_observed",
				InboxItems:      []InboxItemView{{InboxID: in1, Content: "input"}},
			}
			aliases := BuildAliasTable(req)
			muts, err := CompileRawPlan(aliases, tc.rawJSON)
			if err != nil {
				t.Fatalf("CompileRawPlan unexpected error: %v", err)
			}
			err = ValidatePlan(localValidationContext(req), CuratePlan{Mutations: muts})
			if !errors.Is(err, ErrPlanValidation) || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("got err=%v, want ErrPlanValidation containing %q", err, tc.wantError)
			}
		})
	}
}

func TestOpenAICuratorAliasPromptAndOneShotRepair(t *testing.T) {
	ctx := context.Background()
	in1 := uuid.New()
	in2 := uuid.New()
	mem1 := uuid.New()

	req := CurateRequest{
		WorkspaceID:     storage.WorkspaceGeneral,
		WorkspaceKind:   storage.KindGeneral,
		SourceAuthority: "agent_observed",
		InboxItems: []InboxItemView{
			{InboxID: in1, Content: "I prefer concise Go examples."},
			{InboxID: in2, Content: "Deploy Redis cluster on port 6379."},
		},
		Candidates: []CandidateView{
			{MemoryID: mem1, Durability: storage.DurabilityDurable, TopicKey: "user-style", CurrentVersion: 1, SummaryStub: "Concise style", Body: "User likes concise answers."},
		},
	}

	// Case 1: Initial call omits I2 (fails 100% coverage), 1-shot repair succeeds using aliases.
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		var payload struct {
			Messages []chatMessage `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if n == 1 {
			if len(payload.Messages) != 2 {
				t.Errorf("expected 2 messages on initial call, got %d", len(payload.Messages))
			}
			userMsg := payload.Messages[1].Content
			if !strings.Contains(userMsg, `"I1"`) || !strings.Contains(userMsg, `"I2"`) || !strings.Contains(userMsg, `"M1"`) {
				t.Errorf("expected short aliases in user payload, got %s", userMsg)
			}
			if strings.Contains(userMsg, in1.String()) || strings.Contains(userMsg, mem1.String()) {
				t.Errorf("user payload leaked raw UUIDs: %s", userMsg)
			}
			// Invalid plan: omits I2.
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{
					{"message": map[string]any{"content": `{"mutations":[{"inbox_ids":["I1"],"action":"revise","category":"personal_preference","target_memory_id":"M1","durability":"durable","topic_key":"user-style","summary_stub":"Concise Go style","body":"Prefers concise Go examples.","depends_on":[],"rationale":"update"}]}`}},
				},
				"usage": map[string]int{"prompt_tokens": 100, "completion_tokens": 30},
			})
			return
		}

		// Second call must contain 4 messages including error feedback and aliases.
		if len(payload.Messages) != 4 || !strings.Contains(payload.Messages[3].Content, "covered 1 of 2") {
			t.Errorf("expected 4 repair messages citing validation error, got %+v", payload.Messages)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]any{"content": `{"mutations":[{"inbox_ids":["I1"],"action":"revise","category":"personal_preference","target_memory_id":"M1","durability":"durable","topic_key":"","summary_stub":"Concise Go examples","body":"User prefers concise Go examples.","depends_on":[],"rationale":"merge style"},{"inbox_ids":["I2"],"action":"noop","category":"personal_profile","target_memory_id":null,"durability":"","topic_key":"","summary_stub":"","body":"","depends_on":[],"rationale":"technical turn rejected in general"}]}`}},
			},
			"usage": map[string]int{"prompt_tokens": 140, "completion_tokens": 50},
		})
	}))
	defer srv.Close()

	cur, err := NewOpenAICurator(OpenAICuratorConfig{BaseURL: srv.URL, APIKey: "sk-test", Model: "m", Timeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("new curator: %v", err)
	}
	plan, err := cur.CurateBatch(ctx, req)
	if err != nil {
		t.Fatalf("CurateBatch expected repair success, got %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected exactly 2 calls (initial + 1 repair), got %d", calls.Load())
	}
	if plan.PromptTokens != 240 || plan.CompletionTokens != 80 {
		t.Fatalf("expected summed tokens 240/80, got %d/%d", plan.PromptTokens, plan.CompletionTokens)
	}
	if len(plan.Mutations) != 2 || plan.Mutations[0].TargetMemoryID == nil || *plan.Mutations[0].TargetMemoryID != mem1 {
		t.Fatalf("expected resolved TargetMemoryID %s, got %+v", mem1, plan.Mutations)
	}
	if plan.Mutations[0].InboxIDs[0] != in1 || plan.Mutations[1].InboxIDs[0] != in2 || plan.Mutations[1].Action != ActionNoop {
		t.Fatalf("unexpected resolved mutations: %+v", plan.Mutations)
	}

	// Case 2: Both initial and repair fail -> stops after 2 calls (no infinite loop) and returns ErrPlanValidation.
	var failCalls atomic.Int32
	alwaysFailSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]any{"content": `{"mutations":[{"inbox_ids":["I1","I2"],"action":"create","category":"project_technical","target_memory_id":null,"durability":"durable","topic_key":"bad-gen","summary_stub":"Bad","body":"Technical in general","depends_on":[],"rationale":"bad"}]}`}},
			},
		})
	}))
	defer alwaysFailSrv.Close()

	failCur, _ := NewOpenAICurator(OpenAICuratorConfig{BaseURL: alwaysFailSrv.URL, Model: "m", Timeout: 3 * time.Second})
	if _, err := failCur.CurateBatch(ctx, req); !errors.Is(err, ErrPlanValidation) {
		t.Fatalf("expected ErrPlanValidation after 1 repair failure, got %v", err)
	}
	if failCalls.Load() != 2 {
		t.Fatalf("expected exactly 2 calls on unrecoverable validation failure, got %d", failCalls.Load())
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
