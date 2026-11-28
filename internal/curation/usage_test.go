package curation

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/storage"
)

const (
	booleanEdgePlan = `{"mutations":[{"inbox_ids":["I1"],"action":"create","category":"workflow_note","target_memory_id":null,"durability":"working","topic_key":"charge-bench","summary_stub":"Bench","body":"Bench retries","depends_on":[{"target_memory_id":"M1","expected_to_rev":true,"rationale":"x"}],"rationale":"todo"}]}`
	uncoveredPlan   = `{"mutations":[]}`
	validNoopPlan   = `{"mutations":[{"inbox_ids":["I1"],"action":"noop","category":"project_technical","target_memory_id":null,"durability":"","topic_key":"","summary_stub":"","body":"","depends_on":[],"rationale":"filler"}]}`
)

// providerReply is one scripted chat completion: an HTTP status, assistant content, and usage.
type providerReply struct {
	status            int
	content           string
	prompt, completed int
}

func scriptedProvider(t *testing.T, replies ...providerReply) *httptest.Server {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(calls.Add(1)) - 1
		if i >= len(replies) {
			t.Errorf("unexpected provider call %d", i+1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		reply := replies[i]
		if reply.status != 0 && reply.status != http.StatusOK {
			w.WriteHeader(reply.status)
			_, _ = w.Write([]byte(`{"error":{"message":"overloaded"}}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": reply.content}}},
			"usage":   map[string]int{"prompt_tokens": reply.prompt, "completion_tokens": reply.completed},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func usageTestRequest() CurateRequest {
	return CurateRequest{
		WorkspaceID:     "orca-eval-agg",
		WorkspaceKind:   storage.KindProject,
		SourceAuthority: "agent_observed",
		InboxItems:      []InboxItemView{{InboxID: uuid.New(), Content: "WIP TODO: add a benchmark for /v1/charge retries."}},
		Candidates:      []CandidateView{{MemoryID: uuid.New(), TopicKey: "billing-api", CurrentVersion: 2}},
	}
}

func TestOpenAICuratorFailedAttemptCarriesBilledUsage(t *testing.T) {
	cases := []struct {
		name           string
		replies        []providerReply
		wantValidation bool
		want           CurateUsage
	}{
		{
			name:           "compile failure on initial and repair",
			replies:        []providerReply{{content: booleanEdgePlan, prompt: 612, completed: 143}, {content: booleanEdgePlan, prompt: 640, completed: 150}},
			wantValidation: true,
			want:           CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", PromptTokens: 1252, CompletionTokens: 293, APICalls: 2},
		},
		{
			name:           "plan validation failure on initial and repair",
			replies:        []providerReply{{content: uncoveredPlan, prompt: 600, completed: 20}, {content: uncoveredPlan, prompt: 630, completed: 22}},
			wantValidation: true,
			want:           CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", PromptTokens: 1230, CompletionTokens: 42, APICalls: 2},
		},
		{
			name:    "repair call fails after a decoded initial call",
			replies: []providerReply{{content: booleanEdgePlan, prompt: 612, completed: 143}, {status: http.StatusServiceUnavailable}},
			want:    CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", PromptTokens: 612, CompletionTokens: 143, APICalls: 2},
		},
		{
			name:           "empty completion",
			replies:        []providerReply{{content: "", prompt: 580}},
			wantValidation: true,
			want:           CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", PromptTokens: 580, APICalls: 1},
		},
		{
			name:           "decoded responses that report no token usage",
			replies:        []providerReply{{content: uncoveredPlan}, {content: uncoveredPlan}},
			wantValidation: true,
			want:           CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", APICalls: 2},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := scriptedProvider(t, tc.replies...)
			cur, err := NewOpenAICurator(OpenAICuratorConfig{BaseURL: srv.URL, Model: "deepseek-chat", ProviderName: "deepseek", Timeout: 3 * time.Second})
			if err != nil {
				t.Fatalf("new curator: %v", err)
			}
			_, err = cur.CurateBatch(context.Background(), usageTestRequest())
			if err == nil {
				t.Fatal("expected CurateBatch to fail")
			}
			if got := errors.Is(err, ErrPlanValidation); got != tc.wantValidation {
				t.Fatalf("errors.Is(err, ErrPlanValidation) = %v, want %v (err: %v)", got, tc.wantValidation, err)
			}
			var usageErr *CurateUsageError
			if !errors.As(err, &usageErr) {
				t.Fatalf("failed attempt must carry usage as *CurateUsageError, got %T: %v", err, err)
			}
			if usageErr.Usage != tc.want {
				t.Fatalf("usage = %+v, want %+v", usageErr.Usage, tc.want)
			}
		})
	}
}

func TestOpenAICuratorFailureWithoutDecodedResponseCarriesAttribution(t *testing.T) {
	srv := scriptedProvider(t, providerReply{status: http.StatusServiceUnavailable})
	cur, err := NewOpenAICurator(OpenAICuratorConfig{
		BaseURL:      srv.URL,
		Model:        "deepseek-chat",
		ProviderName: "deepseek",
		Timeout:      3 * time.Second,
	})
	if err != nil {
		t.Fatalf("new curator: %v", err)
	}
	_, err = cur.CurateBatch(context.Background(), usageTestRequest())
	if err == nil {
		t.Fatal("expected CurateBatch to fail on HTTP 503")
	}
	if errors.Is(err, ErrPlanValidation) {
		t.Fatalf("HTTP 503 must not match ErrPlanValidation, got %v", err)
	}
	var usageErr *CurateUsageError
	if !errors.As(err, &usageErr) {
		t.Fatalf("HTTP 503 failure must carry attribution as *CurateUsageError, got %T: %v", err, err)
	}
	want := CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", APICalls: 1}
	if usageErr.Usage != want {
		t.Fatalf("usage = %+v, want %+v", usageErr.Usage, want)
	}
}

func TestOpenAICuratorRepairedPlanCarriesCumulativeUsage(t *testing.T) {
	srv := scriptedProvider(t,
		providerReply{content: booleanEdgePlan, prompt: 612, completed: 143},
		providerReply{content: validNoopPlan, prompt: 700, completed: 40},
	)
	cur, err := NewOpenAICurator(OpenAICuratorConfig{BaseURL: srv.URL, Model: "deepseek-chat", ProviderName: "deepseek", Timeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("new curator: %v", err)
	}
	plan, err := cur.CurateBatch(context.Background(), usageTestRequest())
	if err != nil {
		t.Fatalf("CurateBatch after repair: %v", err)
	}
	want := CurateUsage{ProviderName: "deepseek", ModelName: "deepseek-chat", PromptTokens: 1312, CompletionTokens: 183, APICalls: 2}
	if plan.CurateUsage != want {
		t.Fatalf("repaired plan usage = %+v, want %+v", plan.CurateUsage, want)
	}
}
