package curation

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/storage"
)

func TestOpenAICuratorFailedAttemptCarriesBilledUsage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{
				"content": `{"mutations":[{"inbox_ids":["I1"],"action":"create","category":"workflow_note","target_memory_id":null,"durability":"working","topic_key":"charge-bench","summary_stub":"Bench","body":"Bench retries","depends_on":[{"target_memory_id":"M1","expected_to_rev":true,"rationale":"x"}],"rationale":"todo"}]}`,
			}}},
			"usage": map[string]int{"prompt_tokens": 612, "completion_tokens": 143},
		})
	}))
	defer srv.Close()

	cur, err := NewOpenAICurator(OpenAICuratorConfig{BaseURL: srv.URL, Model: "deepseek-chat", ProviderName: "deepseek", Timeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("new curator: %v", err)
	}
	req := CurateRequest{
		WorkspaceID:     "orca-eval-agg",
		WorkspaceKind:   storage.KindProject,
		SourceAuthority: "agent_observed",
		InboxItems:      []InboxItemView{{InboxID: uuid.New(), Content: "WIP TODO: add a benchmark for /v1/charge retries."}},
		Candidates:      []CandidateView{{MemoryID: uuid.New(), TopicKey: "billing-api", CurrentVersion: 2}},
	}
	_, err = cur.CurateBatch(context.Background(), req)
	if !errors.Is(err, ErrPlanValidation) {
		t.Fatalf("expected ErrPlanValidation, got %v", err)
	}
	var attemptErr *CurateAttemptError
	if !errors.As(err, &attemptErr) {
		t.Fatalf("failed attempt must carry billed usage as *CurateAttemptError, got %T: %v", err, err)
	}
	if attemptErr.PromptTokens != 2*612 || attemptErr.CompletionTokens != 2*143 {
		t.Fatalf("failed attempt usage = %d/%d, want initial+repair %d/%d", attemptErr.PromptTokens, attemptErr.CompletionTokens, 2*612, 2*143)
	}
	if attemptErr.ProviderName != "deepseek" || attemptErr.ModelName != "deepseek-chat" {
		t.Fatalf("failed attempt provider/model = %q/%q", attemptErr.ProviderName, attemptErr.ModelName)
	}

	emptySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": ""}}},
			"usage":   map[string]int{"prompt_tokens": 580, "completion_tokens": 0},
		})
	}))
	defer emptySrv.Close()
	emptyCur, err := NewOpenAICurator(OpenAICuratorConfig{BaseURL: emptySrv.URL, Model: "deepseek-chat", Timeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("new curator: %v", err)
	}
	_, err = emptyCur.CurateBatch(context.Background(), req)
	if !errors.As(err, &attemptErr) || attemptErr.PromptTokens != 580 {
		t.Fatalf("empty completion must still report its 580 billed prompt tokens, got %v", err)
	}
}
