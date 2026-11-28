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

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/storage"
)

// typedEdgeExample returns the first JSON object embedded in prompt that carries both
// target_memory_id and expected_to_rev keys, decoded with exact number preservation.
func typedEdgeExample(prompt string) (map[string]any, bool) {
	for i := strings.IndexByte(prompt, '{'); i >= 0; {
		dec := json.NewDecoder(strings.NewReader(prompt[i:]))
		dec.UseNumber()
		var obj map[string]any
		if err := dec.Decode(&obj); err == nil {
			_, hasTarget := obj["target_memory_id"]
			_, hasRev := obj["expected_to_rev"]
			if hasTarget && hasRev {
				return obj, true
			}
		}
		next := strings.IndexByte(prompt[i+1:], '{')
		if next < 0 {
			break
		}
		i += next + 1
	}
	return nil, false
}

func TestJSONObjectFallbackPromptShowsIntegerExpectedToRev(t *testing.T) {
	var fallbackSystemPrompt atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			ResponseFormat map[string]any `json:"response_format"`
			Messages       []chatMessage  `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if payload.ResponseFormat["type"] == "json_schema" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"This response_format type is unavailable now"}}`))
			return
		}
		fallbackSystemPrompt.Store(payload.Messages[0].Content)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{
				"content": `{"mutations":[{"inbox_ids":["I1"],"action":"noop","category":"project_technical","target_memory_id":null,"durability":"","topic_key":"","summary_stub":"","body":"","depends_on":[],"rationale":"filler"}]}`,
			}}},
		})
	}))
	defer srv.Close()

	cur, err := NewOpenAICurator(OpenAICuratorConfig{BaseURL: srv.URL, Model: "deepseek-chat", Timeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("new curator: %v", err)
	}
	req := CurateRequest{
		WorkspaceID:     "orca-eval-agg",
		WorkspaceKind:   storage.KindProject,
		SourceAuthority: "agent_observed",
		InboxItems:      []InboxItemView{{InboxID: uuid.New(), Content: "hello"}},
	}
	if _, err := cur.CurateBatch(context.Background(), req); err != nil {
		t.Fatalf("CurateBatch on json_object fallback: %v", err)
	}

	prompt, ok := fallbackSystemPrompt.Load().(string)
	if !ok {
		t.Fatal("curator never retried with response_format json_object")
	}
	example, found := typedEdgeExample(prompt)
	if !found {
		t.Fatalf("json_object fallback prompt has no depends_on edge example with target_memory_id and expected_to_rev:\n%s", prompt)
	}
	rev, isNumber := example["expected_to_rev"].(json.Number)
	if !isNumber {
		t.Fatalf("expected_to_rev example must be a JSON number, got %T (%v)", example["expected_to_rev"], example["expected_to_rev"])
	}
	if _, err := rev.Int64(); err != nil {
		t.Fatalf("expected_to_rev example must be an integer, got %q", rev)
	}
}

func TestCompileRawPlanReportsBooleanExpectedToRevAsFieldTypeError(t *testing.T) {
	in1, mem1 := uuid.New(), uuid.New()
	aliases := BuildAliasTable(CurateRequest{
		WorkspaceID:   "orca-eval-agg",
		WorkspaceKind: storage.KindProject,
		InboxItems:    []InboxItemView{{InboxID: in1, Content: "WIP TODO: add a benchmark for /v1/charge retries."}},
		Candidates:    []CandidateView{{MemoryID: mem1, TopicKey: "billing-api", CurrentVersion: 2}},
	})
	raw := `{"mutations":[{"inbox_ids":["I1"],"action":"create","category":"workflow_note","target_memory_id":null,"durability":"working","topic_key":"charge-retry-benchmark","summary_stub":"Benchmark /v1/charge retries","body":"Add a benchmark for /v1/charge retries before release.","depends_on":[{"target_memory_id":"M1","expected_to_rev":true,"rationale":"billing stack"}],"rationale":"new todo"}]}`

	_, err := CompileRawPlan(aliases, raw)
	if !errors.Is(err, ErrPlanValidation) {
		t.Fatalf("expected ErrPlanValidation, got %v", err)
	}
	msg := err.Error()
	if strings.Contains(msg, "cannot unmarshal") {
		t.Fatalf("error leaks raw json decoder text instead of a field-level message: %s", msg)
	}
	if !strings.Contains(msg, "mutations.0.depends_on.0.expected_to_rev") || !strings.Contains(msg, "integer") || !strings.Contains(msg, "bool") {
		t.Fatalf("error must name the field, the expected integer type, and the boolean received: %s", msg)
	}
}
