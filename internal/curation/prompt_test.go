package curation

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/storage"
)

// v10CuratorPromptTokens is the cl100k size of curatorSystemPrompt before the v1.1 split rule.
// The v1.1 design caps prompt growth at about 80 tokens over this baseline.
const v10CuratorPromptTokens = 521

const dependsOnRepairReminder = "never a boolean"

func TestCuratorPromptSeparatesReviseFromDownstreamDependsOn(t *testing.T) {
	var rule string
	for _, line := range strings.Split(curatorSystemPrompt, "\n") {
		if strings.Contains(line, "downstream") {
			rule = line
			break
		}
	}
	if rule == "" {
		t.Fatalf("curator prompt has no rule for a downstream component that relies on an upstream decision:\n%s", curatorSystemPrompt)
	}
	for _, want := range []string{`"revise"`, "same decision", "upstream", `"create"`, "depends_on", "current_version"} {
		if !strings.Contains(rule, want) {
			t.Fatalf("split-vs-revise rule must mention %q:\n%s", want, rule)
		}
	}
	if growth := CountTokens(curatorSystemPrompt) - v10CuratorPromptTokens; growth > 80 {
		t.Fatalf("curator prompt grew by %d tokens over v1.0 (%d), budget is 80", growth, v10CuratorPromptTokens)
	}
}

// repairPrompts runs one CurateBatch against a provider whose first answer is firstAnswer
// and returns the final user message of every request after the first.
func repairPrompts(t *testing.T, firstAnswer string) []string {
	t.Helper()
	var (
		mu      sync.Mutex
		calls   int
		repairs []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Messages []chatMessage `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		mu.Lock()
		calls++
		first := calls == 1
		if !first {
			repairs = append(repairs, payload.Messages[len(payload.Messages)-1].Content)
		}
		mu.Unlock()
		content := firstAnswer
		if !first {
			content = `{"mutations":[{"inbox_ids":["I1"],"action":"noop","category":"project_technical","target_memory_id":null,"durability":"","topic_key":"","summary_stub":"","body":"","depends_on":[],"rationale":"filler"}]}`
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": content}}},
			"usage":   map[string]int{"prompt_tokens": 600, "completion_tokens": 100},
		})
	}))
	defer srv.Close()

	cur, err := NewOpenAICurator(OpenAICuratorConfig{BaseURL: srv.URL, Model: "deepseek-chat", Timeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("new curator: %v", err)
	}
	_, err = cur.CurateBatch(context.Background(), CurateRequest{
		WorkspaceID:     "orca-eval-agg",
		WorkspaceKind:   storage.KindProject,
		SourceAuthority: "agent_observed",
		InboxItems:      []InboxItemView{{InboxID: uuid.New(), Content: "WIP TODO: add a benchmark for /v1/charge retries."}},
		Candidates:      []CandidateView{{MemoryID: uuid.New(), TopicKey: "billing-api", CurrentVersion: 2}},
	})
	if err != nil {
		t.Fatalf("CurateBatch after repair: %v", err)
	}
	return repairs
}

func TestRepairPromptRemindsIntegerExpectedToRevAfterDependsOnTypeError(t *testing.T) {
	repairs := repairPrompts(t, `{"mutations":[{"inbox_ids":["I1"],"action":"create","category":"workflow_note","target_memory_id":null,"durability":"working","topic_key":"charge-bench","summary_stub":"Bench","body":"Bench retries","depends_on":[{"target_memory_id":"M1","expected_to_rev":true,"rationale":"billing stack"}],"rationale":"todo"}]}`)
	if len(repairs) != 1 {
		t.Fatalf("expected exactly one repair call, got %d", len(repairs))
	}
	msg := repairs[0]
	if !strings.Contains(msg, "mutations.0.depends_on.0.expected_to_rev must be an integer, got bool") {
		t.Fatalf("repair prompt must keep the exact field error:\n%s", msg)
	}
	if !strings.Contains(msg, "depends_on[].expected_to_rev") || !strings.Contains(msg, "current_version") || !strings.Contains(msg, dependsOnRepairReminder) {
		t.Fatalf("repair prompt must remind that depends_on[].expected_to_rev is the candidate's integer current_version, %s:\n%s", dependsOnRepairReminder, msg)
	}
}

func TestRepairPromptOmitsDependsOnReminderForOtherFailures(t *testing.T) {
	repairs := repairPrompts(t, `{"mutations":[]}`)
	if len(repairs) != 1 {
		t.Fatalf("expected exactly one repair call, got %d", len(repairs))
	}
	if strings.Contains(repairs[0], dependsOnRepairReminder) {
		t.Fatalf("depends_on reminder must only follow a depends_on decoding failure:\n%s", repairs[0])
	}
}
