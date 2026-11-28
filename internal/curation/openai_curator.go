package curation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/storage"
)

const defaultCuratorTimeout = 30 * time.Second

// OpenAICuratorConfig configures an OpenAI-compatible /chat/completions Curator client.
type OpenAICuratorConfig struct {
	BaseURL      string
	APIKey       string
	Model        string
	ProviderName string
	Timeout      time.Duration
	HTTPClient   *http.Client
}

// OpenAICurator invokes a structured-output OpenAI-compatible chat model for curation.
type OpenAICurator struct {
	endpoint      string
	apiKey        string
	model         string
	providerName  string
	timeout       time.Duration
	client        *http.Client
	useJSONObject atomic.Bool
}

// NewOpenAICurator validates cfg and initializes an OpenAICurator.
func NewOpenAICurator(cfg OpenAICuratorConfig) (*OpenAICurator, error) {
	rawBase := strings.TrimSpace(cfg.BaseURL)
	if rawBase == "" {
		return nil, errors.New("curator base URL is required")
	}
	u, err := url.Parse(rawBase)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid curator base URL %q", cfg.BaseURL)
	}
	trimmedPath := strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(trimmedPath, "/chat/completions") {
		u.Path = trimmedPath + "/chat/completions"
	} else {
		u.Path = trimmedPath
	}

	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		return nil, errors.New("curator model is required")
	}

	providerName := strings.TrimSpace(cfg.ProviderName)
	if providerName == "" {
		providerName = "openai_compat"
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultCuratorTimeout
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{}
	}

	return &OpenAICurator{
		endpoint:     u.String(),
		apiKey:       strings.TrimSpace(cfg.APIKey),
		model:        model,
		providerName: providerName,
		timeout:      timeout,
		client:       client,
	}, nil
}

const curatorSystemPrompt = `You are the mem1 background Memory Curator.
Distill inbox turns into structured memory mutations adhering strictly to these rules:
1. Reference inbox items using their short refs (e.g. "I1", "I2") in inbox_ids. Every input ref MUST be cited in at least one mutation (use action="noop" for greetings/chitchat/non-durable filler).
2. Durability: ONLY "durable" or "working". NEVER output durability="constitution".
3. Reserved workspace "general": ONLY store personal_profile or personal_preference with durability="durable". NEVER store technical, code, API, or workflow_note entries in "general" (emit action="noop" instead).
4. Project workspaces: use action="create" for new topics, "revise" on existing target_memory_id (short ref from candidates[].ref such as "M1") for additive updates preserving the same premise, "supersede" on existing target_memory_id when replacing/contradicting a previous decision or fact, "retire" on target_memory_id when temporary work is finished. For "create" and "noop", target_memory_id must be null. Refining the same decision or component is "revise" on it; a downstream component's assumption relying on an upstream decision is a separate "create" topic whose depends_on cites the upstream candidate ref and its integer current_version.
5. topic_key must match ^[a-z0-9][a-z0-9-]{0,127}$ (lowercase alphanumeric + hyphens; no duplicate create topic_key in one batch).
6. summary_stub must be <= 180 characters on a single line (no newlines); body must be <= 1500 tokens.
7. depends_on lists at most 5 edges to active candidates in this workspace and must stay acyclic; use [] when unused. Each edge is an object whose expected_to_rev is that candidate's integer current_version (never true/false); for a candidate M1 whose current_version is 1: {"target_memory_id":"M1","expected_to_rev":1,"rationale":"extends the M1 decision"}.
8. For action="noop", set durability="", topic_key="", summary_stub="", body="", target_memory_id=null, depends_on=[].
Return strictly valid json matching this object shape:
{"mutations":[{"inbox_ids":["I1"],"action":"create|revise|supersede|retire|noop","category":"personal_profile|personal_preference|project_technical|workflow_note","target_memory_id":null,"durability":"durable|working","topic_key":"lowercase-slug","summary_stub":"single line <= 180 chars","body":"<= 1500 tokens","depends_on":[],"rationale":"short reason"}]}`

var curateResponseSchema = map[string]any{
	"type": "json_schema",
	"json_schema": map[string]any{
		"name":   "curate_plan",
		"strict": true,
		"schema": map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []string{"mutations"},
			"properties": map[string]any{
				"mutations": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type":                 "object",
						"additionalProperties": false,
						"required":             []string{"inbox_ids", "action", "category", "target_memory_id", "durability", "topic_key", "summary_stub", "body", "depends_on", "rationale"},
						"properties": map[string]any{
							"inbox_ids": map[string]any{
								"type":  "array",
								"items": map[string]any{"type": "string"},
							},
							"action": map[string]any{
								"type": "string",
								"enum": []string{"create", "revise", "supersede", "retire", "noop"},
							},
							"category": map[string]any{
								"type": "string",
								"enum": []string{"personal_profile", "personal_preference", "project_technical", "workflow_note"},
							},
							"target_memory_id": map[string]any{
								"type": []string{"string", "null"},
							},
							"durability": map[string]any{
								"type": "string",
								"enum": []string{"durable", "working", ""},
							},
							"topic_key":    map[string]any{"type": "string"},
							"summary_stub": map[string]any{"type": "string"},
							"body":         map[string]any{"type": "string"},
							"depends_on": map[string]any{
								"type": []string{"array", "null"},
								"items": map[string]any{
									"type":                 "object",
									"additionalProperties": false,
									"required":             []string{"target_memory_id", "expected_to_rev", "rationale"},
									"properties": map[string]any{
										"target_memory_id": map[string]any{"type": "string"},
										"expected_to_rev":  map[string]any{"type": "integer"},
										"rationale":        map[string]any{"type": "string"},
									},
								},
							},
							"rationale": map[string]any{"type": "string"},
						},
					},
				},
			},
		},
	},
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// CurateBatch calls the OpenAI-compatible chat completions endpoint using short alias
// context injection (I1..In, M1..Mk), compiles the output via CompileRawPlan, and performs
// at most one feedback-guided repair pass if compilation or local validation fails.
func (c *OpenAICurator) CurateBatch(ctx context.Context, req CurateRequest) (CuratePlan, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	aliases := BuildAliasTable(req)
	userJSON, err := json.Marshal(aliases.BuildPromptPayload())
	if err != nil {
		return CuratePlan{}, fmt.Errorf("marshal curate request: %w", err)
	}

	messages := []chatMessage{
		{Role: "system", Content: curatorSystemPrompt},
		{Role: "user", Content: string(userJSON)},
	}

	content, promptTok, completionTok, err := c.invokeAndDecode(reqCtx, messages)
	if err != nil {
		return CuratePlan{}, c.attemptError(promptTok, completionTok, err)
	}

	plan, valErr := c.compileAndValidate(req, aliases, content, promptTok, completionTok)
	if valErr == nil {
		return plan, nil
	}

	// Perform at most one feedback-guided repair round-trip if time remains.
	if reqCtx.Err() == nil {
		badAssistant := content
		if len(badAssistant) > 8192 {
			badAssistant = strings.ToValidUTF8(badAssistant[:8192], "")
		}
		repairPrompt := fmt.Sprintf(
			"Your previous JSON failed validation: %v\nReturn ONLY a corrected valid JSON object matching the schema. Cite every inbox ref (%s) and only valid candidate refs (%s).",
			valErr,
			aliases.InboxRefsSummary(),
			aliases.CandidateRefsSummary(),
		)
		if isDependsOnTypeError(valErr) {
			repairPrompt += " depends_on[].expected_to_rev must be an integer equal to the cited candidate's current_version, never a boolean."
		}
		repairMsgs := []chatMessage{
			messages[0],
			messages[1],
			{Role: "assistant", Content: badAssistant},
			{Role: "user", Content: repairPrompt},
		}
		rContent, rPromptTok, rCompTok, rErr := c.invokeAndDecode(reqCtx, repairMsgs)
		promptTok += rPromptTok
		completionTok += rCompTok
		if rErr != nil {
			return CuratePlan{}, c.attemptError(promptTok, completionTok, rErr)
		}
		plan, valErr = c.compileAndValidate(req, aliases, rContent, promptTok, completionTok)
		if valErr == nil {
			return plan, nil
		}
	}

	return CuratePlan{}, c.attemptError(promptTok, completionTok, valErr)
}

// attemptError attaches tokens the provider already billed to a CurateBatch failure.
func (c *OpenAICurator) attemptError(promptTok, completionTok int, err error) error {
	if promptTok == 0 && completionTok == 0 {
		return err
	}
	return &CurateAttemptError{
		ProviderName:     c.providerName,
		ModelName:        c.model,
		PromptTokens:     promptTok,
		CompletionTokens: completionTok,
		Err:              err,
	}
}

func (c *OpenAICurator) compileAndValidate(
	req CurateRequest,
	aliases AliasTable,
	rawContent string,
	promptTok, completionTok int,
) (CuratePlan, error) {
	mutations, err := CompileRawPlan(aliases, rawContent)
	if err != nil {
		return CuratePlan{}, err
	}
	plan := CuratePlan{
		ProviderName:     c.providerName,
		ModelName:        c.model,
		PromptTokens:     promptTok,
		CompletionTokens: completionTok,
		Mutations:        mutations,
	}
	if len(req.InboxItems) > 0 {
		if err := ValidatePlan(localValidationContext(req), plan); err != nil {
			return CuratePlan{}, err
		}
	}
	return plan, nil
}

func isDependsOnTypeError(err error) bool {
	var fieldErr *planFieldTypeError
	return errors.As(err, &fieldErr) && strings.Contains(fieldErr.field, "depends_on")
}

func localValidationContext(req CurateRequest) ValidationContext {
	claimed := make([]uuid.UUID, len(req.InboxItems))
	for i, it := range req.InboxItems {
		claimed[i] = it.InboxID
	}
	cands := make(map[uuid.UUID]CandidateView, len(req.Candidates))
	for _, cand := range req.Candidates {
		cands[cand.MemoryID] = cand
	}
	return ValidationContext{
		WorkspaceID:      req.WorkspaceID,
		WorkspaceKind:    req.WorkspaceKind,
		SourceAuthority:  req.SourceAuthority,
		ClaimedInboxIDs:  claimed,
		ActiveCandidates: cands,
	}
}

func (c *OpenAICurator) invokeAndDecode(ctx context.Context, messages []chatMessage) (string, int, int, error) {
	status, body, err := c.sendChatWithFormatFallback(ctx, messages)
	if err != nil {
		return "", 0, 0, err
	}
	if status < 200 || status >= 300 {
		snippet := strings.TrimSpace(string(body))
		if len(snippet) > 512 {
			snippet = snippet[:512] + "..."
		}
		return "", 0, 0, fmt.Errorf("curator endpoint returned HTTP %d: %s", status, snippet)
	}

	var decoded chatCompletionResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", 0, 0, fmt.Errorf("%w: decode response envelope: %v", ErrMalformedPlan, err)
	}
	if len(decoded.Choices) == 0 || strings.TrimSpace(decoded.Choices[0].Message.Content) == "" {
		return "", decoded.Usage.PromptTokens, decoded.Usage.CompletionTokens, fmt.Errorf("%w: missing choices content", ErrMalformedPlan)
	}
	return decoded.Choices[0].Message.Content, decoded.Usage.PromptTokens, decoded.Usage.CompletionTokens, nil
}

func (c *OpenAICurator) sendChatWithFormatFallback(ctx context.Context, messages []chatMessage) (int, []byte, error) {
	status, body, err := c.doChatMessages(ctx, messages, c.useJSONObject.Load())
	if err != nil {
		return 0, nil, err
	}
	if !c.useJSONObject.Load() && isUnsupportedJSONSchema(status, body) {
		c.useJSONObject.Store(true)
		return c.doChatMessages(ctx, messages, true)
	}
	return status, body, nil
}

func (c *OpenAICurator) doChatMessages(ctx context.Context, messages []chatMessage, useJSONObject bool) (int, []byte, error) {
	var respFormat any = curateResponseSchema
	if useJSONObject {
		respFormat = map[string]any{"type": "json_object"}
	}
	reqBody := map[string]any{
		"model":           c.model,
		"temperature":     0.0,
		"response_format": respFormat,
		"messages":        messages,
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal chat completion body: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, fmt.Errorf("build curator HTTP request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return 0, nil, fmt.Errorf("call curator endpoint: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return 0, nil, fmt.Errorf("read curator response: %w", err)
	}
	return resp.StatusCode, body, nil
}

func isUnsupportedJSONSchema(status int, body []byte) bool {
	if status != http.StatusBadRequest && status != http.StatusUnprocessableEntity {
		return false
	}
	lower := strings.ToLower(string(body))
	return strings.Contains(lower, "json_schema") || strings.Contains(lower, "response_format")
}

func stripMarkdownCodeFence(s string) string {
	trimmed := strings.TrimSpace(s)
	if !strings.HasPrefix(trimmed, "```") {
		return trimmed
	}
	lines := strings.Split(trimmed, "\n")
	if len(lines) < 2 {
		return trimmed
	}
	lines = lines[1:]
	if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "```" {
		lines = lines[:len(lines)-1]
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// MockCurator provides a deterministic offline Curator implementing topic coupling,
// conflict supersession, explicit working retirement, noop filtering, and 'general' isolation.
type MockCurator struct{}

// NewMockCurator returns an offline deterministic MockCurator.
func NewMockCurator() *MockCurator {
	return &MockCurator{}
}

var topicTagRe = regexp.MustCompile(`(?i)\btopic:([a-z0-9][a-z0-9-]{0,127})\b`)

func (m *MockCurator) CurateBatch(ctx context.Context, req CurateRequest) (CuratePlan, error) {
	if err := ctx.Err(); err != nil {
		return CuratePlan{}, err
	}

	isGeneral := req.WorkspaceKind == storage.KindGeneral || req.WorkspaceID == storage.WorkspaceGeneral
	candByTopic := make(map[string]CandidateView, len(req.Candidates))
	for _, c := range req.Candidates {
		candByTopic[c.TopicKey] = c
	}

	var noopIDs []uuid.UUID
	var noopReason string
	type topicGroup struct {
		topicKey string
		items    []InboxItemView
	}
	groupOrder := make([]string, 0, len(req.InboxItems))
	groups := make(map[string]*topicGroup)

	for _, item := range req.InboxItems {
		trimmed := strings.TrimSpace(item.Content)
		lower := strings.ToLower(trimmed)
		if trimmed == "" || strings.HasPrefix(trimmed, "[NOOP]") || isGreetingOnly(lower) {
			noopIDs = append(noopIDs, item.InboxID)
			if noopReason == "" {
				noopReason = "skipped filler or [NOOP] inbox item"
			}
			continue
		}
		if isGeneral && looksTechnicalOrWorkflow(lower) {
			noopIDs = append(noopIDs, item.InboxID)
			noopReason = "rejected technical/workflow turn from reserved 'general' workspace"
			continue
		}

		topicKey := deriveMockTopicKey(trimmed, req.Candidates)
		grp, exists := groups[topicKey]
		if !exists {
			grp = &topicGroup{topicKey: topicKey}
			groups[topicKey] = grp
			groupOrder = append(groupOrder, topicKey)
		}
		grp.items = append(grp.items, item)
	}

	sort.Strings(groupOrder)
	mutations := make([]CurateMutation, 0, len(groupOrder)+1)

	for _, topic := range groupOrder {
		grp := groups[topic]
		inboxIDs := make([]uuid.UUID, len(grp.items))
		lines := make([]string, len(grp.items))
		for i, it := range grp.items {
			inboxIDs[i] = it.InboxID
			lines[i] = cleanMockTurnText(it.Content)
		}
		latestLine := lines[len(lines)-1]
		combinedNew := strings.Join(lines, "\n")
		combinedLower := strings.ToLower(combinedNew)

		durability := storage.DurabilityDurable
		category := CategoryProjectTechnical
		if isGeneral {
			category = CategoryPersonalProfile
			if containsAnyToken(combinedLower, "prefer", "prefers", "favorite", "language", "concise", "style") {
				category = CategoryPersonalPreference
			}
		} else if containsAnyWordOrPrefix(combinedLower, "wip:", "todo:", "working:", "temporary", "task", "checklist") {
			durability = storage.DurabilityWorking
			category = CategoryWorkflowNote
		}

		cand, hasCand := candByTopic[topic]
		if hasCand {
			targetID := cand.MemoryID
			switch {
			case containsAnyPhrase(combinedLower, "retire ", "retired", "obsolete", "no longer needed", "task finished", "checklist completed", "mark done"):
				mutations = append(mutations, CurateMutation{
					InboxIDs:       inboxIDs,
					Action:         ActionRetire,
					Category:       category,
					TargetMemoryID: &targetID,
					Rationale:      "mock curator retired completed/obsolete topic",
				})
			case containsAnyPhrase(combinedLower, "switched", "changed to", "moved to", "replaced", "instead", "actually", "updated to", "now use", "migrated to", "no longer use"):
				body, _ := TruncateTokens(latestLine, MaxBodyTokens-50)
				mutations = append(mutations, CurateMutation{
					InboxIDs:       inboxIDs,
					Action:         ActionSupersede,
					Category:       category,
					TargetMemoryID: &targetID,
					Durability:     durability,
					TopicKey:       topic,
					SummaryStub:    singleLineStub(latestLine),
					Body:           body,
					Rationale:      "mock curator superseded contradicted premise",
				})
			default:
				merged := strings.TrimSpace(cand.Body) + "\n" + combinedNew
				body, _ := TruncateTokens(merged, MaxBodyTokens-50)
				mutations = append(mutations, CurateMutation{
					InboxIDs:       inboxIDs,
					Action:         ActionRevise,
					Category:       category,
					TargetMemoryID: &targetID,
					Durability:     durability,
					TopicKey:       topic,
					SummaryStub:    singleLineStub(latestLine),
					Body:           body,
					Rationale:      "mock curator coupled additive update onto active topic",
				})
			}
			continue
		}

		// New topic in this batch. If a later item in the batch supersedes an earlier one, keep the latest.
		bodyText := combinedNew
		if len(lines) > 1 && containsAnyPhrase(strings.ToLower(latestLine), "switched", "changed to", "moved to", "replaced", "instead", "actually", "updated to", "now use", "migrated to") {
			bodyText = latestLine
		}
		body, _ := TruncateTokens(bodyText, MaxBodyTokens-50)
		mutations = append(mutations, CurateMutation{
			InboxIDs:    inboxIDs,
			Action:      ActionCreate,
			Category:    category,
			Durability:  durability,
			TopicKey:    topic,
			SummaryStub: singleLineStub(latestLine),
			Body:        body,
			Rationale:   "mock curator created new curated topic",
		})
	}

	if len(noopIDs) > 0 {
		cat := CategoryProjectTechnical
		if isGeneral {
			cat = CategoryPersonalProfile
		}
		mutations = append(mutations, CurateMutation{
			InboxIDs:  noopIDs,
			Action:    ActionNoop,
			Category:  cat,
			Rationale: noopReason,
		})
	}

	return CuratePlan{
		ProviderName: "mock",
		ModelName:    "mock-curator-v1",
		Mutations:    mutations,
	}, nil
}

func isGreetingOnly(lower string) bool {
	switch strings.Trim(lower, ".!? ") {
	case "hi", "hello", "hey", "ok", "thanks", "thank you":
		return true
	default:
		return false
	}
}

func looksTechnicalOrWorkflow(lower string) bool {
	if strings.Contains(lower, "wip:") || strings.Contains(lower, "todo:") {
		return true
	}
	techWords := map[string]struct{}{
		"postgres": {}, "postgresql": {}, "sqlite": {}, "kubernetes": {}, "k8s": {},
		"docker": {}, "redis": {}, "microservice": {}, "migration": {}, "migrations": {},
		"database": {}, "api": {}, "endpoint": {}, "grpc": {}, "sql": {},
		"deploy": {}, "deployment": {}, "pgvector": {}, "hnsw": {}, "server": {},
	}
	for _, w := range splitAlphaNumWords(lower) {
		if _, ok := techWords[w]; ok {
			return true
		}
	}
	return false
}

func deriveMockTopicKey(content string, candidates []CandidateView) string {
	if match := topicTagRe.FindStringSubmatch(content); len(match) == 2 {
		slug := strings.ToLower(match[1])
		if IsValidTopicKey(slug) {
			return slug
		}
	}
	lower := strings.ToLower(content)
	switch {
	case containsAnyToken(lower, "postgres", "postgresql", "sqlite", "database", "mysql", "storage-engine"):
		return "primary-database"
	case containsAnyToken(lower, "port", "listen", "8080", "8787", "9090"):
		return "service-port"
	case containsAnyToken(lower, "timezone", "seoul", "utc", "pst"):
		return "user-timezone"
	case containsAnyToken(lower, "retry", "backoff"):
		return "retry-backoff"
	case containsAnyToken(lower, "language", "korean", "english", "prefer", "prefers"):
		return "user-preferences"
	case containsAnyToken(lower, "auth", "jwt", "oauth", "token"):
		return "auth-scheme"
	}

	words := splitAlphaNumWords(lower)
	var parts []string
	stopwords := map[string]struct{}{
		"the": {}, "and": {}, "for": {}, "with": {}, "our": {}, "user": {},
		"we": {}, "use": {}, "are": {}, "is": {}, "in": {}, "to": {}, "on": {},
		"switched": {}, "changed": {}, "moved": {}, "now": {}, "update": {}, "wip": {},
	}
	for _, w := range words {
		if _, stop := stopwords[w]; stop {
			continue
		}
		parts = append(parts, w)
		if len(parts) == 3 {
			break
		}
	}
	if len(parts) == 0 && len(words) > 0 {
		parts = words[:minInt(3, len(words))]
	}
	slug := strings.Join(parts, "-")
	if len(slug) > 48 {
		slug = strings.TrimRight(slug[:48], "-")
	}
	if !IsValidTopicKey(slug) {
		slug = "general-note"
	}
	_ = candidates
	return slug
}

func cleanMockTurnText(s string) string {
	return strings.TrimSpace(topicTagRe.ReplaceAllString(s, ""))
}

func singleLineStub(text string) string {
	collapsed := strings.Join(strings.Fields(text), " ")
	if collapsed == "" {
		return "curated memory"
	}
	runes := []rune(collapsed)
	if len(runes) <= MaxSummaryStubRunes {
		return collapsed
	}
	return string(runes[:MaxSummaryStubRunes-3]) + "..."
}

func splitAlphaNumWords(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

func containsAnyToken(lower string, targets ...string) bool {
	words := splitAlphaNumWords(lower)
	set := make(map[string]struct{}, len(words))
	for _, w := range words {
		set[w] = struct{}{}
	}
	for _, t := range targets {
		if _, ok := set[t]; ok {
			return true
		}
	}
	return false
}

func containsAnyWordOrPrefix(lower string, targets ...string) bool {
	for _, t := range targets {
		if strings.Contains(lower, t) {
			return true
		}
	}
	return false
}

func containsAnyPhrase(lower string, phrases ...string) bool {
	for _, p := range phrases {
		if strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ = utf8.RuneCountInString
