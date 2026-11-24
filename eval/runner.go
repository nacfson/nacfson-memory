package eval

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/retrieval"
	"github.com/nacfson/nacfson-memory/internal/search"
	"github.com/nacfson/nacfson-memory/internal/security"
	"github.com/nacfson/nacfson-memory/internal/service"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

const (
	SuiteLongMemEval             = "longmemeval_s"
	SuiteMemoryAgentBenchConfl   = "memoryagentbench_conflict"
	defaultDrainPollInterval     = 40 * time.Millisecond
	defaultSampleDrainTimeout    = 90 * time.Second
	longMemEvalMinAccuracy       = 0.75
	conflictResolutionMinAcc     = 0.85
)

// Turn represents a single conversational message in an evaluation session.
type Turn struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Session represents a timestamped session in a benchmark sample.
type Session struct {
	SessionID  string    `json:"session_id"`
	OccurredAt time.Time `json:"occurred_at"`
	Turns      []Turn    `json:"turns"`
}

// Sample models one evaluation record loaded from JSONL or offline mock fixtures.
type Sample struct {
	SampleID       string    `json:"sample_id"`
	Dataset        string    `json:"dataset"`
	Sessions       []Session `json:"sessions"`
	Question       string    `json:"question"`
	ExpectedAnswer string    `json:"expected_answer"`
	Category       string    `json:"category"`
	GeneralTurns   []Turn    `json:"general_turns,omitempty"`
}

// SampleResult captures accuracy and invariant checks for one evaluated sample.
type SampleResult struct {
	SampleID        string `json:"sample_id"`
	Dataset         string `json:"dataset"`
	WorkspaceID     string `json:"workspace_id"`
	Category        string `json:"category"`
	Passed          bool   `json:"passed"`
	JudgeVotes      int    `json:"judge_votes"`
	RecalledTokens  int    `json:"recalled_tokens"`
	TokenCapBreach  bool   `json:"token_cap_breach"`
	BoundaryBreach  bool   `json:"boundary_breach"`
	PredictedAnswer string `json:"predicted_answer"`
	ExpectedAnswer  string `json:"expected_answer"`
}

// SuiteSummary aggregates pass rate per evaluation suite.
type SuiteSummary struct {
	Suite    string  `json:"suite"`
	Total    int     `json:"total"`
	Correct  int     `json:"correct"`
	Accuracy float64 `json:"accuracy"`
	Required float64 `json:"required"`
	Passed   bool    `json:"passed"`
}

// EvalReport summarizes an entire mem1 eval invocation.
type EvalReport struct {
	Mock               bool                    `json:"mock"`
	MaxTokens          int                     `json:"max_tokens"`
	TokenCapViolations int                     `json:"token_cap_violations"`
	BoundaryViolations int                     `json:"boundary_violations"`
	Suites             map[string]SuiteSummary `json:"suites"`
	Results            []SampleResult          `json:"results"`
	Passed             bool                    `json:"passed"`
}

// Reader generates an answer strictly from recalled memory JSON at temperature 0.0.
type Reader interface {
	Answer(ctx context.Context, question string, recalledContextJSON string) (string, error)
}

// Judge grades whether predicted matches expected under a deterministic seed.
type Judge interface {
	Grade(ctx context.Context, question, expected, predicted string, seed int64) (bool, string, error)
}

// OpenAIChatConfig configures an OpenAI-compatible Reader or Judge client.
type OpenAIChatConfig struct {
	BaseURL    string
	APIKey     string
	Model      string
	Timeout    time.Duration
	HTTPClient *http.Client
}

type chatHelper struct {
	endpoint string
	apiKey   string
	model    string
	timeout  time.Duration
	client   *http.Client
}

func newChatHelper(cfg OpenAIChatConfig) (*chatHelper, error) {
	raw := strings.TrimSpace(cfg.BaseURL)
	if raw == "" {
		return nil, errors.New("eval LLM base URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid eval LLM base URL %q", cfg.BaseURL)
	}
	p := strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(p, "/chat/completions") {
		u.Path = p + "/chat/completions"
	} else {
		u.Path = p
	}
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		return nil, errors.New("eval LLM model is required")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	return &chatHelper{
		endpoint: u.String(),
		apiKey:   strings.TrimSpace(cfg.APIKey),
		model:    model,
		timeout:  timeout,
		client:   client,
	}, nil
}

func (c *chatHelper) complete(ctx context.Context, systemPrompt, userPrompt string, seed *int64) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	status, respBytes, err := c.postCompletion(reqCtx, systemPrompt, userPrompt, seed)
	if err != nil {
		return "", err
	}
	if seed != nil && status == http.StatusBadRequest && strings.Contains(strings.ToLower(string(respBytes)), "seed") {
		status, respBytes, err = c.postCompletion(reqCtx, systemPrompt, userPrompt, nil)
		if err != nil {
			return "", err
		}
	}
	if status < 200 || status >= 300 {
		return "", fmt.Errorf("eval LLM returned HTTP %d: %s", status, strings.TrimSpace(string(respBytes)))
	}

	var decoded struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(respBytes, &decoded); err != nil {
		return "", fmt.Errorf("decode eval LLM response: %w", err)
	}
	if len(decoded.Choices) == 0 {
		return "", errors.New("eval LLM returned zero choices")
	}
	return strings.TrimSpace(decoded.Choices[0].Message.Content), nil
}

func (c *chatHelper) postCompletion(ctx context.Context, systemPrompt, userPrompt string, seed *int64) (int, []byte, error) {
	bodyMap := map[string]any{
		"model":       c.model,
		"temperature": 0.0,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": userPrompt},
		},
	}
	if seed != nil {
		bodyMap["seed"] = *seed
	}
	rawBody, err := json.Marshal(bodyMap)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal eval chat body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(rawBody))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, respBytes, nil
}

// OpenAIReader answers benchmark questions strictly from recalled context at temperature 0.0.
type OpenAIReader struct {
	chat *chatHelper
}

// NewOpenAIReader constructs an OpenAI-compatible evaluation Reader.
func NewOpenAIReader(cfg OpenAIChatConfig) (*OpenAIReader, error) {
	ch, err := newChatHelper(cfg)
	if err != nil {
		return nil, err
	}
	return &OpenAIReader{chat: ch}, nil
}

func (r *OpenAIReader) Answer(ctx context.Context, question, recalledContextJSON string) (string, error) {
	const sysPrompt = "Answer the question concisely and accurately using ONLY the provided recalled JSON memory context. If the context does not specify the answer, say 'unknown'."
	userPrompt := fmt.Sprintf("Recalled Context JSON:\n%s\n\nQuestion: %s", recalledContextJSON, question)
	return r.chat.complete(ctx, sysPrompt, userPrompt, nil)
}

// OpenAIJudge grades answers into binary correct | incorrect verdicts using a seed.
type OpenAIJudge struct {
	chat *chatHelper
}

// NewOpenAIJudge constructs an OpenAI-compatible 3-seed Judge.
func NewOpenAIJudge(cfg OpenAIChatConfig) (*OpenAIJudge, error) {
	ch, err := newChatHelper(cfg)
	if err != nil {
		return nil, err
	}
	return &OpenAIJudge{chat: ch}, nil
}

func (j *OpenAIJudge) Grade(ctx context.Context, question, expected, predicted string, seed int64) (bool, string, error) {
	const sysPrompt = `You are a strict grading judge. Return JSON only: {"verdict":"correct"|"incorrect","rationale":"..."}. Grade "correct" ONLY if the predicted answer conveys the same factual answer as the expected answer.`
	userPrompt := fmt.Sprintf("Question: %s\nExpected Answer: %s\nPredicted Answer: %s", question, expected, predicted)
	out, err := j.chat.complete(ctx, sysPrompt, userPrompt, &seed)
	if err != nil {
		return false, "", err
	}

	cleaned := stripJSONFence(out)
	var verdictObj struct {
		Verdict   string `json:"verdict"`
		Rationale string `json:"rationale"`
	}
	if err := json.Unmarshal([]byte(cleaned), &verdictObj); err == nil && verdictObj.Verdict != "" {
		v := strings.ToLower(strings.TrimSpace(verdictObj.Verdict))
		if v == "correct" {
			return true, verdictObj.Rationale, nil
		}
		if v == "incorrect" {
			return false, verdictObj.Rationale, nil
		}
	}

	lower := strings.ToLower(strings.TrimSpace(cleaned))
	if strings.HasPrefix(lower, "correct") {
		return true, cleaned, nil
	}
	if strings.HasPrefix(lower, "incorrect") {
		return false, cleaned, nil
	}
	return false, cleaned, fmt.Errorf("unrecognized judge output %q", out)
}

func stripJSONFence(s string) string {
	t := strings.TrimSpace(s)
	if !strings.HasPrefix(t, "```") {
		return t
	}
	lines := strings.Split(t, "\n")
	if len(lines) < 2 {
		return t
	}
	lines = lines[1:]
	if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "```" {
		lines = lines[:len(lines)-1]
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// MockReader deterministically extracts the best-matching recalled line for offline tests.
type MockReader struct{}

func (MockReader) Answer(ctx context.Context, question, recalledContextJSON string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var resp retrieval.RecallResponse
	if err := json.Unmarshal([]byte(recalledContextJSON), &resp); err != nil {
		return "", fmt.Errorf("mock reader invalid recall JSON: %w", err)
	}

	qTokens := contentTokens(question)
	bestScore := -1
	bestText := ""
	scoreText := func(candidate string) {
		if strings.TrimSpace(candidate) == "" {
			return
		}
		lines := strings.Split(candidate, "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			lToks := contentTokens(line)
			lSet := make(map[string]struct{}, len(lToks))
			for _, lt := range lToks {
				lSet[lt] = struct{}{}
			}
			overlap := 0
			for _, qt := range qTokens {
				if _, ok := lSet[qt]; ok {
					overlap++
				}
			}
			if overlap > bestScore {
				bestScore = overlap
				bestText = line
			}
		}
	}

	for _, m := range resp.Memories {
		scoreText(m.Body)
	}
	for _, s := range resp.Stubs {
		scoreText(s.SummaryStub)
	}
	if bestText == "" {
		return "unknown", nil
	}
	return bestText, nil
}

// MockJudge deterministically checks expected-answer token/substring agreement.
type MockJudge struct{}

func (MockJudge) Grade(ctx context.Context, _, expected, predicted string, _ int64) (bool, string, error) {
	if err := ctx.Err(); err != nil {
		return false, "", err
	}
	expNorm := strings.ToLower(strings.TrimSpace(expected))
	predNorm := strings.ToLower(strings.TrimSpace(predicted))
	if expNorm != "" && strings.Contains(predNorm, expNorm) {
		return true, "exact normalized substring match", nil
	}
	expToks := contentTokens(expNorm)
	if len(expToks) == 0 {
		return false, "empty expected answer", nil
	}
	predSet := make(map[string]struct{})
	for _, t := range contentTokens(predNorm) {
		predSet[t] = struct{}{}
	}
	matched := 0
	for _, t := range expToks {
		if _, ok := predSet[t]; ok {
			matched++
		}
	}
	if matched == len(expToks) {
		return true, "all expected key tokens present", nil
	}
	return false, fmt.Sprintf("matched %d/%d expected tokens", matched, len(expToks)), nil
}

// EvaluateMajority executes 3 deterministic judge seeds and passes on >= 2 correct votes.
func EvaluateMajority(ctx context.Context, judge Judge, question, expected, predicted string) (bool, int, error) {
	seeds := [3]int64{1, 2, 3}
	votes := 0
	for _, seed := range seeds {
		ok, _, err := judge.Grade(ctx, question, expected, predicted, seed)
		if err != nil {
			return false, votes, err
		}
		if ok {
			votes++
		}
	}
	return votes >= 2, votes, nil
}

// WaitForQueueDrain polls until pending inbox items and active/available River jobs reach 0.
func WaitForQueueDrain(
	ctx context.Context,
	interval, timeout time.Duration,
	checkFn func(context.Context) (pendingInbox int64, activeJobs int64, err error),
) error {
	if interval <= 0 {
		interval = defaultDrainPollInterval
	}
	if timeout <= 0 {
		timeout = defaultSampleDrainTimeout
	}
	deadline := time.Now().Add(timeout)
	for {
		pending, jobs, err := checkFn(ctx)
		if err != nil {
			return err
		}
		if pending == 0 && jobs == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for queue drain (pending_inbox=%d active_jobs=%d)", pending, jobs)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// RunOptions configures an execution of Runner.Run.
type RunOptions struct {
	Suites           []string
	DatasetDir       string
	MaxTokens        int
	Limit            int
	Mock             bool
	DrainTimeout     time.Duration
	DrainPollPeriod  time.Duration
}

// Runner orchestrates end-to-end Priority 1 evaluation with embedded River workers.
type Runner struct {
	cfg      config.Config
	store    *storage.Store
	curator  curation.Curator
	embedder embedding.Embedder
	reader   Reader
	judge    Judge
	logger   *slog.Logger
}

// NewRunner builds a Runner.
func NewRunner(
	cfg config.Config,
	store *storage.Store,
	cur curation.Curator,
	emb embedding.Embedder,
	reader Reader,
	judge Judge,
	logger *slog.Logger,
) *Runner {
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{
		cfg:      cfg,
		store:    store,
		curator:  cur,
		embedder: emb,
		reader:   reader,
		judge:    judge,
		logger:   logger,
	}
}

// LoadSamples reads JSONL benchmark samples from <datasetDir>/<suite>.jsonl.
func LoadSamples(datasetDir, suite string, limit int) ([]Sample, error) {
	if strings.TrimSpace(datasetDir) == "" {
		datasetDir = "eval/datasets"
	}
	path := filepath.Join(datasetDir, suite+".jsonl")
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open dataset %q: %w", path, err)
	}
	defer f.Close()

	var samples []Sample
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var s Sample
		if err := json.Unmarshal([]byte(line), &s); err != nil {
			return nil, fmt.Errorf("parse %s:%d: %w", path, lineNo, err)
		}
		if s.Dataset == "" {
			s.Dataset = suite
		}
		samples = append(samples, s)
		if limit > 0 && len(samples) >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}
	return samples, nil
}

// MockFixtures returns the embedded 4-sample offline benchmark suite covering
// multi-session recall, conflict supersession, working updates, and 'general' isolation.
func MockFixtures() []Sample {
	t0 := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	return []Sample{
		{
			SampleID: "mock-lme-1",
			Dataset:  SuiteLongMemEval,
			Category: "multi_session",
			Sessions: []Session{
				{
					SessionID:  "s1",
					OccurredAt: t0,
					Turns: []Turn{
						{Role: "user", Content: "topic:deployment-target Primary deployment target is Kubernetes on GKE."},
					},
				},
				{
					SessionID:  "s2",
					OccurredAt: t0.Add(24 * time.Hour),
					Turns: []Turn{
						{Role: "user", Content: "topic:backup-cron Nightly PostgreSQL backup runs at 02:30 UTC."},
					},
				},
			},
			Question:       "What time does the nightly PostgreSQL backup run?",
			ExpectedAnswer: "02:30 UTC",
		},
		{
			SampleID: "mock-mab-conflict-1",
			Dataset:  SuiteMemoryAgentBenchConfl,
			Category: "conflict_resolution",
			Sessions: []Session{
				{
					SessionID:  "s1",
					OccurredAt: t0,
					Turns: []Turn{
						{Role: "user", Content: "topic:primary-db Primary database choice is SQLite for prototype."},
					},
				},
				{
					SessionID:  "s2",
					OccurredAt: t0.Add(48 * time.Hour),
					Turns: []Turn{
						{Role: "user", Content: "topic:primary-db Switched primary database to PostgreSQL 17."},
					},
				},
			},
			Question:       "What is our current primary database?",
			ExpectedAnswer: "PostgreSQL 17",
		},
		{
			SampleID: "mock-mab-conflict-2",
			Dataset:  SuiteMemoryAgentBenchConfl,
			Category: "conflict_resolution",
			Sessions: []Session{
				{
					SessionID:  "s1",
					OccurredAt: t0,
					Turns: []Turn{
						{Role: "user", Content: "topic:service-port Listen port is 8080."},
					},
				},
				{
					SessionID:  "s2",
					OccurredAt: t0.Add(72 * time.Hour),
					Turns: []Turn{
						{Role: "user", Content: "topic:service-port Updated listen port to 9090 instead of 8080."},
					},
				},
			},
			Question:       "Which listen port does the service use?",
			ExpectedAnswer: "9090",
		},
		{
			SampleID: "mock-lme-boundary-2",
			Dataset:  SuiteLongMemEval,
			Category: "general_boundary",
			GeneralTurns: []Turn{
				{Role: "user", Content: "topic:user-preferences User prefers dark mode and concise Go examples."},
				{Role: "assistant", Content: "Deploy Postgres 17 API microservice on Kubernetes."},
			},
			Sessions: []Session{
				{
					SessionID:  "s1",
					OccurredAt: t0,
					Turns: []Turn{
						{Role: "user", Content: "topic:http-router Use Go stdlib net/http ServeMux."},
					},
				},
			},
			Question:       "Does the user prefer dark mode?",
			ExpectedAnswer: "dark mode",
		},
	}
}

// Run executes the requested evaluation suites using an embedded in-process River client.
func (r *Runner) Run(ctx context.Context, opts RunOptions) (EvalReport, error) {
	maxToks := retrieval.ClampMaxTokens(opts.MaxTokens)
	if len(opts.Suites) == 0 {
		opts.Suites = []string{SuiteLongMemEval, SuiteMemoryAgentBenchConfl}
	}

	var samples []Sample
	if opts.Mock {
		wantSuites := make(map[string]bool, len(opts.Suites))
		for _, s := range opts.Suites {
			wantSuites[strings.TrimSpace(s)] = true
		}
		for _, fx := range MockFixtures() {
			if wantSuites[fx.Dataset] {
				samples = append(samples, fx)
				if opts.Limit > 0 && len(samples) >= opts.Limit {
					break
				}
			}
		}
	} else {
		for _, suite := range opts.Suites {
			suite = strings.TrimSpace(suite)
			if suite == "" {
				continue
			}
			remLimit := 0
			if opts.Limit > 0 {
				remLimit = opts.Limit - len(samples)
				if remLimit <= 0 {
					break
				}
			}
			loaded, err := LoadSamples(opts.DatasetDir, suite, remLimit)
			if err != nil {
				return EvalReport{}, err
			}
			samples = append(samples, loaded...)
		}
	}

	if len(samples) == 0 {
		return EvalReport{}, errors.New("no evaluation samples loaded")
	}

	// Start embedded River client on CurationPool so eval executes workers without mem1 serve.
	curBreaker := curation.NewBreaker(3, 60*time.Second)
	embBreaker := curation.NewBreaker(3, 60*time.Second)
	lx := search.NewLexicalSearch()
	dg := graph.NewDependencyGraph()
	scrubber := security.NewSecretScrubber()
	packer := retrieval.NewPacker(r.store, r.embedder, embBreaker, lx, dg, r.cfg.EmbedTimeout)
	svc := service.New(r.cfg, r.store, scrubber, r.embedder, packer, dg, curBreaker, embBreaker)

	workers := river.NewWorkers()
	curation.RegisterWorkers(workers, curation.WorkerDeps{
		Store:             r.store,
		Curator:           r.curator,
		Embedder:          r.embedder,
		Lexical:           lx,
		Graph:             dg,
		CuratorBreaker:    curBreaker,
		EmbedderBreaker:   embBreaker,
		HistoryRetentionN: r.cfg.HistoryRetentionN,
		Logger:            r.logger,
		SnoozeDuration:    100 * time.Millisecond,
	})
	riverClient, err := river.NewClient(riverpgxv5.New(r.store.CurationPool), &river.Config{
		Queues:            storage.RiverQueues(),
		Workers:           workers,
		FetchCooldown:     20 * time.Millisecond,
		FetchPollInterval: 50 * time.Millisecond,
		TestOnly:          true,
	})
	if err != nil {
		return EvalReport{}, fmt.Errorf("init embedded eval River client: %w", err)
	}
	if err := riverClient.Start(ctx); err != nil {
		return EvalReport{}, fmt.Errorf("start embedded eval River client: %w", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = riverClient.StopAndCancel(stopCtx)
	}()

	runTag := fmt.Sprintf("%x", time.Now().UnixNano()&0xfffffff)
	report := EvalReport{
		Mock:      opts.Mock,
		MaxTokens: maxToks,
		Suites:    make(map[string]SuiteSummary),
		Results:   make([]SampleResult, 0, len(samples)),
	}

	for idx, sample := range samples {
		wsID := fmt.Sprintf("eval-%s-%d", runTag, idx)
		if _, err := svc.CreateWorkspace(ctx, service.CreateWorkspaceRequest{
			WorkspaceID:    wsID,
			InheritGeneral: storage.InheritConstitutionAndDurable,
		}); err != nil {
			return EvalReport{}, fmt.Errorf("create eval workspace %q: %w", wsID, err)
		}

		// Optional 'general' turns to validate cross-workspace inheritance & technical boundary isolation.
		var injectedGeneralTechInbox []uuid.UUID
		for _, gt := range sample.GeneralTurns {
			res, err := svc.IngestWrite(ctx, service.IngestWriteRequest{
				WorkspaceID: storage.WorkspaceGeneral,
				AgentID:     "eval-agent",
				Content:     gt.Content,
			})
			if err != nil {
				return EvalReport{}, fmt.Errorf("ingest general turn on sample %s: %w", sample.SampleID, err)
			}
			if looksTechnicalForBoundaryCheck(gt.Content) {
				injectedGeneralTechInbox = append(injectedGeneralTechInbox, res.InboxID)
			}
		}

		for _, sess := range sample.Sessions {
			for _, turn := range sess.Turns {
				content := strings.TrimSpace(turn.Content)
				if content == "" {
					continue
				}
				if _, err := svc.IngestWrite(ctx, service.IngestWriteRequest{
					WorkspaceID: wsID,
					AgentID:     "eval-agent",
					Content:     content,
					OccurredAt:  sess.OccurredAt,
				}); err != nil {
					return EvalReport{}, fmt.Errorf("ingest turn on sample %s: %w", sample.SampleID, err)
				}
			}
			// Drain after each session so multi-session updates see earlier curated candidates.
			if err := r.drainQueues(ctx, opts.DrainPollPeriod, opts.DrainTimeout); err != nil {
				return EvalReport{}, fmt.Errorf("drain queues after session %s (%s): %w", sess.SessionID, sample.SampleID, err)
			}
		}

		recallResp, err := svc.RecallContext(ctx, retrieval.RecallRequest{
			WorkspaceID: wsID,
			Query:       sample.Question,
			MaxTokens:   maxToks,
		})
		if err != nil {
			return EvalReport{}, fmt.Errorf("recall context on sample %s: %w", sample.SampleID, err)
		}
		respJSON, err := json.Marshal(recallResp)
		if err != nil {
			return EvalReport{}, fmt.Errorf("marshal recall response: %w", err)
		}
		tokCount := curation.CountTokens(string(respJSON))
		tokenBreach := tokCount > maxToks || recallResp.SerializedTokens > maxToks
		if tokenBreach {
			report.TokenCapViolations++
		}

		boundaryBreach, err := r.checkBoundaryViolations(ctx, wsID, injectedGeneralTechInbox)
		if err != nil {
			return EvalReport{}, fmt.Errorf("check boundaries on sample %s: %w", sample.SampleID, err)
		}
		if boundaryBreach {
			report.BoundaryViolations++
		}

		predicted, err := r.reader.Answer(ctx, sample.Question, string(respJSON))
		if err != nil {
			return EvalReport{}, fmt.Errorf("reader answer on sample %s: %w", sample.SampleID, err)
		}
		passed, votes, err := EvaluateMajority(ctx, r.judge, sample.Question, sample.ExpectedAnswer, predicted)
		if err != nil {
			return EvalReport{}, fmt.Errorf("judge majority on sample %s: %w", sample.SampleID, err)
		}

		report.Results = append(report.Results, SampleResult{
			SampleID:        sample.SampleID,
			Dataset:         sample.Dataset,
			WorkspaceID:     wsID,
			Category:        sample.Category,
			Passed:          passed,
			JudgeVotes:      votes,
			RecalledTokens:  tokCount,
			TokenCapBreach:  tokenBreach,
			BoundaryBreach:  boundaryBreach,
			PredictedAnswer: predicted,
			ExpectedAnswer:  sample.ExpectedAnswer,
		})
	}

	report.Passed = finalizeReport(&report)
	return report, nil
}

func (r *Runner) drainQueues(ctx context.Context, pollPeriod, timeout time.Duration) error {
	return WaitForQueueDrain(ctx, pollPeriod, timeout, func(checkCtx context.Context) (int64, int64, error) {
		var pendingInbox, activeJobs int64
		if err := r.store.CurationPool.QueryRow(checkCtx, `
			SELECT count(*) FROM raw_inbox WHERE status = 'pending'
		`).Scan(&pendingInbox); err != nil {
			return 0, 0, fmt.Errorf("poll pending raw_inbox: %w", err)
		}
		if err := r.store.CurationPool.QueryRow(checkCtx, `
			SELECT count(*) FROM river_job
			WHERE queue IN ('curation', 'embedding')
			  AND state IN ('available', 'pending', 'running', 'retryable', 'scheduled')
		`).Scan(&activeJobs); err != nil {
			return 0, 0, fmt.Errorf("poll active river jobs: %w", err)
		}
		return pendingInbox, activeJobs, nil
	})
}

func (r *Runner) checkBoundaryViolations(ctx context.Context, wsID string, injectedTechInboxIDs []uuid.UUID) (bool, error) {
	var violations int64
	if err := r.store.CurationPool.QueryRow(ctx, `
		SELECT count(*)
		FROM memory_manifest
		WHERE (workspace_id = $1 AND durability = 'constitution' AND curation_state <> 'user_explicit')
		   OR (workspace_id = 'general' AND durability = 'working')
		   OR (workspace_id = 'general' AND durability = 'constitution' AND curation_state <> 'user_explicit')
	`, wsID).Scan(&violations); err != nil {
		return false, err
	}
	if violations > 0 {
		return true, nil
	}
	if len(injectedTechInboxIDs) > 0 {
		var leakedTech int64
		if err := r.store.CurationPool.QueryRow(ctx, `
			SELECT count(*)
			FROM curation_decisions
			WHERE workspace_id = 'general'
			  AND action IN ('create', 'revise', 'supersede')
			  AND inbox_ids && $1
		`, injectedTechInboxIDs).Scan(&leakedTech); err != nil {
			return false, err
		}
		if leakedTech > 0 {
			return true, nil
		}
	}
	return false, nil
}

func finalizeReport(report *EvalReport) bool {
	type counts struct {
		total   int
		correct int
	}
	bySuite := make(map[string]*counts)
	for _, res := range report.Results {
		c := bySuite[res.Dataset]
		if c == nil {
			c = &counts{}
			bySuite[res.Dataset] = c
		}
		c.total++
		if res.Passed {
			c.correct++
		}
	}

	allPassed := report.TokenCapViolations == 0 && report.BoundaryViolations == 0
	for suite, c := range bySuite {
		acc := 0.0
		if c.total > 0 {
			acc = float64(c.correct) / float64(c.total)
		}
		req := 0.0
		switch suite {
		case SuiteMemoryAgentBenchConfl:
			req = conflictResolutionMinAcc
		case SuiteLongMemEval:
			req = longMemEvalMinAccuracy
		}
		suitePassed := acc >= req
		if !suitePassed {
			allPassed = false
		}
		report.Suites[suite] = SuiteSummary{
			Suite:    suite,
			Total:    c.total,
			Correct:  c.correct,
			Accuracy: acc,
			Required: req,
			Passed:   suitePassed,
		}
	}
	return allPassed
}

func looksTechnicalForBoundaryCheck(text string) bool {
	lower := strings.ToLower(text)
	for _, tok := range contentTokens(lower) {
		switch tok {
		case "postgres", "postgresql", "sqlite", "kubernetes", "docker", "redis", "microservice", "api":
			return true
		}
	}
	return false
}

var readerStopwords = map[string]struct{}{
	"what": {}, "when": {}, "where": {}, "which": {}, "who": {}, "how": {},
	"does": {}, "do": {}, "did": {}, "is": {}, "are": {}, "was": {}, "were": {},
	"the": {}, "a": {}, "an": {}, "our": {}, "we": {}, "user": {}, "now": {},
	"current": {}, "currently": {}, "use": {}, "used": {}, "using": {}, "for": {},
	"and": {}, "or": {}, "to": {}, "in": {}, "on": {}, "of": {},
}

func contentTokens(s string) []string {
	fields := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != ':'
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		f = strings.Trim(f, ":")
		if len(f) < 2 {
			continue
		}
		if _, stop := readerStopwords[f]; stop {
			continue
		}
		out = append(out, f)
	}
	return out
}
