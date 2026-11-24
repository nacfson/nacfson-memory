package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/nacfson/nacfson-memory/eval"
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
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	envFile, rest, err := extractEnvFileFlag(args)
	if err != nil {
		fmt.Fprintf(stderr, "flag error: %v\n", err)
		return 2
	}
	if len(rest) == 0 {
		printUsage(stderr)
		return 2
	}

	switch rest[0] {
	case "-h", "--help", "help":
		printUsage(stdout)
		return 0
	case "serve", "mcp-stdio", "migrate", "reindex", "eval",
		"list", "explain", "revert", "purge", "confirm-token", "dead-letters":
	default:
		fmt.Fprintf(stderr, "unknown subcommand %q\n", rest[0])
		printUsage(stderr)
		return 2
	}

	envLookup, err := config.ResolveEnvLookup(envFile, os.LookupEnv)
	if err != nil {
		fmt.Fprintf(stderr, "env file error: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch rest[0] {
	case "serve":
		return runServe(ctx, rest[1:], envLookup, stderr)
	case "mcp-stdio":
		return runMCPStdio(ctx, rest[1:], envLookup, stderr)
	case "migrate":
		return runMigrate(ctx, rest[1:], envLookup, stdout, stderr)
	case "reindex":
		return runReindex(ctx, rest[1:], envLookup, stdout, stderr)
	case "eval":
		return runEval(ctx, rest[1:], envLookup, stdout, stderr)
	case "list":
		return runList(ctx, rest[1:], envLookup, stdout, stderr)
	case "explain":
		return runExplain(ctx, rest[1:], envLookup, stdout, stderr)
	case "revert":
		return runRevert(ctx, rest[1:], envLookup, stdout, stderr)
	case "purge":
		return runPurge(ctx, rest[1:], envLookup, stdout, stderr)
	case "confirm-token":
		return runConfirmToken(ctx, rest[1:], envLookup, stdout, stderr)
	case "dead-letters":
		return runDeadLetters(ctx, rest[1:], envLookup, stdout, stderr)
	default:
		return 2
	}
}

func extractEnvFileFlag(args []string) (string, []string, error) {
	var envFile string
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			rest = append(rest, args[i:]...)
			break
		}
		if strings.HasPrefix(arg, "--env-file=") || strings.HasPrefix(arg, "-env-file=") {
			val := strings.TrimSpace(arg[strings.IndexByte(arg, '=')+1:])
			if val == "" {
				return "", nil, errors.New("--env-file requires a non-empty path")
			}
			envFile = val
			continue
		}
		if arg == "--env-file" || arg == "-env-file" {
			if i+1 >= len(args) || strings.TrimSpace(args[i+1]) == "" {
				return "", nil, errors.New("--env-file requires a path argument")
			}
			envFile = strings.TrimSpace(args[i+1])
			i++
			continue
		}
		rest = append(rest, arg)
	}
	return envFile, rest, nil
}

func runServe(ctx context.Context, args []string, envLookup func(string) (string, bool), stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := config.LoadFromLookup(config.ModeServe, envLookup)
	if err != nil {
		fmt.Fprintf(stderr, "config error: %v\n", err)
		return 1
	}

	cur, err := buildCurator(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "curator init failed: %v\n", err)
		return 1
	}
	emb, err := buildEmbedder(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "embedder init failed: %v\n", err)
		return 1
	}

	store, err := storage.OpenPools(ctx, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "database open failed: %v\n", err)
		return 1
	}
	defer store.Close()

	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	svc, lx, dg, curBreaker, embBreaker := buildRuntimeService(cfg, store, emb)

	workers := river.NewWorkers()
	curation.RegisterWorkers(workers, curation.WorkerDeps{
		Store:             store,
		Curator:           cur,
		Embedder:          emb,
		Lexical:           lx,
		Graph:             dg,
		CuratorBreaker:    curBreaker,
		EmbedderBreaker:   embBreaker,
		HistoryRetentionN: cfg.HistoryRetentionN,
		CurationPaused:    func() bool { return cfg.CurationPaused },
		ForceProvisional:  func() bool { return cfg.ForceProvisional },
		Logger:            logger,
	})
	riverClient, err := river.NewClient(riverpgxv5.New(store.CurationPool), &river.Config{
		Queues:       storage.RiverQueues(),
		Workers:      workers,
		PeriodicJobs: curation.PeriodicJobs(),
		Logger:       logger,
	})
	if err != nil {
		fmt.Fprintf(stderr, "river client init failed: %v\n", err)
		return 1
	}
	if err := riverClient.Start(ctx); err != nil {
		fmt.Fprintf(stderr, "river worker start failed: %v\n", err)
		return 1
	}

	mcpHandler := mcpserver.NewStreamableHTTPHandler(svc, logger)
	srv := httpapi.NewServer(cfg, svc, mcpHandler, logger)

	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = riverClient.Stop(stopCtx)
		cancel()
		fmt.Fprintf(stderr, "listen %q failed: %v\n", cfg.HTTPAddr, err)
		return 1
	}

	logger.Info("mem1 serve listening", "addr", ln.Addr().String())
	serveErrCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErrCh <- err
			return
		}
		serveErrCh <- nil
	}()

	var exitCode int
	select {
	case <-ctx.Done():
	case err := <-serveErrCh:
		if err != nil {
			fmt.Fprintf(stderr, "http server failed: %v\n", err)
			exitCode = 1
		}
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		fmt.Fprintf(stderr, "http shutdown error: %v\n", err)
		exitCode = 1
	}
	riverStopCtx, riverStopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer riverStopCancel()
	if err := riverClient.Stop(riverStopCtx); err != nil {
		fmt.Fprintf(stderr, "river shutdown error: %v\n", err)
		exitCode = 1
	}
	return exitCode
}

func runMCPStdio(ctx context.Context, args []string, envLookup func(string) (string, bool), stderr io.Writer) int {
	fs := flag.NewFlagSet("mcp-stdio", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := config.LoadFromLookup(config.ModeMCPStdio, envLookup)
	if err != nil {
		fmt.Fprintf(stderr, "config error: %v\n", err)
		return 1
	}
	emb, err := buildEmbedder(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "embedder init failed: %v\n", err)
		return 1
	}
	store, err := storage.OpenPools(ctx, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "database open failed: %v\n", err)
		return 1
	}
	defer store.Close()

	// Logs must stay strictly on stderr so stdout remains clean JSON-RPC stdio framing.
	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	svc, _, _, _, _ := buildRuntimeService(cfg, store, emb)

	if err := mcpserver.RunStdio(ctx, svc, logger); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(stderr, "mcp-stdio failed: %v\n", err)
		return 1
	}
	return 0
}

func buildRuntimeService(cfg config.Config, store *storage.Store, emb embedding.Embedder) (*service.MemoryService, *search.LexicalSearch, *graph.DependencyGraph, *curation.Breaker, *curation.Breaker) {
	lx := search.NewLexicalSearch()
	dg := graph.NewDependencyGraph()
	scrubber := security.NewSecretScrubber()
	curBreaker := curation.NewBreaker(3, 60*time.Second)
	embBreaker := curation.NewBreaker(3, 60*time.Second)
	packer := retrieval.NewPacker(store, emb, embBreaker, lx, dg, cfg.EmbedTimeout)
	svc := service.New(cfg, store, scrubber, emb, packer, dg, curBreaker, embBreaker)
	return svc, lx, dg, curBreaker, embBreaker
}

func runMigrate(ctx context.Context, args []string, envLookup func(string) (string, bool), stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	down := fs.Bool("down", false, "roll back schema and River migrations")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := config.LoadFromLookup(config.ModeMigrate, envLookup)
	if err != nil {
		fmt.Fprintf(stderr, "config error: %v\n", err)
		return 1
	}

	migCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := storage.Migrate(migCtx, cfg.DatabaseURL, *down); err != nil {
		fmt.Fprintf(stderr, "migration failed: %v\n", err)
		return 1
	}
	if *down {
		fmt.Fprintln(stdout, "migrations rolled back")
	} else {
		fmt.Fprintln(stdout, "migrations applied")
	}
	return 0
}

func runReindex(ctx context.Context, args []string, envLookup func(string) (string, bool), stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("reindex", flag.ContinueOnError)
	fs.SetOutput(stderr)
	batchSize := fs.Int("batch-size", 32, "memories per embedding batch")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *batchSize < 1 {
		fmt.Fprintln(stderr, "reindex failed: --batch-size must be >= 1")
		return 2
	}

	cfg, err := config.LoadFromLookup(config.ModeReindex, envLookup)
	if err != nil {
		fmt.Fprintf(stderr, "config error: %v\n", err)
		return 1
	}

	emb, err := buildEmbedder(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "embedder init failed: %v\n", err)
		return 1
	}

	store, err := storage.OpenPools(ctx, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "database open failed: %v\n", err)
		return 1
	}
	defer store.Close()

	svc := service.New(
		cfg,
		store,
		security.NewSecretScrubber(),
		emb,
		nil,
		graph.NewDependencyGraph(),
		curation.NewBreaker(3, 60*time.Second),
		curation.NewBreaker(3, 60*time.Second),
	)
	res, err := svc.ReindexEmbeddings(ctx, *batchSize)
	if err != nil {
		fmt.Fprintf(stderr, "reindex failed: %v\n", err)
		return 1
	}
	return writeJSON(stdout, stderr, res)
}

func runEval(ctx context.Context, args []string, envLookup func(string) (string, bool), stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("eval", flag.ContinueOnError)
	fs.SetOutput(stderr)
	suiteFlag := fs.String("suite", eval.SuiteLongMemEval+","+eval.SuiteMemoryAgentBenchConfl, "comma-separated evaluation suites")
	maxTokens := fs.Int("max-tokens", retrieval.MaxAgentTokens, "recall token ceiling (<= 2000)")
	datasetDir := fs.String("dataset-dir", "eval/datasets", "directory containing benchmark JSONL files")
	limit := fs.Int("limit", 0, "optional sample limit (0 = all)")
	mock := fs.Bool("mock", false, "run offline synthetic 4-sample mock suite")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *maxTokens < 1 || *maxTokens > retrieval.MaxAgentTokens {
		fmt.Fprintf(stderr, "eval failed: --max-tokens must be between 1 and %d\n", retrieval.MaxAgentTokens)
		return 2
	}

	lookup := envLookup
	if *mock {
		lookup = func(k string) (string, bool) {
			if v, ok := envLookup(k); ok && strings.TrimSpace(v) != "" {
				return v, true
			}
			switch k {
			case "MEM1_CURATOR_PROVIDER":
				return config.ProviderMock, true
			case "MEM1_EMBED_PROVIDER":
				return config.ProviderMock, true
			default:
				return "", false
			}
		}
	}

	cfg, err := config.LoadFromLookup(config.ModeEval, lookup)
	if err != nil {
		fmt.Fprintf(stderr, "config error: %v\n", err)
		return 1
	}
	if !*mock {
		if err := cfg.ValidateEvalLive(); err != nil {
			fmt.Fprintf(stderr, "eval live config error: %v\n", err)
			return 1
		}
	}

	cur, err := buildCurator(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "curator init failed: %v\n", err)
		return 1
	}
	emb, err := buildEmbedder(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "embedder init failed: %v\n", err)
		return 1
	}

	var (
		reader eval.Reader
		judge  eval.Judge
	)
	if *mock {
		reader = eval.MockReader{}
		judge = eval.MockJudge{}
	} else {
		reader, err = eval.NewOpenAIReader(eval.OpenAIChatConfig{
			BaseURL: cfg.EvalReaderBaseURL,
			APIKey:  cfg.EvalReaderAPIKey,
			Model:   cfg.EvalReaderModel,
		})
		if err != nil {
			fmt.Fprintf(stderr, "eval reader init failed: %v\n", err)
			return 1
		}
		judge, err = eval.NewOpenAIJudge(eval.OpenAIChatConfig{
			BaseURL: cfg.EvalJudgeBaseURL,
			APIKey:  cfg.EvalJudgeAPIKey,
			Model:   cfg.EvalJudgeModel,
		})
		if err != nil {
			fmt.Fprintf(stderr, "eval judge init failed: %v\n", err)
			return 1
		}
	}

	store, err := storage.OpenPools(ctx, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "database open failed: %v\n", err)
		return 1
	}
	defer store.Close()

	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	runner := eval.NewRunner(cfg, store, cur, emb, reader, judge, logger)

	suites := splitCSV(*suiteFlag)
	report, err := runner.Run(ctx, eval.RunOptions{
		Suites:     suites,
		DatasetDir: *datasetDir,
		MaxTokens:  *maxTokens,
		Limit:      *limit,
		Mock:       *mock,
	})
	if err != nil {
		fmt.Fprintf(stderr, "eval failed: %v\n", err)
		return 1
	}

	if code := writeJSON(stdout, stderr, report); code != 0 {
		return code
	}
	if !report.Passed {
		return 1
	}
	return 0
}

func runList(ctx context.Context, args []string, envLookup func(string) (string, bool), stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	workspace := fs.String("workspace", "", "filter by workspace_id")
	durability := fs.String("durability", "", "filter by durability (constitution|durable|working)")
	status := fs.String("status", "", "filter by status (active|superseded|retired)")
	curationState := fs.String("curation-state", "", "filter by curation_state")
	premiseDrifted := fs.String("premise-drifted", "", "filter by premise_drifted (true|false)")
	includeRevisions := fs.Bool("include-revisions", false, "include revision history per memory")
	cursor := fs.String("cursor", "", "pagination cursor")
	limit := fs.Int("limit", 100, "page size limit")
	asJSON := fs.Bool("json", false, "output raw JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *limit < 1 {
		fmt.Fprintln(stderr, "list failed: --limit must be >= 1")
		return 2
	}

	q := url.Values{}
	if *workspace != "" {
		q.Set("workspace_id", *workspace)
	}
	if *durability != "" {
		q.Set("durability", *durability)
	}
	if *status != "" {
		q.Set("status", *status)
	}
	if *curationState != "" {
		q.Set("curation_state", *curationState)
	}
	if *premiseDrifted != "" {
		q.Set("premise_drifted", *premiseDrifted)
	}
	if *includeRevisions {
		q.Set("include_revisions", "true")
	}
	if *cursor != "" {
		q.Set("cursor", *cursor)
	}
	q.Set("limit", strconv.Itoa(*limit))

	var page storage.ListMemoriesPage
	if code := callAdminJSON(ctx, http.MethodGet, "/v1/memories?"+q.Encode(), nil, &page, envLookup, stderr); code != 0 {
		return code
	}
	if *asJSON {
		return writeJSON(stdout, stderr, page)
	}
	for _, it := range page.Items {
		drift := ""
		if it.PremiseDrifted {
			drift = " [DRIFTED]"
		}
		fmt.Fprintf(stdout, "%s  %-14s  %-12s  %-10s  v%-2d  %-28s  %s%s\n",
			it.MemoryID, it.WorkspaceID, it.Durability, it.Status, it.CurrentVersion, it.TopicKey, it.SummaryStub, drift)
	}
	if page.NextCursor != "" {
		fmt.Fprintf(stdout, "next_cursor: %s\n", page.NextCursor)
	}
	return 0
}

func runExplain(ctx context.Context, args []string, envLookup func(string) (string, bool), stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("explain", flag.ContinueOnError)
	fs.SetOutput(stderr)
	idFlag := fs.String("id", "", "memory UUID to inspect")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	id, ok := parseRequiredUUIDFlag(*idFlag, "explain", stderr)
	if !ok {
		return 2
	}

	var raw json.RawMessage
	if code := callAdminJSON(ctx, http.MethodGet, "/v1/memories/"+id.String()+"/explain", nil, &raw, envLookup, stderr); code != 0 {
		return code
	}
	return writeJSON(stdout, stderr, raw)
}

func runRevert(ctx context.Context, args []string, envLookup func(string) (string, bool), stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("revert", flag.ContinueOnError)
	fs.SetOutput(stderr)
	idFlag := fs.String("id", "", "memory UUID to revert")
	toVersion := fs.Int("to-version", 0, "target historical version to restore")
	expectedVersion := fs.Int("expected-version", 0, "current manifest version for CAS check")
	reason := fs.String("reason", "", "audit reason for rollback")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	id, ok := parseRequiredUUIDFlag(*idFlag, "revert", stderr)
	if !ok {
		return 2
	}
	if *toVersion < 1 || *expectedVersion < 1 {
		fmt.Fprintln(stderr, "revert failed: --to-version >= 1 and --expected-version >= 1 are required")
		return 2
	}

	var res json.RawMessage
	if code := callAdminJSON(ctx, http.MethodPost, "/v1/memories/"+id.String()+"/revert", map[string]any{
		"expected_version": *expectedVersion,
		"target_version":   *toVersion,
		"reason":           *reason,
	}, &res, envLookup, stderr); code != 0 {
		return code
	}
	return writeJSON(stdout, stderr, res)
}

func runPurge(ctx context.Context, args []string, envLookup func(string) (string, bool), stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("purge", flag.ContinueOnError)
	fs.SetOutput(stderr)
	idFlag := fs.String("id", "", "memory UUID to purge")
	yes := fs.Bool("yes", false, "confirm irreversible hard purge")
	reason := fs.String("reason", "", "audit reason for purge")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	id, ok := parseRequiredUUIDFlag(*idFlag, "purge", stderr)
	if !ok {
		return 2
	}
	if !*yes {
		fmt.Fprintln(stderr, "purge failed: --yes confirmation flag is required")
		return 2
	}

	var res json.RawMessage
	if code := callAdminJSON(ctx, http.MethodPost, "/v1/memories/"+id.String()+"/purge", map[string]any{
		"confirm": true,
		"reason":  *reason,
	}, &res, envLookup, stderr); code != 0 {
		return code
	}
	return writeJSON(stdout, stderr, res)
}

func runConfirmToken(ctx context.Context, args []string, envLookup func(string) (string, bool), stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("confirm-token", flag.ContinueOnError)
	fs.SetOutput(stderr)
	workspace := fs.String("workspace", "", "target workspace_id")
	action := fs.String("action", "", "privileged action (create|edit|promote|revert|purge)")
	targetFlag := fs.String("target", "", "optional target memory UUID")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*workspace) == "" || strings.TrimSpace(*action) == "" {
		fmt.Fprintln(stderr, "confirm-token failed: --workspace and --action are required")
		return 2
	}
	reqBody := map[string]any{
		"workspace_id": strings.TrimSpace(*workspace),
		"action":       strings.TrimSpace(*action),
	}
	if trimmedTarget := strings.TrimSpace(*targetFlag); trimmedTarget != "" {
		uid, err := uuid.Parse(trimmedTarget)
		if err != nil {
			fmt.Fprintf(stderr, "confirm-token failed: invalid --target UUID %q\n", trimmedTarget)
			return 2
		}
		reqBody["target_memory_id"] = uid.String()
	}

	var res service.ConfirmTokenResult
	if code := callAdminJSON(ctx, http.MethodPost, "/v1/auth/confirm-token", reqBody, &res, envLookup, stderr); code != 0 {
		return code
	}
	return writeJSON(stdout, stderr, res)
}

func runDeadLetters(ctx context.Context, args []string, envLookup func(string) (string, bool), stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dead-letters", flag.ContinueOnError)
	fs.SetOutput(stderr)
	limit := fs.Int("limit", 20, "maximum dead-letter records to return")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *limit < 1 {
		fmt.Fprintln(stderr, "dead-letters failed: --limit must be >= 1")
		return 2
	}

	var rep storage.DeadLettersReport
	if code := callAdminJSON(ctx, http.MethodGet, "/v1/ops/dead-letters?limit="+strconv.Itoa(*limit), nil, &rep, envLookup, stderr); code != 0 {
		return code
	}
	return writeJSON(stdout, stderr, rep)
}

func parseRequiredUUIDFlag(raw, cmdName string, stderr io.Writer) (uuid.UUID, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		fmt.Fprintf(stderr, "%s failed: --id=<uuid> is required\n", cmdName)
		return uuid.Nil, false
	}
	id, err := uuid.Parse(trimmed)
	if err != nil {
		fmt.Fprintf(stderr, "%s failed: invalid --id UUID %q\n", cmdName, trimmed)
		return uuid.Nil, false
	}
	return id, true
}

func callAdminJSON(ctx context.Context, method, path string, reqBody, out any, envLookup func(string) (string, bool), stderr io.Writer) int {
	var adminToken string
	if v, ok := envLookup("MEM1_ADMIN_TOKEN"); ok {
		adminToken = strings.TrimSpace(v)
	}
	if adminToken == "" {
		fmt.Fprintln(stderr, "MEM1_ADMIN_TOKEN is required for admin CLI commands")
		return 1
	}

	httpAddr, _ := envLookup("MEM1_HTTP_ADDR")
	base := adminBaseURL(httpAddr)
	var bodyReader io.Reader
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		if err != nil {
			fmt.Fprintf(stderr, "encode request: %v\n", err)
			return 1
		}
		bodyReader = bytes.NewReader(raw)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, method, base+path, bodyReader)
	if err != nil {
		fmt.Fprintf(stderr, "build request: %v\n", err)
		return 1
	}
	req.Header.Set("Authorization", "Bearer "+adminToken)
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		fmt.Fprintf(stderr, "http request failed: %v\n", err)
		return 1
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		fmt.Fprintf(stderr, "read response: %v\n", err)
		return 1
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(stderr, "HTTP %d: %s\n", resp.StatusCode, strings.TrimSpace(string(respBytes)))
		return 1
	}
	if out != nil && len(respBytes) > 0 {
		if err := json.Unmarshal(respBytes, out); err != nil {
			fmt.Fprintf(stderr, "decode JSON response: %v\n", err)
			return 1
		}
	}
	return 0
}

func adminBaseURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "http://127.0.0.1:8787"
	}
	if strings.HasPrefix(trimmed, ":") {
		return "http://127.0.0.1" + strings.TrimRight(trimmed, "/")
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		return strings.TrimRight(trimmed, "/")
	}
	return "http://" + strings.TrimRight(trimmed, "/")
}

func buildEmbedder(cfg config.Config) (embedding.Embedder, error) {
	if cfg.EmbedProvider == config.ProviderMock {
		return embedding.NewMockEmbedder(cfg.EmbedSpaceID), nil
	}
	return embedding.NewOpenAIEmbedder(embedding.OpenAIConfig{
		BaseURL:    cfg.EmbedBaseURL,
		APIKey:     cfg.EmbedAPIKey,
		Model:      cfg.EmbedModel,
		SpaceID:    cfg.EmbedSpaceID,
		Dimensions: cfg.EmbedDim,
		Timeout:    cfg.EmbedTimeout,
	})
}

func buildCurator(cfg config.Config) (curation.Curator, error) {
	if cfg.CuratorProvider == config.ProviderMock {
		return curation.NewMockCurator(), nil
	}
	return curation.NewOpenAICurator(curation.OpenAICuratorConfig{
		BaseURL:      cfg.CuratorBaseURL,
		APIKey:       cfg.CuratorAPIKey,
		Model:        cfg.CuratorModel,
		ProviderName: cfg.CuratorProvider,
		Timeout:      cfg.CuratorTimeout,
	})
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func writeJSON(stdout, stderr io.Writer, v any) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fmt.Fprintf(stderr, "encode JSON output: %v\n", err)
		return 1
	}
	return 0
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: mem1 [--env-file=<path>] <subcommand> [flags]")
	fmt.Fprintln(w, "  loads .env automatically if present (override with --env-file or MEM1_ENV_FILE; shell env takes precedence)")
	fmt.Fprintln(w, "subcommands:")
	fmt.Fprintln(w, "  serve                                          run unified HTTP/MCP server (:8787) and background workers")
	fmt.Fprintln(w, "  mcp-stdio                                      run lightweight MCP stdio server")
	fmt.Fprintln(w, "  migrate [--down]                               apply or roll back PostgreSQL schema migrations")
	fmt.Fprintln(w, "  reindex [--batch-size=32]                      recompute inline memory embeddings in place")
	fmt.Fprintln(w, "  eval [--suite=...] [--max-tokens=2000]         run Priority 1 quality evaluation harness")
	fmt.Fprintln(w, "  list [--workspace=<id>] [--limit=100] [--json] list memories across workspaces")
	fmt.Fprintln(w, "  explain --id=<memory_id>                       inspect memory provenance lineage")
	fmt.Fprintln(w, "  revert --id=<memory_id> --to-version=<k> --expected-version=<n>")
	fmt.Fprintln(w, "  purge --id=<memory_id> --yes [--reason=...]    hard-purge memory and cited raw inbox content")
	fmt.Fprintln(w, "  confirm-token --workspace=<id> --action=<...>  mint single-use confirmation token for MCP")
	fmt.Fprintln(w, "  dead-letters [--limit=20]                      inspect dead-lettered inbox items and discarded jobs")
}
