package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nacfson/nacfson-memory/eval"
	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/retrieval"
	"github.com/nacfson/nacfson-memory/internal/security"
	"github.com/nacfson/nacfson-memory/internal/service"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case "migrate":
		return runMigrate(ctx, args[1:], stdout, stderr)
	case "reindex":
		return runReindex(ctx, args[1:], stdout, stderr)
	case "eval":
		return runEval(ctx, args[1:], stdout, stderr)
	case "-h", "--help", "help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown subcommand %q\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func runMigrate(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	down := fs.Bool("down", false, "roll back schema and River migrations")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := config.Load(config.ModeMigrate)
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

func runReindex(ctx context.Context, args []string, stdout, stderr io.Writer) int {
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

	cfg, err := config.Load(config.ModeReindex)
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

func runEval(ctx context.Context, args []string, stdout, stderr io.Writer) int {
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

	lookup := os.LookupEnv
	if *mock {
		lookup = func(k string) (string, bool) {
			if v, ok := os.LookupEnv(k); ok && strings.TrimSpace(v) != "" {
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
	fmt.Fprintln(w, "usage: mem1 <subcommand> [flags]")
	fmt.Fprintln(w, "subcommands:")
	fmt.Fprintln(w, "  migrate [--down]                               apply or roll back PostgreSQL schema migrations")
	fmt.Fprintln(w, "  reindex [--batch-size=32]                      recompute inline memory embeddings in place")
	fmt.Fprintln(w, "  eval [--suite=...] [--max-tokens=2000]         run Priority 1 quality evaluation harness")
}
