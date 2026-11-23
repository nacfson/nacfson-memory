package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nacfson/nacfson-memory/internal/security"
)

const (
	ModeServe    = "serve"
	ModeMCPStdio = "mcp-stdio"
	ModeMigrate  = "migrate"
	ModeReindex  = "reindex"
	ModeEval     = "eval"

	ProviderOpenAICompat = "openai_compat"
	ProviderMock         = "mock"

	RequiredEmbedDim = 1536
)

// Config holds validated mem1 runtime configuration scoped by CLI mode.
type Config struct {
	Mode string

	// Database & HTTP
	DatabaseURL  string
	HTTPAddr     string
	AllowedHosts []string
	AdminToken   string
	AgentToken   string

	// Pools & limits
	ServingMaxConns      int
	CurationMaxConns     int
	StdioServingMaxConns int
	StdioCurationMaxConns int
	MaxInflight          int
	MaxWriteBytes        int
	HistoryRetentionN    int
	CurationPaused       bool
	ForceProvisional     bool

	// Providers
	CuratorProvider string
	CuratorBaseURL  string
	CuratorModel    string
	CuratorAPIKey   string
	CuratorTimeout  time.Duration

	EmbedProvider string
	EmbedBaseURL  string
	EmbedModel    string
	EmbedAPIKey   string
	EmbedSpaceID  string
	EmbedDim      int
	EmbedTimeout  time.Duration
	AllowNonZDR   bool

	// Evaluation (live mode)
	EvalReaderBaseURL string
	EvalReaderModel   string
	EvalReaderAPIKey  string
	EvalJudgeBaseURL  string
	EvalJudgeModel    string
	EvalJudgeAPIKey   string
}

// Load reads and validates process environment configuration for the given mode.
func Load(mode string) (Config, error) {
	return LoadFromLookup(mode, os.LookupEnv)
}

// LoadFromLookup reads and validates configuration from a custom env lookup function.
func LoadFromLookup(mode string, lookup func(string) (string, bool)) (Config, error) {
	if !isValidMode(mode) {
		return Config{}, fmt.Errorf("unsupported mem1 mode %q", mode)
	}

	var errs []error
	getStr := func(key, def string) string {
		if v, ok := lookup(key); ok {
			trimmed := strings.TrimSpace(v)
			if trimmed != "" {
				return trimmed
			}
		}
		return def
	}
	getInt := func(key string, def int) int {
		v, ok := lookup(key)
		if !ok || strings.TrimSpace(v) == "" {
			return def
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid integer for %s: %w", key, err))
			return def
		}
		return parsed
	}
	getBool := func(key string, def bool) bool {
		v, ok := lookup(key)
		if !ok || strings.TrimSpace(v) == "" {
			return def
		}
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid boolean for %s: %w", key, err))
			return def
		}
		return parsed
	}
	getDur := func(key string, def time.Duration) time.Duration {
		v, ok := lookup(key)
		if !ok || strings.TrimSpace(v) == "" {
			return def
		}
		parsed, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid duration for %s: %w", key, err))
			return def
		}
		return parsed
	}

	cfg := Config{
		Mode:                  mode,
		DatabaseURL:           getStr("MEM1_DATABASE_URL", ""),
		HTTPAddr:              getStr("MEM1_HTTP_ADDR", "127.0.0.1:8787"),
		AllowedHosts:          parseCSV(getStr("MEM1_ALLOWED_HOSTS", "127.0.0.1,localhost,[::1]")),
		AdminToken:            getStr("MEM1_ADMIN_TOKEN", ""),
		AgentToken:            getStr("MEM1_AGENT_TOKEN", ""),
		ServingMaxConns:       getInt("MEM1_SERVING_MAX_CONNS", 28),
		CurationMaxConns:      getInt("MEM1_CURATION_MAX_CONNS", 6),
		StdioServingMaxConns:  getInt("MEM1_STDIO_SERVING_MAX_CONNS", 4),
		StdioCurationMaxConns: getInt("MEM1_STDIO_CURATION_MAX_CONNS", 2),
		MaxInflight:           getInt("MEM1_MAX_INFLIGHT", 512),
		MaxWriteBytes:         getInt("MEM1_MAX_WRITE_BYTES", 65536),
		HistoryRetentionN:     getInt("MEM1_HISTORY_RETENTION_N", 20),
		CurationPaused:        getBool("MEM1_CURATION_PAUSED", false),
		ForceProvisional:      getBool("MEM1_FORCE_PROVISIONAL", false),
		CuratorProvider:       getStr("MEM1_CURATOR_PROVIDER", ProviderOpenAICompat),
		CuratorBaseURL:        getStr("MEM1_CURATOR_BASE_URL", ""),
		CuratorModel:          getStr("MEM1_CURATOR_MODEL", ""),
		CuratorAPIKey:         getStr("MEM1_CURATOR_API_KEY", ""),
		CuratorTimeout:        getDur("MEM1_CURATOR_TIMEOUT", 30*time.Second),
		EmbedProvider:         getStr("MEM1_EMBED_PROVIDER", ProviderOpenAICompat),
		EmbedBaseURL:          getStr("MEM1_EMBED_BASE_URL", ""),
		EmbedModel:            getStr("MEM1_EMBED_MODEL", "text-embedding-3-small"),
		EmbedAPIKey:           getStr("MEM1_EMBED_API_KEY", ""),
		EmbedSpaceID:          getStr("MEM1_EMBED_SPACE_ID", "openai/text-embedding-3-small:1536"),
		EmbedDim:              getInt("MEM1_EMBED_DIM", RequiredEmbedDim),
		EmbedTimeout:          getDur("MEM1_EMBED_TIMEOUT", 2*time.Second),
		AllowNonZDR:           getBool("MEM1_ALLOW_NON_ZDR", false),
		EvalReaderBaseURL:     getStr("MEM1_EVAL_READER_BASE_URL", ""),
		EvalReaderModel:       getStr("MEM1_EVAL_READER_MODEL", ""),
		EvalReaderAPIKey:      getStr("MEM1_EVAL_READER_API_KEY", ""),
		EvalJudgeBaseURL:      getStr("MEM1_EVAL_JUDGE_BASE_URL", ""),
		EvalJudgeModel:        getStr("MEM1_EVAL_JUDGE_MODEL", ""),
		EvalJudgeAPIKey:       getStr("MEM1_EVAL_JUDGE_API_KEY", ""),
	}

	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}

	if err := cfg.validateBase(); err != nil {
		return Config{}, err
	}
	if err := cfg.validateModeSpecific(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// EffectiveServingMaxConns returns the serving pool limit for the active mode.
func (c Config) EffectiveServingMaxConns() int {
	if c.Mode == ModeMCPStdio {
		return c.StdioServingMaxConns
	}
	return c.ServingMaxConns
}

// EffectiveCurationMaxConns returns the curation pool limit for the active mode.
func (c Config) EffectiveCurationMaxConns() int {
	if c.Mode == ModeMCPStdio {
		return c.StdioCurationMaxConns
	}
	return c.CurationMaxConns
}

// ValidateEvalLive ensures live evaluation Reader and Judge endpoints are configured
// and comply with the ZDR endpoint policy.
func (c Config) ValidateEvalLive() error {
	var errs []error
	if err := validateLiveOpenAI("MEM1_EVAL_READER", c.EvalReaderBaseURL, c.EvalReaderModel, c.EvalReaderAPIKey, c.AllowNonZDR); err != nil {
		errs = append(errs, err)
	}
	if err := validateLiveOpenAI("MEM1_EVAL_JUDGE", c.EvalJudgeBaseURL, c.EvalJudgeModel, c.EvalJudgeAPIKey, c.AllowNonZDR); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// IsLoopbackAddr reports whether addr (host:port) binds strictly to loopback.
func IsLoopbackAddr(addr string) (bool, error) {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return false, fmt.Errorf("invalid HTTP address %q: %w", addr, err)
	}
	if host == "" {
		return false, nil
	}
	return security.IsLoopbackHost(host), nil
}

func (c Config) validateBase() error {
	var errs []error

	if err := validatePostgresURL(c.DatabaseURL); err != nil {
		errs = append(errs, err)
	}
	if c.EmbedDim != RequiredEmbedDim {
		errs = append(errs, fmt.Errorf("MEM1_EMBED_DIM must be %d (got %d)", RequiredEmbedDim, c.EmbedDim))
	}
	if c.ServingMaxConns < 1 {
		errs = append(errs, fmt.Errorf("MEM1_SERVING_MAX_CONNS must be >= 1 (got %d)", c.ServingMaxConns))
	}
	if c.CurationMaxConns < 1 {
		errs = append(errs, fmt.Errorf("MEM1_CURATION_MAX_CONNS must be >= 1 (got %d)", c.CurationMaxConns))
	}
	if c.StdioServingMaxConns < 1 {
		errs = append(errs, fmt.Errorf("MEM1_STDIO_SERVING_MAX_CONNS must be >= 1 (got %d)", c.StdioServingMaxConns))
	}
	if c.StdioCurationMaxConns < 1 {
		errs = append(errs, fmt.Errorf("MEM1_STDIO_CURATION_MAX_CONNS must be >= 1 (got %d)", c.StdioCurationMaxConns))
	}
	if c.MaxInflight < 1 {
		errs = append(errs, fmt.Errorf("MEM1_MAX_INFLIGHT must be >= 1 (got %d)", c.MaxInflight))
	}
	if c.MaxWriteBytes < 1 {
		errs = append(errs, fmt.Errorf("MEM1_MAX_WRITE_BYTES must be >= 1 (got %d)", c.MaxWriteBytes))
	}
	if c.HistoryRetentionN < 0 {
		errs = append(errs, fmt.Errorf("MEM1_HISTORY_RETENTION_N must be >= 0 (got %d)", c.HistoryRetentionN))
	}
	if c.CuratorTimeout <= 0 {
		errs = append(errs, fmt.Errorf("MEM1_CURATOR_TIMEOUT must be > 0 (got %s)", c.CuratorTimeout))
	}
	if c.EmbedTimeout <= 0 {
		errs = append(errs, fmt.Errorf("MEM1_EMBED_TIMEOUT must be > 0 (got %s)", c.EmbedTimeout))
	}
	if !isValidProvider(c.CuratorProvider) {
		errs = append(errs, fmt.Errorf("MEM1_CURATOR_PROVIDER must be %q or %q (got %q)", ProviderOpenAICompat, ProviderMock, c.CuratorProvider))
	}
	if !isValidProvider(c.EmbedProvider) {
		errs = append(errs, fmt.Errorf("MEM1_EMBED_PROVIDER must be %q or %q (got %q)", ProviderOpenAICompat, ProviderMock, c.EmbedProvider))
	}

	return errors.Join(errs...)
}

func (c Config) validateModeSpecific() error {
	switch c.Mode {
	case ModeMigrate:
		return nil
	case ModeMCPStdio:
		return c.validateEmbedder()
	case ModeReindex:
		var errs []error
		if err := c.validateEmbedder(); err != nil {
			errs = append(errs, err)
		}
		if c.EmbedSpaceID == "" {
			errs = append(errs, errors.New("MEM1_EMBED_SPACE_ID is required in reindex mode"))
		}
		return errors.Join(errs...)
	case ModeEval:
		var errs []error
		if err := c.validateCurator(); err != nil {
			errs = append(errs, err)
		}
		if err := c.validateEmbedder(); err != nil {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	case ModeServe:
		var errs []error
		isLoopback, err := IsLoopbackAddr(c.HTTPAddr)
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid MEM1_HTTP_ADDR: %w", err))
		}
		if len(c.AllowedHosts) == 0 {
			errs = append(errs, errors.New("MEM1_ALLOWED_HOSTS must contain at least one host"))
		}
		if c.AdminToken == "" {
			errs = append(errs, errors.New("MEM1_ADMIN_TOKEN is required in serve mode"))
		}
		if err == nil && !isLoopback && c.AgentToken == "" {
			errs = append(errs, fmt.Errorf("MEM1_AGENT_TOKEN is required when MEM1_HTTP_ADDR (%q) binds non-loopback", c.HTTPAddr))
		}
		if err := c.validateCurator(); err != nil {
			errs = append(errs, err)
		}
		if err := c.validateEmbedder(); err != nil {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	default:
		return fmt.Errorf("unsupported mem1 mode %q", c.Mode)
	}
}

func (c Config) validateCurator() error {
	if c.CuratorProvider == ProviderMock {
		return nil
	}
	return validateLiveOpenAI("MEM1_CURATOR", c.CuratorBaseURL, c.CuratorModel, c.CuratorAPIKey, c.AllowNonZDR)
}

func (c Config) validateEmbedder() error {
	if c.EmbedProvider == ProviderMock {
		return nil
	}
	return validateLiveOpenAI("MEM1_EMBED", c.EmbedBaseURL, c.EmbedModel, c.EmbedAPIKey, c.AllowNonZDR)
}

func validateLiveOpenAI(prefix, baseURL, model, apiKey string, allowNonZDR bool) error {
	var errs []error
	if strings.TrimSpace(baseURL) == "" {
		errs = append(errs, fmt.Errorf("%s_BASE_URL is required when provider is %s", prefix, ProviderOpenAICompat))
	}
	if strings.TrimSpace(model) == "" {
		errs = append(errs, fmt.Errorf("%s_MODEL is required when provider is %s", prefix, ProviderOpenAICompat))
	}
	if strings.TrimSpace(apiKey) == "" {
		errs = append(errs, fmt.Errorf("%s_API_KEY is required when provider is %s", prefix, ProviderOpenAICompat))
	}
	if strings.TrimSpace(baseURL) != "" {
		if err := security.ValidateProviderEndpoint(baseURL, model, allowNonZDR); err != nil {
			errs = append(errs, fmt.Errorf("%s endpoint policy violation: %w", prefix, err))
		}
	}
	return errors.Join(errs...)
}

func validatePostgresURL(raw string) error {
	if raw == "" {
		return errors.New("MEM1_DATABASE_URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid MEM1_DATABASE_URL: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "postgres" && scheme != "postgresql" {
		return fmt.Errorf("MEM1_DATABASE_URL must use postgres:// or postgresql:// scheme (got %q)", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("MEM1_DATABASE_URL must include a database host")
	}
	return nil
}

func parseCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func isValidMode(mode string) bool {
	switch mode {
	case ModeServe, ModeMCPStdio, ModeMigrate, ModeReindex, ModeEval:
		return true
	default:
		return false
	}
}

func isValidProvider(p string) bool {
	return p == ProviderOpenAICompat || p == ProviderMock
}
