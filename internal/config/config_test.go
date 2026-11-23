package config

import (
	"strings"
	"testing"
	"time"
)

func envLookup(values map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := values[k]
		return v, ok
	}
}

func baseValidServeEnv() map[string]string {
	return map[string]string{
		"MEM1_DATABASE_URL":      "postgres://mem1:mem1@127.0.0.1:5432/mem1?sslmode=disable",
		"MEM1_ADMIN_TOKEN":       "admin-secret-token",
		"MEM1_CURATOR_BASE_URL":  "https://api.openai.com/v1",
		"MEM1_CURATOR_MODEL":     "gpt-4.1-mini",
		"MEM1_CURATOR_API_KEY":   "sk-curator-key",
		"MEM1_EMBED_BASE_URL":    "https://api.openai.com/v1",
		"MEM1_EMBED_API_KEY":     "sk-embed-key",
	}
}

func TestLoadMigrateModeAllowsMissingLLMTokensAndHTTPAuth(t *testing.T) {
	cfg, err := LoadFromLookup(ModeMigrate, envLookup(map[string]string{
		"MEM1_DATABASE_URL": "postgres://mem1:mem1@127.0.0.1:5432/mem1?sslmode=disable",
	}))
	if err != nil {
		t.Fatalf("unexpected error in migrate mode: %v", err)
	}
	if cfg.EmbedDim != 1536 || cfg.ServingMaxConns != 28 || cfg.CurationMaxConns != 6 ||
		cfg.StdioServingMaxConns != 4 || cfg.StdioCurationMaxConns != 2 ||
		cfg.MaxInflight != 512 || cfg.MaxWriteBytes != 65536 || cfg.HistoryRetentionN != 20 ||
		cfg.CuratorTimeout != 30*time.Second || cfg.EmbedTimeout != 2*time.Second ||
		cfg.EmbedSpaceID != "openai/text-embedding-3-small:1536" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.EffectiveServingMaxConns() != 28 || cfg.EffectiveCurationMaxConns() != 6 {
		t.Fatalf("unexpected effective pool sizes in migrate mode")
	}
}

func TestLoadServeValidationAndLoopbackRules(t *testing.T) {
	// Missing MEM1_ADMIN_TOKEN fails serve mode.
	env := baseValidServeEnv()
	delete(env, "MEM1_ADMIN_TOKEN")
	if _, err := LoadFromLookup(ModeServe, envLookup(env)); err == nil || !strings.Contains(err.Error(), "MEM1_ADMIN_TOKEN") {
		t.Fatalf("expected MEM1_ADMIN_TOKEN error, got %v", err)
	}

	// Loopback addresses succeed without MEM1_AGENT_TOKEN.
	for _, addr := range []string{"127.0.0.1:8787", "localhost:8787", "[::1]:8787"} {
		env = baseValidServeEnv()
		env["MEM1_HTTP_ADDR"] = addr
		if _, err := LoadFromLookup(ModeServe, envLookup(env)); err != nil {
			t.Fatalf("loopback %q failed unexpectedly: %v", addr, err)
		}
	}

	// Non-loopback bindings require MEM1_AGENT_TOKEN.
	for _, addr := range []string{":8787", "0.0.0.0:8787", "10.0.0.5:8787"} {
		env = baseValidServeEnv()
		env["MEM1_HTTP_ADDR"] = addr
		if _, err := LoadFromLookup(ModeServe, envLookup(env)); err == nil || !strings.Contains(err.Error(), "MEM1_AGENT_TOKEN") {
			t.Fatalf("non-loopback %q should fail without MEM1_AGENT_TOKEN, got %v", addr, err)
		}
		env["MEM1_AGENT_TOKEN"] = "agent-secret-token"
		if _, err := LoadFromLookup(ModeServe, envLookup(env)); err != nil {
			t.Fatalf("non-loopback %q with MEM1_AGENT_TOKEN failed: %v", addr, err)
		}
	}
}

func TestLoadMCPStdioAndReindexAndEvalModes(t *testing.T) {
	// mcp-stdio requires embedder (unless mock), NOT admin token or curator keys.
	cfg, err := LoadFromLookup(ModeMCPStdio, envLookup(map[string]string{
		"MEM1_DATABASE_URL":   "postgresql://mem1:mem1@127.0.0.1:5432/mem1",
		"MEM1_EMBED_BASE_URL": "http://127.0.0.1:11434/v1",
		"MEM1_EMBED_API_KEY":  "local-key",
	}))
	if err != nil {
		t.Fatalf("mcp-stdio failed: %v", err)
	}
	if cfg.EffectiveServingMaxConns() != 4 || cfg.EffectiveCurationMaxConns() != 2 {
		t.Fatalf("expected stdio pool sizes 4/2, got %d/%d", cfg.EffectiveServingMaxConns(), cfg.EffectiveCurationMaxConns())
	}

	// reindex passes with mock embedder.
	if _, err := LoadFromLookup(ModeReindex, envLookup(map[string]string{
		"MEM1_DATABASE_URL":   "postgres://mem1:mem1@127.0.0.1:5432/mem1",
		"MEM1_EMBED_PROVIDER": "mock",
	})); err != nil {
		t.Fatalf("reindex with mock embedder failed: %v", err)
	}

	// eval with mock providers succeeds; ValidateEvalLive fails until reader/judge set.
	evalCfg, err := LoadFromLookup(ModeEval, envLookup(map[string]string{
		"MEM1_DATABASE_URL":     "postgres://mem1:mem1@127.0.0.1:5432/mem1",
		"MEM1_CURATOR_PROVIDER": "mock",
		"MEM1_EMBED_PROVIDER":   "mock",
	}))
	if err != nil {
		t.Fatalf("eval with mock providers failed: %v", err)
	}
	if err := evalCfg.ValidateEvalLive(); err == nil {
		t.Fatalf("expected ValidateEvalLive error when reader/judge fields empty")
	}

	liveEvalCfg, err := LoadFromLookup(ModeEval, envLookup(map[string]string{
		"MEM1_DATABASE_URL":           "postgres://mem1:mem1@127.0.0.1:5432/mem1",
		"MEM1_CURATOR_PROVIDER":       "mock",
		"MEM1_EMBED_PROVIDER":         "mock",
		"MEM1_EVAL_READER_BASE_URL":   "https://api.openai.com/v1",
		"MEM1_EVAL_READER_MODEL":      "gpt-4.1-mini",
		"MEM1_EVAL_READER_API_KEY":    "sk-reader",
		"MEM1_EVAL_JUDGE_BASE_URL":    "https://api.openai.com/v1",
		"MEM1_EVAL_JUDGE_MODEL":       "gpt-4.1",
		"MEM1_EVAL_JUDGE_API_KEY":     "sk-judge",
	}))
	if err != nil {
		t.Fatalf("eval load failed: %v", err)
	}
	if err := liveEvalCfg.ValidateEvalLive(); err != nil {
		t.Fatalf("ValidateEvalLive failed on valid config: %v", err)
	}
}

func TestLoadValidationErrorsAndZDR(t *testing.T) {
	if _, err := LoadFromLookup("invalid-mode", envLookup(nil)); err == nil {
		t.Fatalf("expected error on unknown mode")
	}

	cases := []struct {
		name   string
		mutate func(map[string]string)
		want   string
	}{
		{
			name:   "missing database url",
			mutate: func(m map[string]string) { delete(m, "MEM1_DATABASE_URL") },
			want:   "MEM1_DATABASE_URL",
		},
		{
			name:   "bad database scheme",
			mutate: func(m map[string]string) { m["MEM1_DATABASE_URL"] = "mysql://root@localhost/db" },
			want:   "postgres://",
		},
		{
			name:   "embed dim != 1536",
			mutate: func(m map[string]string) { m["MEM1_EMBED_DIM"] = "768" },
			want:   "1536",
		},
		{
			name:   "negative history retention",
			mutate: func(m map[string]string) { m["MEM1_HISTORY_RETENTION_N"] = "-1" },
			want:   "MEM1_HISTORY_RETENTION_N",
		},
		{
			name:   "invalid int",
			mutate: func(m map[string]string) { m["MEM1_MAX_INFLIGHT"] = "abc" },
			want:   "MEM1_MAX_INFLIGHT",
		},
		{
			name:   "invalid bool",
			mutate: func(m map[string]string) { m["MEM1_ALLOW_NON_ZDR"] = "notabool" },
			want:   "MEM1_ALLOW_NON_ZDR",
		},
		{
			name:   "invalid duration",
			mutate: func(m map[string]string) { m["MEM1_EMBED_TIMEOUT"] = "soon" },
			want:   "MEM1_EMBED_TIMEOUT",
		},
		{
			name:   "invalid curator provider",
			mutate: func(m map[string]string) { m["MEM1_CURATOR_PROVIDER"] = "unknown" },
			want:   "MEM1_CURATOR_PROVIDER",
		},
		{
			name:   "zdr rejects openrouter free model",
			mutate: func(m map[string]string) { m["MEM1_CURATOR_MODEL"] = "meta-llama/llama:free" },
			want:   "endpoint policy",
		},
		{
			name:   "invalid http addr",
			mutate: func(m map[string]string) { m["MEM1_HTTP_ADDR"] = "not-host-port" },
			want:   "MEM1_HTTP_ADDR",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := baseValidServeEnv()
			tc.mutate(env)
			_, err := LoadFromLookup(ModeServe, envLookup(env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got err=%v, want substring %q", err, tc.want)
			}
		})
	}

	// MEM1_ALLOW_NON_ZDR=true permits non-ZDR curator model.
	env := baseValidServeEnv()
	env["MEM1_CURATOR_BASE_URL"] = "https://openrouter.ai/api/v1"
	env["MEM1_CURATOR_MODEL"] = "meta-llama/llama:free"
	env["MEM1_ALLOW_NON_ZDR"] = "true"
	if _, err := LoadFromLookup(ModeServe, envLookup(env)); err != nil {
		t.Fatalf("expected allowNonZDR override to succeed, got %v", err)
	}
}
