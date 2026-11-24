package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseEnvFileFormatsAndSafety(t *testing.T) {
	input := "\uFEFF" + strings.Join([]string{
		"# leading comment",
		"",
		"  # indented comment",
		"export MEM1_ADMIN_TOKEN=topsecret-admin",
		`MEM1_DATABASE_URL=postgres://user:pass#hash@127.0.0.1:5432/mem1?sslmode=disable # trailing comment`,
		`DOUBLE_ESCAPED="line1\nline2\t\"quoted\"\\end" # ok`,
		`SINGLE_VERBATIM='literal\nvalue # kept' # after`,
		`EMPTY_VAL=`,
		`COMMENT_AFTER_EQ= # ignored comment`,
		`DUP_KEY=first`,
		`DUP_KEY=second`,
	}, "\n")

	vals, err := ParseEnvFile(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseEnvFile failed: %v", err)
	}
	want := map[string]string{
		"MEM1_ADMIN_TOKEN":  "topsecret-admin",
		"MEM1_DATABASE_URL": "postgres://user:pass#hash@127.0.0.1:5432/mem1?sslmode=disable",
		"DOUBLE_ESCAPED":    "line1\nline2\t\"quoted\"\\end",
		"SINGLE_VERBATIM":   `literal\nvalue # kept`,
		"EMPTY_VAL":         "",
		"COMMENT_AFTER_EQ":  "",
		"DUP_KEY":           "second",
	}
	for k, w := range want {
		if got, ok := vals[k]; !ok || got != w {
			t.Fatalf("key %s: got (%q, %v), want %q", k, got, ok, w)
		}
	}

	badLines := []struct {
		name  string
		line  string
		leaks string
	}{
		{"missing equals", "JUST_A_TOKEN_sk_live_secret", "sk_live_secret"},
		{"invalid key", "1BAD_KEY=sk_live_secret", "sk_live_secret"},
		{"unterminated double quote", `KEY="sk_live_secret`, "sk_live_secret"},
		{"unterminated single quote", `KEY='sk_live_secret`, "sk_live_secret"},
		{"trailing garbage double", `KEY="sk_live_secret" trailing`, "sk_live_secret"},
		{"trailing garbage single", `KEY='sk_live_secret' trailing`, "sk_live_secret"},
	}
	for _, tc := range badLines {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseEnvFile(strings.NewReader(tc.line))
			if err == nil {
				t.Fatalf("expected parse error for %s", tc.name)
			}
			if strings.Contains(err.Error(), tc.leaks) {
				t.Fatalf("error leaked secret value: %v", err)
			}
		})
	}
}

func TestResolveEnvLookupPrecedenceAndDotEnvLoad(t *testing.T) {
	dir := t.TempDir()
	defaultEnv := filepath.Join(dir, ".env")
	overrideEnv := filepath.Join(dir, "custom.env")

	if err := os.WriteFile(defaultEnv, []byte("MEM1_DATABASE_URL=postgres://mem1:mem1@127.0.0.1:5432/mem1\nMEM1_ADMIN_TOKEN=from-dotenv\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(overrideEnv, []byte("MEM1_ADMIN_TOKEN=from-custom\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Absent default .env is tolerated when resolving in an empty dir.
	emptyDir := t.TempDir()
	origWD, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(emptyDir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(origWD) })

	lookup, err := ResolveEnvLookup("", envLookup(map[string]string{"FOO": "bar"}))
	if err != nil {
		t.Fatalf("missing default .env should succeed: %v", err)
	}
	if v, ok := lookup("FOO"); !ok || v != "bar" {
		t.Fatalf("expected base lookup FOO=bar, got %q %v", v, ok)
	}

	// Present .env in cwd is picked up by config.Load, with OS env taking precedence (even empty string).
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MEM1_ADMIN_TOKEN", "")
	cfg, err := Load(ModeMigrate)
	if err != nil {
		t.Fatalf("Load(ModeMigrate) with .env failed: %v", err)
	}
	if cfg.DatabaseURL != "postgres://mem1:mem1@127.0.0.1:5432/mem1" {
		t.Fatalf("expected DatabaseURL from .env, got %q", cfg.DatabaseURL)
	}
	if cfg.AdminToken != "" {
		t.Fatalf("expected explicit empty OS env MEM1_ADMIN_TOKEN to override .env, got %q", cfg.AdminToken)
	}

	// MEM1_ENV_FILE resolves custom file, and explicit envFile argument overrides MEM1_ENV_FILE.
	byEnvVar, err := ResolveEnvLookup("", envLookup(map[string]string{
		"MEM1_ENV_FILE": overrideEnv,
	}))
	if err != nil {
		t.Fatalf("ResolveEnvLookup via MEM1_ENV_FILE failed: %v", err)
	}
	if v, ok := byEnvVar("MEM1_ADMIN_TOKEN"); !ok || v != "from-custom" {
		t.Fatalf("expected from-custom, got %q %v", v, ok)
	}

	explicitOverride, err := ResolveEnvLookup(defaultEnv, envLookup(map[string]string{
		"MEM1_ENV_FILE": overrideEnv,
	}))
	if err != nil {
		t.Fatalf("explicit envFile override failed: %v", err)
	}
	if v, ok := explicitOverride("MEM1_ADMIN_TOKEN"); !ok || v != "from-dotenv" {
		t.Fatalf("expected from-dotenv, got %q %v", v, ok)
	}

	if _, err := ResolveEnvLookup(filepath.Join(dir, "missing.env"), envLookup(nil)); err == nil {
		t.Fatal("expected error for missing explicit envFile")
	}
	if _, err := ResolveEnvLookup("", envLookup(map[string]string{"MEM1_ENV_FILE": filepath.Join(dir, "missing.env")})); err == nil {
		t.Fatal("expected error for missing MEM1_ENV_FILE")
	}
}
