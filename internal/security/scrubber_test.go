package security

import (
	"strings"
	"testing"
)

func TestSecretScrubberRulesAndOrdering(t *testing.T) {
	scrubber := NewSecretScrubber()

	pemBlock := "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQE\n-----END PRIVATE KEY-----"
	anthropicKey := "sk-ant-api03-abcdefghijklmnopqrstuvwxyz012345"
	openAIKey := "sk-proj-abcdefghijklmnopqrstuvwxyz012345"
	legacyOpenAIKey := "sk-abcdefghijklmnopqrstuvwxyz012345"
	ghClassic := "ghp_0123456789abcdef0123456789abcdef0123"
	ghFine := "github_pat_0123456789abcdef012345_0123456789abcdef"
	awsKey := "AKIAIOSFODNN7EXAMPLE"
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyLTEyMyJ9.dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	bearer := "Authorization: Bearer mySuperSecretTokenValue123456"
	dsn := "postgresql://mem1:super_secret_pw@127.0.0.1:5432/mem1?sslmode=disable"

	input := strings.Join([]string{
		pemBlock,
		"anthropic=" + anthropicKey,
		"openai1=" + openAIKey,
		"openai2=" + legacyOpenAIKey,
		"gh1=" + ghClassic,
		"gh2=" + ghFine,
		"aws=" + awsKey,
		"jwt=" + jwt,
		bearer,
		"dsn=" + dsn,
	}, "\n")

	scrubbed, redactions := scrubber.Scrub(input)

	for _, secret := range []string{
		"MIIEvQIBADANBgkqhkiG9w0BAQE",
		anthropicKey,
		openAIKey,
		legacyOpenAIKey,
		ghClassic,
		ghFine,
		awsKey,
		jwt,
		"mySuperSecretTokenValue123456",
		"super_secret_pw",
	} {
		if strings.Contains(scrubbed, secret) {
			t.Fatalf("scrubbed output leaked secret fragment %q:\n%s", secret, scrubbed)
		}
	}

	if !strings.Contains(scrubbed, "[REDACTED:PEM_KEY]") ||
		!strings.Contains(scrubbed, "anthropic=[REDACTED:ANTHROPIC_KEY]") ||
		!strings.Contains(scrubbed, "openai1=[REDACTED:OPENAI_KEY]") ||
		!strings.Contains(scrubbed, "Authorization: Bearer [REDACTED:BEARER_TOKEN]") ||
		!strings.Contains(scrubbed, "dsn=postgresql://mem1:[REDACTED:DSN_PASSWORD]@127.0.0.1:5432/mem1?sslmode=disable") {
		t.Fatalf("unexpected redacted format:\n%s", scrubbed)
	}

	wantCounts := map[string]int{
		"PEM_KEY":       1,
		"ANTHROPIC_KEY": 1,
		"OPENAI_KEY":    2,
		"GITHUB_TOKEN":  2,
		"AWS_KEY":       1,
		"JWT":           1,
		"BEARER_TOKEN":  1,
		"DSN_PASSWORD":  1,
	}
	if len(redactions) != len(wantCounts) {
		t.Fatalf("got %d redaction entries %+v, want %d", len(redactions), redactions, len(wantCounts))
	}
	for _, r := range redactions {
		if r.Count != wantCounts[r.Type] {
			t.Fatalf("redaction count for %s = %d, want %d", r.Type, r.Count, wantCounts[r.Type])
		}
	}
}

func TestSecretScrubberPreservesSafeIdentifiersAndEdgeCases(t *testing.T) {
	scrubber := NewSecretScrubber()

	safeText := "commit a94a8fe5ccb19ba61c4c0873d391e987982fbbd3 uuid 550e8400-e29b-41d4-a716-446655440000 bearer=short"
	out, redactions := scrubber.Scrub(safeText)
	if out != safeText || len(redactions) != 0 {
		t.Fatalf("expected safe text untouched, got out=%q redactions=%+v", out, redactions)
	}

	emptyOut, emptyRed := scrubber.Scrub("")
	if emptyOut != "" || len(emptyRed) != 0 {
		t.Fatalf("expected empty input preserved")
	}

	var nilScrubber *SecretScrubber
	nilOut, nilRed := nilScrubber.Scrub("sk-ant-api03-abcdefghijklmnopqrstuvwxyz012345")
	if nilOut == "" || len(nilRed) != 0 {
		t.Fatalf("expected nil scrubber no-op")
	}

	// Ensure JWT inside Authorization: Bearer isn't double-counted once redacted.
	bearerJWT := "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdefghijklmnop123456"
	out, red := scrubber.Scrub(bearerJWT)
	if out != "Authorization: Bearer [REDACTED:JWT]" || len(red) != 1 || red[0].Type != "JWT" {
		t.Fatalf("expected JWT inside Bearer preserved as JWT redaction, got out=%q red=%+v", out, red)
	}
}

func TestValidateProviderEndpoint(t *testing.T) {
	tests := []struct {
		name        string
		baseURL     string
		model       string
		allowNonZDR bool
		wantErr     bool
	}{
		{name: "https openai ok", baseURL: "https://api.openai.com/v1", model: "gpt-4.1-mini"},
		{name: "loopback http ok", baseURL: "http://127.0.0.1:11434/v1", model: "llama3.1"},
		{name: "ipv6 loopback http ok", baseURL: "http://[::1]:8080/v1", model: "local"},
		{name: "localhost http ok", baseURL: "http://localhost:8000/v1", model: "local"},
		{name: "openrouter with zdr path ok", baseURL: "https://openrouter.ai/api/v1/zdr", model: "openai/gpt-4.1-mini"},
		{name: "openrouter with zdr query ok", baseURL: "https://openrouter.ai/api/v1?zdr=true", model: "openai/gpt-4.1-mini"},
		{name: "empty url", baseURL: "", model: "gpt-4.1", wantErr: true},
		{name: "bad scheme", baseURL: "ftp://api.openai.com/v1", model: "gpt-4.1", wantErr: true},
		{name: "missing host", baseURL: "https:///v1", model: "gpt-4.1", wantErr: true},
		{name: "remote http rejected", baseURL: "http://example.com/v1", model: "gpt-4.1", wantErr: true},
		{name: "free suffix rejected", baseURL: "https://openrouter.ai/api/v1/zdr", model: "meta/llama:free", wantErr: true},
		{name: "free path rejected", baseURL: "https://provider.example/free/v1", model: "m1", wantErr: true},
		{name: "openrouter without zdr rejected", baseURL: "https://openrouter.ai/api/v1", model: "openai/gpt-4.1", wantErr: true},
		{name: "trial host rejected", baseURL: "https://api-inference.huggingface.co/v1", model: "m1", wantErr: true},
		{name: "allowNonZDR overrides free", baseURL: "https://openrouter.ai/api/v1", model: "meta/llama:free", allowNonZDR: true},
		{name: "allowNonZDR overrides remote http", baseURL: "http://example.com/v1", model: "m1", allowNonZDR: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateProviderEndpoint(tc.baseURL, tc.model, tc.allowNonZDR)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for %+v", tc)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for %+v: %v", tc, err)
			}
		})
	}
}
