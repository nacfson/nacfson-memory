package security

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
)

// Redaction summarizes how many times one secret category was redacted.
type Redaction struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

type scrubRule struct {
	name string
	re   *regexp.Regexp
	repl string
}

// SecretScrubber deterministically redacts credential patterns from text
// prior to database persistence or external provider egress.
type SecretScrubber struct {
	rules []scrubRule
}

// NewSecretScrubber builds a SecretScrubber with precompiled ordered rules.
func NewSecretScrubber() *SecretScrubber {
	return &SecretScrubber{
		rules: []scrubRule{
			{
				name: "PEM_KEY",
				re:   regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----`),
				repl: `[REDACTED:PEM_KEY]`,
			},
			{
				name: "ANTHROPIC_KEY",
				re:   regexp.MustCompile(`sk-ant-[A-Za-z0-9_-]{20,}`),
				repl: `[REDACTED:ANTHROPIC_KEY]`,
			},
			{
				name: "OPENAI_KEY",
				re:   regexp.MustCompile(`sk-(?:proj-)?[A-Za-z0-9_-]{20,}`),
				repl: `[REDACTED:OPENAI_KEY]`,
			},
			{
				name: "GITHUB_TOKEN",
				re:   regexp.MustCompile(`(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})`),
				repl: `[REDACTED:GITHUB_TOKEN]`,
			},
			{
				name: "AWS_KEY",
				re:   regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
				repl: `[REDACTED:AWS_KEY]`,
			},
			{
				name: "JWT",
				re:   regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b`),
				repl: `[REDACTED:JWT]`,
			},
			{
				name: "BEARER_TOKEN",
				re:   regexp.MustCompile(`(?i)(Authorization:\s*Bearer\s+)([A-Za-z0-9._~+/=-]{16,})`),
				repl: `${1}[REDACTED:BEARER_TOKEN]`,
			},
			{
				name: "DSN_PASSWORD",
				re:   regexp.MustCompile(`(postgres(?:ql)?://[^:@/\s]+:)([^@\s]+)(@)`),
				repl: `${1}[REDACTED:DSN_PASSWORD]${3}`,
			},
		},
	}
}

// Scrub strips recognized secret patterns in deterministic rule order and returns
// the scrubbed string together with per-rule redaction counts.
func (s *SecretScrubber) Scrub(input string) (string, []Redaction) {
	if s == nil || input == "" {
		return input, nil
	}
	cur := input
	var redactions []Redaction
	for _, rule := range s.rules {
		matches := rule.re.FindAllStringIndex(cur, -1)
		if len(matches) == 0 {
			continue
		}
		cur = rule.re.ReplaceAllString(cur, rule.repl)
		redactions = append(redactions, Redaction{
			Type:  rule.name,
			Count: len(matches),
		})
	}
	return cur, redactions
}

var unauthenticatedTrialHosts = map[string]struct{}{
	"api-inference.huggingface.co": {},
	"playground.openai.com":        {},
	"try.ollama.ai":                {},
	"free.gpt.ge":                  {},
	"api.chatanywhere.tech":        {},
}

// IsLoopbackHost reports whether host identifies the local machine.
func IsLoopbackHost(host string) bool {
	h := strings.TrimSpace(strings.ToLower(host))
	h = strings.TrimPrefix(h, "[")
	h = strings.TrimSuffix(h, "]")
	if h == "localhost" {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

// ValidateProviderEndpoint enforces zero-data-retention (ZDR) endpoint policy
// unless allowNonZDR explicitly overrides it.
func ValidateProviderEndpoint(baseURL, model string, allowNonZDR bool) error {
	rawURL := strings.TrimSpace(baseURL)
	if rawURL == "" {
		return errors.New("provider base URL is required")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid provider base URL %q: %w", baseURL, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("provider base URL %q must use http or https scheme", baseURL)
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return fmt.Errorf("provider base URL %q missing host", baseURL)
	}

	if allowNonZDR {
		return nil
	}

	if scheme == "http" && !IsLoopbackHost(host) {
		return fmt.Errorf("non-ZDR unencrypted http endpoint %q forbidden when MEM1_ALLOW_NON_ZDR=false", baseURL)
	}

	lowerModel := strings.ToLower(strings.TrimSpace(model))
	lowerPath := strings.ToLower(u.Path)
	lowerQuery := strings.ToLower(u.RawQuery)
	if strings.HasSuffix(lowerModel, ":free") ||
		strings.Contains(lowerPath, ":free") ||
		strings.Contains(lowerPath, "/free") ||
		strings.Contains(lowerQuery, ":free") {
		return fmt.Errorf("free-tier endpoint or model (%q / %q) forbidden when MEM1_ALLOW_NON_ZDR=false", baseURL, model)
	}

	if host == "openrouter.ai" || strings.HasSuffix(host, ".openrouter.ai") {
		if !strings.Contains(lowerPath, "/zdr") && !strings.Contains(lowerQuery, "zdr=true") {
			return fmt.Errorf("openrouter endpoint %q requires ZDR routing (/zdr or zdr=true) unless MEM1_ALLOW_NON_ZDR=true", baseURL)
		}
	}

	if _, blocked := unauthenticatedTrialHosts[host]; blocked {
		return fmt.Errorf("public trial host %q forbidden when MEM1_ALLOW_NON_ZDR=false", host)
	}

	return nil
}
