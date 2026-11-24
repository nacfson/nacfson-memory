package config

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// ParseEnvFile parses KEY=VALUE assignments from a .env stream without mutating process env.
func ParseEnvFile(r io.Reader) (map[string]string, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	out := make(map[string]string)
	lineNum := 0
	for sc.Scan() {
		lineNum++
		line := sc.Text()
		if lineNum == 1 {
			line = strings.TrimPrefix(line, "\uFEFF")
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasPrefix(trimmed, "export ") || strings.HasPrefix(trimmed, "export\t") {
			trimmed = strings.TrimSpace(trimmed[len("export"):])
		}

		eqIdx := strings.IndexByte(trimmed, '=')
		if eqIdx < 0 {
			return nil, fmt.Errorf("line %d: missing '=' assignment", lineNum)
		}
		key := strings.TrimSpace(trimmed[:eqIdx])
		if !isValidEnvKey(key) {
			return nil, fmt.Errorf("line %d: invalid key name", lineNum)
		}

		val, err := parseEnvValue(trimmed[eqIdx+1:], lineNum)
		if err != nil {
			return nil, err
		}
		out[key] = val
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("line %d: %w", lineNum+1, err)
	}
	return out, nil
}

// LoadEnvFile reads and parses a .env file; when optional is true, a missing file returns nil, nil.
func LoadEnvFile(path string, optional bool) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		if optional && errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("open env file %q: %w", path, err)
	}
	defer f.Close()

	vals, err := ParseEnvFile(f)
	if err != nil {
		return nil, fmt.Errorf("parse env file %q: %w", path, err)
	}
	return vals, nil
}

// ResolveEnvLookup layers an env file behind base (defaulting to os.LookupEnv) so explicit
// environment variables always take precedence.
func ResolveEnvLookup(envFile string, base func(string) (string, bool)) (func(string) (string, bool), error) {
	if base == nil {
		base = os.LookupEnv
	}

	path := strings.TrimSpace(envFile)
	optional := false
	if path == "" {
		if v, ok := base("MEM1_ENV_FILE"); ok && strings.TrimSpace(v) != "" {
			path = strings.TrimSpace(v)
		} else {
			path = ".env"
			optional = true
		}
	}

	fileVals, err := LoadEnvFile(path, optional)
	if err != nil {
		return nil, err
	}

	return func(key string) (string, bool) {
		if v, ok := base(key); ok {
			return v, true
		}
		if fileVals != nil {
			v, ok := fileVals[key]
			return v, ok
		}
		return "", false
	}, nil
}

func parseEnvValue(afterEq string, lineNum int) (string, error) {
	leftTrimmed := strings.TrimLeft(afterEq, " \t")
	if leftTrimmed == "" {
		return "", nil
	}
	if leftTrimmed[0] == '"' {
		return parseDoubleQuotedEnv(leftTrimmed, lineNum)
	}
	if leftTrimmed[0] == '\'' {
		return parseSingleQuotedEnv(leftTrimmed, lineNum)
	}

	// Preserve unquoted '#' (e.g. URL passwords) unless preceded by whitespace as an inline comment.
	raw := afterEq
	for i := 1; i < len(raw); i++ {
		if raw[i] == '#' && (raw[i-1] == ' ' || raw[i-1] == '\t') {
			raw = raw[:i]
			break
		}
	}
	return strings.TrimSpace(raw), nil
}

func parseDoubleQuotedEnv(raw string, lineNum int) (string, error) {
	var b strings.Builder
	for i := 1; i < len(raw); i++ {
		c := raw[i]
		if c == '\\' {
			if i+1 >= len(raw) {
				return "", fmt.Errorf("line %d: unterminated double-quoted value", lineNum)
			}
			i++
			switch raw[i] {
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case '"':
				b.WriteByte('"')
			case '\\':
				b.WriteByte('\\')
			default:
				b.WriteByte('\\')
				b.WriteByte(raw[i])
			}
			continue
		}
		if c == '"' {
			rest := strings.TrimSpace(raw[i+1:])
			if rest != "" && !strings.HasPrefix(rest, "#") {
				return "", fmt.Errorf("line %d: unexpected trailing characters after closing quote", lineNum)
			}
			return b.String(), nil
		}
		b.WriteByte(c)
	}
	return "", fmt.Errorf("line %d: unterminated double-quoted value", lineNum)
}

func parseSingleQuotedEnv(raw string, lineNum int) (string, error) {
	end := strings.IndexByte(raw[1:], '\'')
	if end < 0 {
		return "", fmt.Errorf("line %d: unterminated single-quoted value", lineNum)
	}
	rest := strings.TrimSpace(raw[1+end+1:])
	if rest != "" && !strings.HasPrefix(rest, "#") {
		return "", fmt.Errorf("line %d: unexpected trailing characters after closing quote", lineNum)
	}
	return raw[1 : 1+end], nil
}

func isValidEnvKey(key string) bool {
	if key == "" {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		isAlpha := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'
		isDigit := c >= '0' && c <= '9'
		if i == 0 && !isAlpha {
			return false
		}
		if !isAlpha && !isDigit {
			return false
		}
	}
	return true
}
