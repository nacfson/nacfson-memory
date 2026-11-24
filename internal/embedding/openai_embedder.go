package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
)

const (
	// RequiredDimensions is the fixed halfvec dimension pinned by Migration 0001.
	RequiredDimensions = 1536

	defaultEmbedTimeout = 2 * time.Second
	maxErrorBodyBytes   = 512
)

// Embedder abstracts batched vector embedding generation for storage and retrieval.
type Embedder interface {
	SpaceID() string
	Dimensions() int
	EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)
}

// FormatMemoryText builds the canonical embedding payload for a memory entry.
func FormatMemoryText(topicKey, summaryStub, body string) string {
	return topicKey + "\n" + summaryStub + "\n" + body
}

// OpenAIConfig configures an OpenAI-compatible /v1/embeddings HTTP client.
type OpenAIConfig struct {
	BaseURL    string
	APIKey     string
	Model      string
	SpaceID    string
	Dimensions int
	Timeout    time.Duration
	HTTPClient *http.Client
}

// OpenAIEmbedder calls an OpenAI-compatible /embeddings endpoint with strict 1536-dim checks.
type OpenAIEmbedder struct {
	endpoint string
	apiKey   string
	model    string
	spaceID  string
	dims     int
	timeout  time.Duration
	client   *http.Client
}

// NewOpenAIEmbedder validates cfg and returns an OpenAI-compatible Embedder.
func NewOpenAIEmbedder(cfg OpenAIConfig) (*OpenAIEmbedder, error) {
	rawBase := strings.TrimSpace(cfg.BaseURL)
	if rawBase == "" {
		return nil, errors.New("embedder base URL is required")
	}
	u, err := url.Parse(rawBase)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid embedder base URL %q", cfg.BaseURL)
	}
	trimmedPath := strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(trimmedPath, "/embeddings") {
		u.Path = trimmedPath + "/embeddings"
	} else {
		u.Path = trimmedPath
	}

	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		return nil, errors.New("embedder model is required")
	}
	spaceID := strings.TrimSpace(cfg.SpaceID)
	if spaceID == "" {
		return nil, errors.New("embedder space_id is required")
	}

	dims := cfg.Dimensions
	if dims == 0 {
		dims = RequiredDimensions
	}
	if dims != RequiredDimensions {
		return nil, fmt.Errorf("embedder dimensions must be %d (got %d)", RequiredDimensions, dims)
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultEmbedTimeout
	}

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{}
	}

	return &OpenAIEmbedder{
		endpoint: u.String(),
		apiKey:   strings.TrimSpace(cfg.APIKey),
		model:    model,
		spaceID:  spaceID,
		dims:     dims,
		timeout:  timeout,
		client:   client,
	}, nil
}

func (e *OpenAIEmbedder) SpaceID() string { return e.spaceID }
func (e *OpenAIEmbedder) Dimensions() int { return e.dims }

type embedRequest struct {
	Model      string   `json:"model"`
	Input      []string `json:"input"`
	Dimensions int      `json:"dimensions"`
}

type embedResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// EmbedBatch generates 1536-dimensional embeddings for each input string in order.
func (e *OpenAIEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	payload, err := json.Marshal(embedRequest{
		Model:      e.model,
		Input:      texts,
		Dimensions: e.dims,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal embedding request: %w", err)
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, e.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if e.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.apiKey)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call embedding endpoint: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("read embedding response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet := strings.TrimSpace(string(body))
		if len(snippet) > maxErrorBodyBytes {
			snippet = snippet[:maxErrorBodyBytes] + "..."
		}
		return nil, fmt.Errorf("embedding endpoint returned HTTP %d: %s", resp.StatusCode, snippet)
	}

	var decoded embedResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("decode embedding response: %w", err)
	}
	if len(decoded.Data) != len(texts) {
		return nil, fmt.Errorf("embedding response count mismatch: expected %d, got %d", len(texts), len(decoded.Data))
	}

	out := make([][]float32, len(texts))
	seen := make([]bool, len(texts))
	for _, item := range decoded.Data {
		if item.Index < 0 || item.Index >= len(texts) {
			return nil, fmt.Errorf("embedding response invalid index %d for batch size %d", item.Index, len(texts))
		}
		if seen[item.Index] {
			return nil, fmt.Errorf("embedding response duplicate index %d", item.Index)
		}
		if len(item.Embedding) != e.dims {
			return nil, fmt.Errorf("embedding vector at index %d has %d dimensions (expected %d)", item.Index, len(item.Embedding), e.dims)
		}
		for i, v := range item.Embedding {
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return nil, fmt.Errorf("embedding vector at index %d contains non-finite value at offset %d", item.Index, i)
			}
		}
		seen[item.Index] = true
		out[item.Index] = item.Embedding
	}
	return out, nil
}

// MockEmbedder generates deterministic offline 1536-dim feature-hashed unit vectors
// so overlapping tokens yield higher cosine similarity than unrelated text.
type MockEmbedder struct {
	spaceID string
}

// NewMockEmbedder returns an offline deterministic MockEmbedder.
func NewMockEmbedder(spaceID string) *MockEmbedder {
	trimmed := strings.TrimSpace(spaceID)
	if trimmed == "" {
		trimmed = "mock/embed:1536"
	}
	return &MockEmbedder{spaceID: trimmed}
}

func (m *MockEmbedder) SpaceID() string { return m.spaceID }
func (m *MockEmbedder) Dimensions() int { return RequiredDimensions }

func (m *MockEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(texts) == 0 {
		return nil, nil
	}
	out := make([][]float32, len(texts))
	for i, text := range texts {
		out[i] = mockVectorForText(text)
	}
	return out, nil
}

func mockVectorForText(text string) []float32 {
	vec := make([]float32, RequiredDimensions)
	tokens := tokenizeFeatureWords(text)
	if len(tokens) == 0 {
		tokens = []string{strings.ToLower(strings.TrimSpace(text))}
		if tokens[0] == "" {
			tokens[0] = "empty"
		}
	}

	for _, tok := range tokens {
		h := fnv.New64a()
		_, _ = h.Write([]byte(tok))
		sum := h.Sum64()
		idx := int(sum % RequiredDimensions)
		sign := float32(1.0)
		if (sum>>16)&1 == 1 {
			sign = -1.0
		}
		vec[idx] += sign
	}

	var normSq float64
	for _, v := range vec {
		normSq += float64(v) * float64(v)
	}
	if normSq == 0 {
		vec[0] = 1.0
		return vec
	}
	invNorm := float32(1.0 / math.Sqrt(normSq))
	for i := range vec {
		vec[i] *= invNorm
	}
	return vec
}

func tokenizeFeatureWords(text string) []string {
	lower := strings.ToLower(text)
	fields := strings.FieldsFunc(lower, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	tokens := make([]string, 0, len(fields))
	for _, f := range fields {
		if len(f) >= 2 {
			tokens = append(tokens, f)
		}
	}
	return tokens
}
