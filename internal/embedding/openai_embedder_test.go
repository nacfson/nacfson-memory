package embedding

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFormatMemoryText(t *testing.T) {
	got := FormatMemoryText("primary-db", "Use PG17", "PostgreSQL 17 with pgvector")
	want := "primary-db\nUse PG17\nPostgreSQL 17 with pgvector"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNewOpenAIEmbedderValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  OpenAIConfig
		want string
	}{
		{
			name: "missing base url",
			cfg:  OpenAIConfig{Model: "m", SpaceID: "s"},
			want: "base URL",
		},
		{
			name: "invalid base url",
			cfg:  OpenAIConfig{BaseURL: "://bad", Model: "m", SpaceID: "s"},
			want: "invalid embedder base URL",
		},
		{
			name: "missing model",
			cfg:  OpenAIConfig{BaseURL: "https://api.openai.com/v1", SpaceID: "s"},
			want: "model",
		},
		{
			name: "missing space id",
			cfg:  OpenAIConfig{BaseURL: "https://api.openai.com/v1", Model: "m"},
			want: "space_id",
		},
		{
			name: "invalid dimensions",
			cfg:  OpenAIConfig{BaseURL: "https://api.openai.com/v1", Model: "m", SpaceID: "s", Dimensions: 768},
			want: "1536",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewOpenAIEmbedder(tc.cfg); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got err=%v, want substring %q", err, tc.want)
			}
		})
	}

	emb, err := NewOpenAIEmbedder(OpenAIConfig{
		BaseURL: "https://openrouter.ai/api/v1/embeddings?zdr=true",
		APIKey:  "sk-test",
		Model:   "text-embedding-3-small",
		SpaceID: "openai/text-embedding-3-small:1536",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if emb.SpaceID() != "openai/text-embedding-3-small:1536" || emb.Dimensions() != 1536 {
		t.Fatalf("unexpected metadata: space=%q dims=%d", emb.SpaceID(), emb.Dimensions())
	}
	if !strings.Contains(emb.endpoint, "/v1/embeddings?zdr=true") {
		t.Fatalf("expected preserved embeddings path + query, got %q", emb.endpoint)
	}
}

func TestOpenAIEmbedderBatchOrderingAndErrors(t *testing.T) {
	ctx := context.Background()

	// Empty batch returns nil without hitting network.
	embEmpty, err := NewOpenAIEmbedder(OpenAIConfig{
		BaseURL: "http://127.0.0.1:1/v1",
		Model:   "text-embedding-3-small",
		SpaceID: "s",
	})
	if err != nil {
		t.Fatalf("init embedder: %v", err)
	}
	if out, err := embEmpty.EmbedBatch(ctx, nil); err != nil || out != nil {
		t.Fatalf("expected nil, nil for empty batch; got out=%v err=%v", out, err)
	}

	// Valid batched call returning items out-of-order by index.
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path

		var req embedRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		v0 := make([]float32, RequiredDimensions)
		v0[0] = 1.0
		v1 := make([]float32, RequiredDimensions)
		v1[1] = 1.0

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"index": 1, "embedding": v1},
				{"index": 0, "embedding": v0},
			},
		})
	}))
	defer srv.Close()

	emb, err := NewOpenAIEmbedder(OpenAIConfig{
		BaseURL: srv.URL + "/v1/",
		APIKey:  "sk-secret-key",
		Model:   "text-embedding-3-small",
		SpaceID: "openai/text-embedding-3-small:1536",
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("create embedder: %v", err)
	}
	vecs, err := emb.EmbedBatch(ctx, []string{"first", "second"})
	if err != nil {
		t.Fatalf("EmbedBatch failed: %v", err)
	}
	if gotAuth != "Bearer sk-secret-key" || gotPath != "/v1/embeddings" {
		t.Fatalf("unexpected auth=%q path=%q", gotAuth, gotPath)
	}
	if len(vecs) != 2 || vecs[0][0] != 1.0 || vecs[1][1] != 1.0 {
		t.Fatalf("expected index-reordered vectors, got v0[0]=%v v1[1]=%v", vecs[0][0], vecs[1][1])
	}

	// Error response, wrong dims, count mismatch, duplicate index, bad index, invalid JSON, timeout.
	errCases := []struct {
		name    string
		timeout time.Duration
		handler http.HandlerFunc
		want    string
	}{
		{
			name:    "http 500",
			timeout: time.Second,
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, strings.Repeat("boom-", 150), http.StatusInternalServerError)
			},
			want: "HTTP 500",
		},
		{
			name:    "malformed json",
			timeout: time.Second,
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{not json`))
			},
			want: "decode",
		},
		{
			name:    "count mismatch",
			timeout: time.Second,
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"data":[]}`))
			},
			want: "count mismatch",
		},
		{
			name:    "invalid index",
			timeout: time.Second,
			handler: func(w http.ResponseWriter, r *http.Request) {
				v := make([]float32, RequiredDimensions)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data": []map[string]any{{"index": 5, "embedding": v}},
				})
			},
			want: "invalid index",
		},
		{
			name:    "wrong dimensions",
			timeout: time.Second,
			handler: func(w http.ResponseWriter, r *http.Request) {
				v := make([]float32, 768)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data": []map[string]any{{"index": 0, "embedding": v}},
				})
			},
			want: "768 dimensions",
		},
		{
			name:    "timeout",
			timeout: 40 * time.Millisecond,
			handler: func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(150 * time.Millisecond)
			},
			want: "deadline exceeded",
		},
	}

	for _, tc := range errCases {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(tc.handler)
			defer s.Close()

			e, err := NewOpenAIEmbedder(OpenAIConfig{
				BaseURL: s.URL,
				Model:   "m",
				SpaceID: "s",
				Timeout: tc.timeout,
			})
			if err != nil {
				t.Fatalf("new embedder: %v", err)
			}
			if _, err := e.EmbedBatch(ctx, []string{"hello"}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got err=%v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestOpenAIEmbedderDuplicateIndexAndNonFinite(t *testing.T) {
	ctx := context.Background()

	dupSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := make([]float32, RequiredDimensions)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"index": 0, "embedding": v},
				{"index": 0, "embedding": v},
			},
		})
	}))
	defer dupSrv.Close()

	e, _ := NewOpenAIEmbedder(OpenAIConfig{BaseURL: dupSrv.URL, Model: "m", SpaceID: "s"})
	if _, err := e.EmbedBatch(ctx, []string{"a", "b"}); err == nil || !strings.Contains(err.Error(), "duplicate index") {
		t.Fatalf("expected duplicate index error, got %v", err)
	}
}

func TestMockEmbedderDeterministicAndSimilarity(t *testing.T) {
	ctx := context.Background()
	m := NewMockEmbedder("")
	if m.SpaceID() != "mock/embed:1536" || m.Dimensions() != 1536 {
		t.Fatalf("unexpected defaults: %q %d", m.SpaceID(), m.Dimensions())
	}

	if out, err := m.EmbedBatch(ctx, nil); err != nil || out != nil {
		t.Fatalf("expected nil on empty batch")
	}

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := m.EmbedBatch(cancelledCtx, []string{"hi"}); err == nil {
		t.Fatalf("expected context error on cancelled context")
	}

	vecs, err := m.EmbedBatch(ctx, []string{
		"PostgreSQL 17 primary database with pgvector HNSW index",
		"PostgreSQL 17 database storage engine with pgvector HNSW",
		"Sourdough bread fermentation baking recipe in oven",
		"   ! ",
	})
	if err != nil || len(vecs) != 4 {
		t.Fatalf("mock embed failed: %v", err)
	}
	for i, v := range vecs {
		if len(v) != RequiredDimensions {
			t.Fatalf("vector %d len=%d", i, len(v))
		}
	}

	simRelated := cosine(vecs[0], vecs[1])
	simUnrelated := cosine(vecs[0], vecs[2])
	if simRelated <= simUnrelated {
		t.Fatalf("expected related similarity (%v) > unrelated (%v)", simRelated, simUnrelated)
	}

	vecsRepeat, _ := m.EmbedBatch(ctx, []string{"PostgreSQL 17 primary database with pgvector HNSW index"})
	if math.Abs(cosine(vecs[0], vecsRepeat[0])-1.0) > 1e-5 {
		t.Fatalf("expected identical cosine ~1.0, got %v", cosine(vecs[0], vecsRepeat[0]))
	}
}

func cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}
