package retrieval

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

// sentinelQuery is 23 characters (19 ASCII + space + 3 Hangul) but 29 bytes.
const sentinelQuery = "SENTINEL-QUERY-9d2e 재시도"

type failingEmbedder struct{ *embedding.MockEmbedder }

func (failingEmbedder) EmbedBatch(context.Context, []string) ([][]float32, error) {
	return nil, errors.New("embedding endpoint returned HTTP 503")
}

// unreachableStore returns a Store whose serving pool cannot connect, so RecallContext
// fails right after the pre-DB query embedding this test observes.
func unreachableStore(t *testing.T) *storage.Store {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://mem1:mem1@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return &storage.Store{ServingPool: pool}
}

func embedCallRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("decode log line %q: %v", sc.Text(), err)
		}
		if rec["msg"] == "embed_call" {
			out = append(out, rec)
		}
	}
	return out
}

func TestRecallEmbedCallEventIsMetadataOnly(t *testing.T) {
	tripped := curation.NewBreaker(1, time.Minute)
	tripped.RecordFailure(errors.New("embedder down"))

	mock := embedding.NewMockEmbedder("mock/embed:1536")
	cases := []struct {
		name     string
		embedder embedding.Embedder
		breaker  *curation.Breaker
		outcome  string
		chars    float64
	}{
		{"ok", mock, nil, "ok", 23},
		{"error", failingEmbedder{mock}, nil, "error", 23},
		{"breaker open sends nothing", mock, tripped, "breaker_open", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			p := NewPacker(unreachableStore(t), tc.embedder, tc.breaker, nil, nil, time.Second).
				WithLogger(slog.New(slog.NewJSONHandler(&buf, nil)))

			if _, err := p.RecallContext(context.Background(), RecallRequest{WorkspaceID: "billing-api", Query: "  " + sentinelQuery + "  "}); err == nil {
				t.Fatal("expected recall to fail against an unreachable database")
			}

			recs := embedCallRecords(t, &buf)
			if len(recs) != 1 {
				t.Fatalf("want exactly one embed_call, got %d:\n%s", len(recs), buf.String())
			}
			rec := recs[0]
			want := map[string]any{
				"msg": "embed_call", "level": "INFO", "caller": "recall", "workspace_id": "billing-api",
				"space_id": "mock/embed:1536", "input_chars": tc.chars, "outcome": tc.outcome,
			}
			for k, v := range want {
				if rec[k] != v {
					t.Fatalf("embed_call[%s] = %v, want %v (record %v)", k, rec[k], v, rec)
				}
			}
			if d, ok := rec["duration_ms"].(float64); !ok || d < 0 {
				t.Fatalf("embed_call duration_ms = %v", rec["duration_ms"])
			}
			if len(rec) != len(want)+2 {
				t.Fatalf("embed_call must carry only time, duration_ms and %d metadata keys, got %v", len(want), rec)
			}
			if strings.Contains(buf.String(), "SENTINEL-QUERY") || strings.Contains(buf.String(), "재시도") {
				t.Fatalf("captured logs leak the recall query:\n%s", buf.String())
			}
		})
	}
}

func TestRecallWithoutQueryOrLoggerEmitsNoEmbedCall(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(unreachableStore(t), embedding.NewMockEmbedder(""), nil, nil, nil, time.Second).
		WithLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	_, _ = p.RecallContext(context.Background(), RecallRequest{WorkspaceID: "billing-api", Query: "   "})
	if recs := embedCallRecords(t, &buf); len(recs) != 0 {
		t.Fatalf("blank query must not embed or log embed_call, got %v", recs)
	}

	silent := NewPacker(unreachableStore(t), embedding.NewMockEmbedder(""), nil, nil, nil, time.Second)
	if _, err := silent.RecallContext(context.Background(), RecallRequest{WorkspaceID: "billing-api", Query: sentinelQuery}); err == nil {
		t.Fatal("expected recall to fail against an unreachable database")
	}
}
