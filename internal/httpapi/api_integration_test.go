//go:build integration

package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/mcpserver"
	"github.com/nacfson/nacfson-memory/internal/retrieval"
	"github.com/nacfson/nacfson-memory/internal/search"
	"github.com/nacfson/nacfson-memory/internal/security"
	"github.com/nacfson/nacfson-memory/internal/service"
	"github.com/nacfson/nacfson-memory/internal/storage"
	"github.com/nacfson/nacfson-memory/internal/testutil"
)

func doAdminReq(t *testing.T, h http.Handler, method, target, token string, body any, headers ...[2]string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		r = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, "http://127.0.0.1:8787"+target, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, kv := range headers {
		req.Header.Set(kv[0], kv[1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHTTPAll18EndpointsIntegration(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)

	cfg := config.Config{
		AllowedHosts:      []string{"127.0.0.1", "localhost", "[::1]"},
		AdminToken:        "admin-int-token",
		AgentToken:        "agent-int-token",
		MaxInflight:       512,
		MaxWriteBytes:     65536,
		HistoryRetentionN: 20,
		EmbedTimeout:      2 * time.Second,
	}

	emb := embedding.NewMockEmbedder("mock:1536")
	lx := search.NewLexicalSearch()
	dg := graph.NewDependencyGraph()
	curBreaker := curation.NewBreaker(3, 60*time.Second)
	embBreaker := curation.NewBreaker(3, 60*time.Second)
	packer := retrieval.NewPacker(store, emb, embBreaker, lx, dg, cfg.EmbedTimeout)
	svc := service.New(cfg, store, security.NewSecretScrubber(), emb, packer, dg, curBreaker, embBreaker)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandler(cfg, svc, mcpserver.NewStreamableHTTPHandler(svc, logger), logger)

	// 1. GET /healthz
	if rec := doAdminReq(t, h, http.MethodGet, "/healthz", "", nil); rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz status=%d body=%s", rec.Code, rec.Body.String())
	}

	// 2. POST /v1/workspaces (201) + duplicate (409) + 3. GET /v1/workspaces + 4. PATCH /v1/workspaces/{id}
	if rec := doAdminReq(t, h, http.MethodPost, "/v1/workspaces", cfg.AdminToken, map[string]any{
		"workspace_id": "payments",
	}); rec.Code != http.StatusCreated {
		t.Fatalf("POST /v1/workspaces status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := doAdminReq(t, h, http.MethodPost, "/v1/workspaces", cfg.AdminToken, map[string]any{
		"workspace_id": "payments",
	}); rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 duplicate workspace, got %d", rec.Code)
	}
	if rec := doAdminReq(t, h, http.MethodGet, "/v1/workspaces", cfg.AdminToken, nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"payments"`) {
		t.Fatalf("GET /v1/workspaces status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := doAdminReq(t, h, http.MethodPatch, "/v1/workspaces/payments", cfg.AdminToken, map[string]any{
		"inherit_general": storage.InheritConstitutionOnly,
	}); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), storage.InheritConstitutionOnly) {
		t.Fatalf("PATCH /v1/workspaces/payments status=%d body=%s", rec.Code, rec.Body.String())
	}

	// 5. POST /v1/workspaces/{id}/memories (constitution + durable) -> 201
	constRec := doAdminReq(t, h, http.MethodPost, "/v1/workspaces/payments/memories", cfg.AdminToken, map[string]any{
		"durability": storage.DurabilityConstitution,
		"topic_key":  "always-pin-versions",
		"body":       "Always pin dependencies cleanly.",
	})
	if constRec.Code != http.StatusCreated {
		t.Fatalf("create constitution status=%d body=%s", constRec.Code, constRec.Body.String())
	}
	durRec := doAdminReq(t, h, http.MethodPost, "/v1/workspaces/payments/memories", cfg.AdminToken, map[string]any{
		"durability": storage.DurabilityDurable,
		"topic_key":  "primary-db",
		"body":       "Primary DB uses PostgreSQL 17.",
	})
	if durRec.Code != http.StatusCreated {
		t.Fatalf("create durable status=%d body=%s", durRec.Code, durRec.Body.String())
	}
	var durCreated service.ManualMutationResult
	_ = json.Unmarshal(durRec.Body.Bytes(), &durCreated)

	// 6. GET /v1/workspaces/{id}/constitution + 304 If-None-Match
	cRec := doAdminReq(t, h, http.MethodGet, "/v1/workspaces/payments/constitution", cfg.AdminToken, nil)
	if cRec.Code != http.StatusOK || !strings.Contains(cRec.Body.String(), "always-pin-versions") {
		t.Fatalf("GET constitution status=%d body=%s", cRec.Code, cRec.Body.String())
	}
	etag := cRec.Header().Get("ETag")
	c304 := doAdminReq(t, h, http.MethodGet, "/v1/workspaces/payments/constitution", cfg.AdminToken, nil, [2]string{"If-None-Match", etag})
	if c304.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified on ETag %q, got %d", etag, c304.Code)
	}

	// 7. PATCH /v1/memories/{id} (v1 -> v2) + stale CAS 409
	patchRec := doAdminReq(t, h, http.MethodPatch, "/v1/memories/"+durCreated.MemoryID.String(), cfg.AdminToken, map[string]any{
		"expected_version": 1,
		"body":             "Primary DB uses PostgreSQL 17 with pgvector.",
		"reason":           "add pgvector",
	})
	if patchRec.Code != http.StatusOK {
		t.Fatalf("PATCH memory status=%d body=%s", patchRec.Code, patchRec.Body.String())
	}
	stalePatch := doAdminReq(t, h, http.MethodPatch, "/v1/memories/"+durCreated.MemoryID.String(), cfg.AdminToken, map[string]any{
		"expected_version": 1,
		"body":             "stale overwrite",
	})
	if stalePatch.Code != http.StatusConflict {
		t.Fatalf("expected 409 on stale PATCH, got %d", stalePatch.Code)
	}

	// 8. POST /v1/memories/{id}/revert (v2 -> v3 restoring v1)
	revRec := doAdminReq(t, h, http.MethodPost, "/v1/memories/"+durCreated.MemoryID.String()+"/revert", cfg.AdminToken, map[string]any{
		"expected_version": 2,
		"target_version":   1,
		"reason":           "restore v1",
	})
	if revRec.Code != http.StatusOK || !strings.Contains(revRec.Body.String(), `"restored_from_version":1`) {
		t.Fatalf("POST revert status=%d body=%s", revRec.Code, revRec.Body.String())
	}

	// 9. GET /v1/memories (with include_revisions & limit=500 clamped cleanly <= 50)
	// Seed 52 small memories so we verify include_revisions caps page size at 50.
	for i := 0; i < 52; i++ {
		_ = doAdminReq(t, h, http.MethodPost, "/v1/workspaces/payments/memories", cfg.AdminToken, map[string]any{
			"durability": storage.DurabilityWorking,
			"topic_key":  "bulk-note-" + uuid.New().String()[:8],
			"body":       "bulk working note",
		})
	}
	listRec := doAdminReq(t, h, http.MethodGet, "/v1/memories?workspace_id=payments&include_revisions=true&limit=200", cfg.AdminToken, nil)
	if listRec.Code != http.StatusOK {
		t.Fatalf("GET /v1/memories status=%d body=%s", listRec.Code, listRec.Body.String())
	}
	var listPage storage.ListMemoriesPage
	_ = json.Unmarshal(listRec.Body.Bytes(), &listPage)
	if len(listPage.Items) != 50 || listPage.NextCursor == "" {
		t.Fatalf("expected include_revisions cap at 50 items with next_cursor, got %d cursor=%q", len(listPage.Items), listPage.NextCursor)
	}

	// 10. GET /v1/memories/{id}, 11. /revisions, 12. /explain
	if rec := doAdminReq(t, h, http.MethodGet, "/v1/memories/"+durCreated.MemoryID.String(), cfg.AdminToken, nil); rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/memories/{id} status=%d", rec.Code)
	}
	if rec := doAdminReq(t, h, http.MethodGet, "/v1/memories/"+durCreated.MemoryID.String()+"/revisions", cfg.AdminToken, nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"version":3`) {
		t.Fatalf("GET revisions status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := doAdminReq(t, h, http.MethodGet, "/v1/memories/"+durCreated.MemoryID.String()+"/explain", cfg.AdminToken, nil); rec.Code != http.StatusOK {
		t.Fatalf("GET explain status=%d", rec.Code)
	}

	// 13. DELETE /v1/memories/{id} (soft-retire, empty body allowed)
	if rec := doAdminReq(t, h, http.MethodDelete, "/v1/memories/"+durCreated.MemoryID.String(), cfg.AdminToken, nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"retired"`) {
		t.Fatalf("DELETE memory status=%d body=%s", rec.Code, rec.Body.String())
	}

	// 14. POST /v1/auth/confirm-token
	tokRec := doAdminReq(t, h, http.MethodPost, "/v1/auth/confirm-token", cfg.AdminToken, map[string]any{
		"workspace_id":     "payments",
		"action":           "purge",
		"target_memory_id": durCreated.MemoryID,
	})
	if tokRec.Code != http.StatusOK || !strings.Contains(tokRec.Body.String(), "confirmation_token") {
		t.Fatalf("POST confirm-token status=%d body=%s", tokRec.Code, tokRec.Body.String())
	}

	// 15. POST /v1/memories/{id}/purge
	purgeRec := doAdminReq(t, h, http.MethodPost, "/v1/memories/"+durCreated.MemoryID.String()+"/purge", cfg.AdminToken, map[string]any{
		"confirm": true,
		"reason":  "cleanup",
	})
	if purgeRec.Code != http.StatusOK || !strings.Contains(purgeRec.Body.String(), `"purged":true`) {
		t.Fatalf("POST purge status=%d body=%s", purgeRec.Code, purgeRec.Body.String())
	}

	// Seed a dead-letter inbox item so dead-letters & curation-decisions return concrete rows.
	inRes, err := store.InsertInboxAndEnqueueTx(ctx, storage.InsertInboxParams{
		WorkspaceID: "payments",
		AgentID:     "agent-1",
		Content:     "poison payload",
	})
	if err != nil {
		t.Fatalf("insert inbox: %v", err)
	}
	if err := storage.MarkInboxDeadLetter(ctx, store.CurationPool, "payments", inRes.InboxID, "forced validation failure"); err != nil {
		t.Fatalf("mark dead letter: %v", err)
	}

	// 16. GET /v1/ops/status, 17. GET /v1/ops/curation-decisions, 18. GET /v1/ops/dead-letters
	if rec := doAdminReq(t, h, http.MethodGet, "/v1/ops/status", cfg.AdminToken, nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"dead_letter_inbox":1`) {
		t.Fatalf("GET ops/status status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := doAdminReq(t, h, http.MethodGet, "/v1/ops/curation-decisions?workspace_id=payments", cfg.AdminToken, nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"purge"`) {
		t.Fatalf("GET ops/curation-decisions status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := doAdminReq(t, h, http.MethodGet, "/v1/ops/dead-letters?limit=10", cfg.AdminToken, nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "forced validation failure") {
		t.Fatalf("GET ops/dead-letters status=%d body=%s", rec.Code, rec.Body.String())
	}
}
