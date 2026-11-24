//go:build integration

package mcpserver_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/httpapi"
	"github.com/nacfson/nacfson-memory/internal/mcpserver"
	"github.com/nacfson/nacfson-memory/internal/retrieval"
	"github.com/nacfson/nacfson-memory/internal/search"
	"github.com/nacfson/nacfson-memory/internal/security"
	"github.com/nacfson/nacfson-memory/internal/service"
	"github.com/nacfson/nacfson-memory/internal/storage"
	"github.com/nacfson/nacfson-memory/internal/testutil"
)

type bearerHeaderCaptureTransport struct {
	token  string
	base   http.RoundTripper
	mu     sync.Mutex
	header http.Header
}

func (t *bearerHeaderCaptureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	if t.token != "" {
		clone.Header.Set("Authorization", "Bearer "+t.token)
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(clone)
	if err == nil && resp != nil {
		t.mu.Lock()
		t.header = resp.Header.Clone()
		t.mu.Unlock()
	}
	return resp, err
}

func (t *bearerHeaderCaptureTransport) lastHeaders() http.Header {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.header.Clone()
}

func callToolJSON[T any](t *testing.T, ctx context.Context, session *mcp.ClientSession, name string, args map[string]any) (T, error) {
	t.Helper()
	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      name,
		Arguments: args,
	})
	var zero T
	if err != nil {
		return zero, err
	}
	if res.IsError {
		return zero, &toolIsError{msg: extractText(res)}
	}
	if len(res.Content) == 0 {
		t.Fatalf("tool %s returned empty content", name)
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("tool %s expected TextContent, got %T", name, res.Content[0])
	}
	if err := json.Unmarshal([]byte(tc.Text), &zero); err != nil {
		t.Fatalf("decode tool %s JSON %q: %v", name, tc.Text, err)
	}
	return zero, nil
}

type toolIsError struct{ msg string }

func (e *toolIsError) Error() string { return e.msg }

func assertJSONRPCCode(t *testing.T, err error, wantCode int64, wantSubstr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected JSON-RPC error %d, got nil", wantCode)
	}
	var jerr *jsonrpc.Error
	if !errors.As(err, &jerr) {
		t.Fatalf("expected *jsonrpc.Error (%d), got %T: %v", wantCode, err, err)
	}
	if jerr.Code != wantCode {
		t.Fatalf("expected JSON-RPC code %d, got %d (message=%q)", wantCode, jerr.Code, jerr.Message)
	}
	if wantSubstr != "" && !strings.Contains(jerr.Message, wantSubstr) {
		t.Fatalf("expected JSON-RPC message containing %q, got %q", wantSubstr, jerr.Message)
	}
}

func extractText(res *mcp.CallToolResult) string {
	var parts []string
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			parts = append(parts, tc.Text)
		}
	}
	return strings.Join(parts, "; ")
}

func mintTokenViaHTTP(t *testing.T, baseURL, adminToken, wsID, action string, targetID *uuid.UUID) string {
	t.Helper()
	payload := map[string]any{
		"workspace_id": wsID,
		"action":       action,
	}
	if targetID != nil {
		payload["target_memory_id"] = targetID.String()
	}
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/auth/confirm-token", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("POST /v1/auth/confirm-token: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("confirm-token status=%d body=%s", resp.StatusCode, raw)
	}
	var res service.ConfirmTokenResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("decode confirm-token: %v", err)
	}
	return res.ConfirmationToken
}

func TestStreamableHTTPAndStdioSubprocessIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	store := testutil.OpenTestDB(t)
	cfg := config.Config{
		AllowedHosts:      []string{"127.0.0.1", "localhost", "[::1]"},
		AdminToken:        "admin-mcp-test",
		AgentToken:        "agent-mcp-test",
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

	inflight := &atomic.Int64{}
	mcpHandler := mcpserver.NewStreamableHTTPHandler(svc, logger)
	httpSrv := httptest.NewServer(httpapi.NewHandlerWithInflight(cfg, svc, mcpHandler, logger, inflight))
	defer httpSrv.Close()

	// Seed workspaces + memories for recall / expand / manage checks.
	if _, err := svc.CreateWorkspace(ctx, service.CreateWorkspaceRequest{
		WorkspaceID:    "payments",
		InheritGeneral: storage.InheritConstitutionAndDurable,
	}); err != nil {
		t.Fatalf("create payments workspace: %v", err)
	}
	if _, err := svc.CreateWorkspace(ctx, service.CreateWorkspaceRequest{
		WorkspaceID:    "other",
		InheritGeneral: storage.InheritNone,
	}); err != nil {
		t.Fatalf("create other workspace: %v", err)
	}
	foreignMem, err := svc.ManualCreate(ctx, service.ManualCreateRequest{
		WorkspaceID: "other",
		Durability:  storage.DurabilityDurable,
		TopicKey:    "other-secret",
		Body:        "Other workspace memory",
	})
	if err != nil {
		t.Fatalf("create foreignMem: %v", err)
	}
	workMem, err := svc.ManualCreate(ctx, service.ManualCreateRequest{
		WorkspaceID: "payments",
		Durability:  storage.DurabilityWorking,
		TopicKey:    "wip-port",
		Body:        "Working webhook port 8080",
	})
	if err != nil {
		t.Fatalf("create workMem: %v", err)
	}
	durMem, err := svc.ManualCreate(ctx, service.ManualCreateRequest{
		WorkspaceID: "payments",
		Durability:  storage.DurabilityDurable,
		TopicKey:    "primary-db",
		Body:        "Primary DB is PostgreSQL 17 with pgvector.",
	})
	if err != nil {
		t.Fatalf("create durMem: %v", err)
	}
	// Revise durMem so revert v2 -> v1 is exercisable.
	newBody := "Primary DB migrated to PostgreSQL 17.2."
	if _, err := svc.ManualEdit(ctx, service.ManualEditRequest{
		MemoryID:        durMem.MemoryID,
		ExpectedVersion: 1,
		Body:            &newBody,
	}); err != nil {
		t.Fatalf("revise durMem: %v", err)
	}

	// Connect Streamable HTTP MCP client over /mcp.
	tr := &bearerHeaderCaptureTransport{token: cfg.AgentToken}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:             httpSrv.URL + "/mcp",
		HTTPClient:           &http.Client{Transport: tr},
		DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("connect Streamable HTTP MCP: %v", err)
	}
	defer session.Close()

	// 1. memory_write: accept, replay, mismatch (-32022), >64KiB (-32013).
	w1, err := callToolJSON[mcpserver.WriteOutput](t, ctx, session, "memory_write", map[string]any{
		"workspace_id":    "payments",
		"agent_id":        "claude-code",
		"idempotency_key": "k-1",
		"content":         "Retry backoff uses 200ms jittered exponential backoff.",
	})
	if err != nil || !w1.Accepted || w1.Replayed {
		t.Fatalf("memory_write #1 failed: res=%+v err=%v", w1, err)
	}
	wReplay, err := callToolJSON[mcpserver.WriteOutput](t, ctx, session, "memory_write", map[string]any{
		"workspace_id":    "payments",
		"agent_id":        "claude-code",
		"idempotency_key": "k-1",
		"content":         "Retry backoff uses 200ms jittered exponential backoff.",
	})
	if err != nil || !wReplay.Replayed || wReplay.InboxID != w1.InboxID {
		t.Fatalf("memory_write replay failed: res=%+v err=%v", wReplay, err)
	}
	_, err = callToolJSON[mcpserver.WriteOutput](t, ctx, session, "memory_write", map[string]any{
		"workspace_id":    "payments",
		"idempotency_key": "k-1",
		"content":         "Different content with same idempotency_key",
	})
	assertJSONRPCCode(t, err, -32022, "idempotency_key")
	_, err = callToolJSON[mcpserver.WriteOutput](t, ctx, session, "memory_write", map[string]any{
		"workspace_id": "payments",
		"content":      strings.Repeat("x", 66000),
	})
	assertJSONRPCCode(t, err, -32013, "")

	// 2. memory_recall: normal + saturated degraded mode (no IsError + degraded headers).
	recOK, err := callToolJSON[retrieval.RecallResponse](t, ctx, session, "memory_recall", map[string]any{
		"workspace_id": "payments",
		"query":        "PostgreSQL",
		"max_tokens":   2000,
	})
	if err != nil || recOK.Degraded || len(recOK.Memories) == 0 {
		t.Fatalf("expected normal recall hit, got res=%+v err=%v", recOK, err)
	}

	// Saturate inflight counter so /mcp memory_recall returns degraded=true + HTTP headers.
	inflight.Store(int64(cfg.MaxInflight))
	recDeg, err := callToolJSON[retrieval.RecallResponse](t, ctx, session, "memory_recall", map[string]any{
		"workspace_id": "payments",
		"query":        "PostgreSQL",
	})
	inflight.Store(0)
	if err != nil || !recDeg.Degraded {
		t.Fatalf("expected fail-open degraded recall without IsError, got res=%+v err=%v", recDeg, err)
	}
	hdrs := tr.lastHeaders()
	if hdrs.Get("X-Memory-Degraded") != "true" || hdrs.Get("Cache-Control") != "no-store" {
		t.Fatalf("expected X-Memory-Degraded and Cache-Control headers, got %+v", hdrs)
	}

	// 3. memory_expand: allowed works, foreign ID rejected (-32602).
	expOK, err := callToolJSON[retrieval.ExpandResponse](t, ctx, session, "memory_expand", map[string]any{
		"workspace_id": "payments",
		"memory_ids":   []string{durMem.MemoryID.String()},
	})
	if err != nil || len(expOK.Memories) != 1 {
		t.Fatalf("memory_expand failed: res=%+v err=%v", expOK, err)
	}
	_, err = callToolJSON[retrieval.ExpandResponse](t, ctx, session, "memory_expand", map[string]any{
		"workspace_id": "payments",
		"memory_ids":   []string{foreignMem.MemoryID.String()},
	})
	assertJSONRPCCode(t, err, -32602, "not accessible")

	// 4. memory_manage:
	// (a) working edit rejected with invalid_action (-32602)
	_, err = callToolJSON[mcpserver.ManageOutput](t, ctx, session, "memory_manage", map[string]any{
		"workspace_id":     "payments",
		"action":           "edit",
		"target_memory_id": workMem.MemoryID.String(),
		"expected_version": 1,
		"body":             "illegal edit on working",
	})
	assertJSONRPCCode(t, err, -32602, "invalid_action")
	// (b) direct retire on working succeeds without confirmation token
	retOut, err := callToolJSON[mcpserver.ManageOutput](t, ctx, session, "memory_manage", map[string]any{
		"workspace_id":     "payments",
		"action":           "retire",
		"target_memory_id": workMem.MemoryID.String(),
		"expected_version": 1,
	})
	if err != nil || retOut.Status != storage.StatusRetired {
		t.Fatalf("direct retire failed: out=%+v err=%v", retOut, err)
	}
	// (c) privileged promote & revert via single-use confirmation_token
	promTok := mintTokenViaHTTP(t, httpSrv.URL, cfg.AdminToken, "payments", "promote", &durMem.MemoryID)
	promOut, err := callToolJSON[mcpserver.ManageOutput](t, ctx, session, "memory_manage", map[string]any{
		"workspace_id":       "payments",
		"action":             "promote",
		"target_memory_id":   durMem.MemoryID.String(),
		"expected_version":   2,
		"confirmation_token": promTok,
	})
	if err != nil || promOut.Version != 3 {
		t.Fatalf("privileged promote failed: out=%+v err=%v", promOut, err)
	}
	revTok := mintTokenViaHTTP(t, httpSrv.URL, cfg.AdminToken, "payments", "revert", &durMem.MemoryID)
	revOut, err := callToolJSON[mcpserver.ManageOutput](t, ctx, session, "memory_manage", map[string]any{
		"workspace_id":       "payments",
		"action":             "revert",
		"target_memory_id":   durMem.MemoryID.String(),
		"expected_version":   3,
		"target_version":     1,
		"confirmation_token": revTok,
	})
	if err != nil || revOut.Version != 4 || revOut.RestoredFromVersion == nil || *revOut.RestoredFromVersion != 1 {
		t.Fatalf("privileged revert failed: out=%+v err=%v", revOut, err)
	}

	// Retryable atomic confirmation_token: failed CAS revert must NOT burn the token.
	revTok2 := mintTokenViaHTTP(t, httpSrv.URL, cfg.AdminToken, "payments", "revert", &durMem.MemoryID)
	_, err = callToolJSON[mcpserver.ManageOutput](t, ctx, session, "memory_manage", map[string]any{
		"workspace_id":       "payments",
		"action":             "revert",
		"target_memory_id":   durMem.MemoryID.String(),
		"expected_version":   99,
		"target_version":     1,
		"confirmation_token": revTok2,
	})
	assertJSONRPCCode(t, err, -32022, "")
	revOut2, err := callToolJSON[mcpserver.ManageOutput](t, ctx, session, "memory_manage", map[string]any{
		"workspace_id":       "payments",
		"action":             "revert",
		"target_memory_id":   durMem.MemoryID.String(),
		"expected_version":   4,
		"target_version":     1,
		"confirmation_token": revTok2,
	})
	if err != nil || revOut2.Version != 5 {
		t.Fatalf("retry revert with unburned token failed: out=%+v err=%v", revOut2, err)
	}

	// Verify MCP direct retire recorded "agent_retire" rationale in audit lineage.
	workLineage, err := svc.ExplainLineage(ctx, workMem.MemoryID)
	if err != nil {
		t.Fatalf("explain retired workMem: %v", err)
	}
	sawAgentRetire := false
	for _, d := range workLineage.Decisions {
		if d.Action == "retire" && d.Rationale == "agent_retire" {
			sawAgentRetire = true
		}
	}
	if !sawAgentRetire {
		t.Fatalf("expected agent_retire decision in workMem lineage, got %+v", workLineage.Decisions)
	}

	// Verify memory_manage(purge) returns workspace inheritance +general ETag.
	tmpMem, err := svc.ManualCreate(ctx, service.ManualCreateRequest{
		WorkspaceID: "payments",
		Durability:  storage.DurabilityDurable,
		TopicKey:    "purge-etag-check",
		Body:        "Temporary durable item for purge ETag verification.",
	})
	if err != nil {
		t.Fatalf("create tmpMem: %v", err)
	}
	purgeTok := mintTokenViaHTTP(t, httpSrv.URL, cfg.AdminToken, "payments", "purge", &tmpMem.MemoryID)
	purgeOut, err := callToolJSON[mcpserver.ManageOutput](t, ctx, session, "memory_manage", map[string]any{
		"workspace_id":       "payments",
		"action":             "purge",
		"target_memory_id":   tmpMem.MemoryID.String(),
		"confirmation_token": purgeTok,
	})
	if err != nil || !purgeOut.Purged || !strings.Contains(purgeOut.ManifestETag, "+gen:") {
		t.Fatalf("expected purged response with +gen: ETag, got out=%+v err=%v", purgeOut, err)
	}

	// 5. Resource read mem1://workspaces/payments/constitution
	rRes, err := session.ReadResource(ctx, &mcp.ReadResourceParams{
		URI: "mem1://workspaces/payments/constitution",
	})
	if err != nil || len(rRes.Contents) == 0 || !strings.Contains(rRes.Contents[0].Text, "manifest_etag") {
		t.Fatalf("ReadResource constitution failed: res=%+v err=%v", rRes, err)
	}

	// 6. Live subprocess IPC: build ./cmd/mem1 and spawn `mem1 mcp-stdio`.
	binPath := filepath.Join(t.TempDir(), "mem1")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binPath, "../../cmd/mem1")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build mem1 for stdio subprocess test: %v\n%s", err, out)
	}

	stdioCmd := exec.Command(binPath, "mcp-stdio")
	stdioCmd.Env = append(os.Environ(),
		"MEM1_DATABASE_URL="+testutil.TestDSN(),
		"MEM1_EMBED_PROVIDER=mock",
	)
	stdioClient := mcp.NewClient(&mcp.Implementation{Name: "stdio-test-client", Version: "1.0.0"}, nil)
	stdioSess, err := stdioClient.Connect(ctx, &mcp.CommandTransport{Command: stdioCmd}, nil)
	if err != nil {
		t.Fatalf("connect mem1 mcp-stdio subprocess: %v", err)
	}
	defer stdioSess.Close()

	// Cross-process confirmation token minted over HTTP /v1/auth/confirm-token and consumed in mem1 mcp-stdio!
	createTok := mintTokenViaHTTP(t, httpSrv.URL, cfg.AdminToken, "payments", "create", nil)
	constBody := "Always verify migrations before deploy."
	constOut, err := callToolJSON[mcpserver.ManageOutput](t, ctx, stdioSess, "memory_manage", map[string]any{
		"workspace_id":       "payments",
		"action":             "create",
		"durability":         storage.DurabilityConstitution,
		"topic_key":          "verify-migrations",
		"body":               constBody,
		"confirmation_token": createTok,
	})
	if err != nil || constOut.Version != 1 || constOut.Status != storage.StatusActive {
		t.Fatalf("cross-process constitution create in mcp-stdio failed: out=%+v err=%v", constOut, err)
	}
	// Re-using the single-use confirmation_token must fail.
	_, err = callToolJSON[mcpserver.ManageOutput](t, ctx, stdioSess, "memory_manage", map[string]any{
		"workspace_id":       "payments",
		"action":             "create",
		"durability":         storage.DurabilityConstitution,
		"topic_key":          "verify-migrations-2",
		"body":               constBody,
		"confirmation_token": createTok,
	})
	assertJSONRPCCode(t, err, -32022, "")

	// Read constitution resource from mcp-stdio subprocess and verify newly created rule appears.
	stdioConst, err := stdioSess.ReadResource(ctx, &mcp.ReadResourceParams{
		URI: "mem1://workspaces/payments/constitution",
	})
	if err != nil || len(stdioConst.Contents) == 0 || !strings.Contains(stdioConst.Contents[0].Text, "verify-migrations") {
		t.Fatalf("mcp-stdio ReadResource failed: res=%+v err=%v", stdioConst, err)
	}

	// Cross-process memory_write + memory_recall via mcp-stdio subprocess.
	stdioWrite, err := callToolJSON[mcpserver.WriteOutput](t, ctx, stdioSess, "memory_write", map[string]any{
		"workspace_id": "payments",
		"agent_id":     "stdio-agent",
		"content":      "Note written over stdio subprocess.",
	})
	if err != nil || !stdioWrite.Accepted {
		t.Fatalf("mcp-stdio memory_write failed: out=%+v err=%v", stdioWrite, err)
	}
	stdioRecall, err := callToolJSON[retrieval.RecallResponse](t, ctx, stdioSess, "memory_recall", map[string]any{
		"workspace_id": "payments",
		"query":        "verify migrations",
	})
	if err != nil || stdioRecall.Degraded || len(stdioRecall.Memories) == 0 {
		t.Fatalf("mcp-stdio memory_recall failed: out=%+v err=%v", stdioRecall, err)
	}
}
