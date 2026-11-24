package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/internal/mcpserver"
	"github.com/nacfson/nacfson-memory/internal/retrieval"
	"github.com/nacfson/nacfson-memory/internal/service"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

type fakeService struct {
	health        service.HealthCheckResult
	constRes      service.ConstitutionResult
	constErr      error
	listWorkspacesFn func() ([]storage.WorkspaceSummary, error)
	createMemErr  error
	editMemErr    error
	revertMemErr  error
	purgeErr      error
	ingestRes     service.IngestWriteResult
	ingestErr     error
	recallRes     retrieval.RecallResponse
	recallErr     error
	opsRes        service.OpsStatusResult
}

func (f *fakeService) HealthCheck(context.Context) service.HealthCheckResult {
	return f.health
}
func (f *fakeService) ListWorkspaces(context.Context) ([]storage.WorkspaceSummary, error) {
	if f.listWorkspacesFn != nil {
		return f.listWorkspacesFn()
	}
	return []storage.WorkspaceSummary{}, nil
}
func (f *fakeService) CreateWorkspace(context.Context, service.CreateWorkspaceRequest) (storage.Workspace, error) {
	return storage.Workspace{WorkspaceID: "app", Kind: storage.KindProject, InheritGeneral: storage.InheritConstitutionAndDurable}, nil
}
func (f *fakeService) UpdateWorkspace(context.Context, string, string) (storage.Workspace, error) {
	return storage.Workspace{WorkspaceID: "app", Kind: storage.KindProject, InheritGeneral: storage.InheritNone}, nil
}
func (f *fakeService) GetConstitution(context.Context, string) (service.ConstitutionResult, error) {
	return f.constRes, f.constErr
}
func (f *fakeService) ListMemoriesKeyset(context.Context, storage.ListMemoriesParams) (storage.ListMemoriesPage, error) {
	return storage.ListMemoriesPage{Items: []storage.MemoryListItem{}}, nil
}
func (f *fakeService) GetMemory(context.Context, uuid.UUID) (service.MemoryDetailResult, error) {
	return service.MemoryDetailResult{}, storage.ErrNotFound
}
func (f *fakeService) GetRevisions(context.Context, uuid.UUID) ([]storage.MemoryRevision, error) {
	return []storage.MemoryRevision{}, nil
}
func (f *fakeService) ExplainLineage(context.Context, uuid.UUID) (storage.MemoryLineage, error) {
	return storage.MemoryLineage{}, storage.ErrNotFound
}
func (f *fakeService) ManualCreate(context.Context, service.ManualCreateRequest) (service.ManualMutationResult, error) {
	if f.createMemErr != nil {
		return service.ManualMutationResult{}, f.createMemErr
	}
	return service.ManualMutationResult{MemoryID: uuid.New(), Version: 1, Status: storage.StatusActive, ManifestETag: `W/"ws:1+gen:0"`}, nil
}
func (f *fakeService) ManualEdit(context.Context, service.ManualEditRequest) (service.ManualMutationResult, error) {
	return service.ManualMutationResult{}, f.editMemErr
}
func (f *fakeService) RevertMemory(context.Context, service.RevertMemoryRequest) (service.ManualMutationResult, error) {
	return service.ManualMutationResult{}, f.revertMemErr
}
func (f *fakeService) SoftRetire(context.Context, service.SoftRetireRequest) (service.ManualMutationResult, error) {
	return service.ManualMutationResult{Status: storage.StatusRetired}, nil
}
func (f *fakeService) HardPurge(context.Context, service.HardPurgeRequest) (storage.HardPurgeResult, error) {
	return storage.HardPurgeResult{}, f.purgeErr
}
func (f *fakeService) IssueConfirmToken(context.Context, service.IssueConfirmTokenRequest) (service.ConfirmTokenResult, error) {
	return service.ConfirmTokenResult{ConfirmationToken: "tok", ExpiresAt: time.Now().Add(5 * time.Minute)}, nil
}
func (f *fakeService) OpsStatus(context.Context) (service.OpsStatusResult, error) {
	return f.opsRes, nil
}
func (f *fakeService) ListCurationDecisions(context.Context, storage.ListDecisionsParams) (storage.ListDecisionsPage, error) {
	return storage.ListDecisionsPage{Decisions: []storage.CurationDecision{}}, nil
}
func (f *fakeService) ListDeadLetters(context.Context, int) (storage.DeadLettersReport, error) {
	return storage.DeadLettersReport{}, nil
}

// Also satisfy mcpserver.Service on fakeService for saturation/degraded /mcp tests.
func (f *fakeService) IngestWrite(context.Context, service.IngestWriteRequest) (service.IngestWriteResult, error) {
	return f.ingestRes, f.ingestErr
}
func (f *fakeService) RecallContext(context.Context, retrieval.RecallRequest) (retrieval.RecallResponse, error) {
	return f.recallRes, f.recallErr
}
func (f *fakeService) ExpandMemories(context.Context, retrieval.ExpandRequest) (retrieval.ExpandResponse, error) {
	return retrieval.ExpandResponse{}, nil
}
func (f *fakeService) ConsumeConfirmToken(context.Context, service.ConsumeConfirmTokenRequest) error {
	return nil
}

func testConfig() config.Config {
	return config.Config{
		AllowedHosts: []string{"127.0.0.1", "localhost", "[::1]"},
		AdminToken:   "admin-tok",
		AgentToken:   "agent-tok",
		MaxInflight:  512,
	}
}

func TestSplitTokenAuthHostOriginAndBodyCaps(t *testing.T) {
	svc := &fakeService{
		health: service.HealthCheckResult{Status: "ok", DBOk: true},
		constRes: service.ConstitutionResult{
			Memories:             []storage.Memory{},
			ConstitutionRevision: 3,
			ManifestETag:         `W/"const:3+gen:1"`,
		},
	}
	mcpHandler := mcpserver.NewStreamableHTTPHandler(svc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h := NewHandler(testConfig(), svc, mcpHandler, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// 1. /healthz open without auth; returns 503 when db_ok=false.
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8787/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /healthz, got %d", rec.Code)
	}
	svc.health = service.HealthCheckResult{Status: "degraded", DBOk: false}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on degraded /healthz, got %d", rec.Code)
	}

	// 2. Host & Origin validation rejects untrusted headers with 403.
	badHostReq := httptest.NewRequest(http.MethodGet, "http://evil.example.com/healthz", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, badHostReq)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 on bad Host, got %d", rec.Code)
	}
	badOriginReq := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8787/healthz", nil)
	badOriginReq.Header.Set("Origin", "https://attacker.example")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, badOriginReq)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 on bad Origin, got %d", rec.Code)
	}

	// 3. Split tokens: unauthenticated or agent token rejected on /v1/*.
	for _, hdr := range []string{"", "Bearer wrong", "Bearer agent-tok"} {
		v1Req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8787/v1/workspaces", nil)
		if hdr != "" {
			v1Req.Header.Set("Authorization", hdr)
		}
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, v1Req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 on /v1/workspaces with auth=%q, got %d", hdr, rec.Code)
		}
	}

	// /mcp rejects wrong/missing token when MEM1_AGENT_TOKEN is set.
	mcpUnauth := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8787/mcp", strings.NewReader(`{}`))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, mcpUnauth)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on /mcp without agent token, got %d", rec.Code)
	}

	// Loopback /mcp open when AgentToken == "", while /v1/* STILL requires AdminToken.
	cfgNoAgent := testConfig()
	cfgNoAgent.AgentToken = ""
	hNoAgent := NewHandler(cfgNoAgent, svc, mcpHandler, slog.New(slog.NewTextHandler(io.Discard, nil)))
	v1NoTok := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8787/v1/workspaces", nil)
	rec = httptest.NewRecorder()
	hNoAgent.ServeHTTP(rec, v1NoTok)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on /v1/* even when AgentToken is empty, got %d", rec.Code)
	}

	// 4. 64 KiB body cap on /v1/* and 80 KiB cap on /mcp -> 413 payload_too_large.
	over64 := bytes.Repeat([]byte("a"), 65*1024)
	v1Big := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8787/v1/workspaces", bytes.NewReader(over64))
	v1Big.Header.Set("Authorization", "Bearer admin-tok")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, v1Big)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 on >64KiB /v1 body, got %d", rec.Code)
	}

	over80 := bytes.Repeat([]byte("b"), 81*1024)
	mcpBig := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8787/mcp", bytes.NewReader(over80))
	mcpBig.Header.Set("Authorization", "Bearer agent-tok")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, mcpBig)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 on >80KiB /mcp body, got %d", rec.Code)
	}

	// 5. ETag / If-None-Match -> 304 Not Modified on constitution.
	cReq := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8787/v1/workspaces/app/constitution", nil)
	cReq.Header.Set("Authorization", "Bearer admin-tok")
	cReq.Header.Set("If-None-Match", `W/"const:3+gen:1"`)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, cReq)
	if rec.Code != http.StatusNotModified || rec.Header().Get("ETag") != `W/"const:3+gen:1"` {
		t.Fatalf("expected 304 Not Modified with ETag, got status=%d etag=%q", rec.Code, rec.Header().Get("ETag"))
	}
}

func TestInflightSaturationAndErrorMappings(t *testing.T) {
	svc := &fakeService{
		health: service.HealthCheckResult{Status: "ok", DBOk: true},
		recallRes: retrieval.RecallResponse{
			Memories: []retrieval.RecalledMemory{},
			Stubs:    []retrieval.MemoryStub{},
		},
	}
	mcpHandler := mcpserver.NewStreamableHTTPHandler(svc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	cfg := testConfig()
	cfg.MaxInflight = 1
	inflight := &atomic.Int64{}
	inflight.Store(1) // Pre-saturate so next request hits cur=2 > MaxInflight(1).

	h := NewHandlerWithInflight(cfg, svc, mcpHandler, slog.New(slog.NewTextHandler(io.Discard, nil)), inflight)

	// /v1/* returns 503 + Retry-After on saturation.
	v1Req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8787/v1/workspaces", nil)
	v1Req.Header.Set("Authorization", "Bearer admin-tok")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, v1Req)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("expected 503 + Retry-After on saturated /v1/*, got status=%d retry=%q", rec.Code, rec.Header().Get("Retry-After"))
	}

	// /mcp memory_recall remains fail-open under saturation with degraded=true + X-Memory-Degraded: true + Cache-Control: no-store.
	rpcCall := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"memory_recall","arguments":{"workspace_id":"app","query":"postgres"}}}`
	mcpReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8787/mcp", strings.NewReader(rpcCall))
	mcpReq.Header.Set("Authorization", "Bearer agent-tok")
	mcpReq.Header.Set("Content-Type", "application/json")
	mcpReq.Header.Set("Accept", "application/json, text/event-stream")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, mcpReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 fail-open on saturated /mcp memory_recall, got %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Memory-Degraded") != "true" || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected degraded headers, got %+v", rec.Header())
	}
	if !strings.Contains(rec.Body.String(), `"degraded":true`) {
		t.Fatalf("expected degraded:true in MCP response body, got %s", rec.Body.String())
	}

	// Error mapping checks on /v1/* when not saturated.
	inflight.Store(0)
	cases := []struct {
		name   string
		err    error
		method string
		path   string
		body   string
		want   int
	}{
		{"403 forbidden", service.ErrForbiddenAction, http.MethodPost, "/v1/workspaces/general/memories", `{"durability":"working","topic_key":"k","body":"b"}`, http.StatusForbidden},
		{"404 not found", storage.ErrNotFound, http.MethodGet, "/v1/memories/" + uuid.New().String(), "", http.StatusNotFound},
		{"409 conflict", storage.ErrVersionConflict, http.MethodPatch, "/v1/memories/" + uuid.New().String(), `{"expected_version":1}`, http.StatusConflict},
		{"422 idempotency", storage.ErrIdempotencyMismatch, http.MethodPost, "/v1/memories/" + uuid.New().String() + "/purge", `{"confirm":true}`, http.StatusUnprocessableEntity},
		{"413 payload", service.ErrPayloadTooLarge, http.MethodPost, "/v1/workspaces/app/memories", `{"durability":"durable","topic_key":"k","body":"b"}`, http.StatusRequestEntityTooLarge},
		{"503 outage", errors.New("pg down"), http.MethodPost, "/v1/memories/" + uuid.New().String() + "/revert", `{"expected_version":1,"target_version":1}`, http.StatusServiceUnavailable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc.createMemErr = tc.err
			svc.editMemErr = tc.err
			svc.revertMemErr = tc.err
			svc.purgeErr = tc.err

			req := httptest.NewRequest(tc.method, "http://127.0.0.1:8787"+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer admin-tok")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("expected status %d, got %d body=%s", tc.want, rec.Code, rec.Body.String())
			}
			var errObj apiErrorBody
			if err := json.Unmarshal(rec.Body.Bytes(), &errObj); err != nil || errObj.Code == "" || errObj.Error == "" {
				t.Fatalf("expected canonical {error, code} JSON, got %s", rec.Body.String())
			}
		})
	}
}

func TestMCPStaticCapabilitiesAvoidIdleSSEAndSupportFlush(t *testing.T) {
	svc := &fakeService{
		health: service.HealthCheckResult{Status: "ok", DBOk: true},
		recallRes: retrieval.RecallResponse{
			Memories: []retrieval.RecalledMemory{},
			Stubs:    []retrieval.MemoryStub{},
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mcpHandler := mcpserver.NewStreamableHTTPHandler(svc, logger)
	cfg := testConfig()
	cfg.AgentToken = ""
	inflight := &atomic.Int64{}
	srv := httptest.NewServer(NewHandlerWithInflight(cfg, svc, mcpHandler, logger, inflight))
	defer srv.Close()

	waitZeroInflight := func(phase string) {
		t.Helper()
		deadline := time.Now().Add(500 * time.Millisecond)
		for inflight.Load() != 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if got := inflight.Load(); got != 0 {
			t.Fatalf("%s: expected inflight == 0 (no idle SSE subscription stream), got %d", phase, got)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Even when the client installs list-changed handlers, static ServerCapabilities
	// (ListChanged: false) must prevent go-sdk from opening an idle subscriptions/listen stream.
	client := mcp.NewClient(&mcp.Implementation{Name: "static-probe", Version: "1.0.0"}, &mcp.ClientOptions{
		ToolListChangedHandler:     func(context.Context, *mcp.ToolListChangedRequest) {},
		ResourceListChangedHandler: func(context.Context, *mcp.ResourceListChangedRequest) {},
	})
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint: srv.URL + "/mcp",
	}, nil)
	if err != nil {
		t.Fatalf("connect Streamable HTTP MCP: %v", err)
	}
	defer session.Close()

	initRes := session.InitializeResult()
	if initRes == nil || initRes.Capabilities == nil {
		t.Fatalf("expected non-nil InitializeResult.Capabilities, got %+v", initRes)
	}
	if initRes.Capabilities.Tools == nil || initRes.Capabilities.Tools.ListChanged {
		t.Fatalf("expected Tools capability with ListChanged=false, got %+v", initRes.Capabilities.Tools)
	}
	if initRes.Capabilities.Resources == nil || initRes.Capabilities.Resources.ListChanged || initRes.Capabilities.Resources.Subscribe {
		t.Fatalf("expected Resources capability with ListChanged=false, Subscribe=false, got %+v", initRes.Capabilities.Resources)
	}
	waitZeroInflight("post-connect")

	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "memory_recall",
		Arguments: map[string]any{
			"workspace_id": "app",
			"query":        "test",
		},
	})
	if err != nil || res == nil || res.IsError {
		t.Fatalf("memory_recall failed: res=%+v err=%v", res, err)
	}
	waitZeroInflight("post-call")

	// Part 2: Verify MCPDegradedBufferMiddleware + LoggingMiddleware still flush SSE
	// immediately if an explicit text/event-stream handler writes and flushes before exit.
	sseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write([]byte("data: ready\n\n"))
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	})
	sseSrv := httptest.NewServer(NewHandler(cfg, svc, sseHandler, logger))
	defer sseSrv.Close()

	sseCtx, sseCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer sseCancel()
	req, err := http.NewRequestWithContext(sseCtx, http.MethodPost, sseSrv.URL+"/mcp", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("build SSE request: %v", err)
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("SSE request failed before headers flushed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on SSE stream, got %d", resp.StatusCode)
	}
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != "data: ready\n" {
		t.Fatalf("expected flushed SSE line %q before cancel, got %q err=%v", "data: ready\n", line, err)
	}
	sseCancel()
}
