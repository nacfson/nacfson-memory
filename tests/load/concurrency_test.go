//go:build integration && load

package load_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

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

var loadMigrateOnce sync.Once

// FailingPoolProxy forwards TCP traffic between pgxpool and PostgreSQL and can inject
// abrupt connection outages by severing active sockets and rejecting new dials.
type FailingPoolProxy struct {
	ln     net.Listener
	target string
	outage atomic.Bool
	closed atomic.Bool

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// NewFailingPoolProxy starts a loopback TCP proxy targeting upstreamDSN's host:port.
func NewFailingPoolProxy(t *testing.T, upstreamDSN string) *FailingPoolProxy {
	t.Helper()

	u, err := url.Parse(upstreamDSN)
	if err != nil {
		t.Fatalf("parse upstream DSN: %v", err)
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "5432"
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}

	p := &FailingPoolProxy{
		ln:     ln,
		target: net.JoinHostPort(host, port),
		conns:  make(map[net.Conn]struct{}),
	}
	p.wg.Add(1)
	go p.acceptLoop()
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// RewriteDSN returns a PostgreSQL DSN pointed at the proxy with a 1s connect timeout.
func (p *FailingPoolProxy) RewriteDSN(rawDSN string) string {
	u, err := url.Parse(rawDSN)
	if err != nil {
		return rawDSN
	}
	u.Host = p.ln.Addr().String()
	q := u.Query()
	q.Set("connect_timeout", "1")
	u.RawQuery = q.Encode()
	return u.String()
}

// SetOutage enables or disables PostgreSQL outage simulation, dropping active sockets on enable.
func (p *FailingPoolProxy) SetOutage(on bool) {
	p.outage.Store(on)
	if on {
		p.closeAllActiveConns()
	}
}

// Close shuts down the proxy listener, active connections, and relay goroutines.
func (p *FailingPoolProxy) Close() error {
	p.closed.Store(true)
	err := p.ln.Close()
	p.closeAllActiveConns()
	p.wg.Wait()
	return err
}

func (p *FailingPoolProxy) trackConn(c net.Conn) {
	p.mu.Lock()
	p.conns[c] = struct{}{}
	p.mu.Unlock()
}

func (p *FailingPoolProxy) untrackAndClose(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
	_ = c.Close()
}

func (p *FailingPoolProxy) closeAllActiveConns() {
	p.mu.Lock()
	snapshot := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		snapshot = append(snapshot, c)
		delete(p.conns, c)
	}
	p.mu.Unlock()
	for _, c := range snapshot {
		_ = c.Close()
	}
}

func (p *FailingPoolProxy) acceptLoop() {
	defer p.wg.Done()
	for {
		clientConn, err := p.ln.Accept()
		if err != nil {
			if p.closed.Load() {
				return
			}
			continue
		}
		if p.outage.Load() || p.closed.Load() {
			_ = clientConn.Close()
			continue
		}

		upConn, err := net.DialTimeout("tcp", p.target, 2*time.Second)
		if err != nil {
			_ = clientConn.Close()
			continue
		}

		p.trackConn(clientConn)
		p.trackConn(upConn)
		if p.outage.Load() || p.closed.Load() {
			p.untrackAndClose(clientConn)
			p.untrackAndClose(upConn)
			continue
		}

		p.wg.Add(2)
		go p.relay(upConn, clientConn)
		go p.relay(clientConn, upConn)
	}
}

func (p *FailingPoolProxy) relay(dst, src net.Conn) {
	defer p.wg.Done()
	_, _ = io.Copy(dst, src)
	p.untrackAndClose(src)
	p.untrackAndClose(dst)
}

// FailingCurator wraps MockCurator with an atomic failure toggle for chaos testing.
type FailingCurator struct {
	inner     *curation.MockCurator
	fail      atomic.Bool
	failCalls atomic.Int64
}

func (f *FailingCurator) SetFail(on bool) {
	f.fail.Store(on)
}

func (f *FailingCurator) CurateBatch(ctx context.Context, req curation.CurateRequest) (curation.CuratePlan, error) {
	if f.fail.Load() {
		f.failCalls.Add(1)
		return curation.CuratePlan{}, errors.New("injected external curator outage")
	}
	return f.inner.CurateBatch(ctx, req)
}

func openLoadStore(t *testing.T, directDSN, runtimeDSN string) (*storage.Store, config.Config) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var migrateErr error
	loadMigrateOnce.Do(func() {
		migrateErr = storage.Migrate(ctx, directDSN, false)
	})
	if migrateErr != nil {
		t.Fatalf("migrate load database: %v", migrateErr)
	}

	directConn, err := pgx.Connect(ctx, directDSN)
	if err != nil {
		t.Fatalf("connect direct DB for truncate: %v", err)
	}
	if _, err := directConn.Exec(ctx, `
		TRUNCATE TABLE
			confirmation_tokens,
			curation_decisions,
			memory_edges,
			memory_citations,
			memory_revisions,
			memory_manifest,
			raw_inbox,
			workspaces,
			river_job,
			river_leader,
			river_queue
		RESTART IDENTITY CASCADE;
		INSERT INTO workspaces (workspace_id, kind, inherit_general, manifest_revision, constitution_revision)
		VALUES ('general', 'general', 'none', 0, 0)
		ON CONFLICT (workspace_id) DO NOTHING;
	`); err != nil {
		_ = directConn.Close(ctx)
		t.Fatalf("truncate load tables: %v", err)
	}
	_ = directConn.Close(ctx)

	cfg, err := config.LoadFromLookup(config.ModeServe, func(k string) (string, bool) {
		switch k {
		case "MEM1_DATABASE_URL":
			return runtimeDSN, true
		case "MEM1_ADMIN_TOKEN":
			return "load-admin-token", true
		case "MEM1_AGENT_TOKEN":
			return "load-agent-token", true
		case "MEM1_CURATOR_PROVIDER":
			return config.ProviderMock, true
		case "MEM1_EMBED_PROVIDER":
			return config.ProviderMock, true
		case "MEM1_SERVING_MAX_CONNS":
			return "32", true
		case "MEM1_CURATION_MAX_CONNS":
			return "8", true
		case "MEM1_MAX_INFLIGHT":
			return "512", true
		default:
			return "", false
		}
	})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	store, err := storage.OpenPools(ctx, cfg)
	if err != nil {
		t.Fatalf("open load pools: %v", err)
	}
	t.Cleanup(store.Close)
	return store, cfg
}

func newLoadHTTPStack(
	t *testing.T,
	cfg config.Config,
	store *storage.Store,
	emb embedding.Embedder,
	curBreaker, embBreaker *curation.Breaker,
) (*service.MemoryService, *httptest.Server, *slog.Logger) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	lx := search.NewLexicalSearch()
	dg := graph.NewDependencyGraph()
	packer := retrieval.NewPacker(store, emb, embBreaker, lx, dg, cfg.EmbedTimeout)
	svc := service.New(cfg, store, security.NewSecretScrubber(), emb, packer, dg, curBreaker, embBreaker)
	mcpHandler := mcpserver.NewStreamableHTTPHandler(svc, logger)
	srv := httptest.NewServer(httpapi.NewHandler(cfg, svc, mcpHandler, logger))
	t.Cleanup(srv.Close)
	return svc, srv, logger
}

func newLoadHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			MaxConnsPerHost:     256,
		},
	}
}

type mcpRPCResponse struct {
	Result *struct {
		IsError bool `json:"isError"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	} `json:"result"`
	Error *struct {
		Code    int64  `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func callMCPTool(
	ctx context.Context,
	client *http.Client,
	baseURL, token string,
	rpcID int64,
	toolName string,
	args map[string]any,
) (httpStatus int, hdrs http.Header, rpcCode int64, bodyText string, err error) {
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      rpcID,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      toolName,
			"arguments": args,
		},
	})
	if err != nil {
		return 0, nil, 0, "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/mcp", bytes.NewReader(payload))
	if err != nil {
		return 0, nil, 0, "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, 0, "", err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, resp.Header.Clone(), 0, "", err
	}

	var rpcResp mcpRPCResponse
	if uErr := json.Unmarshal(raw, &rpcResp); uErr != nil {
		return resp.StatusCode, resp.Header.Clone(), 0, string(raw), fmt.Errorf("decode rpc response (status %d): %w", resp.StatusCode, uErr)
	}
	if rpcResp.Error != nil {
		return resp.StatusCode, resp.Header.Clone(), rpcResp.Error.Code, rpcResp.Error.Message, nil
	}
	if rpcResp.Result == nil || len(rpcResp.Result.Content) == 0 {
		return resp.StatusCode, resp.Header.Clone(), 0, string(raw), errors.New("empty MCP tool result")
	}
	if rpcResp.Result.IsError {
		return resp.StatusCode, resp.Header.Clone(), -32000, rpcResp.Result.Content[0].Text, nil
	}
	return resp.StatusCode, resp.Header.Clone(), 0, rpcResp.Result.Content[0].Text, nil
}

type manageTarget struct {
	workspaceID string
	memoryID    uuid.UUID
}

func TestConcurrencyAndChaos(t *testing.T) {
	directDSN := testutil.TestDSN()
	proxy := NewFailingPoolProxy(t, directDSN)
	proxyDSN := proxy.RewriteDSN(directDSN)

	store, cfg := openLoadStore(t, directDSN, proxyDSN)
	emb := embedding.NewMockEmbedder("mock:1536")
	curBreaker := curation.NewBreaker(3, 3*time.Second)
	embBreaker := curation.NewBreaker(3, 3*time.Second)
	svc, srv, logger := newLoadHTTPStack(t, cfg, store, emb, curBreaker, embBreaker)

	failCurator := &FailingCurator{inner: curation.NewMockCurator()}
	workers := river.NewWorkers()
	curation.RegisterWorkers(workers, curation.WorkerDeps{
		Store:           store,
		Curator:         failCurator,
		Embedder:        emb,
		Lexical:         search.NewLexicalSearch(),
		Graph:           graph.NewDependencyGraph(),
		CuratorBreaker:  curBreaker,
		EmbedderBreaker: embBreaker,
		Logger:          logger,
		SnoozeDuration:  250 * time.Millisecond,
	})
	periodic := []*river.PeriodicJob{
		river.NewPeriodicJob(
			river.PeriodicInterval(500*time.Millisecond),
			func() (river.JobArgs, *river.InsertOpts) {
				opts := storage.ProvisionalReplayJobArgs{}.InsertOpts()
				return storage.ProvisionalReplayJobArgs{}, &opts
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
	}
	riverClient, err := river.NewClient(riverpgxv5.New(store.CurationPool), &river.Config{
		Queues:       storage.RiverQueues(),
		Workers:      workers,
		PeriodicJobs: periodic,
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("create load river client: %v", err)
	}

	baseCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	if err := riverClient.Start(baseCtx); err != nil {
		t.Fatalf("start load river client: %v", err)
	}
	t.Cleanup(func() {
		proxy.SetOutage(false)
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopCancel()
		if err := riverClient.Stop(stopCtx); err != nil {
			t.Errorf("stop load river client: %v", err)
		}
	})

	// Seed 10 workspaces with permanent constitution/durable memories plus 700 disjoint working notes.
	const numWorkspaces = 10
	const workingPerWorkspace = 70
	managePool := make([]manageTarget, 0, numWorkspaces*workingPerWorkspace)
	for i := 0; i < numWorkspaces; i++ {
		wsID := "ws-" + strconv.Itoa(i)
		if _, err := svc.CreateWorkspace(baseCtx, service.CreateWorkspaceRequest{
			WorkspaceID:    wsID,
			InheritGeneral: storage.InheritNone,
		}); err != nil {
			t.Fatalf("seed workspace %s: %v", wsID, err)
		}
		if _, err := svc.ManualCreate(baseCtx, service.ManualCreateRequest{
			WorkspaceID: wsID,
			Durability:  storage.DurabilityConstitution,
			TopicKey:    fmt.Sprintf("rule-%d", i),
			Body:        fmt.Sprintf("Always keep PostgreSQL 17 healthy for %s.", wsID),
		}); err != nil {
			t.Fatalf("seed constitution %s: %v", wsID, err)
		}
		for j := 0; j < 2; j++ {
			if _, err := svc.ManualCreate(baseCtx, service.ManualCreateRequest{
				WorkspaceID: wsID,
				Durability:  storage.DurabilityDurable,
				TopicKey:    fmt.Sprintf("pgvector-%d-%d", i, j),
				Body:        fmt.Sprintf("Primary database PostgreSQL 17 pgvector note %d for %s.", j, wsID),
			}); err != nil {
				t.Fatalf("seed durable %s: %v", wsID, err)
			}
		}
		for j := 0; j < workingPerWorkspace; j++ {
			created, err := svc.ManualCreate(baseCtx, service.ManualCreateRequest{
				WorkspaceID: wsID,
				Durability:  storage.DurabilityWorking,
				TopicKey:    fmt.Sprintf("wip-%d-%d", i, j),
				Body:        fmt.Sprintf("Disjoint working item %d for retire load test in %s.", j, wsID),
			})
			if err != nil {
				t.Fatalf("seed working %s-%d: %v", wsID, j, err)
			}
			managePool = append(managePool, manageTarget{workspaceID: wsID, memoryID: created.MemoryID})
		}
	}

	httpClient := newLoadHTTPClient()
	t.Cleanup(httpClient.CloseIdleConnections)

	const (
		numWorkers   = 100
		loadDuration = 60 * time.Second
		batchPerTick = 15 // 15 * 10 ticks/sec = 150 RPS
		tickInterval = 100 * time.Millisecond
	)

	var (
		totalReqs            atomic.Int64
		recallCount          atomic.Int64
		writeCount           atomic.Int64
		manageCount          atomic.Int64
		manageSucceeded      atomic.Int64
		unexpectedManageErrs atomic.Int64
		unexpectedRecallErrs atomic.Int64
		degradedRecalls      atomic.Int64
		writeConflicts       atomic.Int64
		silentEmpty200       atomic.Int64
		badDegradedHdrs      atomic.Int64
		manageCursor         atomic.Int64
		sawProvisional       atomic.Int64

		failSamplesMu sync.Mutex
		failSamples   []string
	)
	recordSample := func(msg string) {
		failSamplesMu.Lock()
		if len(failSamples) < 10 {
			failSamples = append(failSamples, msg)
		}
		failSamplesMu.Unlock()
	}

	// Fault injection schedule:
	//  - t=15s..17.5s: PostgreSQL proxy outage (verifies fail-open degraded recall headers & zero silent 200 OK []).
	//  - t=30s..33.0s: External Curator outage (trips CuratorBreaker + creates provisional:<inbox_id> rows).
	faultDone := make(chan struct{})
	var faultWG sync.WaitGroup
	faultWG.Add(1)
	go func() {
		defer faultWG.Done()

		select {
		case <-time.After(15 * time.Second):
		case <-faultDone:
			return
		}
		proxy.SetOutage(true)
		select {
		case <-time.After(2500 * time.Millisecond):
		case <-faultDone:
			proxy.SetOutage(false)
			return
		}
		proxy.SetOutage(false)

		select {
		case <-time.After(12500 * time.Millisecond):
		case <-faultDone:
			return
		}
		failCurator.SetFail(true)

		// Sample direct DB during/just after Curator failure window to observe active provisional rows.
		outageEnd := time.NewTimer(3 * time.Second)
		sampleTick := time.NewTicker(300 * time.Millisecond)
	sampleLoop:
		for {
			select {
			case <-outageEnd.C:
				break sampleLoop
			case <-faultDone:
				break sampleLoop
			case <-sampleTick.C:
				if cnt, err := countActiveProvisionalDirect(baseCtx, directDSN); err == nil && cnt > 0 {
					sawProvisional.Add(cnt)
				}
			}
		}
		sampleTick.Stop()
		outageEnd.Stop()
		failCurator.SetFail(false)
	}()

	tokens := make(chan int64, 512)
	var workersWG sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for seq := range tokens {
				totalReqs.Add(1)
				wsID := "ws-" + strconv.FormatInt(seq%numWorkspaces, 10)
				opSlot := seq % 20

				// 5% manage on disjoint pre-seeded working IDs (fall back to recall if pool exhausted).
				if opSlot == 19 {
					idx := int(manageCursor.Add(1) - 1)
					if idx < len(managePool) {
						manageCount.Add(1)
						target := managePool[idx]
						status, _, rpcCode, bodyText, callErr := callMCPTool(baseCtx, httpClient, srv.URL, cfg.AgentToken, seq, "memory_manage", map[string]any{
							"workspace_id":     target.workspaceID,
							"action":           "retire",
							"target_memory_id": target.memoryID.String(),
							"expected_version": 1,
							"reason":           "load test retire",
						})
						switch {
						case callErr == nil && status == http.StatusOK && rpcCode == 0:
							manageSucceeded.Add(1)
						case rpcCode == -32602 || rpcCode == -32022 || (callErr == nil && status == http.StatusOK && rpcCode != 0 && rpcCode != -32053):
							unexpectedManageErrs.Add(1)
							recordSample(fmt.Sprintf("unexpected manage error status=%d rpcCode=%d body=%s", status, rpcCode, bodyText))
						}
						continue
					}
				}

				if opSlot >= 14 && opSlot <= 18 {
					// 25% memory_write.
					writeCount.Add(1)
					status, _, rpcCode, bodyText, callErr := callMCPTool(baseCtx, httpClient, srv.URL, cfg.AgentToken, seq, "memory_write", map[string]any{
						"workspace_id":    wsID,
						"agent_id":        "load-worker",
						"idempotency_key": fmt.Sprintf("load-w-%d", seq),
						"content":         fmt.Sprintf("topic:load-%d Worker %d wrote PostgreSQL 17 tuning for %s.", seq%5, seq, wsID),
					})
					if callErr == nil && (status == http.StatusConflict || rpcCode == -32022) {
						writeConflicts.Add(1)
						recordSample(fmt.Sprintf("write conflict status=%d rpcCode=%d body=%s", status, rpcCode, bodyText))
					}
					continue
				}

				// 70% memory_recall.
				recallCount.Add(1)
				status, hdrs, rpcCode, bodyText, callErr := callMCPTool(baseCtx, httpClient, srv.URL, cfg.AgentToken, seq, "memory_recall", map[string]any{
					"workspace_id": wsID,
					"query":        "PostgreSQL pgvector",
					"max_tokens":   2000,
				})
				if callErr != nil || rpcCode != 0 {
					unexpectedRecallErrs.Add(1)
					recordSample(fmt.Sprintf("unexpected recall failure status=%d rpcCode=%d err=%v body=%s", status, rpcCode, callErr, bodyText))
					continue
				}
				var rec retrieval.RecallResponse
				if uErr := json.Unmarshal([]byte(bodyText), &rec); uErr != nil {
					unexpectedRecallErrs.Add(1)
					recordSample(fmt.Sprintf("decode recall response: %v (body=%s)", uErr, bodyText))
					continue
				}
				if status == http.StatusOK && !rec.Degraded && len(rec.Memories) == 0 {
					silentEmpty200.Add(1)
					recordSample(fmt.Sprintf("silent 200 OK [] on %s", wsID))
				}
				if rec.Degraded {
					degradedRecalls.Add(1)
					if hdrs.Get("X-Memory-Degraded") != "true" || !strings.Contains(hdrs.Get("Cache-Control"), "no-store") {
						badDegradedHdrs.Add(1)
						recordSample(fmt.Sprintf("missing degraded headers: X-Memory-Degraded=%q Cache-Control=%q", hdrs.Get("X-Memory-Degraded"), hdrs.Get("Cache-Control")))
					}
				}
			}
		}()
	}

	// Drive 150 RPS across 60s.
	var seq int64
	loadEnd := time.After(loadDuration)
	ticker := time.NewTicker(tickInterval)
feedLoop:
	for {
		select {
		case <-loadEnd:
			break feedLoop
		case <-ticker.C:
			for i := 0; i < batchPerTick; i++ {
				seq++
				select {
				case tokens <- seq:
				case <-loadEnd:
					break feedLoop
				}
			}
		}
	}
	ticker.Stop()
	close(tokens)
	workersWG.Wait()
	close(faultDone)
	faultWG.Wait()
	proxy.SetOutage(false)
	failCurator.SetFail(false)

	t.Logf("load summary: total=%d recalls=%d writes=%d manages=%d manageSucceeded=%d unexpectedManageErrs=%d unexpectedRecallErrs=%d degradedRecalls=%d writeConflicts=%d silentEmpty200=%d badDegradedHdrs=%d sawProvisional=%d failCuratorCalls=%d",
		totalReqs.Load(), recallCount.Load(), writeCount.Load(), manageCount.Load(),
		manageSucceeded.Load(), unexpectedManageErrs.Load(), unexpectedRecallErrs.Load(),
		degradedRecalls.Load(), writeConflicts.Load(), silentEmpty200.Load(),
		badDegradedHdrs.Load(), sawProvisional.Load(), failCurator.failCalls.Load())

	failSamplesMu.Lock()
	samplesCopy := append([]string(nil), failSamples...)
	failSamplesMu.Unlock()

	if totalReqs.Load() < 8000 {
		t.Fatalf("expected >= 8000 sustained requests over 60s, got %d", totalReqs.Load())
	}
	if unexpectedRecallErrs.Load() != 0 {
		t.Fatalf("expected 0 unexpected recall failures, got %d (samples: %v)", unexpectedRecallErrs.Load(), samplesCopy)
	}
	if unexpectedManageErrs.Load() != 0 {
		t.Fatalf("expected 0 unexpected manage failures, got %d (samples: %v)", unexpectedManageErrs.Load(), samplesCopy)
	}
	if manageSucceeded.Load() == 0 {
		t.Fatalf("expected manageSucceeded > 0")
	}
	if writeConflicts.Load() != 0 {
		t.Fatalf("expected 0 write 409/idempotency conflicts, got %d (samples: %v)", writeConflicts.Load(), samplesCopy)
	}
	if silentEmpty200.Load() != 0 {
		t.Fatalf("expected 0 silent 200 OK [] recalls during load/outage, got %d (samples: %v)", silentEmpty200.Load(), samplesCopy)
	}
	if badDegradedHdrs.Load() != 0 {
		t.Fatalf("degraded recalls missing X-Memory-Degraded / Cache-Control headers: %d (samples: %v)", badDegradedHdrs.Load(), samplesCopy)
	}
	if degradedRecalls.Load() == 0 {
		t.Fatalf("expected degraded recalls > 0 during injected PostgreSQL outage")
	}
	if sawProvisional.Load() == 0 || failCurator.failCalls.Load() < 3 {
		t.Fatalf("expected Curator outage fallback to engage (sawProvisional=%d failCalls=%d)", sawProvisional.Load(), failCurator.failCalls.Load())
	}

	// Wait for ProvisionalReplayWorker + CurateWorkspaceWorker to drain all provisional & pending rows.
	waitStart := time.Now()
	drainDeadline := waitStart.Add(45 * time.Second)
	var lastCounts inboxStateCounts
	nudgedPending := false
	for time.Now().Before(drainDeadline) {
		counts, err := queryInboxAndProvisionalCounts(baseCtx, directDSN)
		if err == nil {
			lastCounts = counts
			if counts.provisionalManifest == 0 && counts.provisionalInbox == 0 && counts.pendingInbox == 0 && counts.deadLetterInbox == 0 {
				t.Logf("post-chaos provisional & pending drain completed cleanly in %s", time.Since(waitStart))
				return
			}
			// If a PG-outage curate_workspace job is sitting in River retry backoff, nudge pending workspaces once after 5s.
			if !nudgedPending && time.Since(waitStart) > 5*time.Second && counts.pendingInbox > 0 {
				nudgedPending = true
				for i := 0; i < numWorkspaces; i++ {
					_, _ = riverClient.Insert(baseCtx, storage.CurateWorkspaceJobArgs{WorkspaceID: "ws-" + strconv.Itoa(i)}, nil)
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf("post-chaos provisional/pending state did not drain within 45s: %+v", lastCounts)
}

type inboxStateCounts struct {
	provisionalManifest int64
	provisionalInbox    int64
	pendingInbox        int64
	deadLetterInbox     int64
}

func countActiveProvisionalDirect(ctx context.Context, dsn string) (int64, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)
	var cnt int64
	err = conn.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM memory_manifest
		WHERE topic_key LIKE 'provisional:%' AND status = 'active'
	`).Scan(&cnt)
	return cnt, err
}

func queryInboxAndProvisionalCounts(ctx context.Context, dsn string) (inboxStateCounts, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return inboxStateCounts{}, err
	}
	defer conn.Close(ctx)

	var out inboxStateCounts
	if err := conn.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM memory_manifest WHERE topic_key LIKE 'provisional:%' AND status = 'active'),
			(SELECT COUNT(*) FROM raw_inbox WHERE status = 'provisional'),
			(SELECT COUNT(*) FROM raw_inbox WHERE status = 'pending'),
			(SELECT COUNT(*) FROM raw_inbox WHERE status = 'dead_letter')
	`).Scan(&out.provisionalManifest, &out.provisionalInbox, &out.pendingInbox, &out.deadLetterInbox); err != nil {
		return inboxStateCounts{}, err
	}
	return out, nil
}

func TestRecallLatencySLO(t *testing.T) {
	dsn := testutil.TestDSN()
	store, cfg := openLoadStore(t, dsn, dsn)
	emb := embedding.NewMockEmbedder("mock:1536")
	curBreaker := curation.NewBreaker(3, 60*time.Second)
	embBreaker := curation.NewBreaker(3, 60*time.Second)
	svc, srv, _ := newLoadHTTPStack(t, cfg, store, emb, curBreaker, embBreaker)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Seed 10 workspaces with realistic active vector + lexical corpus.
	const numWorkspaces = 10
	for i := 0; i < numWorkspaces; i++ {
		wsID := "slo-" + strconv.Itoa(i)
		if _, err := svc.CreateWorkspace(ctx, service.CreateWorkspaceRequest{
			WorkspaceID:    wsID,
			InheritGeneral: storage.InheritNone,
		}); err != nil {
			t.Fatalf("seed SLO workspace %s: %v", wsID, err)
		}
		if _, err := svc.ManualCreate(ctx, service.ManualCreateRequest{
			WorkspaceID: wsID,
			Durability:  storage.DurabilityConstitution,
			TopicKey:    fmt.Sprintf("slo-const-%d", i),
			Body:        fmt.Sprintf("Always enforce PostgreSQL 17 p95 recall <= 100ms in %s.", wsID),
		}); err != nil {
			t.Fatalf("seed SLO constitution %s: %v", wsID, err)
		}
		for j := 0; j < 20; j++ {
			if _, err := svc.ManualCreate(ctx, service.ManualCreateRequest{
				WorkspaceID: wsID,
				Durability:  storage.DurabilityDurable,
				TopicKey:    fmt.Sprintf("slo-doc-%d-%d", i, j),
				Body:        fmt.Sprintf("PostgreSQL 17 HNSW halfvec and GIN full-text benchmark item %d for %s.", j, wsID),
			}); err != nil {
				t.Fatalf("seed SLO durable %s-%d: %v", wsID, j, err)
			}
		}
	}

	httpClient := newLoadHTTPClient()
	t.Cleanup(httpClient.CloseIdleConnections)

	// Warm up pool connections and HNSW cache.
	for i := 0; i < 20; i++ {
		wsID := "slo-" + strconv.Itoa(i%numWorkspaces)
		status, _, rpcCode, bodyText, err := callMCPTool(ctx, httpClient, srv.URL, cfg.AgentToken, int64(i+1), "memory_recall", map[string]any{
			"workspace_id": wsID,
			"query":        "PostgreSQL HNSW GIN benchmark",
			"max_tokens":   2000,
		})
		if err != nil || status != http.StatusOK || rpcCode != 0 {
			t.Fatalf("warmup recall failed: status=%d rpcCode=%d err=%v body=%s", status, rpcCode, err, bodyText)
		}
	}

	const (
		numWorkers      = 100
		callsPerWorker  = 30
	)
	latencies := make([][]time.Duration, numWorkers)
	var (
		wg        sync.WaitGroup
		errCount  atomic.Int64
		degCount  atomic.Int64
		emptyHits atomic.Int64
		rpcSeq    atomic.Int64
	)

	for w := 0; w < numWorkers; w++ {
		wIdx := w
		latencies[wIdx] = make([]time.Duration, 0, callsPerWorker)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := 0; c < callsPerWorker; c++ {
				id := rpcSeq.Add(1)
				wsID := "slo-" + strconv.Itoa((wIdx+c)%numWorkspaces)
				start := time.Now()
				status, _, rpcCode, bodyText, err := callMCPTool(ctx, httpClient, srv.URL, cfg.AgentToken, id, "memory_recall", map[string]any{
					"workspace_id": wsID,
					"query":        "PostgreSQL HNSW GIN benchmark",
					"max_tokens":   2000,
				})
				elapsed := time.Since(start)
				latencies[wIdx] = append(latencies[wIdx], elapsed)

				if err != nil || status != http.StatusOK || rpcCode != 0 {
					errCount.Add(1)
					continue
				}
				var rec retrieval.RecallResponse
				if uErr := json.Unmarshal([]byte(bodyText), &rec); uErr != nil {
					errCount.Add(1)
					continue
				}
				if rec.Degraded {
					degCount.Add(1)
				}
				if len(rec.Memories) == 0 {
					emptyHits.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if errCount.Load() != 0 || degCount.Load() != 0 || emptyHits.Load() != 0 {
		t.Fatalf("SLO run had errors=%d degraded=%d empty=%d", errCount.Load(), degCount.Load(), emptyHits.Load())
	}

	all := make([]time.Duration, 0, numWorkers*callsPerWorker)
	for _, list := range latencies {
		all = append(all, list...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	p50 := all[(len(all)*50)/100]
	p95 := all[(len(all)*95)/100]
	p99 := all[(len(all)*99)/100]
	maxLat := all[len(all)-1]
	t.Logf("SLO latency over %d recalls (%d concurrent workers): p50=%s p95=%s p99=%s max=%s",
		len(all), numWorkers, p50, p95, p99, maxLat)

	if p95 > 100*time.Millisecond {
		t.Fatalf("recall p95 latency %s exceeded 100ms SLO", p95)
	}
}
