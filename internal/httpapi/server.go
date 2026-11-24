package httpapi

import (
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/nacfson/nacfson-memory/internal/config"
)

// NewHandler wires the unified HTTP mux (/mcp + /healthz + 17 /v1/* routes) and middleware chain.
func NewHandler(cfg config.Config, svc Service, mcpHandler http.Handler, logger *slog.Logger) http.Handler {
	inflight := &atomic.Int64{}
	return NewHandlerWithInflight(cfg, svc, mcpHandler, logger, inflight)
}

// NewHandlerWithInflight is identical to NewHandler while allowing callers/tests to inspect or share the inflight counter.
func NewHandlerWithInflight(cfg config.Config, svc Service, mcpHandler http.Handler, logger *slog.Logger, inflight *atomic.Int64) http.Handler {
	if inflight == nil {
		inflight = &atomic.Int64{}
	}
	mux := http.NewServeMux()
	h := &handlers{svc: svc, inflight: inflight}
	h.register(mux)

	if mcpHandler != nil {
		mux.Handle("/mcp", mcpHandler)
		mux.Handle("/mcp/", mcpHandler)
	}

	var handler http.Handler = mux
	handler = MCPDegradedBufferMiddleware(handler)
	handler = BodyLimitMiddleware(handler)
	handler = InflightMiddleware(cfg.MaxInflight, inflight, handler)
	handler = AuthMiddleware(cfg.AdminToken, cfg.AgentToken, handler)
	handler = HostOriginMiddleware(cfg.AllowedHosts, handler)
	handler = LoggingMiddleware(logger, handler)
	return handler
}

// NewServer constructs the configured *http.Server bound to cfg.HTTPAddr.
func NewServer(cfg config.Config, svc Service, mcpHandler http.Handler, logger *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           NewHandler(cfg, svc, mcpHandler, logger),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
