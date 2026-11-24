package httpapi

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nacfson/nacfson-memory/internal/mcpserver"
)

const (
	// MaxAdminBodyBytes caps /v1/* JSON request bodies at 64 KiB.
	MaxAdminBodyBytes int64 = 64 * 1024
	// MaxMCPBodyBytes caps /mcp JSON-RPC request bodies at 80 KiB.
	MaxMCPBodyBytes int64 = mcpserver.MaxMCPBodyBytes
)

type apiErrorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func writeJSONError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiErrorBody{
		Error: msg,
		Code:  code,
	})
}

// HostOriginMiddleware rejects requests whose Host header or Origin URL hostname
// is outside allowedHosts with 403 Forbidden.
func HostOriginMiddleware(allowedHosts []string, next http.Handler) http.Handler {
	allowed := make(map[string]struct{}, len(allowedHosts))
	for _, h := range allowedHosts {
		norm := normalizeHost(h)
		if norm != "" {
			allowed[norm] = struct{}{}
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqHost := normalizeHost(r.Host)
		if reqHost == "" {
			writeJSONError(w, http.StatusForbidden, "forbidden", "missing Host header")
			return
		}
		if _, ok := allowed[reqHost]; !ok {
			writeJSONError(w, http.StatusForbidden, "forbidden", fmt.Sprintf("host %q not in MEM1_ALLOWED_HOSTS", reqHost))
			return
		}

		if origin := strings.TrimSpace(r.Header.Get("Origin")); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || u.Host == "" || u.User != nil || (!strings.EqualFold(u.Scheme, "http") && !strings.EqualFold(u.Scheme, "https")) {
				writeJSONError(w, http.StatusForbidden, "forbidden", "invalid Origin header")
				return
			}
			originHost := normalizeHost(u.Host)
			if _, ok := allowed[originHost]; !ok {
				writeJSONError(w, http.StatusForbidden, "forbidden", fmt.Sprintf("origin host %q not in MEM1_ALLOWED_HOSTS", originHost))
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func normalizeHost(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "[") {
		if host, _, err := net.SplitHostPort(s); err == nil {
			return strings.Trim(host, "[]")
		}
		return strings.Trim(s, "[]")
	}
	if host, _, err := net.SplitHostPort(s); err == nil {
		return host
	}
	return s
}

func isMCPPath(path string) bool {
	return path == "/mcp" || strings.HasPrefix(path, "/mcp/")
}

// AuthMiddleware enforces split Bearer tokens: /healthz open, /v1/* mandatory adminToken,
// and /mcp agentToken when configured.
func AuthMiddleware(adminToken, agentToken string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/healthz":
			next.ServeHTTP(w, r)
			return
		case isMCPPath(path):
			if agentToken != "" && !bearerMatch(r.Header.Get("Authorization"), agentToken) {
				writeJSONError(w, http.StatusUnauthorized, "unauthorized", "valid MEM1_AGENT_TOKEN bearer token required")
				return
			}
			next.ServeHTTP(w, r)
			return
		case path == "/v1" || strings.HasPrefix(path, "/v1/"):
			if adminToken == "" || !bearerMatch(r.Header.Get("Authorization"), adminToken) {
				writeJSONError(w, http.StatusUnauthorized, "unauthorized", "valid MEM1_ADMIN_TOKEN bearer token required")
				return
			}
			next.ServeHTTP(w, r)
			return
		default:
			next.ServeHTTP(w, r)
		}
	})
}

func bearerMatch(header, expected string) bool {
	if expected == "" {
		return false
	}
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

// InflightMiddleware tracks active requests against maxInflight.
// /v1/* fails fast with 503 + Retry-After; /mcp propagates saturation context for fail-open recall.
func InflightMiddleware(maxInflight int, inflight *atomic.Int64, next http.Handler) http.Handler {
	if maxInflight <= 0 {
		maxInflight = 512
	}
	if inflight == nil {
		inflight = &atomic.Int64{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		cur := inflight.Add(1)
		defer inflight.Add(-1)

		if cur > int64(maxInflight) {
			if isMCPPath(r.URL.Path) {
				req := r.WithContext(mcpserver.WithInflightSaturated(r.Context()))
				next.ServeHTTP(w, req)
				return
			}
			w.Header().Set("Retry-After", "1")
			writeJSONError(w, http.StatusServiceUnavailable, "service_unavailable", "max inflight capacity exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// BodyLimitMiddleware caps raw request body sizes (80 KiB on /mcp, 64 KiB on /v1/*).
func BodyLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit := MaxAdminBodyBytes
		if isMCPPath(r.URL.Path) {
			limit = MaxMCPBodyBytes
		}
		if r.ContentLength > limit {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "payload_too_large", fmt.Sprintf("request body exceeds %d bytes", limit))
			return
		}
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		next.ServeHTTP(w, r)
	})
}

type bufferedMCPWriter struct {
	mu          sync.Mutex
	dst         http.ResponseWriter
	degraded    *atomic.Bool
	header      http.Header
	status      int
	wroteHeader bool
	buf         bytes.Buffer
	streaming   bool
}

func newBufferedMCPWriter(dst http.ResponseWriter, degraded *atomic.Bool) *bufferedMCPWriter {
	return &bufferedMCPWriter{
		dst:      dst,
		degraded: degraded,
		header:   make(http.Header),
		status:   http.StatusOK,
	}
}

func (b *bufferedMCPWriter) isSSELocked() bool {
	return strings.HasPrefix(strings.ToLower(b.header.Get("Content-Type")), "text/event-stream")
}

func (b *bufferedMCPWriter) applyHeadersLocked() {
	dst := b.dst.Header()
	for k, vals := range b.header {
		for _, v := range vals {
			dst.Add(k, v)
		}
	}
	if b.degraded != nil && b.degraded.Load() {
		dst.Set("X-Memory-Degraded", "true")
		dst.Set("Cache-Control", "no-store")
	}
}

func (b *bufferedMCPWriter) promoteToStreamingLocked() {
	b.streaming = true
	b.wroteHeader = true
	b.applyHeadersLocked()
	b.dst.WriteHeader(b.status)
	if b.buf.Len() > 0 {
		_, _ = b.dst.Write(b.buf.Bytes())
		b.buf.Reset()
	}
	_ = http.NewResponseController(b.dst).Flush()
}

func (b *bufferedMCPWriter) Header() http.Header {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.streaming {
		return b.dst.Header()
	}
	return b.header
}

func (b *bufferedMCPWriter) WriteHeader(statusCode int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.streaming || b.wroteHeader {
		return
	}
	b.wroteHeader = true
	b.status = statusCode
	if b.isSSELocked() {
		b.promoteToStreamingLocked()
	}
}

func (b *bufferedMCPWriter) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.wroteHeader = true
	if !b.streaming && b.isSSELocked() {
		b.promoteToStreamingLocked()
	}
	if b.streaming {
		return b.dst.Write(p)
	}
	return b.buf.Write(p)
}

func (b *bufferedMCPWriter) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.streaming {
		b.promoteToStreamingLocked()
		return
	}
	_ = http.NewResponseController(b.dst).Flush()
}

func (b *bufferedMCPWriter) finish() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.streaming {
		return
	}
	b.applyHeadersLocked()
	b.dst.WriteHeader(b.status)
	if b.buf.Len() > 0 {
		_, _ = b.dst.Write(b.buf.Bytes())
	}
}

// MCPDegradedBufferMiddleware buffers non-streaming /mcp responses long enough to inject
// X-Memory-Degraded: true and Cache-Control: no-store headers whenever recall degraded,
// while flushing SSE streams (e.g. subscriptions/listen) immediately without deadlocking.
func MCPDegradedBufferMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isMCPPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		var degraded atomic.Bool
		bw := newBufferedMCPWriter(w, &degraded)
		req := r.WithContext(mcpserver.WithDegradedSignal(r.Context(), &degraded))
		next.ServeHTTP(bw, req)
		bw.finish()
	})
}

type statusCaptureWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusCaptureWriter) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap exposes the underlying ResponseWriter so http.ResponseController can Flush SSE streams.
func (s *statusCaptureWriter) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}

// LoggingMiddleware emits structured slog request metrics without logging secrets or payloads.
func LoggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusCaptureWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		logger.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}
