package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/service"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

// Service defines the MemoryService operations backing the 18 HTTP endpoints.
type Service interface {
	HealthCheck(ctx context.Context) service.HealthCheckResult
	ListWorkspaces(ctx context.Context) ([]storage.WorkspaceSummary, error)
	CreateWorkspace(ctx context.Context, req service.CreateWorkspaceRequest) (storage.Workspace, error)
	UpdateWorkspace(ctx context.Context, workspaceID, inheritGeneral string) (storage.Workspace, error)
	GetConstitution(ctx context.Context, workspaceID string) (service.ConstitutionResult, error)
	ListMemoriesKeyset(ctx context.Context, params storage.ListMemoriesParams) (storage.ListMemoriesPage, error)
	GetMemory(ctx context.Context, memoryID uuid.UUID) (service.MemoryDetailResult, error)
	GetRevisions(ctx context.Context, memoryID uuid.UUID) ([]storage.MemoryRevision, error)
	ExplainLineage(ctx context.Context, memoryID uuid.UUID) (storage.MemoryLineage, error)
	ManualCreate(ctx context.Context, req service.ManualCreateRequest) (service.ManualMutationResult, error)
	ManualEdit(ctx context.Context, req service.ManualEditRequest) (service.ManualMutationResult, error)
	RevertMemory(ctx context.Context, req service.RevertMemoryRequest) (service.ManualMutationResult, error)
	SoftRetire(ctx context.Context, req service.SoftRetireRequest) (service.ManualMutationResult, error)
	HardPurge(ctx context.Context, req service.HardPurgeRequest) (storage.HardPurgeResult, error)
	IssueConfirmToken(ctx context.Context, req service.IssueConfirmTokenRequest) (service.ConfirmTokenResult, error)
	OpsStatus(ctx context.Context) (service.OpsStatusResult, error)
	ListCurationDecisions(ctx context.Context, params storage.ListDecisionsParams) (storage.ListDecisionsPage, error)
	ListDeadLetters(ctx context.Context, limit int) (storage.DeadLettersReport, error)
}

type handlers struct {
	svc      Service
	inflight *atomic.Int64
}

func (h *handlers) register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.handleHealthz)
	mux.HandleFunc("GET /v1/workspaces", h.handleListWorkspaces)
	mux.HandleFunc("POST /v1/workspaces", h.handleCreateWorkspace)
	mux.HandleFunc("PATCH /v1/workspaces/{id}", h.handleUpdateWorkspace)
	mux.HandleFunc("GET /v1/workspaces/{id}/constitution", h.handleGetConstitution)
	mux.HandleFunc("GET /v1/memories", h.handleListMemories)
	mux.HandleFunc("GET /v1/memories/{id}", h.handleGetMemory)
	mux.HandleFunc("GET /v1/memories/{id}/revisions", h.handleGetRevisions)
	mux.HandleFunc("GET /v1/memories/{id}/explain", h.handleExplainMemory)
	mux.HandleFunc("POST /v1/workspaces/{id}/memories", h.handleCreateMemory)
	mux.HandleFunc("PATCH /v1/memories/{id}", h.handleEditMemory)
	mux.HandleFunc("POST /v1/memories/{id}/revert", h.handleRevertMemory)
	mux.HandleFunc("DELETE /v1/memories/{id}", h.handleDeleteMemory)
	mux.HandleFunc("POST /v1/memories/{id}/purge", h.handlePurgeMemory)
	mux.HandleFunc("POST /v1/auth/confirm-token", h.handleConfirmToken)
	mux.HandleFunc("GET /v1/ops/status", h.handleOpsStatus)
	mux.HandleFunc("GET /v1/ops/curation-decisions", h.handleListDecisions)
	mux.HandleFunc("GET /v1/ops/dead-letters", h.handleDeadLetters)
}

func (h *handlers) handleHealthz(w http.ResponseWriter, r *http.Request) {
	res := h.svc.HealthCheck(r.Context())
	status := http.StatusOK
	if !res.DBOk {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, res)
}

func (h *handlers) handleListWorkspaces(w http.ResponseWriter, r *http.Request) {
	list, err := h.svc.ListWorkspaces(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if list == nil {
		list = []storage.WorkspaceSummary{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"workspaces": list})
}

func (h *handlers) handleCreateWorkspace(w http.ResponseWriter, r *http.Request) {
	var req service.CreateWorkspaceRequest
	if !decodeJSONBody(w, r, &req, false) {
		return
	}
	ws, err := h.svc.CreateWorkspace(r.Context(), req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, ws)
}

func (h *handlers) handleUpdateWorkspace(w http.ResponseWriter, r *http.Request) {
	wsID := r.PathValue("id")
	var req struct {
		InheritGeneral string `json:"inherit_general"`
	}
	if !decodeJSONBody(w, r, &req, false) {
		return
	}
	ws, err := h.svc.UpdateWorkspace(r.Context(), wsID, req.InheritGeneral)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ws)
}

func (h *handlers) handleGetConstitution(w http.ResponseWriter, r *http.Request) {
	wsID := r.PathValue("id")
	res, err := h.svc.GetConstitution(r.Context(), wsID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	w.Header().Set("ETag", res.ManifestETag)
	w.Header().Set("Cache-Control", "no-cache")
	if etagMatches(r.Header.Get("If-None-Match"), res.ManifestETag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *handlers) handleListMemories(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	params := storage.ListMemoriesParams{
		WorkspaceID:   strings.TrimSpace(q.Get("workspace_id")),
		Durability:    strings.TrimSpace(q.Get("durability")),
		Status:        strings.TrimSpace(q.Get("status")),
		CurationState: strings.TrimSpace(q.Get("curation_state")),
		Cursor:        strings.TrimSpace(q.Get("cursor")),
	}
	if rawDrifted := strings.TrimSpace(q.Get("premise_drifted")); rawDrifted != "" {
		b, err := strconv.ParseBool(rawDrifted)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid premise_drifted boolean")
			return
		}
		params.PremiseDrifted = &b
	}
	if rawInc := strings.TrimSpace(q.Get("include_revisions")); rawInc != "" {
		b, err := strconv.ParseBool(rawInc)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid include_revisions boolean")
			return
		}
		params.IncludeRevisions = b
	}
	if rawLimit := strings.TrimSpace(q.Get("limit")); rawLimit != "" {
		n, err := strconv.Atoi(rawLimit)
		if err != nil || n < 1 {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid limit")
			return
		}
		params.Limit = n
	}

	page, err := h.svc.ListMemoriesKeyset(r.Context(), params)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if page.Items == nil {
		page.Items = []storage.MemoryListItem{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *handlers) handleGetMemory(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathUUID(w, r, "id")
	if !ok {
		return
	}
	detail, err := h.svc.GetMemory(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if detail.OutgoingEdges == nil {
		detail.OutgoingEdges = []graph.OutgoingEdge{}
	}
	if detail.IncomingEdges == nil {
		detail.IncomingEdges = []graph.IncomingEdge{}
	}
	writeJSON(w, http.StatusOK, detail)
}

func (h *handlers) handleGetRevisions(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathUUID(w, r, "id")
	if !ok {
		return
	}
	revs, err := h.svc.GetRevisions(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if revs == nil {
		revs = []storage.MemoryRevision{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"memory_id": id,
		"revisions": revs,
	})
}

func (h *handlers) handleExplainMemory(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathUUID(w, r, "id")
	if !ok {
		return
	}
	lineage, err := h.svc.ExplainLineage(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, lineage)
}

func (h *handlers) handleCreateMemory(w http.ResponseWriter, r *http.Request) {
	wsID := r.PathValue("id")
	var body struct {
		Durability  string           `json:"durability"`
		Category    string           `json:"category"`
		TopicKey    string           `json:"topic_key"`
		SummaryStub string           `json:"summary_stub"`
		Body        string           `json:"body"`
		DependsOn   []graph.EdgeSpec `json:"depends_on"`
		AgentID     string           `json:"agent_id"`
		Reason      string           `json:"reason"`
	}
	if !decodeJSONBody(w, r, &body, false) {
		return
	}
	res, err := h.svc.ManualCreate(r.Context(), service.ManualCreateRequest{
		WorkspaceID: wsID,
		Durability:  body.Durability,
		Category:    body.Category,
		TopicKey:    body.TopicKey,
		SummaryStub: body.SummaryStub,
		Body:        body.Body,
		DependsOn:   body.DependsOn,
		AgentID:     body.AgentID,
		Reason:      body.Reason,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, res)
}

func (h *handlers) handleEditMemory(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathUUID(w, r, "id")
	if !ok {
		return
	}
	var body struct {
		ExpectedVersion int              `json:"expected_version"`
		Durability      *string          `json:"durability"`
		Category        string           `json:"category"`
		SummaryStub     *string          `json:"summary_stub"`
		Body            *string          `json:"body"`
		DependsOn       []graph.EdgeSpec `json:"depends_on"`
		AgentID         string           `json:"agent_id"`
		Reason          string           `json:"reason"`
	}
	if !decodeJSONBody(w, r, &body, false) {
		return
	}
	res, err := h.svc.ManualEdit(r.Context(), service.ManualEditRequest{
		MemoryID:        id,
		ExpectedVersion: body.ExpectedVersion,
		Durability:      body.Durability,
		Category:        body.Category,
		SummaryStub:     body.SummaryStub,
		Body:            body.Body,
		DependsOn:       body.DependsOn,
		AgentID:         body.AgentID,
		Reason:          body.Reason,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *handlers) handleRevertMemory(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathUUID(w, r, "id")
	if !ok {
		return
	}
	var body struct {
		ExpectedVersion int    `json:"expected_version"`
		TargetVersion   int    `json:"target_version"`
		Reason          string `json:"reason"`
	}
	if !decodeJSONBody(w, r, &body, false) {
		return
	}
	res, err := h.svc.RevertMemory(r.Context(), service.RevertMemoryRequest{
		MemoryID:        id,
		ExpectedVersion: body.ExpectedVersion,
		TargetVersion:   body.TargetVersion,
		Reason:          body.Reason,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *handlers) handleDeleteMemory(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathUUID(w, r, "id")
	if !ok {
		return
	}
	var body struct {
		ExpectedVersion int    `json:"expected_version"`
		Reason          string `json:"reason"`
	}
	if !decodeJSONBody(w, r, &body, true) {
		return
	}
	res, err := h.svc.SoftRetire(r.Context(), service.SoftRetireRequest{
		MemoryID:        id,
		ExpectedVersion: body.ExpectedVersion,
		Reason:          body.Reason,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *handlers) handlePurgeMemory(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathUUID(w, r, "id")
	if !ok {
		return
	}
	var body struct {
		Confirm bool   `json:"confirm"`
		Reason  string `json:"reason"`
	}
	if !decodeJSONBody(w, r, &body, false) {
		return
	}
	res, err := h.svc.HardPurge(r.Context(), service.HardPurgeRequest{
		MemoryID: id,
		Confirm:  body.Confirm,
		Reason:   body.Reason,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"purged":              true,
		"memory_id":           res.MemoryID,
		"workspace_id":        res.WorkspaceID,
		"durability":          res.Durability,
		"deleted_inbox_count": res.DeletedInboxCount,
		"revisions":           res.Revisions,
	})
}

func (h *handlers) handleConfirmToken(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkspaceID    string  `json:"workspace_id"`
		Action         string  `json:"action"`
		TargetMemoryID *string `json:"target_memory_id"`
	}
	if !decodeJSONBody(w, r, &body, false) {
		return
	}
	var targetPtr *uuid.UUID
	if body.TargetMemoryID != nil && strings.TrimSpace(*body.TargetMemoryID) != "" {
		uid, err := uuid.Parse(strings.TrimSpace(*body.TargetMemoryID))
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("invalid target_memory_id: %v", err))
			return
		}
		targetPtr = &uid
	}
	res, err := h.svc.IssueConfirmToken(r.Context(), service.IssueConfirmTokenRequest{
		WorkspaceID:    body.WorkspaceID,
		Action:         body.Action,
		TargetMemoryID: targetPtr,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *handlers) handleOpsStatus(w http.ResponseWriter, r *http.Request) {
	res, err := h.svc.OpsStatus(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	var inflight int64
	if h.inflight != nil {
		inflight = h.inflight.Load()
	}
	writeJSON(w, http.StatusOK, struct {
		Inflight int64 `json:"inflight"`
		service.OpsStatusResult
	}{
		Inflight:        inflight,
		OpsStatusResult: res,
	})
}

func (h *handlers) handleListDecisions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	params := storage.ListDecisionsParams{
		WorkspaceID: strings.TrimSpace(q.Get("workspace_id")),
		Action:      strings.TrimSpace(q.Get("action")),
		Cursor:      strings.TrimSpace(q.Get("cursor")),
	}
	if rawSince := strings.TrimSpace(q.Get("since")); rawSince != "" {
		t, err := time.Parse(time.RFC3339Nano, rawSince)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid since RFC3339 timestamp")
			return
		}
		params.Since = &t
	}
	if rawLimit := strings.TrimSpace(q.Get("limit")); rawLimit != "" {
		n, err := strconv.Atoi(rawLimit)
		if err != nil || n < 1 {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid limit")
			return
		}
		params.Limit = n
	}

	page, err := h.svc.ListCurationDecisions(r.Context(), params)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if page.Decisions == nil {
		page.Decisions = []storage.CurationDecision{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *handlers) handleDeadLetters(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
		n, err := strconv.Atoi(rawLimit)
		if err != nil || n < 1 {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid limit")
			return
		}
		limit = n
	}
	rep, err := h.svc.ListDeadLetters(r.Context(), limit)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func parsePathUUID(w http.ResponseWriter, r *http.Request, param string) (uuid.UUID, bool) {
	raw := strings.TrimSpace(r.PathValue(param))
	uid, err := uuid.Parse(raw)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("invalid UUID path parameter %q", raw))
		return uuid.Nil, false
	}
	return uid, true
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any, allowEmpty bool) bool {
	if r.Body == nil {
		if allowEmpty {
			return true
		}
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "request body is required")
		return false
	}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "payload_too_large", fmt.Sprintf("request body exceeds %d bytes", maxErr.Limit))
			return false
		}
		if allowEmpty && errors.Is(err, io.EOF) {
			return true
		}
		writeJSONError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("invalid JSON body: %v", err))
		return false
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "payload_too_large", fmt.Sprintf("request body exceeds %d bytes", maxErr.Limit))
			return false
		}
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body: trailing data")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrPayloadTooLarge):
		writeJSONError(w, http.StatusRequestEntityTooLarge, "payload_too_large", err.Error())
	case errors.Is(err, storage.ErrIdempotencyMismatch):
		writeJSONError(w, http.StatusUnprocessableEntity, "unprocessable_entity", err.Error())
	case errors.Is(err, storage.ErrInvalidToken):
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", err.Error())
	case errors.Is(err, service.ErrForbiddenAction):
		writeJSONError(w, http.StatusForbidden, "forbidden", err.Error())
	case errors.Is(err, storage.ErrNotFound):
		writeJSONError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, storage.ErrConflict):
		writeJSONError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, storage.ErrInvalidWorkspaceID), errors.Is(err, storage.ErrInvalidInput):
		writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
	default:
		writeJSONError(w, http.StatusServiceUnavailable, "service_unavailable", err.Error())
	}
}

func etagMatches(header, current string) bool {
	if strings.TrimSpace(header) == "" || current == "" {
		return false
	}
	for _, part := range strings.Split(header, ",") {
		tag := strings.TrimSpace(part)
		if tag == "*" || tag == current {
			return true
		}
	}
	return false
}
