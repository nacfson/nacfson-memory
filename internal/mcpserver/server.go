package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/retrieval"
	"github.com/nacfson/nacfson-memory/internal/security"
	"github.com/nacfson/nacfson-memory/internal/service"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

const (
	// MaxMCPBodyBytes bounds raw JSON-RPC request bodies on /mcp (80 KiB).
	MaxMCPBodyBytes int64 = 80 * 1024

	codeInvalidParams  int64 = -32602
	codePayloadTooBig  int64 = -32013
	codeUnprocessable  int64 = -32022
	codeServiceUnavail int64 = -32053
)

type ctxKey int

const (
	ctxKeyInflightSaturated ctxKey = iota + 1
	ctxKeyDegradedSignal
)

// WithInflightSaturated marks an HTTP /mcp request context as arriving while MEM1_MAX_INFLIGHT is saturated.
func WithInflightSaturated(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyInflightSaturated, true)
}

// IsInflightSaturated reports whether the request arrived during inflight saturation.
func IsInflightSaturated(ctx context.Context) bool {
	v, _ := ctx.Value(ctxKeyInflightSaturated).(bool)
	return v
}

// WithDegradedSignal attaches a shared atomic flag used to emit X-Memory-Degraded HTTP headers.
func WithDegradedSignal(ctx context.Context, flag *atomic.Bool) context.Context {
	return context.WithValue(ctx, ctxKeyDegradedSignal, flag)
}

// MarkDegraded sets the degraded header signal on ctx if attached.
func MarkDegraded(ctx context.Context) {
	if flag, ok := ctx.Value(ctxKeyDegradedSignal).(*atomic.Bool); ok && flag != nil {
		flag.Store(true)
	}
}

// Service captures the MemoryService operations exposed over MCP.
type Service interface {
	IngestWrite(ctx context.Context, req service.IngestWriteRequest) (service.IngestWriteResult, error)
	RecallContext(ctx context.Context, req retrieval.RecallRequest) (retrieval.RecallResponse, error)
	ExpandMemories(ctx context.Context, req retrieval.ExpandRequest) (retrieval.ExpandResponse, error)
	GetConstitution(ctx context.Context, workspaceID string) (service.ConstitutionResult, error)
	GetMemory(ctx context.Context, memoryID uuid.UUID) (service.MemoryDetailResult, error)
	ManualCreate(ctx context.Context, req service.ManualCreateRequest) (service.ManualMutationResult, error)
	ManualEdit(ctx context.Context, req service.ManualEditRequest) (service.ManualMutationResult, error)
	RevertMemory(ctx context.Context, req service.RevertMemoryRequest) (service.ManualMutationResult, error)
	SoftRetire(ctx context.Context, req service.SoftRetireRequest) (service.ManualMutationResult, error)
	HardPurge(ctx context.Context, req service.HardPurgeRequest) (storage.HardPurgeResult, error)
	ConsumeConfirmToken(ctx context.Context, req service.ConsumeConfirmTokenRequest) error
}

// WriteInput defines arguments for the memory_write MCP tool.
type WriteInput struct {
	WorkspaceID    string `json:"workspace_id" jsonschema:"Target workspace ID (e.g. 'general' for personal preferences, or project slug)"`
	AgentID        string `json:"agent_id,omitempty" jsonschema:"Optional caller agent identifier"`
	IdempotencyKey string `json:"idempotency_key,omitempty" jsonschema:"Optional idempotency key for safe retries"`
	OccurredAt     string `json:"occurred_at,omitempty" jsonschema:"Optional RFC3339 timestamp when the turn occurred"`
	Content        string `json:"content" jsonschema:"Concise note or observation to store (<= 64 KiB; split larger context by topic)"`
}

// WriteOutput defines the structured response of memory_write.
type WriteOutput struct {
	Accepted          bool                 `json:"accepted"`
	InboxID           uuid.UUID            `json:"inbox_id"`
	RequestID         uuid.UUID            `json:"request_id"`
	WorkspaceRevision int64                `json:"workspace_revision"`
	IdempotencyKey    string               `json:"idempotency_key,omitempty"`
	Replayed          bool                 `json:"replayed,omitempty"`
	Redactions        []security.Redaction `json:"redactions,omitempty"`
}

// RecallInput defines arguments for the memory_recall MCP tool.
type RecallInput struct {
	WorkspaceID string `json:"workspace_id" jsonschema:"Target project workspace ID"`
	Query       string `json:"query" jsonschema:"Search query or current task context"`
	MaxTokens   int    `json:"max_tokens,omitempty" jsonschema:"Maximum response token budget (1..2000, default 2000)"`
}

// ExpandInput defines arguments for the memory_expand MCP tool.
type ExpandInput struct {
	WorkspaceID string   `json:"workspace_id" jsonschema:"Workspace ID requesting expansion"`
	MemoryIDs   []string `json:"memory_ids" jsonschema:"List of memory UUIDs to expand"`
	MaxTokens   int      `json:"max_tokens,omitempty" jsonschema:"Maximum response token budget (1..2000, default 2000)"`
}

// ManageEdgeInput defines an intra-workspace DEPENDS_ON edge in memory_manage inputs.
type ManageEdgeInput struct {
	TargetMemoryID string `json:"target_memory_id" jsonschema:"Target active memory UUID"`
	ExpectedToRev  int    `json:"expected_to_rev" jsonschema:"Pinned target version"`
	Rationale      string `json:"rationale" jsonschema:"Reason for the dependency"`
}

// ManageInput defines arguments for the memory_manage MCP tool.
type ManageInput struct {
	WorkspaceID       string            `json:"workspace_id" jsonschema:"Target workspace ID"`
	Action            string            `json:"action" jsonschema:"Action: create, edit, promote, retire, revert, or purge"`
	TargetMemoryID    string            `json:"target_memory_id,omitempty" jsonschema:"Target memory UUID (required for edit, promote, retire, revert, purge)"`
	Durability        string            `json:"durability,omitempty" jsonschema:"Durability layer (constitution required for create/edit/promote)"`
	Category          string            `json:"category,omitempty" jsonschema:"Optional category"`
	TopicKey          string            `json:"topic_key,omitempty" jsonschema:"Slug topic_key for create"`
	SummaryStub       *string           `json:"summary_stub,omitempty" jsonschema:"Optional 1-line summary (<= 180 chars)"`
	Body              *string           `json:"body,omitempty" jsonschema:"Memory body text"`
	DependsOn         []ManageEdgeInput `json:"depends_on,omitempty" jsonschema:"Optional intra-workspace DEPENDS_ON links"`
	Reason            string            `json:"reason,omitempty" jsonschema:"Audit reason for mutation"`
	ExpectedVersion   int               `json:"expected_version,omitempty" jsonschema:"Current version for optimistic CAS checks"`
	TargetVersion     int               `json:"target_version,omitempty" jsonschema:"Prior revision version to restore on revert"`
	ConfirmationToken string            `json:"confirmation_token,omitempty" jsonschema:"Single-use confirmation token required for privileged mutations"`
}

// ManageOutput defines the structured response of memory_manage.
type ManageOutput struct {
	MemoryID            uuid.UUID `json:"memory_id"`
	WorkspaceID         string    `json:"workspace_id,omitempty"`
	Version             int       `json:"version"`
	RestoredFromVersion *int      `json:"restored_from_version,omitempty"`
	Status              string    `json:"status"`
	ManifestETag        string    `json:"manifest_etag,omitempty"`
	Purged              bool      `json:"purged,omitempty"`
	DeletedInboxCount   int64     `json:"deleted_inbox_count,omitempty"`
}

// NewServer constructs an MCP Server exposing all 4 mem1 tools and the constitution resource template.
func NewServer(svc Service, logger *slog.Logger) *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{
		Name:    "mem1",
		Version: "1.0.0",
	}, &mcp.ServerOptions{
		Logger: logger,
		Capabilities: &mcp.ServerCapabilities{
			Tools:     &mcp.ToolCapabilities{ListChanged: false},
			Resources: &mcp.ResourceCapabilities{ListChanged: false, Subscribe: false},
		},
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "memory_write",
		Description: "Append a concise agent observation or decision (<= 64 KiB; split larger context by topic) into the non-blocking mem1 raw inbox for asynchronous curation.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in WriteInput) (*mcp.CallToolResult, any, error) {
		if IsInflightSaturated(ctx) {
			return nil, nil, rpcErr(codeServiceUnavail, "server saturated; retry memory_write")
		}
		var occurredAt time.Time
		if raw := strings.TrimSpace(in.OccurredAt); raw != "" {
			parsed, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				return nil, nil, rpcErr(codeInvalidParams, fmt.Sprintf("invalid occurred_at timestamp: %v", err))
			}
			occurredAt = parsed
		}

		res, err := svc.IngestWrite(ctx, service.IngestWriteRequest{
			WorkspaceID:    in.WorkspaceID,
			AgentID:        in.AgentID,
			IdempotencyKey: in.IdempotencyKey,
			Content:        in.Content,
			OccurredAt:     occurredAt,
		})
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		return nil, WriteOutput{
			Accepted:          res.Accepted,
			InboxID:           res.InboxID,
			RequestID:         res.RequestID,
			WorkspaceRevision: res.WorkspaceRevision,
			IdempotencyKey:    strings.TrimSpace(in.IdempotencyKey),
			Replayed:          res.Replayed,
			Redactions:        res.Redactions,
		}, nil
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "memory_recall",
		Description: "Fail-open read-only memory recall for a workspace within <= 2,000 tokens, returning constitution rules, ranked memories, 1-line overflow stubs, and premise drift flags.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in RecallInput) (*mcp.CallToolResult, any, error) {
		if err := storage.ValidateWorkspaceID(in.WorkspaceID); err != nil {
			return nil, nil, rpcErr(codeInvalidParams, err.Error())
		}
		if IsInflightSaturated(ctx) {
			MarkDegraded(ctx)
			return nil, degradedRecallResponse(), nil
		}

		resp, err := svc.RecallContext(ctx, retrieval.RecallRequest{
			WorkspaceID: in.WorkspaceID,
			Query:       in.Query,
			MaxTokens:   in.MaxTokens,
		})
		if err != nil {
			MarkDegraded(ctx)
			return nil, degradedRecallResponse(), nil
		}
		if resp.Memories == nil {
			resp.Memories = []retrieval.RecalledMemory{}
		}
		if resp.Stubs == nil {
			resp.Stubs = []retrieval.MemoryStub{}
		}
		if resp.Degraded {
			MarkDegraded(ctx)
		}
		return nil, resp, nil
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "memory_expand",
		Description: "Expand full memory bodies and dependency details for specific memory IDs visible to a workspace within <= 2,000 tokens.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ExpandInput) (*mcp.CallToolResult, any, error) {
		if IsInflightSaturated(ctx) {
			return nil, nil, rpcErr(codeServiceUnavail, "server saturated; retry memory_expand")
		}
		ids := make([]uuid.UUID, 0, len(in.MemoryIDs))
		for _, raw := range in.MemoryIDs {
			uid, err := uuid.Parse(strings.TrimSpace(raw))
			if err != nil {
				return nil, nil, rpcErr(codeInvalidParams, fmt.Sprintf("invalid memory_id %q: %v", raw, err))
			}
			ids = append(ids, uid)
		}

		resp, err := svc.ExpandMemories(ctx, retrieval.ExpandRequest{
			WorkspaceID: in.WorkspaceID,
			MemoryIDs:   ids,
			MaxTokens:   in.MaxTokens,
		})
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		if resp.Memories == nil {
			resp.Memories = []retrieval.RecalledMemory{}
		}
		if resp.Edges == nil {
			resp.Edges = []retrieval.MemoryDependencyView{}
		}
		return nil, resp, nil
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "memory_manage",
		Description: "Manage memory lifecycle: retire working/durable memories directly, or perform privileged user_explicit actions (constitution create/edit/promote, revert, purge) with a single-use confirmation_token.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ManageInput) (*mcp.CallToolResult, any, error) {
		if IsInflightSaturated(ctx) {
			return nil, nil, rpcErr(codeServiceUnavail, "server saturated; retry memory_manage")
		}
		return handleManage(ctx, svc, in)
	})

	srv.AddResourceTemplate(&mcp.ResourceTemplate{
		Name:        "workspace-constitution",
		Title:       "Workspace Constitution",
		Description: "Merged active constitution rules for a workspace with weak ETag metadata",
		MIMEType:    "application/json",
		URITemplate: "mem1://workspaces/{id}/constitution",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		uri := req.Params.URI
		wsID, ok := parseConstitutionURI(uri)
		if !ok {
			return nil, mcp.ResourceNotFoundError(uri)
		}
		res, err := svc.GetConstitution(ctx, wsID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, mcp.ResourceNotFoundError(uri)
			}
			return nil, mapServiceErr(err)
		}
		raw, err := json.Marshal(res)
		if err != nil {
			return nil, rpcErr(codeServiceUnavail, fmt.Sprintf("encode constitution JSON: %v", err))
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      uri,
					MIMEType: "application/json",
					Text:     string(raw),
				},
			},
		}, nil
	})

	return srv
}

// NewStreamableHTTPHandler returns the stateless Streamable HTTP handler mounted on /mcp.
func NewStreamableHTTPHandler(svc Service, logger *slog.Logger) http.Handler {
	srv := NewServer(svc, logger)
	return mcp.NewStreamableHTTPHandler(func(_ *http.Request) *mcp.Server {
		return srv
	}, &mcp.StreamableHTTPOptions{
		Stateless:                  true,
		JSONResponse:               true,
		Logger:                     logger,
		DisableLocalhostProtection: true,
		MaxRequestBodyBytes:        MaxMCPBodyBytes,
	})
}

// RunStdio serves MCP over stdin/stdout until ctx is canceled or EOF is reached.
func RunStdio(ctx context.Context, svc Service, logger *slog.Logger) error {
	srv := NewServer(svc, logger)
	return srv.Run(ctx, &mcp.StdioTransport{})
}

func handleManage(ctx context.Context, svc Service, in ManageInput) (*mcp.CallToolResult, any, error) {
	if err := storage.ValidateWorkspaceID(in.WorkspaceID); err != nil {
		return nil, nil, rpcErr(codeInvalidParams, err.Error())
	}

	action := strings.ToLower(strings.TrimSpace(in.Action))
	switch action {
	case "retire":
		targetID, err := parseRequiredTarget(in.TargetMemoryID)
		if err != nil {
			return nil, nil, err
		}
		detail, err := svc.GetMemory(ctx, targetID)
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		if detail.Memory.WorkspaceID != in.WorkspaceID {
			return nil, nil, rpcErr(codeUnprocessable, fmt.Sprintf("memory %s does not belong to workspace %q", targetID, in.WorkspaceID))
		}
		if detail.Memory.Durability == storage.DurabilityConstitution {
			return nil, nil, rpcErr(codeInvalidParams, "invalid_action: cannot directly retire constitution memory via agent_observed; use privileged purge or manual edit")
		}
		res, err := svc.SoftRetire(ctx, service.SoftRetireRequest{
			MemoryID:        targetID,
			WorkspaceID:     in.WorkspaceID,
			ExpectedVersion: in.ExpectedVersion,
			Reason:          in.Reason,
		})
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		return nil, mutationToManageOutput(res), nil

	case "create":
		if strings.TrimSpace(in.Durability) != storage.DurabilityConstitution {
			return nil, nil, rpcErr(codeInvalidParams, "invalid_action: use memory_write for working/durable memories")
		}
		if in.Body == nil || strings.TrimSpace(*in.Body) == "" || strings.TrimSpace(in.TopicKey) == "" {
			return nil, nil, rpcErr(codeInvalidParams, "topic_key and non-empty body are required for constitution create")
		}
		deps, err := parseManageEdges(in.DependsOn)
		if err != nil {
			return nil, nil, err
		}
		if err := consumeToken(ctx, svc, in.ConfirmationToken, in.WorkspaceID, "create", nil); err != nil {
			return nil, nil, err
		}
		stub := ""
		if in.SummaryStub != nil {
			stub = *in.SummaryStub
		}
		res, err := svc.ManualCreate(ctx, service.ManualCreateRequest{
			WorkspaceID: in.WorkspaceID,
			Durability:  storage.DurabilityConstitution,
			Category:    in.Category,
			TopicKey:    in.TopicKey,
			SummaryStub: stub,
			Body:        *in.Body,
			DependsOn:   deps,
			Reason:      in.Reason,
		})
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		return nil, mutationToManageOutput(res), nil

	case "edit":
		targetID, err := parseRequiredTarget(in.TargetMemoryID)
		if err != nil {
			return nil, nil, err
		}
		if in.ExpectedVersion < 1 {
			return nil, nil, rpcErr(codeInvalidParams, "expected_version >= 1 is required for edit")
		}
		deps, err := parseManageEdges(in.DependsOn)
		if err != nil {
			return nil, nil, err
		}
		detail, err := svc.GetMemory(ctx, targetID)
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		if detail.Memory.WorkspaceID != in.WorkspaceID {
			return nil, nil, rpcErr(codeUnprocessable, fmt.Sprintf("memory %s does not belong to workspace %q", targetID, in.WorkspaceID))
		}
		if detail.Memory.Durability != storage.DurabilityConstitution {
			return nil, nil, rpcErr(codeInvalidParams, "invalid_action: use memory_write for working/durable memories")
		}
		if d := strings.TrimSpace(in.Durability); d != "" && d != storage.DurabilityConstitution {
			return nil, nil, rpcErr(codeInvalidParams, "invalid_action: edit via memory_manage only supports constitution durability")
		}
		if err := consumeToken(ctx, svc, in.ConfirmationToken, in.WorkspaceID, "edit", &targetID); err != nil {
			return nil, nil, err
		}
		constDur := storage.DurabilityConstitution
		res, err := svc.ManualEdit(ctx, service.ManualEditRequest{
			MemoryID:        targetID,
			ExpectedVersion: in.ExpectedVersion,
			Durability:      &constDur,
			Category:        in.Category,
			SummaryStub:     in.SummaryStub,
			Body:            in.Body,
			DependsOn:       deps,
			Reason:          in.Reason,
		})
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		return nil, mutationToManageOutput(res), nil

	case "promote":
		targetID, err := parseRequiredTarget(in.TargetMemoryID)
		if err != nil {
			return nil, nil, err
		}
		if in.ExpectedVersion < 1 {
			return nil, nil, rpcErr(codeInvalidParams, "expected_version >= 1 is required for promote")
		}
		if d := strings.TrimSpace(in.Durability); d != "" && d != storage.DurabilityConstitution {
			return nil, nil, rpcErr(codeInvalidParams, "invalid_action: promote target durability must be constitution")
		}
		deps, err := parseManageEdges(in.DependsOn)
		if err != nil {
			return nil, nil, err
		}
		detail, err := svc.GetMemory(ctx, targetID)
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		if detail.Memory.WorkspaceID != in.WorkspaceID {
			return nil, nil, rpcErr(codeUnprocessable, fmt.Sprintf("memory %s does not belong to workspace %q", targetID, in.WorkspaceID))
		}
		if err := consumeToken(ctx, svc, in.ConfirmationToken, in.WorkspaceID, "promote", &targetID); err != nil {
			return nil, nil, err
		}
		constDur := storage.DurabilityConstitution
		res, err := svc.ManualEdit(ctx, service.ManualEditRequest{
			MemoryID:        targetID,
			ExpectedVersion: in.ExpectedVersion,
			Durability:      &constDur,
			Category:        in.Category,
			SummaryStub:     in.SummaryStub,
			Body:            in.Body,
			DependsOn:       deps,
			Reason:          in.Reason,
		})
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		return nil, mutationToManageOutput(res), nil

	case "revert":
		targetID, err := parseRequiredTarget(in.TargetMemoryID)
		if err != nil {
			return nil, nil, err
		}
		if in.ExpectedVersion < 1 || in.TargetVersion < 1 {
			return nil, nil, rpcErr(codeInvalidParams, "expected_version >= 1 and target_version >= 1 are required for revert")
		}
		detail, err := svc.GetMemory(ctx, targetID)
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		if detail.Memory.WorkspaceID != in.WorkspaceID {
			return nil, nil, rpcErr(codeUnprocessable, fmt.Sprintf("memory %s does not belong to workspace %q", targetID, in.WorkspaceID))
		}
		if err := consumeToken(ctx, svc, in.ConfirmationToken, in.WorkspaceID, "revert", &targetID); err != nil {
			return nil, nil, err
		}
		res, err := svc.RevertMemory(ctx, service.RevertMemoryRequest{
			MemoryID:        targetID,
			ExpectedVersion: in.ExpectedVersion,
			TargetVersion:   in.TargetVersion,
			Reason:          in.Reason,
		})
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		return nil, mutationToManageOutput(res), nil

	case "purge":
		targetID, err := parseRequiredTarget(in.TargetMemoryID)
		if err != nil {
			return nil, nil, err
		}
		detail, err := svc.GetMemory(ctx, targetID)
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		if detail.Memory.WorkspaceID != in.WorkspaceID {
			return nil, nil, rpcErr(codeUnprocessable, fmt.Sprintf("memory %s does not belong to workspace %q", targetID, in.WorkspaceID))
		}
		if err := consumeToken(ctx, svc, in.ConfirmationToken, in.WorkspaceID, "purge", &targetID); err != nil {
			return nil, nil, err
		}
		res, err := svc.HardPurge(ctx, service.HardPurgeRequest{
			MemoryID: targetID,
			Confirm:  true,
			Reason:   in.Reason,
		})
		if err != nil {
			return nil, nil, mapServiceErr(err)
		}
		return nil, ManageOutput{
			MemoryID:          res.MemoryID,
			WorkspaceID:       res.WorkspaceID,
			Status:            "purged",
			ManifestETag:      storage.ComputeMetadataETag(res.Revisions.ManifestRevision, 0, storage.InheritNone),
			Purged:            true,
			DeletedInboxCount: res.DeletedInboxCount,
		}, nil

	default:
		return nil, nil, rpcErr(codeInvalidParams, fmt.Sprintf("invalid_action: unsupported memory_manage action %q", in.Action))
	}
}

func parseManageEdges(in []ManageEdgeInput) ([]graph.EdgeSpec, error) {
	if in == nil {
		return nil, nil
	}
	out := make([]graph.EdgeSpec, len(in))
	for i, e := range in {
		uid, err := uuid.Parse(strings.TrimSpace(e.TargetMemoryID))
		if err != nil {
			return nil, rpcErr(codeInvalidParams, fmt.Sprintf("invalid depends_on[%d].target_memory_id %q: %v", i, e.TargetMemoryID, err))
		}
		out[i] = graph.EdgeSpec{
			TargetMemoryID: uid,
			ExpectedToRev:  e.ExpectedToRev,
			Rationale:      e.Rationale,
		}
	}
	return out, nil
}

func consumeToken(ctx context.Context, svc Service, rawToken, wsID, action string, targetID *uuid.UUID) error {
	if strings.TrimSpace(rawToken) == "" {
		return rpcErr(codeInvalidParams, "confirmation_token is required for privileged memory_manage action")
	}
	if err := svc.ConsumeConfirmToken(ctx, service.ConsumeConfirmTokenRequest{
		Token:          rawToken,
		WorkspaceID:    wsID,
		Action:         action,
		TargetMemoryID: targetID,
	}); err != nil {
		return mapServiceErr(err)
	}
	return nil
}

func parseRequiredTarget(raw string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return uuid.Nil, rpcErr(codeInvalidParams, "target_memory_id is required")
	}
	uid, err := uuid.Parse(trimmed)
	if err != nil {
		return uuid.Nil, rpcErr(codeInvalidParams, fmt.Sprintf("invalid target_memory_id %q: %v", raw, err))
	}
	return uid, nil
}

func mutationToManageOutput(res service.ManualMutationResult) ManageOutput {
	return ManageOutput{
		MemoryID:            res.MemoryID,
		WorkspaceID:         res.WorkspaceID,
		Version:             res.Version,
		RestoredFromVersion: res.RestoredFromVersion,
		Status:              res.Status,
		ManifestETag:        res.ManifestETag,
	}
}

func degradedRecallResponse() retrieval.RecallResponse {
	return retrieval.RecallResponse{
		Memories:          []retrieval.RecalledMemory{},
		Stubs:             []retrieval.MemoryStub{},
		ManifestETag:      "",
		WorkspaceRevision: 0,
		Degraded:          true,
	}
}

func parseConstitutionURI(uri string) (string, bool) {
	const prefix = "mem1://workspaces/"
	const suffix = "/constitution"
	if !strings.HasPrefix(uri, prefix) || !strings.HasSuffix(uri, suffix) {
		return "", false
	}
	wsID := strings.TrimSuffix(strings.TrimPrefix(uri, prefix), suffix)
	if wsID == "" || strings.Contains(wsID, "/") {
		return "", false
	}
	return wsID, true
}

func rpcErr(code int64, msg string) *jsonrpc.Error {
	return &jsonrpc.Error{
		Code:    code,
		Message: msg,
	}
}

func mapServiceErr(err error) *jsonrpc.Error {
	switch {
	case errors.Is(err, service.ErrPayloadTooLarge):
		return rpcErr(codePayloadTooBig, err.Error())
	case errors.Is(err, storage.ErrIdempotencyMismatch),
		errors.Is(err, storage.ErrConflict),
		errors.Is(err, storage.ErrNotFound),
		errors.Is(err, storage.ErrInvalidToken):
		return rpcErr(codeUnprocessable, err.Error())
	case errors.Is(err, storage.ErrInvalidWorkspaceID),
		errors.Is(err, storage.ErrInvalidInput),
		errors.Is(err, service.ErrForbiddenAction):
		return rpcErr(codeInvalidParams, err.Error())
	default:
		return rpcErr(codeServiceUnavail, err.Error())
	}
}
