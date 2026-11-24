package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/retrieval"
	"github.com/nacfson/nacfson-memory/internal/security"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

var (
	// ErrPayloadTooLarge indicates write content exceeds MEM1_MAX_WRITE_BYTES.
	ErrPayloadTooLarge = errors.New("payload exceeds MEM1_MAX_WRITE_BYTES")
	// ErrForbiddenAction indicates a policy-prohibited workspace or durability operation.
	ErrForbiddenAction = errors.New("forbidden action")
)

// HealthCheckResult reports PostgreSQL reachability for /healthz.
type HealthCheckResult struct {
	Status string `json:"status"`
	DBOk   bool   `json:"db_ok"`
}

// IngestWriteRequest carries an agent_observed write into raw_inbox.
type IngestWriteRequest struct {
	WorkspaceID    string
	AgentID        string
	IdempotencyKey string
	Content        string
	OccurredAt     time.Time
}

// IngestWriteResult summarizes raw_inbox acceptance and inline redaction counts.
type IngestWriteResult struct {
	Accepted          bool                 `json:"accepted"`
	InboxID           uuid.UUID            `json:"inbox_id"`
	RequestID         uuid.UUID            `json:"request_id"`
	WorkspaceRevision int64                `json:"workspace_revision"`
	Replayed          bool                 `json:"replayed"`
	Redactions        []security.Redaction `json:"redactions,omitempty"`
}

// ConstitutionResult carries merged active constitution rules with weak ETag metadata.
type ConstitutionResult struct {
	Memories             []storage.Memory `json:"memories"`
	ConstitutionRevision int64            `json:"constitution_revision"`
	ManifestETag         string           `json:"manifest_etag"`
}

// CreateWorkspaceRequest configures explicit project workspace creation.
type CreateWorkspaceRequest struct {
	WorkspaceID    string `json:"workspace_id"`
	InheritGeneral string `json:"inherit_general"`
}

// MemoryDetailResult exposes a memory head plus active incoming and outgoing edges.
type MemoryDetailResult struct {
	Memory        storage.Memory       `json:"memory"`
	OutgoingEdges []graph.OutgoingEdge `json:"outgoing_edges"`
	IncomingEdges []graph.IncomingEdge `json:"incoming_edges"`
}

// ManualCreateRequest carries a user_explicit memory creation.
type ManualCreateRequest struct {
	WorkspaceID  string                      `json:"workspace_id"`
	Durability   string                      `json:"durability"`
	Category     string                      `json:"category,omitempty"`
	TopicKey     string                      `json:"topic_key"`
	SummaryStub  string                      `json:"summary_stub,omitempty"`
	Body         string                      `json:"body"`
	DependsOn    []graph.EdgeSpec            `json:"depends_on,omitempty"`
	AgentID      string                      `json:"agent_id,omitempty"`
	Reason       string                      `json:"reason,omitempty"`
	ConfirmToken *ConsumeConfirmTokenRequest `json:"-"`
}

// ManualEditRequest configures a CAS-guarded manual partial update.
type ManualEditRequest struct {
	WorkspaceID              string                      `json:"workspace_id,omitempty"`
	MemoryID                 uuid.UUID                   `json:"memory_id"`
	ExpectedVersion          int                         `json:"expected_version"`
	RequireCurrentDurability string                      `json:"-"`
	Durability               *string                     `json:"durability,omitempty"`
	Category                 string                      `json:"category,omitempty"`
	SummaryStub              *string                     `json:"summary_stub,omitempty"`
	Body                     *string                     `json:"body,omitempty"`
	DependsOn                []graph.EdgeSpec            `json:"depends_on,omitempty"`
	AgentID                  string                      `json:"agent_id,omitempty"`
	Reason                   string                      `json:"reason,omitempty"`
	ConfirmToken             *ConsumeConfirmTokenRequest `json:"-"`
}

// RevertMemoryRequest configures a monotonic cherry-pick rollback to TargetVersion.
type RevertMemoryRequest struct {
	WorkspaceID     string                      `json:"workspace_id,omitempty"`
	MemoryID        uuid.UUID                   `json:"memory_id"`
	ExpectedVersion int                         `json:"expected_version"`
	TargetVersion   int                         `json:"target_version"`
	Reason          string                      `json:"reason,omitempty"`
	ConfirmToken    *ConsumeConfirmTokenRequest `json:"-"`
}

// SoftRetireRequest explicitly retires an active memory and its outgoing edges.
type SoftRetireRequest struct {
	MemoryID           uuid.UUID `json:"memory_id"`
	WorkspaceID        string    `json:"workspace_id,omitempty"`
	ExpectedVersion    int       `json:"expected_version,omitempty"`
	ForbidConstitution bool      `json:"-"`
	Reason             string    `json:"reason,omitempty"`
}

// HardPurgeRequest explicitly deletes a memory and redacts/removes exclusively cited sources.
type HardPurgeRequest struct {
	WorkspaceID  string                      `json:"workspace_id,omitempty"`
	MemoryID     uuid.UUID                   `json:"memory_id"`
	Confirm      bool                        `json:"confirm"`
	Reason       string                      `json:"reason,omitempty"`
	ConfirmToken *ConsumeConfirmTokenRequest `json:"-"`
}

// ManualMutationResult summarizes a committed manifest transition.
type ManualMutationResult struct {
	MemoryID             uuid.UUID `json:"memory_id"`
	WorkspaceID          string    `json:"workspace_id"`
	Version              int       `json:"version"`
	RestoredFromVersion  *int      `json:"restored_from_version,omitempty"`
	Status               string    `json:"status"`
	WorkspaceRevision    int64     `json:"workspace_revision"`
	ConstitutionRevision int64     `json:"constitution_revision"`
	ManifestETag         string    `json:"manifest_etag"`
}

// OpsStatusResult snapshots pool, breaker, inbox, and River operational counters.
type OpsStatusResult struct {
	ServingPool     storage.PoolStatSnapshot `json:"serving_pool"`
	CurationPool    storage.PoolStatSnapshot `json:"curation_pool"`
	CuratorBreaker  curation.BreakerStatus   `json:"curator_breaker"`
	EmbedderBreaker curation.BreakerStatus   `json:"embedder_breaker"`
	PendingInbox    int64                    `json:"pending_inbox"`
	Provisional     int64                    `json:"provisional_inbox"`
	ProvisionalAct  int64                    `json:"provisional_active"`
	DeadLetterInbox int64                    `json:"dead_letter_inbox"`
	RiverQueues     []storage.RiverQueueStat `json:"river_queues"`
}

// IssueConfirmTokenRequest mints a single-use privileged action confirmation token.
type IssueConfirmTokenRequest struct {
	WorkspaceID    string        `json:"workspace_id"`
	Action         string        `json:"action"`
	TargetMemoryID *uuid.UUID    `json:"target_memory_id,omitempty"`
	TTL            time.Duration `json:"ttl,omitempty"`
}

// ConfirmTokenResult returns a newly minted raw token and expiration timestamp.
type ConfirmTokenResult struct {
	ConfirmationToken string    `json:"confirmation_token"`
	ExpiresAt         time.Time `json:"expires_at"`
}

// ConsumeConfirmTokenRequest consumes a single-use privileged confirmation token.
type ConsumeConfirmTokenRequest struct {
	Token          string
	WorkspaceID    string
	Action         string
	TargetMemoryID *uuid.UUID
}

// ReindexResult summarizes an offline batch re-embedding pass.
type ReindexResult struct {
	Processed int    `json:"processed"`
	Updated   int    `json:"updated"`
	SpaceID   string `json:"space_id"`
}

type backend interface {
	Ping(ctx context.Context) error
	InsertInboxAndEnqueue(ctx context.Context, params storage.InsertInboxParams) (storage.InsertInboxResult, error)
	RecallContext(ctx context.Context, req retrieval.RecallRequest) (retrieval.RecallResponse, error)
	ExpandMemories(ctx context.Context, req retrieval.ExpandRequest) (retrieval.ExpandResponse, error)
	GetConstitutionBundle(ctx context.Context, workspaceID string) (storage.Workspace, int64, []storage.Memory, error)
	ListWorkspaces(ctx context.Context) ([]storage.WorkspaceSummary, error)
	CreateWorkspace(ctx context.Context, workspaceID, inheritGeneral string) (storage.Workspace, error)
	UpdateInheritGeneral(ctx context.Context, workspaceID, inheritGeneral string) (storage.Workspace, error)
	ListAllMemoriesKeyset(ctx context.Context, params storage.ListMemoriesParams) (storage.ListMemoriesPage, error)
	GetMemoryDetail(ctx context.Context, memoryID uuid.UUID) (MemoryDetailResult, error)
	GetRevisions(ctx context.Context, memoryID uuid.UUID) ([]storage.MemoryRevision, error)
	ExplainLineage(ctx context.Context, memoryID uuid.UUID) (storage.MemoryLineage, error)
	GetWorkspace(ctx context.Context, workspaceID string) (storage.Workspace, error)
	ManualCreateTx(ctx context.Context, params storage.CreateMemoryParams, dependsOn []graph.EdgeSpec, needsEmbedJob bool, confirmTok *ConsumeConfirmTokenRequest) (storage.Memory, storage.WorkspaceRevisions, int64, error)
	ManualEditTx(ctx context.Context, oldDur string, params storage.EditMemoryParams, dependsOn []graph.EdgeSpec, retentionN int, needsEmbedJob bool, confirmTok *ConsumeConfirmTokenRequest) (storage.Memory, storage.WorkspaceRevisions, int64, error)
	ManualRevertTx(ctx context.Context, oldDur string, params storage.RevertMemoryParams, retentionN int, needsEmbedJob bool, confirmTok *ConsumeConfirmTokenRequest) (storage.Memory, storage.WorkspaceRevisions, int64, error)
	SoftRetireTx(ctx context.Context, params storage.SoftRetireParams, reason string) (storage.Memory, storage.WorkspaceRevisions, int64, error)
	HardPurgeTx(ctx context.Context, memoryID uuid.UUID, workspaceID, reason string, confirmTok *ConsumeConfirmTokenRequest) (storage.HardPurgeResult, error)
	OpsSnapshot(ctx context.Context) (storage.AllPoolStats, storage.InboxStatusCounts, []storage.RiverQueueStat, error)
	ListCurationDecisions(ctx context.Context, params storage.ListDecisionsParams) (storage.ListDecisionsPage, error)
	ListDeadLetters(ctx context.Context, limit int) (storage.DeadLettersReport, error)
	IssueConfirmToken(ctx context.Context, workspaceID, action string, targetMemoryID *uuid.UUID, ttl time.Duration) (string, time.Time, error)
	ConsumeConfirmToken(ctx context.Context, token, workspaceID, action string, targetMemoryID *uuid.UUID) error
	ListMemoriesForReindexKeyset(ctx context.Context, afterID *uuid.UUID, batchSize int) ([]storage.Memory, error)
	UpdateMemoryEmbedding(ctx context.Context, memoryID uuid.UUID, vec []float32, spaceID string) error
}

// MemoryService is the unified application facade shared by HTTP, MCP, eval, and CLI layers.
type MemoryService struct {
	cfg             config.Config
	scrubber        *security.SecretScrubber
	embedder        embedding.Embedder
	curatorBreaker  *curation.Breaker
	embedderBreaker *curation.Breaker
	be              backend
}

// New builds a production MemoryService backed by PostgreSQL storage, Packer, and DependencyGraph.
func New(
	cfg config.Config,
	store *storage.Store,
	scrubber *security.SecretScrubber,
	embedder embedding.Embedder,
	packer *retrieval.Packer,
	depGraph *graph.DependencyGraph,
	curatorBreaker, embedderBreaker *curation.Breaker,
) *MemoryService {
	if scrubber == nil {
		scrubber = security.NewSecretScrubber()
	}
	if depGraph == nil {
		depGraph = graph.NewDependencyGraph()
	}
	if curatorBreaker == nil {
		curatorBreaker = curation.NewBreaker(3, 60*time.Second)
	}
	if embedderBreaker == nil {
		embedderBreaker = curation.NewBreaker(3, 60*time.Second)
	}
	return newWithBackend(cfg, &pgBackend{
		store:    store,
		packer:   packer,
		depGraph: depGraph,
	}, scrubber, embedder, curatorBreaker, embedderBreaker)
}

func newWithBackend(
	cfg config.Config,
	be backend,
	scrubber *security.SecretScrubber,
	embedder embedding.Embedder,
	curatorBreaker, embedderBreaker *curation.Breaker,
) *MemoryService {
	if scrubber == nil {
		scrubber = security.NewSecretScrubber()
	}
	if cfg.MaxWriteBytes <= 0 {
		cfg.MaxWriteBytes = 65536
	}
	if cfg.EmbedTimeout <= 0 {
		cfg.EmbedTimeout = 2 * time.Second
	}
	return &MemoryService{
		cfg:             cfg,
		scrubber:        scrubber,
		embedder:        embedder,
		curatorBreaker:  curatorBreaker,
		embedderBreaker: embedderBreaker,
		be:              be,
	}
}

func (s *MemoryService) HealthCheck(ctx context.Context) HealthCheckResult {
	if err := s.be.Ping(ctx); err != nil {
		return HealthCheckResult{Status: "degraded", DBOk: false}
	}
	return HealthCheckResult{Status: "ok", DBOk: true}
}

func (s *MemoryService) IngestWrite(ctx context.Context, req IngestWriteRequest) (IngestWriteResult, error) {
	if err := storage.ValidateWorkspaceID(req.WorkspaceID); err != nil {
		return IngestWriteResult{}, err
	}
	if len(req.Content) > s.cfg.MaxWriteBytes {
		return IngestWriteResult{}, fmt.Errorf("%w (%d > %d bytes)", ErrPayloadTooLarge, len(req.Content), s.cfg.MaxWriteBytes)
	}
	if strings.TrimSpace(req.Content) == "" {
		return IngestWriteResult{}, fmt.Errorf("content must not be empty: %w", storage.ErrInvalidInput)
	}

	scrubbed, redactions := s.scrubber.Scrub(req.Content)
	res, err := s.be.InsertInboxAndEnqueue(ctx, storage.InsertInboxParams{
		WorkspaceID:    req.WorkspaceID,
		AgentID:        req.AgentID,
		IdempotencyKey: req.IdempotencyKey,
		Content:        scrubbed,
		OccurredAt:     req.OccurredAt,
	})
	if err != nil {
		return IngestWriteResult{}, err
	}
	return IngestWriteResult{
		Accepted:          true,
		InboxID:           res.InboxID,
		RequestID:         res.RequestID,
		WorkspaceRevision: res.WorkspaceRevision,
		Replayed:          res.Replayed,
		Redactions:        redactions,
	}, nil
}

func (s *MemoryService) RecallContext(ctx context.Context, req retrieval.RecallRequest) (retrieval.RecallResponse, error) {
	return s.be.RecallContext(ctx, req)
}

func (s *MemoryService) ExpandMemories(ctx context.Context, req retrieval.ExpandRequest) (retrieval.ExpandResponse, error) {
	return s.be.ExpandMemories(ctx, req)
}

func (s *MemoryService) GetConstitution(ctx context.Context, workspaceID string) (ConstitutionResult, error) {
	if err := storage.ValidateWorkspaceID(workspaceID); err != nil {
		return ConstitutionResult{}, err
	}
	ws, genConstRev, mems, err := s.be.GetConstitutionBundle(ctx, workspaceID)
	if err != nil {
		return ConstitutionResult{}, err
	}
	if mems == nil {
		mems = []storage.Memory{}
	}
	return ConstitutionResult{
		Memories:             mems,
		ConstitutionRevision: ws.ConstitutionRevision,
		ManifestETag:         storage.ComputeConstitutionETag(ws.ConstitutionRevision, genConstRev, ws.InheritGeneral),
	}, nil
}

func (s *MemoryService) ListWorkspaces(ctx context.Context) ([]storage.WorkspaceSummary, error) {
	return s.be.ListWorkspaces(ctx)
}

func (s *MemoryService) CreateWorkspace(ctx context.Context, req CreateWorkspaceRequest) (storage.Workspace, error) {
	return s.be.CreateWorkspace(ctx, req.WorkspaceID, req.InheritGeneral)
}

func (s *MemoryService) UpdateWorkspace(ctx context.Context, workspaceID, inheritGeneral string) (storage.Workspace, error) {
	return s.be.UpdateInheritGeneral(ctx, workspaceID, inheritGeneral)
}

func (s *MemoryService) ListMemoriesKeyset(ctx context.Context, params storage.ListMemoriesParams) (storage.ListMemoriesPage, error) {
	return s.be.ListAllMemoriesKeyset(ctx, params)
}

func (s *MemoryService) GetMemory(ctx context.Context, memoryID uuid.UUID) (MemoryDetailResult, error) {
	if memoryID == uuid.Nil {
		return MemoryDetailResult{}, fmt.Errorf("memory_id is required: %w", storage.ErrInvalidInput)
	}
	return s.be.GetMemoryDetail(ctx, memoryID)
}

func (s *MemoryService) GetRevisions(ctx context.Context, memoryID uuid.UUID) ([]storage.MemoryRevision, error) {
	if memoryID == uuid.Nil {
		return nil, fmt.Errorf("memory_id is required: %w", storage.ErrInvalidInput)
	}
	return s.be.GetRevisions(ctx, memoryID)
}

func (s *MemoryService) ExplainLineage(ctx context.Context, memoryID uuid.UUID) (storage.MemoryLineage, error) {
	if memoryID == uuid.Nil {
		return storage.MemoryLineage{}, fmt.Errorf("memory_id is required: %w", storage.ErrInvalidInput)
	}
	return s.be.ExplainLineage(ctx, memoryID)
}

func (s *MemoryService) ManualCreate(ctx context.Context, req ManualCreateRequest) (ManualMutationResult, error) {
	if err := storage.ValidateWorkspaceID(req.WorkspaceID); err != nil {
		return ManualMutationResult{}, err
	}
	if !isValidDurability(req.Durability) {
		return ManualMutationResult{}, fmt.Errorf("invalid durability %q: %w", req.Durability, storage.ErrInvalidInput)
	}
	if !curation.IsValidTopicKey(req.TopicKey) {
		return ManualMutationResult{}, fmt.Errorf("invalid topic_key %q: %w", req.TopicKey, storage.ErrInvalidInput)
	}
	if strings.TrimSpace(req.Body) == "" {
		return ManualMutationResult{}, fmt.Errorf("body must not be empty: %w", storage.ErrInvalidInput)
	}

	scrubbedBody, _ := s.scrubber.Scrub(req.Body)
	stub := strings.TrimSpace(req.SummaryStub)
	if stub == "" {
		stub = storage.GenerateSummaryStub(scrubbedBody)
	} else {
		stub, _ = s.scrubber.Scrub(stub)
	}
	if !curation.IsValidSummaryStub(stub) {
		return ManualMutationResult{}, fmt.Errorf("invalid summary_stub (must be non-empty, single-line, <= 180 runes): %w", storage.ErrInvalidInput)
	}

	if req.WorkspaceID == storage.WorkspaceGeneral {
		if req.Durability == storage.DurabilityWorking {
			return ManualMutationResult{}, fmt.Errorf("working durability forbidden in 'general': %w", ErrForbiddenAction)
		}
		if req.Durability != storage.DurabilityConstitution {
			if err := curation.ValidateGeneralCategory(req.Category); err != nil {
				return ManualMutationResult{}, fmt.Errorf("%v: %w", err, ErrForbiddenAction)
			}
		}
	}

	tokenCount := curation.CountTokens(scrubbedBody)
	if err := validateTokenLimit(req.Durability, tokenCount); err != nil {
		return ManualMutationResult{}, err
	}
	if err := validateManualEdges(req.DependsOn); err != nil {
		return ManualMutationResult{}, err
	}

	ws, err := s.be.GetWorkspace(ctx, req.WorkspaceID)
	if err != nil {
		return ManualMutationResult{}, err
	}

	var vec []float32
	var spaceID string
	needsEmbedJob := false
	if req.Durability != storage.DurabilityConstitution && s.embedder != nil {
		vec, spaceID = s.tryPreTxEmbed(ctx, req.TopicKey, stub, scrubbedBody)
		if len(vec) == 0 {
			needsEmbedJob = true
		}
	}

	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "manual_create"
	}

	mem, revs, genRev, err := s.be.ManualCreateTx(ctx, storage.CreateMemoryParams{
		WorkspaceID:      req.WorkspaceID,
		Durability:       req.Durability,
		TopicKey:         req.TopicKey,
		CurationState:    storage.CurationUserExplicit,
		SummaryStub:      stub,
		Body:             scrubbedBody,
		TokenCount:       tokenCount,
		Embedding:        vec,
		EmbeddingSpaceID: spaceID,
		AgentID:          req.AgentID,
		SourceAuthority:  "user_explicit",
		ChangeReason:     reason,
	}, req.DependsOn, needsEmbedJob, req.ConfirmToken)
	if err != nil {
		return ManualMutationResult{}, err
	}

	return toManualMutationResult(mem, revs, genRev, ws.InheritGeneral, nil), nil
}

func (s *MemoryService) ManualEdit(ctx context.Context, req ManualEditRequest) (ManualMutationResult, error) {
	if req.MemoryID == uuid.Nil || req.ExpectedVersion < 1 {
		return ManualMutationResult{}, fmt.Errorf("memory_id and expected_version >= 1 required: %w", storage.ErrInvalidInput)
	}
	if err := validateManualEdges(req.DependsOn); err != nil {
		return ManualMutationResult{}, err
	}

	detail, err := s.be.GetMemoryDetail(ctx, req.MemoryID)
	if err != nil {
		return ManualMutationResult{}, err
	}
	cur := detail.Memory
	if req.WorkspaceID != "" && cur.WorkspaceID != req.WorkspaceID {
		return ManualMutationResult{}, fmt.Errorf("memory %s not in workspace %q: %w", req.MemoryID, req.WorkspaceID, storage.ErrNotFound)
	}
	if req.RequireCurrentDurability != "" && cur.Durability != req.RequireCurrentDurability {
		return ManualMutationResult{}, fmt.Errorf("invalid_action: expected durability %q, found %q: %w", req.RequireCurrentDurability, cur.Durability, storage.ErrInvalidInput)
	}
	ws, err := s.be.GetWorkspace(ctx, cur.WorkspaceID)
	if err != nil {
		return ManualMutationResult{}, err
	}

	newDur := cur.Durability
	if req.Durability != nil && *req.Durability != "" {
		if !isValidDurability(*req.Durability) {
			return ManualMutationResult{}, fmt.Errorf("invalid durability %q: %w", *req.Durability, storage.ErrInvalidInput)
		}
		newDur = *req.Durability
	}
	if cur.WorkspaceID == storage.WorkspaceGeneral {
		if newDur == storage.DurabilityWorking {
			return ManualMutationResult{}, fmt.Errorf("working durability forbidden in 'general': %w", ErrForbiddenAction)
		}
		if newDur != storage.DurabilityConstitution {
			if err := curation.ValidateGeneralCategory(req.Category); err != nil {
				return ManualMutationResult{}, fmt.Errorf("%v: %w", err, ErrForbiddenAction)
			}
		}
	}

	newBody := cur.Body
	var scrubbedBodyPtr *string
	if req.Body != nil {
		scrubbed, _ := s.scrubber.Scrub(*req.Body)
		if strings.TrimSpace(scrubbed) == "" {
			return ManualMutationResult{}, fmt.Errorf("body must not be empty: %w", storage.ErrInvalidInput)
		}
		newBody = scrubbed
		scrubbedBodyPtr = &newBody
	}

	newStub := cur.SummaryStub
	var scrubbedStubPtr *string
	if req.SummaryStub != nil {
		scrubbed, _ := s.scrubber.Scrub(*req.SummaryStub)
		if !curation.IsValidSummaryStub(scrubbed) {
			return ManualMutationResult{}, fmt.Errorf("invalid summary_stub: %w", storage.ErrInvalidInput)
		}
		newStub = scrubbed
		scrubbedStubPtr = &newStub
	} else if scrubbedBodyPtr != nil && newBody != cur.Body {
		newStub = storage.GenerateSummaryStub(newBody)
	}

	tokCount := curation.CountTokens(newBody)
	if err := validateTokenLimit(newDur, tokCount); err != nil {
		return ManualMutationResult{}, err
	}

	var vec []float32
	var spaceID string
	needsEmbedJob := false
	textChanged := (scrubbedBodyPtr != nil && newBody != cur.Body) || (scrubbedStubPtr != nil && newStub != cur.SummaryStub)
	if newDur != storage.DurabilityConstitution && s.embedder != nil && (textChanged || cur.EmbeddingSpaceID == "") {
		vec, spaceID = s.tryPreTxEmbed(ctx, cur.TopicKey, newStub, newBody)
		if len(vec) == 0 {
			needsEmbedJob = true
		}
	}

	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "manual_edit"
	}

	var durPtr *string
	if req.Durability != nil {
		durPtr = &newDur
	}
	mem, revs, genRev, err := s.be.ManualEditTx(ctx, cur.Durability, storage.EditMemoryParams{
		MemoryID:                 req.MemoryID,
		WorkspaceID:              cur.WorkspaceID,
		ExpectedVersion:          req.ExpectedVersion,
		RequireCurrentDurability: req.RequireCurrentDurability,
		Durability:               durPtr,
		SummaryStub:              scrubbedStubPtr,
		Body:                     scrubbedBodyPtr,
		TokenCount:               &tokCount,
		CurationState:            storage.CurationUserExplicit,
		Embedding:                vec,
		EmbeddingSpaceID:         spaceID,
		AgentID:                  req.AgentID,
		SourceAuthority:          "user_explicit",
		ChangeReason:             reason,
	}, req.DependsOn, s.cfg.HistoryRetentionN, needsEmbedJob, req.ConfirmToken)
	if err != nil {
		return ManualMutationResult{}, err
	}

	return toManualMutationResult(mem, revs, genRev, ws.InheritGeneral, nil), nil
}

func (s *MemoryService) RevertMemory(ctx context.Context, req RevertMemoryRequest) (ManualMutationResult, error) {
	if req.MemoryID == uuid.Nil || req.ExpectedVersion < 1 || req.TargetVersion < 1 {
		return ManualMutationResult{}, fmt.Errorf("memory_id, expected_version >= 1, and target_version >= 1 required: %w", storage.ErrInvalidInput)
	}

	detail, err := s.be.GetMemoryDetail(ctx, req.MemoryID)
	if err != nil {
		return ManualMutationResult{}, err
	}
	if req.WorkspaceID != "" && detail.Memory.WorkspaceID != req.WorkspaceID {
		return ManualMutationResult{}, fmt.Errorf("memory %s not in workspace %q: %w", req.MemoryID, req.WorkspaceID, storage.ErrNotFound)
	}
	ws, err := s.be.GetWorkspace(ctx, detail.Memory.WorkspaceID)
	if err != nil {
		return ManualMutationResult{}, err
	}

	revsList, err := s.be.GetRevisions(ctx, req.MemoryID)
	if err != nil {
		return ManualMutationResult{}, err
	}
	var targetRev *storage.MemoryRevision
	for i := range revsList {
		if revsList[i].Version == req.TargetVersion {
			targetRev = &revsList[i]
			break
		}
	}
	if targetRev == nil {
		return ManualMutationResult{}, fmt.Errorf("target revision v%d for memory %s %w", req.TargetVersion, req.MemoryID, storage.ErrNotFound)
	}
	if targetRev.BodyPruned || targetRev.Body == nil {
		return ManualMutationResult{}, fmt.Errorf("revision v%d for memory %s: %w", req.TargetVersion, req.MemoryID, storage.ErrRevisionPruned)
	}

	tokCount := curation.CountTokens(*targetRev.Body)
	var vec []float32
	var spaceID string
	needsEmbedJob := false
	if targetRev.Durability != storage.DurabilityConstitution && s.embedder != nil {
		vec, spaceID = s.tryPreTxEmbed(ctx, detail.Memory.TopicKey, targetRev.SummaryStub, *targetRev.Body)
		if len(vec) == 0 {
			needsEmbedJob = true
		}
	}

	mem, revs, genRev, err := s.be.ManualRevertTx(ctx, detail.Memory.Durability, storage.RevertMemoryParams{
		MemoryID:         req.MemoryID,
		WorkspaceID:      detail.Memory.WorkspaceID,
		ExpectedVersion:  req.ExpectedVersion,
		TargetVersion:    req.TargetVersion,
		TokenCount:       tokCount,
		Embedding:        vec,
		EmbeddingSpaceID: spaceID,
		Reason:           strings.TrimSpace(req.Reason),
	}, s.cfg.HistoryRetentionN, needsEmbedJob, req.ConfirmToken)
	if err != nil {
		return ManualMutationResult{}, err
	}

	restoredFrom := req.TargetVersion
	return toManualMutationResult(mem, revs, genRev, ws.InheritGeneral, &restoredFrom), nil
}

func (s *MemoryService) SoftRetire(ctx context.Context, req SoftRetireRequest) (ManualMutationResult, error) {
	if req.MemoryID == uuid.Nil {
		return ManualMutationResult{}, fmt.Errorf("memory_id is required: %w", storage.ErrInvalidInput)
	}
	detail, err := s.be.GetMemoryDetail(ctx, req.MemoryID)
	if err != nil {
		return ManualMutationResult{}, err
	}
	wsID := detail.Memory.WorkspaceID
	if req.WorkspaceID != "" && req.WorkspaceID != wsID {
		return ManualMutationResult{}, fmt.Errorf("memory %s not in workspace %q: %w", req.MemoryID, req.WorkspaceID, storage.ErrNotFound)
	}
	ws, err := s.be.GetWorkspace(ctx, wsID)
	if err != nil {
		return ManualMutationResult{}, err
	}

	mem, revs, genRev, err := s.be.SoftRetireTx(ctx, storage.SoftRetireParams{
		MemoryID:           req.MemoryID,
		WorkspaceID:        wsID,
		ExpectedVersion:    req.ExpectedVersion,
		ForbidConstitution: req.ForbidConstitution,
	}, strings.TrimSpace(req.Reason))
	if err != nil {
		return ManualMutationResult{}, err
	}
	return toManualMutationResult(mem, revs, genRev, ws.InheritGeneral, nil), nil
}

func (s *MemoryService) HardPurge(ctx context.Context, req HardPurgeRequest) (storage.HardPurgeResult, error) {
	if req.MemoryID == uuid.Nil {
		return storage.HardPurgeResult{}, fmt.Errorf("memory_id is required: %w", storage.ErrInvalidInput)
	}
	if !req.Confirm {
		return storage.HardPurgeResult{}, fmt.Errorf("confirm=true is required for hard_purge: %w", storage.ErrInvalidInput)
	}
	return s.be.HardPurgeTx(ctx, req.MemoryID, req.WorkspaceID, strings.TrimSpace(req.Reason), req.ConfirmToken)
}

func (s *MemoryService) OpsStatus(ctx context.Context) (OpsStatusResult, error) {
	pools, counts, queues, err := s.be.OpsSnapshot(ctx)
	if err != nil {
		return OpsStatusResult{}, err
	}
	if queues == nil {
		queues = []storage.RiverQueueStat{}
	}
	return OpsStatusResult{
		ServingPool:     pools.Serving,
		CurationPool:    pools.Curation,
		CuratorBreaker:  s.curatorBreaker.Status(),
		EmbedderBreaker: s.embedderBreaker.Status(),
		PendingInbox:    counts.Pending,
		Provisional:     counts.Provisional,
		ProvisionalAct:  counts.ProvisionalActive,
		DeadLetterInbox: counts.DeadLetter,
		RiverQueues:     queues,
	}, nil
}

func (s *MemoryService) ListCurationDecisions(ctx context.Context, params storage.ListDecisionsParams) (storage.ListDecisionsPage, error) {
	return s.be.ListCurationDecisions(ctx, params)
}

func (s *MemoryService) ListDeadLetters(ctx context.Context, limit int) (storage.DeadLettersReport, error) {
	return s.be.ListDeadLetters(ctx, limit)
}

func (s *MemoryService) IssueConfirmToken(ctx context.Context, req IssueConfirmTokenRequest) (ConfirmTokenResult, error) {
	tok, exp, err := s.be.IssueConfirmToken(ctx, req.WorkspaceID, req.Action, req.TargetMemoryID, req.TTL)
	if err != nil {
		return ConfirmTokenResult{}, err
	}
	return ConfirmTokenResult{
		ConfirmationToken: tok,
		ExpiresAt:         exp,
	}, nil
}

func (s *MemoryService) ConsumeConfirmToken(ctx context.Context, req ConsumeConfirmTokenRequest) error {
	return s.be.ConsumeConfirmToken(ctx, req.Token, req.WorkspaceID, req.Action, req.TargetMemoryID)
}

// ReindexEmbeddings re-computes inline halfvec embeddings for all active memories
// without bumping current_version.
func (s *MemoryService) ReindexEmbeddings(ctx context.Context, batchSize int) (ReindexResult, error) {
	if s.embedder == nil {
		return ReindexResult{}, errors.New("embedder is required for reindex")
	}
	if batchSize <= 0 {
		batchSize = 32
	}

	var (
		afterID   *uuid.UUID
		processed int
		updated   int
	)
	spaceID := s.embedder.SpaceID()

	for {
		batch, err := s.be.ListMemoriesForReindexKeyset(ctx, afterID, batchSize)
		if err != nil {
			return ReindexResult{}, err
		}
		if len(batch) == 0 {
			break
		}

		texts := make([]string, len(batch))
		for i, m := range batch {
			texts[i] = embedding.FormatMemoryText(m.TopicKey, m.SummaryStub, m.Body)
		}
		vecs, err := s.embedder.EmbedBatch(ctx, texts)
		if err != nil {
			return ReindexResult{}, fmt.Errorf("embed reindex batch: %w", err)
		}
		if len(vecs) != len(batch) {
			return ReindexResult{}, fmt.Errorf("reindex embed count mismatch: expected %d, got %d", len(batch), len(vecs))
		}

		for i, m := range batch {
			if err := s.be.UpdateMemoryEmbedding(ctx, m.MemoryID, vecs[i], spaceID); err != nil {
				return ReindexResult{}, err
			}
			updated++
		}
		processed += len(batch)
		lastID := batch[len(batch)-1].MemoryID
		afterID = &lastID

		if len(batch) < batchSize {
			break
		}
	}

	return ReindexResult{
		Processed: processed,
		Updated:   updated,
		SpaceID:   spaceID,
	}, nil
}

func (s *MemoryService) tryPreTxEmbed(ctx context.Context, topicKey, summaryStub, body string) ([]float32, string) {
	if s.embedder == nil || !s.embedderBreaker.Allow() {
		return nil, ""
	}
	embCtx, cancel := context.WithTimeout(ctx, s.cfg.EmbedTimeout)
	defer cancel()
	vecs, err := s.embedder.EmbedBatch(embCtx, []string{embedding.FormatMemoryText(topicKey, summaryStub, body)})
	if err != nil || len(vecs) == 0 {
		if err == nil {
			err = errors.New("empty embedding result")
		}
		s.embedderBreaker.RecordFailure(err)
		return nil, ""
	}
	s.embedderBreaker.RecordSuccess()
	return vecs[0], s.embedder.SpaceID()
}

func toManualMutationResult(mem storage.Memory, revs storage.WorkspaceRevisions, genRev int64, inheritGeneral string, restoredFrom *int) ManualMutationResult {
	return ManualMutationResult{
		MemoryID:             mem.MemoryID,
		WorkspaceID:          mem.WorkspaceID,
		Version:              mem.CurrentVersion,
		RestoredFromVersion:  restoredFrom,
		Status:               mem.Status,
		WorkspaceRevision:    revs.ManifestRevision,
		ConstitutionRevision: revs.ConstitutionRevision,
		ManifestETag:         storage.ComputeMetadataETag(revs.ManifestRevision, genRev, inheritGeneral),
	}
}

func isValidDurability(d string) bool {
	switch d {
	case storage.DurabilityConstitution, storage.DurabilityDurable, storage.DurabilityWorking:
		return true
	default:
		return false
	}
}

func validateTokenLimit(durability string, tokens int) error {
	if durability == storage.DurabilityConstitution && tokens > curation.MaxConstitutionTokens {
		return fmt.Errorf("constitution body has %d tokens (max %d): %w", tokens, curation.MaxConstitutionTokens, storage.ErrInvalidInput)
	}
	if tokens > curation.MaxBodyTokens {
		return fmt.Errorf("body has %d tokens (max %d): %w", tokens, curation.MaxBodyTokens, storage.ErrInvalidInput)
	}
	return nil
}

func validateManualEdges(edges []graph.EdgeSpec) error {
	if len(edges) > graph.MaxActiveOutgoingEdges {
		return fmt.Errorf("%w: got %d (%w)", graph.ErrTooManyEdges, len(edges), storage.ErrInvalidInput)
	}
	for _, e := range edges {
		if e.TargetMemoryID == uuid.Nil || e.ExpectedToRev < 1 || strings.TrimSpace(e.Rationale) == "" {
			return fmt.Errorf("invalid depends_on edge specification: %w", storage.ErrInvalidInput)
		}
	}
	return nil
}

type pgBackend struct {
	store    *storage.Store
	packer   *retrieval.Packer
	depGraph *graph.DependencyGraph
}

func (p *pgBackend) Ping(ctx context.Context) error {
	return p.store.Ping(ctx)
}

func (p *pgBackend) InsertInboxAndEnqueue(ctx context.Context, params storage.InsertInboxParams) (storage.InsertInboxResult, error) {
	return p.store.InsertInboxAndEnqueueTx(ctx, params)
}

func (p *pgBackend) RecallContext(ctx context.Context, req retrieval.RecallRequest) (retrieval.RecallResponse, error) {
	return p.packer.RecallContext(ctx, req)
}

func (p *pgBackend) ExpandMemories(ctx context.Context, req retrieval.ExpandRequest) (retrieval.ExpandResponse, error) {
	return p.packer.ExpandMemories(ctx, req)
}

func (p *pgBackend) GetConstitutionBundle(ctx context.Context, workspaceID string) (storage.Workspace, int64, []storage.Memory, error) {
	tx, err := p.store.ServingPool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return storage.Workspace{}, 0, nil, fmt.Errorf("begin constitution tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ws, err := storage.GetWorkspace(ctx, tx, workspaceID)
	if err != nil {
		return storage.Workspace{}, 0, nil, err
	}
	var genConstRev int64
	if ws.WorkspaceID != storage.WorkspaceGeneral && ws.InheritGeneral != storage.InheritNone {
		if genWS, err := storage.GetWorkspace(ctx, tx, storage.WorkspaceGeneral); err == nil {
			genConstRev = genWS.ConstitutionRevision
		}
	}
	mems, err := storage.GetConstitution(ctx, tx, ws.WorkspaceID, ws.InheritGeneral)
	if err != nil {
		return storage.Workspace{}, 0, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return storage.Workspace{}, 0, nil, err
	}
	return ws, genConstRev, mems, nil
}

func (p *pgBackend) ListWorkspaces(ctx context.Context) ([]storage.WorkspaceSummary, error) {
	return storage.ListWorkspaces(ctx, p.store.CurationPool)
}

func (p *pgBackend) CreateWorkspace(ctx context.Context, workspaceID, inheritGeneral string) (storage.Workspace, error) {
	return storage.CreateWorkspace(ctx, p.store.CurationPool, workspaceID, inheritGeneral)
}

func (p *pgBackend) UpdateInheritGeneral(ctx context.Context, workspaceID, inheritGeneral string) (storage.Workspace, error) {
	return storage.UpdateInheritGeneral(ctx, p.store.CurationPool, workspaceID, inheritGeneral)
}

func (p *pgBackend) ListAllMemoriesKeyset(ctx context.Context, params storage.ListMemoriesParams) (storage.ListMemoriesPage, error) {
	return storage.ListAllMemoriesKeyset(ctx, p.store.CurationPool, params)
}

func (p *pgBackend) GetMemoryDetail(ctx context.Context, memoryID uuid.UUID) (MemoryDetailResult, error) {
	m, err := storage.GetMemory(ctx, p.store.CurationPool, memoryID)
	if err != nil {
		return MemoryDetailResult{}, err
	}
	outMap, err := p.depGraph.LookupOutgoing(ctx, p.store.CurationPool, m.WorkspaceID, []uuid.UUID{m.MemoryID})
	if err != nil {
		return MemoryDetailResult{}, err
	}
	inc, err := p.depGraph.LookupReverseDependents(ctx, p.store.CurationPool, m.WorkspaceID, m.MemoryID)
	if err != nil {
		return MemoryDetailResult{}, err
	}
	out := outMap[m.MemoryID]
	if out == nil {
		out = []graph.OutgoingEdge{}
	}
	if inc == nil {
		inc = []graph.IncomingEdge{}
	}
	return MemoryDetailResult{
		Memory:        m,
		OutgoingEdges: out,
		IncomingEdges: inc,
	}, nil
}

func (p *pgBackend) GetRevisions(ctx context.Context, memoryID uuid.UUID) ([]storage.MemoryRevision, error) {
	return storage.GetRevisions(ctx, p.store.CurationPool, memoryID)
}

func (p *pgBackend) ExplainLineage(ctx context.Context, memoryID uuid.UUID) (storage.MemoryLineage, error) {
	return storage.ExplainLineage(ctx, p.store.CurationPool, memoryID)
}

func (p *pgBackend) GetWorkspace(ctx context.Context, workspaceID string) (storage.Workspace, error) {
	return storage.GetWorkspace(ctx, p.store.CurationPool, workspaceID)
}

func consumeConfirmTokenTx(ctx context.Context, tx pgx.Tx, tok *ConsumeConfirmTokenRequest) error {
	if tok == nil {
		return nil
	}
	return storage.ConsumeConfirmToken(ctx, tx, tok.Token, tok.WorkspaceID, tok.Action, tok.TargetMemoryID)
}

func (p *pgBackend) ManualCreateTx(ctx context.Context, params storage.CreateMemoryParams, dependsOn []graph.EdgeSpec, needsEmbedJob bool, confirmTok *ConsumeConfirmTokenRequest) (storage.Memory, storage.WorkspaceRevisions, int64, error) {
	tx, err := p.store.CurationPool.Begin(ctx)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, fmt.Errorf("begin manual create tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := storage.LockWorkspaceTx(ctx, tx, params.WorkspaceID); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	if err := consumeConfirmTokenTx(ctx, tx, confirmTok); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	mem, err := storage.CreateMemoryTx(ctx, tx, params)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	if err := p.depGraph.UpsertEdges(ctx, tx, params.WorkspaceID, mem.MemoryID, mem.CurrentVersion, dependsOn); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, mapGraphErr(err)
	}
	if _, err := storage.InsertCurationDecisionTx(ctx, tx, storage.CurationDecision{
		WorkspaceID:    params.WorkspaceID,
		InboxIDs:       []uuid.UUID{},
		Action:         "create",
		TargetMemoryID: &mem.MemoryID,
		TargetVersion:  &mem.CurrentVersion,
		Rationale:      params.ChangeReason,
	}); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	if needsEmbedJob {
		if _, err := p.store.RiverInsert.InsertTx(ctx, tx, storage.EmbedMemoryJobArgs{
			WorkspaceID: mem.WorkspaceID,
			MemoryID:    mem.MemoryID,
			Version:     mem.CurrentVersion,
		}, nil); err != nil {
			return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
		}
	}
	revs, err := storage.BumpRevisionTx(ctx, tx, params.WorkspaceID, mem.Durability == storage.DurabilityConstitution)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	genRev := readGeneralManifestRev(ctx, tx, params.WorkspaceID)
	if err := tx.Commit(ctx); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, fmt.Errorf("commit manual create tx: %w", err)
	}
	return mem, revs, genRev, nil
}

func (p *pgBackend) ManualEditTx(ctx context.Context, oldDur string, params storage.EditMemoryParams, dependsOn []graph.EdgeSpec, retentionN int, needsEmbedJob bool, confirmTok *ConsumeConfirmTokenRequest) (storage.Memory, storage.WorkspaceRevisions, int64, error) {
	tx, err := p.store.CurationPool.Begin(ctx)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, fmt.Errorf("begin manual edit tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := storage.LockWorkspaceTx(ctx, tx, params.WorkspaceID); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	if err := consumeConfirmTokenTx(ctx, tx, confirmTok); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	mem, err := storage.EditMemoryTx(ctx, tx, params)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	if err := p.depGraph.UpsertEdges(ctx, tx, mem.WorkspaceID, mem.MemoryID, mem.CurrentVersion, dependsOn); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, mapGraphErr(err)
	}
	if retentionN > 0 {
		if _, err := storage.PruneRevisionBodiesTx(ctx, tx, mem.MemoryID, retentionN); err != nil {
			return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
		}
	}
	if _, err := storage.InsertCurationDecisionTx(ctx, tx, storage.CurationDecision{
		WorkspaceID:    mem.WorkspaceID,
		InboxIDs:       []uuid.UUID{},
		Action:         "manual_edit",
		TargetMemoryID: &mem.MemoryID,
		TargetVersion:  &mem.CurrentVersion,
		Rationale:      params.ChangeReason,
	}); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	if needsEmbedJob {
		if _, err := p.store.RiverInsert.InsertTx(ctx, tx, storage.EmbedMemoryJobArgs{
			WorkspaceID: mem.WorkspaceID,
			MemoryID:    mem.MemoryID,
			Version:     mem.CurrentVersion,
		}, nil); err != nil {
			return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
		}
	}
	bumpConst := oldDur == storage.DurabilityConstitution || mem.Durability == storage.DurabilityConstitution
	revs, err := storage.BumpRevisionTx(ctx, tx, mem.WorkspaceID, bumpConst)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	genRev := readGeneralManifestRev(ctx, tx, mem.WorkspaceID)
	if err := tx.Commit(ctx); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, fmt.Errorf("commit manual edit tx: %w", err)
	}
	return mem, revs, genRev, nil
}

func (p *pgBackend) ManualRevertTx(ctx context.Context, oldDur string, params storage.RevertMemoryParams, retentionN int, needsEmbedJob bool, confirmTok *ConsumeConfirmTokenRequest) (storage.Memory, storage.WorkspaceRevisions, int64, error) {
	tx, err := p.store.CurationPool.Begin(ctx)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, fmt.Errorf("begin manual revert tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := storage.LockWorkspaceTx(ctx, tx, params.WorkspaceID); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	if err := consumeConfirmTokenTx(ctx, tx, confirmTok); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	mem, err := storage.RevertToVersionTx(ctx, tx, params)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	if retentionN > 0 {
		if _, err := storage.PruneRevisionBodiesTx(ctx, tx, mem.MemoryID, retentionN); err != nil {
			return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
		}
	}
	reason := params.Reason
	if reason == "" {
		reason = fmt.Sprintf("revert to version %d", params.TargetVersion)
	}
	if _, err := storage.InsertCurationDecisionTx(ctx, tx, storage.CurationDecision{
		WorkspaceID:    mem.WorkspaceID,
		InboxIDs:       []uuid.UUID{},
		Action:         "rollback",
		TargetMemoryID: &mem.MemoryID,
		TargetVersion:  &mem.CurrentVersion,
		Rationale:      reason,
	}); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	if needsEmbedJob {
		if _, err := p.store.RiverInsert.InsertTx(ctx, tx, storage.EmbedMemoryJobArgs{
			WorkspaceID: mem.WorkspaceID,
			MemoryID:    mem.MemoryID,
			Version:     mem.CurrentVersion,
		}, nil); err != nil {
			return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
		}
	}
	bumpConst := oldDur == storage.DurabilityConstitution || mem.Durability == storage.DurabilityConstitution
	revs, err := storage.BumpRevisionTx(ctx, tx, mem.WorkspaceID, bumpConst)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	genRev := readGeneralManifestRev(ctx, tx, mem.WorkspaceID)
	if err := tx.Commit(ctx); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, fmt.Errorf("commit manual revert tx: %w", err)
	}
	return mem, revs, genRev, nil
}

func (p *pgBackend) SoftRetireTx(ctx context.Context, params storage.SoftRetireParams, reason string) (storage.Memory, storage.WorkspaceRevisions, int64, error) {
	tx, err := p.store.CurationPool.Begin(ctx)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, fmt.Errorf("begin soft retire tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := storage.LockWorkspaceTx(ctx, tx, params.WorkspaceID); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	mem, err := storage.SoftRetireTx(ctx, tx, params)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	if reason == "" {
		reason = "manual_retire"
	}
	if _, err := storage.InsertCurationDecisionTx(ctx, tx, storage.CurationDecision{
		WorkspaceID:    mem.WorkspaceID,
		InboxIDs:       []uuid.UUID{},
		Action:         "retire",
		TargetMemoryID: &mem.MemoryID,
		TargetVersion:  &mem.CurrentVersion,
		Rationale:      reason,
	}); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	revs, err := storage.BumpRevisionTx(ctx, tx, mem.WorkspaceID, mem.Durability == storage.DurabilityConstitution)
	if err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, err
	}
	genRev := readGeneralManifestRev(ctx, tx, mem.WorkspaceID)
	if err := tx.Commit(ctx); err != nil {
		return storage.Memory{}, storage.WorkspaceRevisions{}, 0, fmt.Errorf("commit soft retire tx: %w", err)
	}
	return mem, revs, genRev, nil
}

func (p *pgBackend) HardPurgeTx(ctx context.Context, memoryID uuid.UUID, workspaceID, reason string, confirmTok *ConsumeConfirmTokenRequest) (storage.HardPurgeResult, error) {
	tx, err := p.store.CurationPool.Begin(ctx)
	if err != nil {
		return storage.HardPurgeResult{}, fmt.Errorf("begin hard purge tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	res, err := storage.HardPurgeTx(ctx, tx, memoryID, reason)
	if err != nil {
		return storage.HardPurgeResult{}, err
	}
	if workspaceID != "" && res.WorkspaceID != workspaceID {
		return storage.HardPurgeResult{}, fmt.Errorf("memory %s does not belong to workspace %q: %w", memoryID, workspaceID, storage.ErrNotFound)
	}
	if err := consumeConfirmTokenTx(ctx, tx, confirmTok); err != nil {
		return storage.HardPurgeResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return storage.HardPurgeResult{}, fmt.Errorf("commit hard purge tx: %w", err)
	}
	return res, nil
}

func (p *pgBackend) OpsSnapshot(ctx context.Context) (storage.AllPoolStats, storage.InboxStatusCounts, []storage.RiverQueueStat, error) {
	pools := p.store.PoolStats()
	counts, err := p.store.InboxCounts(ctx)
	if err != nil {
		return storage.AllPoolStats{}, storage.InboxStatusCounts{}, nil, err
	}
	queues, err := p.store.RiverQueueStats(ctx)
	if err != nil {
		return storage.AllPoolStats{}, storage.InboxStatusCounts{}, nil, err
	}
	return pools, counts, queues, nil
}

func (p *pgBackend) ListCurationDecisions(ctx context.Context, params storage.ListDecisionsParams) (storage.ListDecisionsPage, error) {
	return storage.ListCurationDecisions(ctx, p.store.CurationPool, params)
}

func (p *pgBackend) ListDeadLetters(ctx context.Context, limit int) (storage.DeadLettersReport, error) {
	return p.store.ListDeadLetters(ctx, limit)
}

func (p *pgBackend) IssueConfirmToken(ctx context.Context, workspaceID, action string, targetMemoryID *uuid.UUID, ttl time.Duration) (string, time.Time, error) {
	return p.store.IssueConfirmToken(ctx, workspaceID, action, targetMemoryID, ttl)
}

func (p *pgBackend) ConsumeConfirmToken(ctx context.Context, token, workspaceID, action string, targetMemoryID *uuid.UUID) error {
	return p.store.ConsumeConfirmToken(ctx, token, workspaceID, action, targetMemoryID)
}

func (p *pgBackend) ListMemoriesForReindexKeyset(ctx context.Context, afterID *uuid.UUID, batchSize int) ([]storage.Memory, error) {
	return storage.ListMemoriesForReindexKeyset(ctx, p.store.CurationPool, afterID, batchSize)
}

func (p *pgBackend) UpdateMemoryEmbedding(ctx context.Context, memoryID uuid.UUID, vec []float32, spaceID string) error {
	return storage.UpdateMemoryEmbeddingTx(ctx, p.store.CurationPool, memoryID, vec, spaceID)
}

func readGeneralManifestRev(ctx context.Context, db storage.DBTX, workspaceID string) int64 {
	if workspaceID == storage.WorkspaceGeneral {
		return 0
	}
	if ws, err := storage.GetWorkspace(ctx, db, storage.WorkspaceGeneral); err == nil {
		return ws.ManifestRevision
	}
	return 0
}

func mapGraphErr(err error) error {
	switch {
	case errors.Is(err, graph.ErrSelfEdge),
		errors.Is(err, graph.ErrCrossWorkspaceEdge),
		errors.Is(err, graph.ErrEdgeCycle),
		errors.Is(err, graph.ErrTooManyEdges),
		errors.Is(err, graph.ErrEmptyRationale),
		errors.Is(err, graph.ErrDuplicateEdge):
		return fmt.Errorf("%v: %w", err, storage.ErrInvalidInput)
	default:
		return err
	}
}
