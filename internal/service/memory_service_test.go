package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/internal/curation"
	"github.com/nacfson/nacfson-memory/internal/embedding"
	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/retrieval"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

type fakeBackend struct {
	pingErr       error
	lastInbox     storage.InsertInboxParams
	inboxRes      storage.InsertInboxResult
	ws            storage.Workspace
	genConstRev   int64
	constMems     []storage.Memory
	detail        MemoryDetailResult
	revisions     []storage.MemoryRevision
	createNeedJob bool
	editNeedJob   bool
	revertNeedJob bool
	purgeRes      storage.HardPurgeResult
	reindexMems   []storage.Memory
	reindexedIDs  []uuid.UUID
	embedErr      error
}

func (f *fakeBackend) Ping(context.Context) error { return f.pingErr }
func (f *fakeBackend) InsertInboxAndEnqueue(_ context.Context, params storage.InsertInboxParams) (storage.InsertInboxResult, error) {
	f.lastInbox = params
	return f.inboxRes, nil
}
func (f *fakeBackend) RecallContext(_ context.Context, req retrieval.RecallRequest) (retrieval.RecallResponse, error) {
	return retrieval.RecallResponse{ManifestETag: `W/"ws:1"`}, nil
}
func (f *fakeBackend) ExpandMemories(_ context.Context, req retrieval.ExpandRequest) (retrieval.ExpandResponse, error) {
	return retrieval.ExpandResponse{ManifestETag: `W/"ws:1"`}, nil
}
func (f *fakeBackend) GetConstitutionBundle(_ context.Context, _ string) (storage.Workspace, int64, []storage.Memory, error) {
	return f.ws, f.genConstRev, f.constMems, nil
}
func (f *fakeBackend) ListWorkspaces(context.Context) ([]storage.WorkspaceSummary, error) {
	return []storage.WorkspaceSummary{{Workspace: f.ws, ActiveCount: 2}}, nil
}
func (f *fakeBackend) CreateWorkspace(_ context.Context, id, inherit string) (storage.Workspace, error) {
	return storage.Workspace{WorkspaceID: id, InheritGeneral: inherit, Kind: storage.KindProject}, nil
}
func (f *fakeBackend) UpdateInheritGeneral(_ context.Context, id, inherit string) (storage.Workspace, error) {
	return storage.Workspace{WorkspaceID: id, InheritGeneral: inherit, Kind: storage.KindProject}, nil
}
func (f *fakeBackend) ListAllMemoriesKeyset(context.Context, storage.ListMemoriesParams) (storage.ListMemoriesPage, error) {
	return storage.ListMemoriesPage{}, nil
}
func (f *fakeBackend) GetMemoryDetail(context.Context, uuid.UUID) (MemoryDetailResult, error) {
	return f.detail, nil
}
func (f *fakeBackend) GetRevisions(context.Context, uuid.UUID) ([]storage.MemoryRevision, error) {
	return f.revisions, nil
}
func (f *fakeBackend) ExplainLineage(_ context.Context, id uuid.UUID) (storage.MemoryLineage, error) {
	return storage.MemoryLineage{MemoryID: id, CurrentVersion: 1}, nil
}
func (f *fakeBackend) GetWorkspace(context.Context, string) (storage.Workspace, error) {
	return f.ws, nil
}
func (f *fakeBackend) ManualCreateTx(_ context.Context, params storage.CreateMemoryParams, _ []graph.EdgeSpec, needsEmbedJob bool) (storage.Memory, storage.WorkspaceRevisions, int64, error) {
	f.createNeedJob = needsEmbedJob
	return storage.Memory{
		MemoryID: uuid.New(), WorkspaceID: params.WorkspaceID, Durability: params.Durability,
		TopicKey: params.TopicKey, Status: storage.StatusActive, CurrentVersion: 1,
		SummaryStub: params.SummaryStub, Body: params.Body,
	}, storage.WorkspaceRevisions{ManifestRevision: 2}, 1, nil
}
func (f *fakeBackend) ManualEditTx(_ context.Context, _ string, params storage.EditMemoryParams, _ []graph.EdgeSpec, _ int, needsEmbedJob bool) (storage.Memory, storage.WorkspaceRevisions, int64, error) {
	f.editNeedJob = needsEmbedJob
	return storage.Memory{
		MemoryID: params.MemoryID, WorkspaceID: params.WorkspaceID, Durability: storage.DurabilityDurable,
		Status: storage.StatusActive, CurrentVersion: params.ExpectedVersion + 1,
	}, storage.WorkspaceRevisions{ManifestRevision: 3}, 1, nil
}
func (f *fakeBackend) ManualRevertTx(_ context.Context, _ string, params storage.RevertMemoryParams, _ int, needsEmbedJob bool) (storage.Memory, storage.WorkspaceRevisions, int64, error) {
	f.revertNeedJob = needsEmbedJob
	return storage.Memory{
		MemoryID: params.MemoryID, WorkspaceID: params.WorkspaceID, Durability: storage.DurabilityDurable,
		Status: storage.StatusActive, CurrentVersion: params.ExpectedVersion + 1,
	}, storage.WorkspaceRevisions{ManifestRevision: 4}, 1, nil
}
func (f *fakeBackend) SoftRetireTx(_ context.Context, params storage.SoftRetireParams, _ string) (storage.Memory, storage.WorkspaceRevisions, int64, error) {
	return storage.Memory{
		MemoryID: params.MemoryID, WorkspaceID: params.WorkspaceID, Status: storage.StatusRetired, CurrentVersion: 1,
	}, storage.WorkspaceRevisions{ManifestRevision: 5}, 0, nil
}
func (f *fakeBackend) HardPurgeTx(context.Context, uuid.UUID, string) (storage.HardPurgeResult, error) {
	return f.purgeRes, nil
}
func (f *fakeBackend) OpsSnapshot(context.Context) (storage.AllPoolStats, storage.InboxStatusCounts, []storage.RiverQueueStat, error) {
	return storage.AllPoolStats{Serving: storage.PoolStatSnapshot{MaxConns: 28}}, storage.InboxStatusCounts{Pending: 3}, nil, nil
}
func (f *fakeBackend) ListCurationDecisions(context.Context, storage.ListDecisionsParams) (storage.ListDecisionsPage, error) {
	return storage.ListDecisionsPage{}, nil
}
func (f *fakeBackend) ListDeadLetters(context.Context, int) (storage.DeadLettersReport, error) {
	return storage.DeadLettersReport{}, nil
}
func (f *fakeBackend) IssueConfirmToken(context.Context, string, string, *uuid.UUID, time.Duration) (string, time.Time, error) {
	return "tok-123", time.Now().Add(5 * time.Minute), nil
}
func (f *fakeBackend) ConsumeConfirmToken(context.Context, string, string, string, *uuid.UUID) error {
	return nil
}
func (f *fakeBackend) ListMemoriesForReindexKeyset(_ context.Context, afterID *uuid.UUID, batchSize int) ([]storage.Memory, error) {
	if afterID == nil {
		if len(f.reindexMems) > batchSize {
			return f.reindexMems[:batchSize], nil
		}
		return f.reindexMems, nil
	}
	for i, m := range f.reindexMems {
		if m.MemoryID == *afterID {
			rem := f.reindexMems[i+1:]
			if len(rem) > batchSize {
				return rem[:batchSize], nil
			}
			return rem, nil
		}
	}
	return nil, nil
}
func (f *fakeBackend) UpdateMemoryEmbedding(_ context.Context, memoryID uuid.UUID, _ []float32, _ string) error {
	f.reindexedIDs = append(f.reindexedIDs, memoryID)
	return f.embedErr
}

type brokenEmbedder struct{}

func (brokenEmbedder) SpaceID() string { return "broken:1536" }
func (brokenEmbedder) Dimensions() int { return 1536 }
func (brokenEmbedder) EmbedBatch(context.Context, []string) ([][]float32, error) {
	return nil, errors.New("provider timeout")
}

func TestIngestWriteHealthAndPassThroughs(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBackend{
		inboxRes:    storage.InsertInboxResult{InboxID: uuid.New(), RequestID: uuid.New(), WorkspaceRevision: 7},
		ws:          storage.Workspace{WorkspaceID: "payments", Kind: storage.KindProject, InheritGeneral: storage.InheritConstitutionAndDurable, ConstitutionRevision: 2},
		genConstRev: 1,
	}
	svc := newWithBackend(config.Config{MaxWriteBytes: 128}, fb, nil, embedding.NewMockEmbedder(""), curation.NewBreaker(3, time.Minute), curation.NewBreaker(3, time.Minute))

	if h := svc.HealthCheck(ctx); h.Status != "ok" || !h.DBOk {
		t.Fatalf("expected healthy check, got %+v", h)
	}
	fb.pingErr = errors.New("pg down")
	if h := svc.HealthCheck(ctx); h.Status != "degraded" || h.DBOk {
		t.Fatalf("expected degraded health check, got %+v", h)
	}
	fb.pingErr = nil

	// Oversized payload rejected before scrubbing or DB call.
	if _, err := svc.IngestWrite(ctx, IngestWriteRequest{
		WorkspaceID: "payments", Content: strings.Repeat("a", 129),
	}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
	// Empty payload rejected.
	if _, err := svc.IngestWrite(ctx, IngestWriteRequest{
		WorkspaceID: "payments", Content: "   ",
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on empty write, got %v", err)
	}

	// Secret scrubbed inline prior to storage.InsertInboxParams.
	res, err := svc.IngestWrite(ctx, IngestWriteRequest{
		WorkspaceID: "payments",
		AgentID:     "claude-code",
		Content:     "Key sk-ant-api03-abcdefghijklmnopqrstuv must be redacted",
	})
	if err != nil {
		t.Fatalf("IngestWrite failed: %v", err)
	}
	if !strings.Contains(fb.lastInbox.Content, "[REDACTED:ANTHROPIC_KEY]") || len(res.Redactions) != 1 {
		t.Fatalf("expected secret scrubbed, content=%q redactions=%+v", fb.lastInbox.Content, res.Redactions)
	}

	// Constitution ETag computation & OpsStatus.
	cRes, err := svc.GetConstitution(ctx, "payments")
	if err != nil || cRes.ManifestETag != `W/"const:2+gen:1"` {
		t.Fatalf("unexpected constitution ETag: %+v err=%v", cRes, err)
	}
	if _, err := svc.CreateWorkspace(ctx, CreateWorkspaceRequest{WorkspaceID: "payments"}); err != nil {
		t.Fatalf("CreateWorkspace failed: %v", err)
	}
	if _, err := svc.UpdateWorkspace(ctx, "payments", storage.InheritNone); err != nil {
		t.Fatalf("UpdateWorkspace failed: %v", err)
	}
	if _, err := svc.ListWorkspaces(ctx); err != nil {
		t.Fatalf("ListWorkspaces failed: %v", err)
	}
	if _, err := svc.RecallContext(ctx, retrieval.RecallRequest{WorkspaceID: "payments"}); err != nil {
		t.Fatalf("RecallContext pass-through failed: %v", err)
	}
	if _, err := svc.ExpandMemories(ctx, retrieval.ExpandRequest{WorkspaceID: "payments"}); err != nil {
		t.Fatalf("ExpandMemories pass-through failed: %v", err)
	}
	if _, err := svc.OpsStatus(ctx); err != nil {
		t.Fatalf("OpsStatus failed: %v", err)
	}
	if _, err := svc.ListCurationDecisions(ctx, storage.ListDecisionsParams{}); err != nil {
		t.Fatalf("ListCurationDecisions failed: %v", err)
	}
	if _, err := svc.ListDeadLetters(ctx, 10); err != nil {
		t.Fatalf("ListDeadLetters failed: %v", err)
	}
	if tok, err := svc.IssueConfirmToken(ctx, IssueConfirmTokenRequest{WorkspaceID: "payments", Action: "purge"}); err != nil || tok.ConfirmationToken == "" {
		t.Fatalf("IssueConfirmToken failed: %+v err=%v", tok, err)
	}
	if err := svc.ConsumeConfirmToken(ctx, ConsumeConfirmTokenRequest{Token: "tok-123", WorkspaceID: "payments", Action: "purge"}); err != nil {
		t.Fatalf("ConsumeConfirmToken failed: %v", err)
	}
}

func TestManualCurationRulesFallbackAndReindex(t *testing.T) {
	ctx := context.Background()
	memID := uuid.New()
	bodyText := "Old body"
	fb := &fakeBackend{
		ws: storage.Workspace{WorkspaceID: "general", Kind: storage.KindGeneral, InheritGeneral: storage.InheritNone},
		detail: MemoryDetailResult{
			Memory: storage.Memory{
				MemoryID: memID, WorkspaceID: "general", Durability: storage.DurabilityDurable,
				TopicKey: "user-lang", CurrentVersion: 2, SummaryStub: "English", Body: bodyText,
			},
		},
		revisions: []storage.MemoryRevision{
			{Version: 1, Durability: storage.DurabilityDurable, SummaryStub: "English", Body: &bodyText, BodyPruned: false},
			{Version: 2, Durability: storage.DurabilityDurable, SummaryStub: "Pruned", BodyPruned: true},
		},
		reindexMems: []storage.Memory{
			{MemoryID: uuid.New(), TopicKey: "t1", SummaryStub: "s1", Body: "b1"},
			{MemoryID: uuid.New(), TopicKey: "t2", SummaryStub: "s2", Body: "b2"},
			{MemoryID: uuid.New(), TopicKey: "t3", SummaryStub: "s3", Body: "b3"},
		},
	}

	svc := newWithBackend(config.Config{HistoryRetentionN: 20}, fb, nil, brokenEmbedder{}, curation.NewBreaker(3, time.Minute), curation.NewBreaker(3, time.Minute))

	// 'general' rejects working durability and project_technical category.
	if _, err := svc.ManualCreate(ctx, ManualCreateRequest{
		WorkspaceID: storage.WorkspaceGeneral, Durability: storage.DurabilityWorking,
		Category: curation.CategoryPersonalProfile, TopicKey: "ok", Body: "hello",
	}); !errors.Is(err, ErrForbiddenAction) {
		t.Fatalf("expected ErrForbiddenAction for working in general, got %v", err)
	}
	if _, err := svc.ManualCreate(ctx, ManualCreateRequest{
		WorkspaceID: storage.WorkspaceGeneral, Durability: storage.DurabilityDurable,
		Category: curation.CategoryProjectTechnical, TopicKey: "ok", Body: "hello",
	}); !errors.Is(err, ErrForbiddenAction) {
		t.Fatalf("expected ErrForbiddenAction for project_technical in general, got %v", err)
	}

	// Constitution > 500 tokens rejected.
	if _, err := svc.ManualCreate(ctx, ManualCreateRequest{
		WorkspaceID: storage.WorkspaceGeneral, Durability: storage.DurabilityConstitution,
		TopicKey: "long-rule", Body: strings.Repeat("word ", 520),
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on > 500 token constitution, got %v", err)
	}

	// Embedder failure falls back to queueing EmbedMemoryJob cleanly instead of failing write.
	fb.ws = storage.Workspace{WorkspaceID: "app", Kind: storage.KindProject, InheritGeneral: storage.InheritNone}
	fb.detail.Memory.WorkspaceID = "app"
	if _, err := svc.ManualCreate(ctx, ManualCreateRequest{
		WorkspaceID: "app", Durability: storage.DurabilityDurable,
		TopicKey: "db-choice", Body: "Use PG17 with ghp_1234567890123456789012345",
	}); err != nil || !fb.createNeedJob {
		t.Fatalf("expected create to succeed with EmbedMemoryJob fallback, needJob=%v err=%v", fb.createNeedJob, err)
	}

	newBody := "Updated body"
	if _, err := svc.ManualEdit(ctx, ManualEditRequest{
		MemoryID: memID, ExpectedVersion: 2, Body: &newBody,
	}); err != nil || !fb.editNeedJob {
		t.Fatalf("expected edit to succeed with EmbedMemoryJob fallback, needJob=%v err=%v", fb.editNeedJob, err)
	}

	// Revert to pruned revision fails with ErrRevisionPruned; revert to v1 succeeds with fallback job.
	if _, err := svc.RevertMemory(ctx, RevertMemoryRequest{
		MemoryID: memID, ExpectedVersion: 2, TargetVersion: 2,
	}); !errors.Is(err, storage.ErrRevisionPruned) {
		t.Fatalf("expected ErrRevisionPruned reverting v2, got %v", err)
	}
	if _, err := svc.RevertMemory(ctx, RevertMemoryRequest{
		MemoryID: memID, ExpectedVersion: 2, TargetVersion: 1,
	}); err != nil || !fb.revertNeedJob {
		t.Fatalf("expected revert v1 to succeed with EmbedMemoryJob fallback, err=%v", err)
	}

	// HardPurge requires confirm=true.
	if _, err := svc.HardPurge(ctx, HardPurgeRequest{MemoryID: memID, Confirm: false}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput without confirm=true, got %v", err)
	}
	if _, err := svc.HardPurge(ctx, HardPurgeRequest{MemoryID: memID, Confirm: true}); err != nil {
		t.Fatalf("HardPurge confirm=true failed: %v", err)
	}
	if _, err := svc.SoftRetire(ctx, SoftRetireRequest{MemoryID: memID}); err != nil {
		t.Fatalf("SoftRetire failed: %v", err)
	}

	// Reindex batches in chunks of batchSize=2 without bumping versions.
	svcMockEmb := newWithBackend(config.Config{}, fb, nil, embedding.NewMockEmbedder("mock/reindexed:1536"), curation.NewBreaker(3, time.Minute), curation.NewBreaker(3, time.Minute))
	reRes, err := svcMockEmb.ReindexEmbeddings(ctx, 2)
	if err != nil || reRes.Processed != 3 || reRes.Updated != 3 || len(fb.reindexedIDs) != 3 {
		t.Fatalf("unexpected reindex result: %+v ids=%d err=%v", reRes, len(fb.reindexedIDs), err)
	}

	// ManualCreate & ManualEdit validation error branches.
	if _, err := svcMockEmb.ManualCreate(ctx, ManualCreateRequest{
		WorkspaceID: "app", Durability: "invalid", TopicKey: "ok", Body: "b",
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on bad durability, got %v", err)
	}
	if _, err := svcMockEmb.ManualCreate(ctx, ManualCreateRequest{
		WorkspaceID: "app", Durability: storage.DurabilityDurable, TopicKey: "BAD_TOPIC", Body: "b",
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on bad topic_key, got %v", err)
	}
	if _, err := svcMockEmb.ManualCreate(ctx, ManualCreateRequest{
		WorkspaceID: "app", Durability: storage.DurabilityDurable, TopicKey: "ok-edge", Body: "b",
		DependsOn: make([]graph.EdgeSpec, graph.MaxActiveOutgoingEdges+1),
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on too many edges, got %v", err)
	}
	if _, err := svcMockEmb.ManualEdit(ctx, ManualEditRequest{
		MemoryID: uuid.Nil, ExpectedVersion: 1,
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on nil MemoryID, got %v", err)
	}
	if _, err := svcMockEmb.GetMemory(ctx, uuid.Nil); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on nil GetMemory ID, got %v", err)
	}
	if _, err := svcMockEmb.GetRevisions(ctx, uuid.Nil); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on nil GetRevisions ID, got %v", err)
	}
	if _, err := svcMockEmb.ExplainLineage(ctx, uuid.Nil); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on nil ExplainLineage ID, got %v", err)
	}

	// Additional ManualEdit, RevertMemory, SoftRetire, and ReindexEmbeddings guard branches.
	if _, err := svcMockEmb.ManualCreate(ctx, ManualCreateRequest{
		WorkspaceID: "app", Durability: storage.DurabilityDurable, TopicKey: "empty-body", Body: "   ",
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on empty ManualCreate body, got %v", err)
	}
	if _, err := svcMockEmb.ManualCreate(ctx, ManualCreateRequest{
		WorkspaceID: "app", Durability: storage.DurabilityDurable, TopicKey: "bad-stub", SummaryStub: "bad\nstub", Body: "ok",
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on multiline ManualCreate stub, got %v", err)
	}
	badDur := "ephemeral"
	if _, err := svcMockEmb.ManualEdit(ctx, ManualEditRequest{
		MemoryID: memID, ExpectedVersion: 2, Durability: &badDur,
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on invalid edit durability, got %v", err)
	}
	workDur := storage.DurabilityWorking
	fb.detail.Memory.WorkspaceID = storage.WorkspaceGeneral
	if _, err := svcMockEmb.ManualEdit(ctx, ManualEditRequest{
		MemoryID: memID, ExpectedVersion: 2, Durability: &workDur,
	}); !errors.Is(err, ErrForbiddenAction) {
		t.Fatalf("expected ErrForbiddenAction editing working into general, got %v", err)
	}
	if _, err := svcMockEmb.ManualEdit(ctx, ManualEditRequest{
		MemoryID: memID, ExpectedVersion: 2, Category: curation.CategoryProjectTechnical,
	}); !errors.Is(err, ErrForbiddenAction) {
		t.Fatalf("expected ErrForbiddenAction editing project_technical into general, got %v", err)
	}
	fb.detail.Memory.WorkspaceID = "app"
	emptyBody := "   "
	if _, err := svcMockEmb.ManualEdit(ctx, ManualEditRequest{
		MemoryID: memID, ExpectedVersion: 2, Body: &emptyBody,
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput editing empty body, got %v", err)
	}
	badStub := "bad\nstub"
	if _, err := svcMockEmb.ManualEdit(ctx, ManualEditRequest{
		MemoryID: memID, ExpectedVersion: 2, SummaryStub: &badStub,
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput editing multiline stub, got %v", err)
	}
	if _, err := svcMockEmb.RevertMemory(ctx, RevertMemoryRequest{
		MemoryID: uuid.Nil, ExpectedVersion: 2, TargetVersion: 1,
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on nil revert ID, got %v", err)
	}
	if _, err := svcMockEmb.RevertMemory(ctx, RevertMemoryRequest{
		MemoryID: memID, ExpectedVersion: 2, TargetVersion: 99,
	}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound on missing revert target version, got %v", err)
	}
	if _, err := svcMockEmb.SoftRetire(ctx, SoftRetireRequest{MemoryID: uuid.Nil}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on nil SoftRetire ID, got %v", err)
	}
	if _, err := svcMockEmb.SoftRetire(ctx, SoftRetireRequest{MemoryID: memID, WorkspaceID: "other"}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound on cross-workspace SoftRetire, got %v", err)
	}
	svcNilEmb := newWithBackend(config.Config{}, fb, nil, nil, curation.NewBreaker(3, time.Minute), curation.NewBreaker(3, time.Minute))
	if _, err := svcNilEmb.ReindexEmbeddings(ctx, 0); err == nil {
		t.Fatalf("expected error reindexing without embedder")
	}
}
