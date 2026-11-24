//go:build integration

package service_test

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
	"github.com/nacfson/nacfson-memory/internal/search"
	"github.com/nacfson/nacfson-memory/internal/security"
	"github.com/nacfson/nacfson-memory/internal/service"
	"github.com/nacfson/nacfson-memory/internal/storage"
	"github.com/nacfson/nacfson-memory/internal/testutil"
)

func TestServiceIntegrationEndToEndAndReindex(t *testing.T) {
	ctx := context.Background()
	store := testutil.OpenTestDB(t)
	emb := embedding.NewMockEmbedder("mock/v1:1536")
	embBreaker := curation.NewBreaker(3, time.Minute)
	curBreaker := curation.NewBreaker(3, time.Minute)
	lx := search.NewLexicalSearch()
	dg := graph.NewDependencyGraph()
	packer := retrieval.NewPacker(store, emb, embBreaker, lx, dg, 2*time.Second)

	cfg := config.Config{
		MaxWriteBytes:     65536,
		HistoryRetentionN: 20,
		EmbedTimeout:      2 * time.Second,
	}
	svc := service.New(cfg, store, security.NewSecretScrubber(), emb, packer, dg, curBreaker, embBreaker)

	if h := svc.HealthCheck(ctx); !h.DBOk || h.Status != "ok" {
		t.Fatalf("unexpected health: %+v", h)
	}
	if _, err := svc.CreateWorkspace(ctx, service.CreateWorkspaceRequest{
		WorkspaceID:    "svcws",
		InheritGeneral: storage.InheritConstitutionAndDurable,
	}); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}

	// Constitution + durable creation via ManualCreate.
	constRes, err := svc.ManualCreate(ctx, service.ManualCreateRequest{
		WorkspaceID: "svcws", Durability: storage.DurabilityConstitution,
		TopicKey: "strict-rule", Body: "Never leak credentials.",
	})
	if err != nil || constRes.ConstitutionRevision != 1 {
		t.Fatalf("ManualCreate constitution failed: %+v err=%v", constRes, err)
	}
	durRes, err := svc.ManualCreate(ctx, service.ManualCreateRequest{
		WorkspaceID: "svcws", Durability: storage.DurabilityDurable,
		TopicKey: "api-auth", Body: "Bearer header sk-proj-123456789012345678901234",
	})
	if err != nil || durRes.Version != 1 {
		t.Fatalf("ManualCreate durable failed: %+v err=%v", durRes, err)
	}

	detail, err := svc.GetMemory(ctx, durRes.MemoryID)
	if err != nil || !strings.Contains(detail.Memory.Body, "[REDACTED:OPENAI_KEY]") || detail.Memory.EmbeddingSpaceID != "mock/v1:1536" {
		t.Fatalf("expected scrubbed and embedded durable memory, got %+v err=%v", detail, err)
	}

	// Edit v1 -> v2, then revert back to v1 -> v3, then soft retire, then hard purge.
	updatedBody := "Updated OAuth2 flow"
	editRes, err := svc.ManualEdit(ctx, service.ManualEditRequest{
		MemoryID: durRes.MemoryID, ExpectedVersion: 1, Body: &updatedBody, Reason: "switch auth",
	})
	if err != nil || editRes.Version != 2 {
		t.Fatalf("ManualEdit failed: %+v err=%v", editRes, err)
	}

	revRes, err := svc.RevertMemory(ctx, service.RevertMemoryRequest{
		MemoryID: durRes.MemoryID, ExpectedVersion: 2, TargetVersion: 1, Reason: "restore v1",
	})
	if err != nil || revRes.Version != 3 {
		t.Fatalf("RevertMemory failed: %+v err=%v", revRes, err)
	}

	// Reindex updates embedding_space_id WITHOUT incrementing current_version!
	embV2 := embedding.NewMockEmbedder("mock/v2:1536")
	svcV2 := service.New(cfg, store, nil, embV2, packer, dg, curBreaker, embBreaker)
	rx, err := svcV2.ReindexEmbeddings(ctx, 1)
	if err != nil || rx.Updated != 2 || rx.SpaceID != "mock/v2:1536" {
		t.Fatalf("ReindexEmbeddings failed: %+v err=%v", rx, err)
	}
	afterRx, err := svcV2.GetMemory(ctx, durRes.MemoryID)
	if err != nil || afterRx.Memory.CurrentVersion != 3 || afterRx.Memory.EmbeddingSpaceID != "mock/v2:1536" {
		t.Fatalf("expected version 3 preserved and space updated, got %+v err=%v", afterRx, err)
	}

	// Confirm token issue + single-use consume.
	tokRes, err := svc.IssueConfirmToken(ctx, service.IssueConfirmTokenRequest{
		WorkspaceID: "svcws", Action: "purge", TargetMemoryID: &durRes.MemoryID,
	})
	if err != nil {
		t.Fatalf("IssueConfirmToken: %v", err)
	}
	if err := svc.ConsumeConfirmToken(ctx, service.ConsumeConfirmTokenRequest{
		Token: tokRes.ConfirmationToken, WorkspaceID: "svcws", Action: "purge", TargetMemoryID: &durRes.MemoryID,
	}); err != nil {
		t.Fatalf("ConsumeConfirmToken: %v", err)
	}
	if err := svc.ConsumeConfirmToken(ctx, service.ConsumeConfirmTokenRequest{
		Token: tokRes.ConfirmationToken, WorkspaceID: "svcws", Action: "purge", TargetMemoryID: &durRes.MemoryID,
	}); !errors.Is(err, storage.ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken on replay, got %v", err)
	}

	if _, err := svc.GetConstitution(ctx, "svcws"); err != nil {
		t.Fatalf("GetConstitution: %v", err)
	}
	if _, err := svc.IngestWrite(ctx, service.IngestWriteRequest{
		WorkspaceID: "svcws", AgentID: "claude-code", Content: "Skilled secret sk-ant-api03-abcdefghijklmnopqrstuv turn",
	}); err != nil {
		t.Fatalf("IngestWrite: %v", err)
	}
	if wss, err := svc.ListWorkspaces(ctx); err != nil || len(wss) < 2 {
		t.Fatalf("ListWorkspaces failed: %+v err=%v", wss, err)
	}
	if wsUpd, err := svc.UpdateWorkspace(ctx, "svcws", storage.InheritConstitutionOnly); err != nil || wsUpd.InheritGeneral != storage.InheritConstitutionOnly {
		t.Fatalf("UpdateWorkspace failed: %+v err=%v", wsUpd, err)
	}
	if memPage, err := svc.ListMemoriesKeyset(ctx, storage.ListMemoriesParams{WorkspaceID: "svcws", Limit: 10}); err != nil || len(memPage.Items) != 2 {
		t.Fatalf("ListMemoriesKeyset failed: %+v err=%v", memPage, err)
	}
	if revs, err := svc.GetRevisions(ctx, durRes.MemoryID); err != nil || len(revs) != 3 {
		t.Fatalf("GetRevisions failed: len=%d err=%v", len(revs), err)
	}
	if lin, err := svc.ExplainLineage(ctx, durRes.MemoryID); err != nil || lin.CurrentVersion != 3 {
		t.Fatalf("ExplainLineage failed: %+v err=%v", lin, err)
	}
	if rec, err := svc.RecallContext(ctx, retrieval.RecallRequest{WorkspaceID: "svcws", Query: "OAuth2", MaxTokens: 1000}); err != nil || len(rec.Memories) == 0 {
		t.Fatalf("RecallContext failed: %+v err=%v", rec, err)
	}
	if exp, err := svc.ExpandMemories(ctx, retrieval.ExpandRequest{WorkspaceID: "svcws", MemoryIDs: []uuid.UUID{durRes.MemoryID}, MaxTokens: 1000}); err != nil || len(exp.Memories) != 1 {
		t.Fatalf("ExpandMemories failed: %+v err=%v", exp, err)
	}
	if decs, err := svc.ListCurationDecisions(ctx, storage.ListDecisionsParams{WorkspaceID: "svcws", Limit: 10}); err != nil || len(decs.Decisions) == 0 {
		t.Fatalf("ListCurationDecisions failed: %+v err=%v", decs, err)
	}
	if _, err := svc.ListDeadLetters(ctx, 10); err != nil {
		t.Fatalf("ListDeadLetters failed: %v", err)
	}
	// Trigger mapGraphErr via cross-workspace / missing dependency target on ManualCreate.
	if _, err := svc.ManualCreate(ctx, service.ManualCreateRequest{
		WorkspaceID: "svcws", Durability: storage.DurabilityDurable, TopicKey: "bad-edge", Body: "edge test",
		DependsOn: []graph.EdgeSpec{{TargetMemoryID: uuid.New(), ExpectedToRev: 1, Rationale: "missing target"}},
	}); !errors.Is(err, storage.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on invalid dependency target, got %v", err)
	}
	if _, err := svc.OpsStatus(ctx); err != nil {
		t.Fatalf("OpsStatus: %v", err)
	}
	if _, err := svc.SoftRetire(ctx, service.SoftRetireRequest{MemoryID: durRes.MemoryID}); err != nil {
		t.Fatalf("SoftRetire: %v", err)
	}
	if _, err := svc.HardPurge(ctx, service.HardPurgeRequest{MemoryID: durRes.MemoryID, Confirm: true}); err != nil {
		t.Fatalf("HardPurge: %v", err)
	}

	// Verify pgBackend queues EmbedMemoryJob when Embedder fails during ManualCreate, ManualEdit, RevertMemory,
	// and exercises 'general' workspace readGeneralManifestRev path.
	brokenSvc := service.New(cfg, store, nil, failingSvcEmbedder{}, packer, dg, curation.NewBreaker(3, time.Minute), curation.NewBreaker(3, time.Minute))
	genRes, err := brokenSvc.ManualCreate(ctx, service.ManualCreateRequest{
		WorkspaceID: storage.WorkspaceGeneral,
		Durability:  storage.DurabilityDurable,
		Category:    curation.CategoryPersonalProfile,
		TopicKey:    "user-home",
		Body:        "User works from Seoul.",
	})
	if err != nil {
		t.Fatalf("ManualCreate in general with failing embedder failed: %v", err)
	}
	editedGen := "User lives and works in Seoul."
	if _, err := brokenSvc.ManualEdit(ctx, service.ManualEditRequest{
		MemoryID: genRes.MemoryID, ExpectedVersion: 1, Category: curation.CategoryPersonalProfile, Body: &editedGen,
	}); err != nil {
		t.Fatalf("ManualEdit in general with failing embedder failed: %v", err)
	}
	if _, err := brokenSvc.RevertMemory(ctx, service.RevertMemoryRequest{
		MemoryID: genRes.MemoryID, ExpectedVersion: 2, TargetVersion: 1,
	}); err != nil {
		t.Fatalf("RevertMemory in general with failing embedder failed: %v", err)
	}
}

type failingSvcEmbedder struct{}

func (failingSvcEmbedder) SpaceID() string { return "fail:1536" }
func (failingSvcEmbedder) Dimensions() int { return 1536 }
func (failingSvcEmbedder) EmbedBatch(context.Context, []string) ([][]float32, error) {
	return nil, errors.New("embedder unavailable")
}
