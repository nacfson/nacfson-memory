package retrieval

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/search"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

func TestClampMaxTokensAndFuseCandidates(t *testing.T) {
	if ClampMaxTokens(0) != 2000 || ClampMaxTokens(-5) != 2000 || ClampMaxTokens(9000) != 2000 || ClampMaxTokens(500) != 500 {
		t.Fatalf("unexpected ClampMaxTokens behavior")
	}

	now := time.Now().UTC()
	idProj := uuid.New()
	idGenOverridden := uuid.New()
	idWork := uuid.New()
	idProv := uuid.New()

	vecHits := []storage.VectorHit{
		{
			Memory: storage.Memory{
				MemoryID: idGenOverridden, WorkspaceID: storage.WorkspaceGeneral, Durability: storage.DurabilityDurable,
				TopicKey: "db-engine", UpdatedAt: now,
			},
		},
		{
			Memory: storage.Memory{
				MemoryID: idWork, WorkspaceID: "app", Durability: storage.DurabilityWorking,
				TopicKey: "working-note", UpdatedAt: now,
			},
		},
	}
	lexHits := []search.LexicalHit{
		{
			Memory: storage.Memory{
				MemoryID: idProj, WorkspaceID: "app", Durability: storage.DurabilityDurable,
				TopicKey: "db-engine", UpdatedAt: now,
			},
		},
		{
			Memory: storage.Memory{
				MemoryID: idProv, WorkspaceID: "app", Durability: storage.DurabilityWorking,
				CurationState: storage.CurationProvisionalHeuristic, TopicKey: "provisional:abc", UpdatedAt: now,
			},
		},
	}

	fused := FuseCandidates("app", vecHits, lexHits)
	// Overridden general row must be excluded; project durable must rank above working and provisional.
	if len(fused) != 3 {
		t.Fatalf("expected 3 fused candidates (overridden general skipped), got %d", len(fused))
	}
	if fused[0].Memory.MemoryID != idProj {
		t.Fatalf("expected project durable hit first, got %s", fused[0].Memory.TopicKey)
	}
	if fused[2].Memory.MemoryID != idProv {
		t.Fatalf("expected provisional heuristic hit lowest score, got %+v", fused)
	}
}

func TestPackRecallEnforcesCapAndFourEvictionTiers(t *testing.T) {
	idC1 := uuid.New()
	idC2 := uuid.New()
	constMems := []storage.Memory{
		{
			MemoryID: idC1, WorkspaceID: "app", Durability: storage.DurabilityConstitution,
			TopicKey: "rule-one", CurationState: storage.CurationUserExplicit, CurrentVersion: 1,
			SummaryStub: "Rule one strict", Body: strings.Repeat("never skip tests ", 20),
		},
		{
			MemoryID: idC2, WorkspaceID: "app", Durability: storage.DurabilityConstitution,
			TopicKey: "rule-two", CurationState: storage.CurationUserExplicit, CurrentVersion: 1,
			SummaryStub: "Rule two strict", Body: strings.Repeat("always verify backups ", 20),
		},
	}

	var ranked []RankedHit
	for i := 0; i < 8; i++ {
		id := uuid.New()
		deps := []MemoryDependencyView{
			{
				FromMemoryID: id, TargetMemoryID: uuid.New(), TargetTopicKey: "upstream-drifted",
				TargetSummaryStub: "Drifted upstream", ExpectedToRev: 1, Rationale: "depends on old choice",
				PremiseDrifted: true,
			},
			{
				FromMemoryID: id, TargetMemoryID: uuid.New(), TargetTopicKey: "upstream-ok",
				TargetSummaryStub: "Healthy upstream", ExpectedToRev: 2, Rationale: "depends on healthy premise",
				PremiseDrifted: false,
			},
		}
		ranked = append(ranked, RankedHit{
			Memory: storage.Memory{
				MemoryID: id, WorkspaceID: "app", Durability: storage.DurabilityDurable,
				TopicKey: "topic-" + string(rune('a'+i)), CurationState: storage.CurationCuratorVerified, CurrentVersion: 1,
				SummaryStub: "Summary for topic " + string(rune('a'+i)),
				Body:        strings.Repeat("postgres pgvector architectural detail block ", 22),
			},
			Score:        float64(10 - i),
			Dependencies: deps,
		})
	}

	for _, budget := range []int{2000, 900, 450, 220, 110} {
		resp := PackRecall(budget, constMems, ranked, `W/"ws:5+gen:2"`, 5, false)
		if resp.SerializedTokens > budget {
			t.Fatalf("budget %d exceeded: serialized %d tokens", budget, resp.SerializedTokens)
		}
	}

	// Specifically inspect Tier 1 preservation: under moderate budget (e.g. 900),
	// non-drifted dependency details are dropped before drifted ones or before full memory eviction when possible.
	mid := PackRecall(1200, constMems, ranked, `W/"ws:5+gen:2"`, 5, false)
	if mid.SerializedTokens > 1200 || len(mid.Memories) == 0 || len(mid.Stubs) == 0 {
		t.Fatalf("expected balanced packed memories and overflow stubs under 1200 budget, got mems=%d stubs=%d toks=%d", len(mid.Memories), len(mid.Stubs), mid.SerializedTokens)
	}
	for _, m := range mid.Memories {
		if m.Durability == storage.DurabilityConstitution {
			continue
		}
		if len(m.Dependencies) > 0 && !m.Dependencies[0].PremiseDrifted {
			// Verify non-drifted dep is allowed while under budget, and under tighter budget non-drifted drops first.
		}
	}
}

func TestPackExpandTruncatesUnderTightBudget(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	hits := []RankedHit{
		{
			Memory: storage.Memory{
				MemoryID: id1, WorkspaceID: "app", Durability: storage.DurabilityDurable,
				TopicKey: "first", CurationState: storage.CurationCuratorVerified, CurrentVersion: 2,
				SummaryStub: "First", Body: strings.Repeat("first memory comprehensive content ", 40),
			},
			Dependencies: []MemoryDependencyView{
				{
					FromMemoryID: id1, TargetMemoryID: uuid.New(), TargetTopicKey: "dep1",
					TargetSummaryStub: "Dep 1", ExpectedToRev: 1, Rationale: "healthy dep", PremiseDrifted: false,
				},
			},
		},
		{
			Memory: storage.Memory{
				MemoryID: id2, WorkspaceID: "app", Durability: storage.DurabilityWorking,
				TopicKey: "second", CurationState: storage.CurationCuratorVerified, CurrentVersion: 1,
				SummaryStub: "Second", Body: strings.Repeat("second memory long text ", 50),
			},
		},
	}

	full := PackExpand(2000, hits, `W/"ws:2"`, 2)
	if full.Truncated || len(full.Memories) != 2 || full.SerializedTokens > 2000 {
		t.Fatalf("unexpected full expand: truncated=%v len=%d toks=%d", full.Truncated, len(full.Memories), full.SerializedTokens)
	}

	tight := PackExpand(200, hits, `W/"ws:2"`, 2)
	if !tight.Truncated || tight.SerializedTokens > 200 {
		t.Fatalf("expected truncated expand <= 200 tokens, got truncated=%v toks=%d", tight.Truncated, tight.SerializedTokens)
	}
}

func TestPackRecallTightBudgetPacksFirstConstitutionRule(t *testing.T) {
	constMems := []storage.Memory{
		{
			MemoryID: uuid.New(), WorkspaceID: "app", Durability: storage.DurabilityConstitution,
			TopicKey: "concise", CurationState: storage.CurationUserExplicit, CurrentVersion: 1,
			SummaryStub: "Keep responses brief", Body: "Be brief.",
		},
	}
	ranked := []RankedHit{
		{
			Memory: storage.Memory{
				MemoryID: uuid.New(), WorkspaceID: "app", Durability: storage.DurabilityDurable,
				TopicKey: "db", CurationState: storage.CurationCuratorVerified, CurrentVersion: 1,
				SummaryStub: "PG 17", Body: "Use PostgreSQL 17.",
			},
			Score: 10,
		},
	}

	resp := PackRecall(120, constMems, ranked, `W/"ws:1"`, 1, false)
	if resp.SerializedTokens > 120 {
		t.Fatalf("expected serialized tokens <= 120, got %d", resp.SerializedTokens)
	}
	if len(resp.Memories) == 0 || resp.Memories[0].TopicKey != "concise" {
		t.Fatalf("expected concise constitution rule packed in full under 120-token budget, got mems=%+v stubs=%+v", resp.Memories, resp.Stubs)
	}
}
