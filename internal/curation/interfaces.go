package curation

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/pkoukk/tiktoken-go"
	tiktoken_loader "github.com/pkoukk/tiktoken-go-loader"

	"github.com/nacfson/nacfson-memory/internal/graph"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

const (
	ActionCreate    = "create"
	ActionRevise    = "revise"
	ActionSupersede = "supersede"
	ActionRetire    = "retire"
	ActionNoop      = "noop"

	CategoryPersonalProfile    = "personal_profile"
	CategoryPersonalPreference = "personal_preference"
	CategoryProjectTechnical   = "project_technical"
	CategoryWorkflowNote       = "workflow_note"

	MaxSummaryStubRunes   = 180
	MaxBodyTokens         = 1500
	MaxConstitutionTokens = 500
)

var (
	// ErrPlanValidation indicates a Curator plan violated deterministic policy or schema rules.
	ErrPlanValidation = errors.New("curation plan validation failed")
	// ErrMalformedPlan indicates unparseable or structurally invalid LLM JSON output.
	ErrMalformedPlan = fmt.Errorf("malformed curator output: %w", ErrPlanValidation)

	topicKeyRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,127}$`)

	cl100kOnce sync.Once
	cl100kEnc  *tiktoken.Tiktoken
	cl100kErr  error
)

func getEncoding() (*tiktoken.Tiktoken, error) {
	cl100kOnce.Do(func() {
		tiktoken.SetBpeLoader(tiktoken_loader.NewOfflineLoader())
		cl100kEnc, cl100kErr = tiktoken.GetEncoding("cl100k_base")
	})
	return cl100kEnc, cl100kErr
}

// CountTokens returns the cl100k_base ordinary token count for text offline.
func CountTokens(text string) int {
	if text == "" {
		return 0
	}
	enc, err := getEncoding()
	if err != nil {
		return (len(text) + 3) / 4
	}
	return len(enc.EncodeOrdinary(text))
}

// TruncateTokens truncates text to at most maxTokens cl100k_base tokens, reporting
// whether truncation occurred.
func TruncateTokens(text string, maxTokens int) (string, bool) {
	if maxTokens <= 0 {
		return "", text != ""
	}
	enc, err := getEncoding()
	if err != nil {
		if len(text) <= maxTokens*4 {
			return text, false
		}
		return text[:maxTokens*4], true
	}
	ids := enc.EncodeOrdinary(text)
	if len(ids) <= maxTokens {
		return text, false
	}
	limit := maxTokens
	for limit > 0 {
		candidate := enc.Decode(ids[:limit])
		// Ensure boundary decode yields valid UTF-8 without exceeding maxTokens upon re-encode.
		candidate = strings.ToValidUTF8(candidate, "")
		if candidate == "" {
			limit--
			continue
		}
		if len(enc.EncodeOrdinary(candidate)) <= maxTokens {
			return candidate, true
		}
		limit--
	}
	return "", true
}

// IsValidTopicKey checks the slug format ^[a-z0-9][a-z0-9-]{0,127}$.
func IsValidTopicKey(key string) bool {
	return topicKeyRegex.MatchString(key)
}

// IsValidSummaryStub verifies a non-empty single-line summary within 180 runes.
func IsValidSummaryStub(stub string) bool {
	if strings.TrimSpace(stub) == "" {
		return false
	}
	if strings.ContainsAny(stub, "\n\r") {
		return false
	}
	return utf8.RuneCountInString(stub) <= MaxSummaryStubRunes
}

// IsValidCategory validates the closed set of mutation categories.
func IsValidCategory(category string) bool {
	switch category {
	case CategoryPersonalProfile, CategoryPersonalPreference, CategoryProjectTechnical, CategoryWorkflowNote:
		return true
	default:
		return false
	}
}

// ValidateGeneralCategory permits only non-technical personal categories in 'general'.
func ValidateGeneralCategory(category string) error {
	switch category {
	case CategoryPersonalProfile, CategoryPersonalPreference:
		return nil
	default:
		return fmt.Errorf("workspace 'general' only permits personal_profile or personal_preference (got %q)", category)
	}
}

// Curator distills pending inbox turns into structured memory mutations.
type Curator interface {
	CurateBatch(ctx context.Context, req CurateRequest) (CuratePlan, error)
}

// EdgeSpec aliases graph.EdgeSpec for dependency specifications in Curator plans.
type EdgeSpec = graph.EdgeSpec

// InboxItemView presents one raw_inbox turn to the Curator.
type InboxItemView struct {
	InboxID    uuid.UUID `json:"inbox_id"`
	AgentID    string    `json:"agent_id,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
	Content    string    `json:"content"`
}

// CandidateView presents an existing active memory candidate (and optional prior revision) to Curator.
type CandidateView struct {
	MemoryID        uuid.UUID `json:"memory_id"`
	Durability      string    `json:"durability"`
	TopicKey        string    `json:"topic_key"`
	CurrentVersion  int       `json:"current_version"`
	SummaryStub     string    `json:"summary_stub"`
	Body            string    `json:"body"`
	PrevSummaryStub string    `json:"prev_summary_stub,omitempty"`
	PrevBody        string    `json:"prev_body,omitempty"`
}

// CurateRequest carries batch turns and candidate context for one workspace.
type CurateRequest struct {
	WorkspaceID     string          `json:"workspace_id"`
	WorkspaceKind   string          `json:"workspace_kind"`
	SourceAuthority string          `json:"source_authority"`
	InboxItems      []InboxItemView `json:"inbox_items"`
	Candidates      []CandidateView `json:"candidates"`
}

// CuratePlan captures the structured output and token telemetry from a Curator invocation.
type CuratePlan struct {
	ProviderName     string           `json:"provider_name"`
	ModelName        string           `json:"model_name"`
	PromptTokens     int              `json:"prompt_tokens"`
	CompletionTokens int              `json:"completion_tokens"`
	Mutations        []CurateMutation `json:"mutations"`
}

// CurateMutation specifies one state transition cited back to pending inbox items.
type CurateMutation struct {
	InboxIDs       []uuid.UUID `json:"inbox_ids"`
	Action         string      `json:"action"`
	Category       string      `json:"category"`
	TargetMemoryID *uuid.UUID  `json:"target_memory_id,omitempty"`
	Durability     string      `json:"durability"`
	TopicKey       string      `json:"topic_key"`
	SummaryStub    string      `json:"summary_stub"`
	Body           string      `json:"body"`
	DependsOn      []EdgeSpec  `json:"depends_on,omitempty"`
	Rationale      string      `json:"rationale"`
}

// ValidationContext provides snapshot state required by ValidatePlan.
type ValidationContext struct {
	WorkspaceID      string
	WorkspaceKind    string
	SourceAuthority  string
	ClaimedInboxIDs  []uuid.UUID
	ActiveCandidates map[uuid.UUID]CandidateView
	ExistingEdges    map[uuid.UUID][]uuid.UUID
}

// ValidatePlan enforces deterministic policy, completeness, category isolation,
// token/rune limits, and combined DAG acyclicity on a Curator plan.
func ValidatePlan(vctx ValidationContext, plan CuratePlan) error {
	if len(vctx.ClaimedInboxIDs) == 0 {
		return fmt.Errorf("%w: no claimed inbox items in context", ErrPlanValidation)
	}
	claimed := make(map[uuid.UUID]struct{}, len(vctx.ClaimedInboxIDs))
	for _, id := range vctx.ClaimedInboxIDs {
		claimed[id] = struct{}{}
	}
	covered := make(map[uuid.UUID]struct{}, len(vctx.ClaimedInboxIDs))

	targeted := make(map[uuid.UUID]struct{})
	createdTopics := make(map[string]struct{})
	retiringOrSuperseding := make(map[uuid.UUID]struct{})
	isGeneral := vctx.WorkspaceKind == storage.KindGeneral || vctx.WorkspaceID == storage.WorkspaceGeneral

	// First pass: validate per-mutation invariants and track targets being retired/superseded.
	for i, mut := range plan.Mutations {
		if len(mut.InboxIDs) == 0 {
			return fmt.Errorf("%w: mutation %d missing inbox_ids", ErrPlanValidation, i)
		}
		for _, id := range mut.InboxIDs {
			if _, ok := claimed[id]; !ok {
				return fmt.Errorf("%w: mutation %d cites unclaimed inbox_id %s", ErrPlanValidation, i, id)
			}
			covered[id] = struct{}{}
		}
		if strings.TrimSpace(mut.Rationale) == "" {
			return fmt.Errorf("%w: mutation %d missing rationale", ErrPlanValidation, i)
		}

		switch mut.Action {
		case ActionNoop:
			continue
		case ActionCreate, ActionRevise, ActionSupersede, ActionRetire:
		default:
			return fmt.Errorf("%w: mutation %d has unsupported action %q", ErrPlanValidation, i, mut.Action)
		}

		if !IsValidCategory(mut.Category) {
			return fmt.Errorf("%w: mutation %d has invalid category %q", ErrPlanValidation, i, mut.Category)
		}
		if mut.Durability == storage.DurabilityConstitution {
			return fmt.Errorf("%w: mutation %d cannot write constitution durability", ErrPlanValidation, i)
		}
		if isGeneral {
			if err := ValidateGeneralCategory(mut.Category); err != nil {
				return fmt.Errorf("%w: mutation %d: %v", ErrPlanValidation, i, err)
			}
			if mut.Durability == storage.DurabilityWorking {
				return fmt.Errorf("%w: mutation %d cannot write working durability in 'general'", ErrPlanValidation, i)
			}
		}

		if mut.Action == ActionCreate {
			if mut.TargetMemoryID != nil && *mut.TargetMemoryID != uuid.Nil {
				return fmt.Errorf("%w: mutation %d create must not specify target_memory_id", ErrPlanValidation, i)
			}
		} else {
			if mut.TargetMemoryID == nil || *mut.TargetMemoryID == uuid.Nil {
				return fmt.Errorf("%w: mutation %d %s requires target_memory_id", ErrPlanValidation, i, mut.Action)
			}
			cand, ok := vctx.ActiveCandidates[*mut.TargetMemoryID]
			if !ok {
				return fmt.Errorf("%w: mutation %d target_memory_id %s is not an active candidate in workspace %q", ErrPlanValidation, i, *mut.TargetMemoryID, vctx.WorkspaceID)
			}
			if cand.Durability == storage.DurabilityConstitution {
				return fmt.Errorf("%w: mutation %d cannot modify constitution memory %s", ErrPlanValidation, i, *mut.TargetMemoryID)
			}
			if _, dupTarget := targeted[*mut.TargetMemoryID]; dupTarget {
				return fmt.Errorf("%w: mutation %d duplicates target_memory_id %s in same batch", ErrPlanValidation, i, *mut.TargetMemoryID)
			}
			targeted[*mut.TargetMemoryID] = struct{}{}
			if mut.Action == ActionRetire || mut.Action == ActionSupersede {
				retiringOrSuperseding[*mut.TargetMemoryID] = struct{}{}
			}
		}

		if mut.Action == ActionRetire {
			if len(mut.DependsOn) > 0 {
				return fmt.Errorf("%w: mutation %d retire must not attach depends_on edges", ErrPlanValidation, i)
			}
			continue
		}

		if mut.Durability != storage.DurabilityDurable && mut.Durability != storage.DurabilityWorking {
			return fmt.Errorf("%w: mutation %d has invalid durability %q", ErrPlanValidation, i, mut.Durability)
		}
		if !IsValidTopicKey(mut.TopicKey) {
			return fmt.Errorf("%w: mutation %d has invalid topic_key %q", ErrPlanValidation, i, mut.TopicKey)
		}
		if !IsValidSummaryStub(mut.SummaryStub) {
			return fmt.Errorf("%w: mutation %d has invalid summary_stub", ErrPlanValidation, i)
		}
		if strings.TrimSpace(mut.Body) == "" {
			return fmt.Errorf("%w: mutation %d has empty body", ErrPlanValidation, i)
		}
		if tokens := CountTokens(mut.Body); tokens > MaxBodyTokens {
			return fmt.Errorf("%w: mutation %d body has %d tokens (max %d)", ErrPlanValidation, i, tokens, MaxBodyTokens)
		}

		if mut.Action == ActionCreate {
			if _, dupTopic := createdTopics[mut.TopicKey]; dupTopic {
				return fmt.Errorf("%w: duplicate create topic_key %q in batch", ErrPlanValidation, mut.TopicKey)
			}
			createdTopics[mut.TopicKey] = struct{}{}
		}

		if len(mut.DependsOn) > graph.MaxActiveOutgoingEdges {
			return fmt.Errorf("%w: mutation %d exceeds max %d depends_on edges", ErrPlanValidation, i, graph.MaxActiveOutgoingEdges)
		}
		seenEdgeTargets := make(map[uuid.UUID]struct{}, len(mut.DependsOn))
		for _, edge := range mut.DependsOn {
			if edge.TargetMemoryID == uuid.Nil {
				return fmt.Errorf("%w: mutation %d has nil depends_on target_memory_id", ErrPlanValidation, i)
			}
			if mut.TargetMemoryID != nil && mut.Action == ActionRevise && edge.TargetMemoryID == *mut.TargetMemoryID {
				return fmt.Errorf("%w: mutation %d contains self-dependency on %s", ErrPlanValidation, i, edge.TargetMemoryID)
			}
			if _, ok := vctx.ActiveCandidates[edge.TargetMemoryID]; !ok {
				return fmt.Errorf("%w: mutation %d depends_on target %s not in workspace active candidates", ErrPlanValidation, i, edge.TargetMemoryID)
			}
			if edge.ExpectedToRev < 1 {
				return fmt.Errorf("%w: mutation %d depends_on expected_to_rev must be >= 1", ErrPlanValidation, i)
			}
			if strings.TrimSpace(edge.Rationale) == "" {
				return fmt.Errorf("%w: mutation %d depends_on missing rationale", ErrPlanValidation, i)
			}
			if _, dup := seenEdgeTargets[edge.TargetMemoryID]; dup {
				return fmt.Errorf("%w: mutation %d duplicates depends_on target %s", ErrPlanValidation, i, edge.TargetMemoryID)
			}
			seenEdgeTargets[edge.TargetMemoryID] = struct{}{}
		}
	}

	if len(covered) != len(claimed) {
		return fmt.Errorf("%w: plan covered %d of %d claimed inbox items", ErrPlanValidation, len(covered), len(claimed))
	}

	// Second pass: check create topic_key collisions against non-retired active candidates
	// and prohibit new edges pointing to targets retired/superseded in the same batch.
	activeByTopic := make(map[string]uuid.UUID, len(vctx.ActiveCandidates))
	for id, cand := range vctx.ActiveCandidates {
		if _, retired := retiringOrSuperseding[id]; retired {
			continue
		}
		activeByTopic[cand.TopicKey] = id
	}
	for i, mut := range plan.Mutations {
		switch mut.Action {
		case ActionCreate:
			if ownerID, conflict := activeByTopic[mut.TopicKey]; conflict {
				return fmt.Errorf("%w: mutation %d create topic_key %q collides with active memory %s", ErrPlanValidation, i, mut.TopicKey, ownerID)
			}
		case ActionSupersede:
			if ownerID, conflict := activeByTopic[mut.TopicKey]; conflict && (mut.TargetMemoryID == nil || ownerID != *mut.TargetMemoryID) {
				return fmt.Errorf("%w: mutation %d supersede topic_key %q collides with active memory %s", ErrPlanValidation, i, mut.TopicKey, ownerID)
			}
		}
		for _, edge := range mut.DependsOn {
			if _, retired := retiringOrSuperseding[edge.TargetMemoryID]; retired {
				return fmt.Errorf("%w: mutation %d depends_on target %s is retired/superseded in the same batch", ErrPlanValidation, i, edge.TargetMemoryID)
			}
		}
	}

	// Build effective workspace DAG edges and verify acyclicity cleanly.
	effectiveExisting := make(map[uuid.UUID][]uuid.UUID, len(vctx.ExistingEdges))
	for from, tos := range vctx.ExistingEdges {
		copied := make([]uuid.UUID, len(tos))
		copy(copied, tos)
		effectiveExisting[from] = copied
	}
	batchAdditions := make(map[uuid.UUID][]uuid.UUID)
	for _, mut := range plan.Mutations {
		switch mut.Action {
		case ActionRetire, ActionSupersede:
			if mut.TargetMemoryID != nil {
				delete(effectiveExisting, *mut.TargetMemoryID)
			}
		case ActionRevise:
			if mut.TargetMemoryID != nil && mut.DependsOn != nil {
				delete(effectiveExisting, *mut.TargetMemoryID)
			}
		}
		if (mut.Action == ActionCreate || mut.Action == ActionRevise || mut.Action == ActionSupersede) && len(mut.DependsOn) > 0 {
			fromID := uuid.New()
			if mut.Action == ActionRevise && mut.TargetMemoryID != nil {
				fromID = *mut.TargetMemoryID
			}
			targets := make([]uuid.UUID, len(mut.DependsOn))
			for idx, e := range mut.DependsOn {
				targets[idx] = e.TargetMemoryID
			}
			batchAdditions[fromID] = targets
		}
	}

	if err := graph.ValidateBatchDAG(effectiveExisting, batchAdditions); err != nil {
		return fmt.Errorf("%w: %v", ErrPlanValidation, err)
	}
	return nil
}
