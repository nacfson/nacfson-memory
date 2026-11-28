package curation

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/storage"
)

// PromptInboxItem is the compact alias-keyed view of an InboxItemView sent to the LLM.
type PromptInboxItem struct {
	Ref        string    `json:"ref"`
	AgentID    string    `json:"agent_id,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
	Content    string    `json:"content"`
}

// PromptCandidate is the compact alias-keyed view of a CandidateView sent to the LLM.
type PromptCandidate struct {
	Ref             string `json:"ref"`
	Durability      string `json:"durability"`
	TopicKey        string `json:"topic_key"`
	CurrentVersion  int    `json:"current_version"`
	SummaryStub     string `json:"summary_stub"`
	Body            string `json:"body"`
	PrevSummaryStub string `json:"prev_summary_stub,omitempty"`
	PrevBody        string `json:"prev_body,omitempty"`
}

// PromptPayload is the deterministic alias-mapped user JSON sent to the Curator LLM.
type PromptPayload struct {
	WorkspaceID     string            `json:"workspace_id"`
	WorkspaceKind   string            `json:"workspace_kind"`
	SourceAuthority string            `json:"source_authority"`
	InboxItems      []PromptInboxItem `json:"inbox_items"`
	Candidates      []PromptCandidate `json:"candidates"`
}

// AliasTable maps short deterministic aliases (I1..In, M1..Mk) to real UUIDs and
// resolves both aliases and canonical UUIDs emitted by the Curator LLM.
type AliasTable struct {
	workspaceID     string
	workspaceKind   string
	sourceAuthority string
	inboxPrompts    []PromptInboxItem
	candPrompts     []PromptCandidate
	inboxByRef      map[string]uuid.UUID
	inboxSet        map[uuid.UUID]struct{}
	memByRef        map[string]uuid.UUID
	candidatesByID  map[uuid.UUID]CandidateView
}

// BuildAliasTable assigns 1-indexed short refs I1..In and M1..Mk to req items and candidates.
func BuildAliasTable(req CurateRequest) AliasTable {
	t := AliasTable{
		workspaceID:     req.WorkspaceID,
		workspaceKind:   req.WorkspaceKind,
		sourceAuthority: req.SourceAuthority,
		inboxPrompts:    make([]PromptInboxItem, len(req.InboxItems)),
		candPrompts:     make([]PromptCandidate, len(req.Candidates)),
		inboxByRef:      make(map[string]uuid.UUID, len(req.InboxItems)),
		inboxSet:        make(map[uuid.UUID]struct{}, len(req.InboxItems)),
		memByRef:        make(map[string]uuid.UUID, len(req.Candidates)),
		candidatesByID:  make(map[uuid.UUID]CandidateView, len(req.Candidates)),
	}
	for i, it := range req.InboxItems {
		ref := fmt.Sprintf("I%d", i+1)
		t.inboxByRef[ref] = it.InboxID
		t.inboxSet[it.InboxID] = struct{}{}
		t.inboxPrompts[i] = PromptInboxItem{
			Ref:        ref,
			AgentID:    it.AgentID,
			OccurredAt: it.OccurredAt,
			Content:    it.Content,
		}
	}
	for i, c := range req.Candidates {
		ref := fmt.Sprintf("M%d", i+1)
		t.memByRef[ref] = c.MemoryID
		t.candidatesByID[c.MemoryID] = c
		t.candPrompts[i] = PromptCandidate{
			Ref:             ref,
			Durability:      c.Durability,
			TopicKey:        c.TopicKey,
			CurrentVersion:  c.CurrentVersion,
			SummaryStub:     c.SummaryStub,
			Body:            c.Body,
			PrevSummaryStub: c.PrevSummaryStub,
			PrevBody:        c.PrevBody,
		}
	}
	return t
}

// BuildPromptPayload returns the compact alias-mapped JSON payload without raw UUIDs.
func (a AliasTable) BuildPromptPayload() PromptPayload {
	return PromptPayload{
		WorkspaceID:     a.workspaceID,
		WorkspaceKind:   a.workspaceKind,
		SourceAuthority: a.sourceAuthority,
		InboxItems:      a.inboxPrompts,
		Candidates:      a.candPrompts,
	}
}

// InboxRefsSummary formats the valid inbox alias range (e.g. "I1..I3") for prompts.
func (a AliasTable) InboxRefsSummary() string {
	switch len(a.inboxPrompts) {
	case 0:
		return "none"
	case 1:
		return "I1"
	default:
		return fmt.Sprintf("I1..I%d", len(a.inboxPrompts))
	}
}

// CandidateRefsSummary formats the valid candidate alias range (e.g. "M1..M4") for prompts.
func (a AliasTable) CandidateRefsSummary() string {
	switch len(a.candPrompts) {
	case 0:
		return "none"
	case 1:
		return "M1"
	default:
		return fmt.Sprintf("M1..M%d", len(a.candPrompts))
	}
}

// IsGeneral reports whether the target workspace is the reserved 'general' workspace.
func (a AliasTable) IsGeneral() bool {
	return a.workspaceKind == storage.KindGeneral || a.workspaceID == storage.WorkspaceGeneral
}

// CandidateByID looks up a candidate memory by UUID.
func (a AliasTable) CandidateByID(id uuid.UUID) (CandidateView, bool) {
	c, ok := a.candidatesByID[id]
	return c, ok
}

// ResolveInboxRef resolves either a case-insensitive short alias (I1) or an exact UUID string.
func (a AliasTable) ResolveInboxRef(raw string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return uuid.Nil, fmt.Errorf("%w: empty inbox reference", ErrMalformedPlan)
	}
	if id, ok := a.inboxByRef[strings.ToUpper(trimmed)]; ok {
		return id, nil
	}
	uid, err := uuid.Parse(trimmed)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: invalid inbox ref %q: %v", ErrMalformedPlan, raw, err)
	}
	if len(a.inboxSet) > 0 {
		if _, ok := a.inboxSet[uid]; !ok {
			return uuid.Nil, fmt.Errorf("%w: unknown inbox_id %s", ErrMalformedPlan, uid)
		}
	}
	return uid, nil
}

// ResolveMemoryRef resolves either a case-insensitive short alias (M1) or an exact UUID string.
// Empty strings resolve to uuid.Nil with nil error so create/noop mutations stay clean.
func (a AliasTable) ResolveMemoryRef(raw string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || strings.EqualFold(trimmed, "null") {
		return uuid.Nil, nil
	}
	if id, ok := a.memByRef[strings.ToUpper(trimmed)]; ok {
		return id, nil
	}
	uid, err := uuid.Parse(trimmed)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: invalid memory ref %q: %v", ErrMalformedPlan, raw, err)
	}
	if uid == uuid.Nil {
		return uuid.Nil, nil
	}
	if len(a.candidatesByID) > 0 {
		if _, ok := a.candidatesByID[uid]; !ok {
			return uuid.Nil, fmt.Errorf("%w: unknown target_memory_id %s", ErrMalformedPlan, uid)
		}
	}
	return uid, nil
}

// SlugifyTopicKey normalizes an arbitrary string into a valid lowercase hyphenated slug <= 128 bytes.
func SlugifyTopicKey(raw string) string {
	lower := strings.ToLower(strings.TrimSpace(raw))
	if lower == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(lower))
	lastHyphen := true // prevents leading hyphens
	for i := 0; i < len(lower); i++ {
		c := lower[i]
		isAlphaNum := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
		if isAlphaNum {
			b.WriteByte(c)
			lastHyphen = false
		} else if !lastHyphen {
			b.WriteByte('-')
			lastHyphen = true
		}
	}
	slug := strings.Trim(b.String(), "-")
	if len(slug) > 128 {
		slug = strings.TrimRight(slug[:128], "-")
	}
	if !IsValidTopicKey(slug) {
		return ""
	}
	return slug
}

// NormalizeSummaryStub collapses newlines/whitespace and truncates to <= 180 runes,
// falling back to body when stub is blank.
func NormalizeSummaryStub(stub, body string) string {
	collapsed := strings.Join(strings.Fields(stub), " ")
	if collapsed == "" {
		collapsed = strings.Join(strings.Fields(body), " ")
	}
	if collapsed == "" {
		return ""
	}
	runes := []rune(collapsed)
	if len(runes) <= MaxSummaryStubRunes {
		return collapsed
	}
	return string(runes[:MaxSummaryStubRunes-3]) + "..."
}

type rawPlanPayload struct {
	Mutations []rawMutationPayload `json:"mutations"`
}

type rawEdgePayload struct {
	TargetMemoryID string `json:"target_memory_id"`
	Target         string `json:"target"`
	ExpectedToRev  int    `json:"expected_to_rev"`
	Rationale      string `json:"rationale"`
}

type rawMutationPayload struct {
	InboxIDs       []string         `json:"inbox_ids"`
	Refs           []string         `json:"refs"`
	Action         string           `json:"action"`
	Op             string           `json:"op"`
	Category       string           `json:"category"`
	TargetMemoryID *string          `json:"target_memory_id"`
	Target         *string          `json:"target"`
	Durability     string           `json:"durability"`
	TopicKey       string           `json:"topic_key"`
	SummaryStub    string           `json:"summary_stub"`
	Summary        string           `json:"summary"`
	Body           string           `json:"body"`
	Text           string           `json:"text"`
	DependsOn      []rawEdgePayload `json:"depends_on"`
	Rationale      string           `json:"rationale"`
}

// planFieldTypeError is a curator output field whose JSON type does not match the plan
// schema, such as a boolean depends_on[].expected_to_rev.
type planFieldTypeError struct {
	field, want, got string
}

func (e *planFieldTypeError) Error() string {
	return fmt.Sprintf("%v: field %s must be %s, got %s", ErrMalformedPlan, e.field, e.want, e.got)
}

func (e *planFieldTypeError) Unwrap() error { return ErrMalformedPlan }

// CompileRawPlan decodes raw LLM JSON (supporting short aliases and shorthand keys),
// resolves aliases back to exact UUIDs, and deterministically normalizes formatting fields
// without masking security invariants enforced by ValidatePlan.
func CompileRawPlan(aliases AliasTable, rawContent string) ([]CurateMutation, error) {
	cleaned := stripMarkdownCodeFence(rawContent)
	var rawPlan rawPlanPayload
	if err := json.Unmarshal([]byte(cleaned), &rawPlan); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) && typeErr.Field != "" {
			return nil, &planFieldTypeError{field: typeErr.Field, want: jsonTypeName(typeErr.Type), got: typeErr.Value}
		}
		return nil, fmt.Errorf("%w: decode mutations JSON: %v", ErrMalformedPlan, err)
	}
	if rawPlan.Mutations == nil {
		return nil, fmt.Errorf("%w: missing mutations array", ErrMalformedPlan)
	}

	mutations := make([]CurateMutation, 0, len(rawPlan.Mutations))
	for i, rm := range rawPlan.Mutations {
		rawIDs := rm.InboxIDs
		if len(rawIDs) == 0 && len(rm.Refs) > 0 {
			rawIDs = rm.Refs
		}
		inboxIDs := make([]uuid.UUID, 0, len(rawIDs))
		seenInbox := make(map[uuid.UUID]struct{}, len(rawIDs))
		for _, rawID := range rawIDs {
			uid, err := aliases.ResolveInboxRef(rawID)
			if err != nil {
				return nil, fmt.Errorf("%w (mutation %d)", err, i)
			}
			if _, dup := seenInbox[uid]; dup {
				continue
			}
			seenInbox[uid] = struct{}{}
			inboxIDs = append(inboxIDs, uid)
		}

		action := strings.ToLower(strings.TrimSpace(rm.Action))
		if action == "" {
			action = strings.ToLower(strings.TrimSpace(rm.Op))
		}
		category := strings.ToLower(strings.TrimSpace(rm.Category))
		durability := strings.ToLower(strings.TrimSpace(rm.Durability))

		rawTarget := rm.TargetMemoryID
		if rawTarget == nil || strings.TrimSpace(*rawTarget) == "" {
			rawTarget = rm.Target
		}
		var targetID *uuid.UUID
		if rawTarget != nil && strings.TrimSpace(*rawTarget) != "" {
			uid, err := aliases.ResolveMemoryRef(*rawTarget)
			if err != nil {
				return nil, fmt.Errorf("%w (mutation %d)", err, i)
			}
			if uid != uuid.Nil {
				targetID = &uid
			}
		}

		rationale := strings.TrimSpace(rm.Rationale)

		if action == ActionNoop {
			if rationale == "" {
				rationale = "skipped non-durable turn"
			}
			mutations = append(mutations, CurateMutation{
				InboxIDs:  inboxIDs,
				Action:    ActionNoop,
				Category:  category,
				Rationale: rationale,
			})
			continue
		}

		if action == ActionRetire {
			category = defaultProjectCategory(category, durability, aliases.IsGeneral())
			if rationale == "" {
				rationale = "retired obsolete memory"
			}
			mutations = append(mutations, CurateMutation{
				InboxIDs:       inboxIDs,
				Action:         ActionRetire,
				Category:       category,
				TargetMemoryID: targetID,
				Durability:     durability,
				Rationale:      rationale,
			})
			continue
		}

		// create, revise, supersede (or unsupported action preserved for ValidatePlan).
		if action == ActionCreate || action == ActionRevise || action == ActionSupersede {
			category = defaultProjectCategory(category, durability, aliases.IsGeneral())
		}

		body := strings.TrimSpace(rm.Body)
		if body == "" {
			body = strings.TrimSpace(rm.Text)
		}

		rawTopic := strings.TrimSpace(rm.TopicKey)
		topicKey := SlugifyTopicKey(rawTopic)
		if topicKey == "" && (action == ActionRevise || action == ActionSupersede) && targetID != nil {
			if cand, ok := aliases.CandidateByID(*targetID); ok {
				topicKey = cand.TopicKey
			}
		}

		rawStub := rm.SummaryStub
		if strings.TrimSpace(rawStub) == "" {
			rawStub = rm.Summary
		}
		summaryStub := NormalizeSummaryStub(rawStub, body)

		if rationale == "" && action != "" {
			rationale = "curated: " + action
		}

		var deps []EdgeSpec
		if rm.DependsOn != nil {
			deps = make([]EdgeSpec, 0, len(rm.DependsOn))
			seenEdges := make(map[uuid.UUID]struct{}, len(rm.DependsOn))
			for _, re := range rm.DependsOn {
				rawEdgeTarget := re.TargetMemoryID
				if strings.TrimSpace(rawEdgeTarget) == "" {
					rawEdgeTarget = re.Target
				}
				tid, err := aliases.ResolveMemoryRef(rawEdgeTarget)
				if err != nil {
					return nil, fmt.Errorf("%w (mutation %d depends_on)", err, i)
				}
				if tid == uuid.Nil {
					return nil, fmt.Errorf("%w: mutation %d empty depends_on target", ErrMalformedPlan, i)
				}
				if _, dup := seenEdges[tid]; dup {
					continue
				}
				seenEdges[tid] = struct{}{}

				expRev := re.ExpectedToRev
				if expRev <= 0 {
					if cand, ok := aliases.CandidateByID(tid); ok {
						expRev = cand.CurrentVersion
					}
				}
				edgeRat := strings.TrimSpace(re.Rationale)
				if edgeRat == "" {
					if cand, ok := aliases.CandidateByID(tid); ok {
						edgeRat = "depends on " + cand.TopicKey
					}
				}
				deps = append(deps, EdgeSpec{
					TargetMemoryID: tid,
					ExpectedToRev:  expRev,
					Rationale:      edgeRat,
				})
			}
		}

		mutations = append(mutations, CurateMutation{
			InboxIDs:       inboxIDs,
			Action:         action,
			Category:       category,
			TargetMemoryID: targetID,
			Durability:     durability,
			TopicKey:       topicKey,
			SummaryStub:    summaryStub,
			Body:           body,
			DependsOn:      deps,
			Rationale:      rationale,
		})
	}

	return mutations, nil
}

// jsonTypeName names the JSON type expected by a rawPlanPayload field kind.
func jsonTypeName(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Int:
		return "an integer"
	case reflect.String:
		return "a string"
	case reflect.Slice:
		return "an array"
	case reflect.Pointer:
		return jsonTypeName(t.Elem()) + " or null"
	default:
		return "an object"
	}
}

func defaultProjectCategory(category, durability string, isGeneral bool) string {
	if isGeneral || category != "" {
		return category
	}
	if durability == storage.DurabilityWorking {
		return CategoryWorkflowNote
	}
	return CategoryProjectTechnical
}
