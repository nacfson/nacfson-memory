package curation

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
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

	cl100kBPEURL     = "https://openaipublic.blob.core.windows.net/encodings/cl100k_base.tiktoken"
	maxSmallBPEPiece = 256
)

var (
	// ErrPlanValidation indicates a Curator plan violated deterministic policy or schema rules.
	ErrPlanValidation = errors.New("curation plan validation failed")
	// ErrMalformedPlan indicates unparseable or structurally invalid LLM JSON output.
	ErrMalformedPlan = fmt.Errorf("malformed curator output: %w", ErrPlanValidation)

	topicKeyRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,127}$`)

	cl100kRanksOnce sync.Once
	cl100kRanks     map[string]int
	cl100kRanksErr  error
)

func loadCL100KRanks() (map[string]int, error) {
	cl100kRanksOnce.Do(func() {
		cl100kRanks, cl100kRanksErr = tiktoken_loader.NewOfflineLoader().LoadTiktokenBpe(cl100kBPEURL)
	})
	return cl100kRanks, cl100kRanksErr
}

// CountTokens returns the cl100k_base ordinary token count for text offline
// using a zero-allocation pure-Go cl100k pretokenizer and stack-buffered BPE merge.
func CountTokens(text string) int {
	if text == "" {
		return 0
	}
	ranks, err := loadCL100KRanks()
	if err != nil || len(ranks) == 0 {
		return (len(text) + 3) / 4
	}

	total := 0
	for pos := 0; pos < len(text); {
		end := nextCL100KPiece(text, pos)
		piece := text[pos:end]
		if _, ok := ranks[piece]; ok {
			total++
		} else {
			total += bytePairMergeCount(piece, ranks)
		}
		pos = end
	}
	return total
}

func asciiLower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

func decodeAt(s string, i int) (rune, int) {
	if i >= len(s) {
		return 0, 0
	}
	if b := s[i]; b < utf8.RuneSelf {
		return rune(b), 1
	}
	return utf8.DecodeRuneInString(s[i:])
}

func isCL100KLetter(r rune) bool {
	if r < utf8.RuneSelf {
		return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
	}
	return unicode.IsLetter(r)
}

func isCL100KNumber(r rune) bool {
	if r < utf8.RuneSelf {
		return r >= '0' && r <= '9'
	}
	return unicode.IsNumber(r)
}

func isCL100KSpace(r rune) bool {
	if r < utf8.RuneSelf {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\v' || r == '\f'
	}
	return unicode.IsSpace(r)
}

func scanLetters(text string, pos int) int {
	for pos < len(text) {
		r, w := decodeAt(text, pos)
		if !isCL100KLetter(r) {
			break
		}
		pos += w
	}
	return pos
}

func scanPunctAndNewlines(text string, pos int) int {
	for pos < len(text) {
		r, w := decodeAt(text, pos)
		if isCL100KSpace(r) || isCL100KLetter(r) || isCL100KNumber(r) {
			break
		}
		pos += w
	}
	for pos < len(text) && (text[pos] == '\r' || text[pos] == '\n') {
		pos++
	}
	return pos
}

// nextCL100KPiece advances across one piece matching the cl100k_base regex:
// (?i:'s|'t|'re|'ve|'m|'ll|'d)|[^\r\n\p{L}\p{N}]?\p{L}+|\p{N}{1,3}| ?[^\s\p{L}\p{N}]+[\r\n]*|\s*[\r\n]+|\s+(?!\S)|\s+
func nextCL100KPiece(text string, pos int) int {
	if text[pos] == '\'' && pos+1 < len(text) {
		switch asciiLower(text[pos+1]) {
		case 's', 't', 'm', 'd':
			return pos + 2
		case 'r', 'v':
			if pos+2 < len(text) && asciiLower(text[pos+2]) == 'e' {
				return pos + 3
			}
		case 'l':
			if pos+2 < len(text) && asciiLower(text[pos+2]) == 'l' {
				return pos + 3
			}
		}
	}

	r0, w0 := decodeAt(text, pos)

	if isCL100KLetter(r0) {
		return scanLetters(text, pos+w0)
	}
	if r0 != '\r' && r0 != '\n' && !isCL100KNumber(r0) {
		if r1, w1 := decodeAt(text, pos+w0); w1 > 0 && isCL100KLetter(r1) {
			return scanLetters(text, pos+w0+w1)
		}
	}

	if isCL100KNumber(r0) {
		end := pos + w0
		for count := 1; count < 3 && end < len(text); count++ {
			r, w := decodeAt(text, end)
			if !isCL100KNumber(r) {
				break
			}
			end += w
		}
		return end
	}

	if r0 == ' ' {
		if r1, w1 := decodeAt(text, pos+1); w1 > 0 && !isCL100KSpace(r1) && !isCL100KLetter(r1) && !isCL100KNumber(r1) {
			return scanPunctAndNewlines(text, pos+1+w1)
		}
	} else if !isCL100KSpace(r0) {
		return scanPunctAndNewlines(text, pos+w0)
	}

	end := pos + w0
	lastWSStart := pos
	lastNewlineEnd := -1
	if r0 == '\r' || r0 == '\n' {
		lastNewlineEnd = end
	}
	for end < len(text) {
		r, w := decodeAt(text, end)
		if !isCL100KSpace(r) {
			break
		}
		lastWSStart = end
		end += w
		if r == '\r' || r == '\n' {
			lastNewlineEnd = end
		}
	}
	if lastNewlineEnd >= 0 {
		return lastNewlineEnd
	}
	if end == len(text) {
		return end
	}
	if lastWSStart > pos {
		return lastWSStart
	}
	return end
}

func bpePairRank(piece string, parts [][2]int, ranks map[string]int, startIdx, skip int) int {
	if startIdx+skip+2 < len(parts) {
		if rank, ok := ranks[piece[parts[startIdx][0]:parts[startIdx+skip+2][0]]]; ok {
			return rank
		}
	}
	return -1
}

func bytePairMergeCount(piece string, ranks map[string]int) int {
	if len(piece) <= 1 {
		return len(piece)
	}
	if len(piece) > maxSmallBPEPiece {
		return len(bytePairMergeLarge(piece, ranks)) - 1
	}
	var stackBuf [64][2]int
	var parts [][2]int
	if len(piece)+1 <= len(stackBuf) {
		parts = stackBuf[:len(piece)+1]
	} else {
		parts = make([][2]int, len(piece)+1)
	}
	for i := 0; i < len(parts); i++ {
		parts[i][0], parts[i][1] = i, math.MaxInt
	}

	for i := 0; i < len(parts)-2; i++ {
		if rank := bpePairRank(piece, parts, ranks, i, 0); rank >= 0 {
			parts[i][1] = rank
		}
	}

	for len(parts) > 1 {
		minRank, minIdx := math.MaxInt, -1
		for i := 0; i < len(parts)-1; i++ {
			if parts[i][1] < minRank {
				minRank, minIdx = parts[i][1], i
			}
		}
		if minRank >= math.MaxInt {
			break
		}
		i := minIdx
		if rank := bpePairRank(piece, parts, ranks, i, 1); rank >= 0 {
			parts[i][1] = rank
		} else {
			parts[i][1] = math.MaxInt
		}
		if i > 0 {
			if rk := bpePairRank(piece, parts, ranks, i-1, 1); rk >= 0 {
				parts[i-1][1] = rk
			} else {
				parts[i-1][1] = math.MaxInt
			}
		}
		parts = append(parts[:i+1], parts[i+2:]...)
	}

	return len(parts) - 1
}

type bpeHeapItem struct {
	rank    int
	pos     int
	spanEnd int
}

type bpeMinHeap []bpeHeapItem

func (h bpeMinHeap) Len() int { return len(h) }
func (h bpeMinHeap) Less(i, j int) bool {
	if h[i].rank != h[j].rank {
		return h[i].rank < h[j].rank
	}
	return h[i].pos < h[j].pos
}
func (h bpeMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *bpeMinHeap) Push(x any)   { *h = append(*h, x.(bpeHeapItem)) }
func (h *bpeMinHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

func bytePairMergeParts(piece string, ranks map[string]int) []int {
	if len(piece) == 0 {
		return []int{0}
	}
	if len(piece) == 1 {
		return []int{0, 1}
	}
	if len(piece) > maxSmallBPEPiece {
		return bytePairMergeLarge(piece, ranks)
	}
	parts := make([][2]int, len(piece)+1)
	for i := 0; i < len(parts); i++ {
		parts[i][0], parts[i][1] = i, math.MaxInt
	}
	for i := 0; i < len(parts)-2; i++ {
		if rank := bpePairRank(piece, parts, ranks, i, 0); rank >= 0 {
			parts[i][1] = rank
		}
	}
	for len(parts) > 1 {
		minRank, minIdx := math.MaxInt, -1
		for i := 0; i < len(parts)-1; i++ {
			if parts[i][1] < minRank {
				minRank, minIdx = parts[i][1], i
			}
		}
		if minRank >= math.MaxInt {
			break
		}
		i := minIdx
		if rank := bpePairRank(piece, parts, ranks, i, 1); rank >= 0 {
			parts[i][1] = rank
		} else {
			parts[i][1] = math.MaxInt
		}
		if i > 0 {
			if rk := bpePairRank(piece, parts, ranks, i-1, 1); rk >= 0 {
				parts[i-1][1] = rk
			} else {
				parts[i-1][1] = math.MaxInt
			}
		}
		parts = append(parts[:i+1], parts[i+2:]...)
	}
	out := make([]int, len(parts))
	for i, p := range parts {
		out[i] = p[0]
	}
	return out
}

func bytePairMergeLarge(piece string, ranks map[string]int) []int {
	n := len(piece)
	next := make([]int, n+1)
	prev := make([]int, n+1)
	rankAt := make([]int, n+1)
	alive := make([]bool, n+1)
	for i := 0; i <= n; i++ {
		alive[i] = true
		prev[i] = i - 1
		if i < n {
			next[i] = i + 1
		} else {
			next[i] = -1
		}
		rankAt[i] = math.MaxInt
	}

	h := make(bpeMinHeap, 0, n)
	for i := 0; i <= n-2; i++ {
		if rank, ok := ranks[piece[i:i+2]]; ok {
			rankAt[i] = rank
			h = append(h, bpeHeapItem{rank: rank, pos: i, spanEnd: i + 2})
		}
	}
	heap.Init(&h)

	updatePair := func(k int) {
		if k < 0 || !alive[k] {
			return
		}
		j := next[k]
		if j < 0 || next[j] < 0 {
			rankAt[k] = math.MaxInt
			return
		}
		end := next[j]
		if rank, ok := ranks[piece[k:end]]; ok {
			rankAt[k] = rank
			heap.Push(&h, bpeHeapItem{rank: rank, pos: k, spanEnd: end})
		} else {
			rankAt[k] = math.MaxInt
		}
	}

	for h.Len() > 0 {
		it := heap.Pop(&h).(bpeHeapItem)
		i := it.pos
		if !alive[i] || rankAt[i] != it.rank {
			continue
		}
		j := next[i]
		if j < 0 || next[j] != it.spanEnd {
			continue
		}
		r := next[j]
		alive[j] = false
		next[i] = r
		prev[r] = i
		updatePair(i)
		updatePair(prev[i])
	}

	out := make([]int, 0, n+1)
	for cur := 0; cur >= 0; cur = next[cur] {
		out = append(out, cur)
	}
	return out
}

// TruncateTokens truncates text to at most maxTokens cl100k_base tokens, reporting
// whether truncation occurred.
func TruncateTokens(text string, maxTokens int) (string, bool) {
	if maxTokens <= 0 {
		return "", text != ""
	}
	ranks, err := loadCL100KRanks()
	if err != nil || len(ranks) == 0 {
		if len(text) <= maxTokens*4 {
			return text, false
		}
		cut := maxTokens * 4
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		return strings.ToValidUTF8(text[:cut], ""), true
	}

	total := 0
	for pos := 0; pos < len(text); {
		end := nextCL100KPiece(text, pos)
		piece := text[pos:end]
		var (
			pieceTokens int
			parts       []int
		)
		if _, ok := ranks[piece]; ok {
			pieceTokens = 1
		} else {
			parts = bytePairMergeParts(piece, ranks)
			pieceTokens = len(parts) - 1
		}
		if total+pieceTokens > maxTokens {
			cut := pos
			rem := maxTokens - total
			if parts != nil && rem > 0 && rem < len(parts) {
				cut = pos + parts[rem]
			}
			for cut > 0 {
				for cut > 0 && !utf8.RuneStart(text[cut]) {
					cut--
				}
				candidate := strings.ToValidUTF8(text[:cut], "")
				if candidate != "" && CountTokens(candidate) <= maxTokens {
					return candidate, true
				}
				if cut == 0 {
					break
				}
				_, size := utf8.DecodeLastRuneInString(text[:cut])
				if size <= 0 {
					cut--
				} else {
					cut -= size
				}
			}
			return "", true
		}
		total += pieceTokens
		pos = end
	}
	return text, false
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
