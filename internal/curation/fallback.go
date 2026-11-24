package curation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/nacfson/nacfson-memory/internal/storage"
)

const (
	// ProvisionalTruncatedSuffix marks deterministically truncated provisional fallback bodies.
	ProvisionalTruncatedSuffix = "\n[truncated — pending full curation]"

	defaultBreakerThreshold = 3
	defaultBreakerCooldown  = 60 * time.Second
)

// BreakerState names the operational state of a provider circuit breaker.
type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"
	BreakerOpen     BreakerState = "open"
	BreakerHalfOpen BreakerState = "half_open"
)

// BreakerStatus summarizes a circuit breaker snapshot for /v1/ops/status.
type BreakerStatus struct {
	State               BreakerState `json:"state"`
	ConsecutiveFailures int          `json:"consecutive_failures"`
	OpenedAt            *time.Time   `json:"opened_at,omitempty"`
	CooldownRemainingMs int64        `json:"cooldown_remaining_ms,omitempty"`
	LastError           string       `json:"last_error,omitempty"`
}

// Breaker is a thread-safe failure circuit breaker shared across background workers and readers.
type Breaker struct {
	mu                  sync.Mutex
	threshold           int
	cooldown            time.Duration
	nowFn               func() time.Time
	state               BreakerState
	consecutiveFailures int
	openedAt            time.Time
	lastError           string
}

// NewBreaker creates a Breaker with default UTC wall clock.
func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	return NewBreakerWithClock(threshold, cooldown, func() time.Time {
		return time.Now().UTC()
	})
}

// NewBreakerWithClock creates a Breaker with an injectable clock for deterministic tests.
func NewBreakerWithClock(threshold int, cooldown time.Duration, nowFn func() time.Time) *Breaker {
	if threshold <= 0 {
		threshold = defaultBreakerThreshold
	}
	if cooldown <= 0 {
		cooldown = defaultBreakerCooldown
	}
	if nowFn == nil {
		nowFn = func() time.Time { return time.Now().UTC() }
	}
	return &Breaker{
		threshold: threshold,
		cooldown:  cooldown,
		nowFn:     nowFn,
		state:     BreakerClosed,
	}
}

// Allow reports whether provider calls should proceed (transitioning expired open -> half_open).
func (b *Breaker) Allow() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == BreakerOpen {
		if b.nowFn().Sub(b.openedAt) >= b.cooldown {
			b.state = BreakerHalfOpen
			return true
		}
		return false
	}
	return true
}

// IsOpen reports whether the breaker is currently blocking calls inside its cooldown window.
func (b *Breaker) IsOpen() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.state == BreakerOpen && b.nowFn().Sub(b.openedAt) < b.cooldown
}

// RecordSuccess resets the failure counter and closes the breaker.
func (b *Breaker) RecordSuccess() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	b.state = BreakerClosed
	b.consecutiveFailures = 0
	b.openedAt = time.Time{}
	b.lastError = ""
}

// RecordFailure tracks a provider error and returns true if this failure opened/re-opened the breaker.
func (b *Breaker) RecordFailure(err error) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.nowFn()
	if err != nil {
		b.lastError = err.Error()
	}
	if b.state == BreakerHalfOpen {
		b.state = BreakerOpen
		b.openedAt = now
		b.consecutiveFailures = b.threshold
		return true
	}
	b.consecutiveFailures++
	if b.state == BreakerClosed && b.consecutiveFailures >= b.threshold {
		b.state = BreakerOpen
		b.openedAt = now
		return true
	}
	return false
}

// Status returns an atomic snapshot of the breaker state.
func (b *Breaker) Status() BreakerStatus {
	if b == nil {
		return BreakerStatus{State: BreakerClosed}
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.nowFn()
	st := BreakerStatus{
		State:               b.state,
		ConsecutiveFailures: b.consecutiveFailures,
		LastError:           b.lastError,
	}
	if b.state == BreakerOpen {
		opened := b.openedAt
		st.OpenedAt = &opened
		if rem := b.cooldown - now.Sub(b.openedAt); rem > 0 {
			st.CooldownRemainingMs = rem.Milliseconds()
		} else {
			st.State = BreakerHalfOpen
		}
	}
	return st
}

// TruncateProvisionalBody bounds provisional text to <= 1,500 cl100k_base tokens,
// appending the canonical provisional truncation suffix whenever shortened.
func TruncateProvisionalBody(content string) (string, int) {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		trimmed = "[empty]"
	}
	if n := CountTokens(trimmed); n <= MaxBodyTokens {
		return trimmed, n
	}

	suffixTokens := CountTokens(ProvisionalTruncatedSuffix) + 2
	budget := MaxBodyTokens - suffixTokens
	if budget < 1 {
		budget = 1
	}
	prefix, _ := TruncateTokens(trimmed, budget)
	out := prefix + ProvisionalTruncatedSuffix
	for CountTokens(out) > MaxBodyTokens && budget > 1 {
		budget -= 16
		if budget < 1 {
			budget = 1
		}
		prefix, _ = TruncateTokens(trimmed, budget)
		out = prefix + ProvisionalTruncatedSuffix
	}
	return out, CountTokens(out)
}

// ExtractProvisionalSummary derives a single-line <= 180-rune summary stub for fallback rows.
func ExtractProvisionalSummary(content string) string {
	collapsed := strings.Join(strings.Fields(content), " ")
	if collapsed == "" {
		return "provisional note"
	}
	if utf8.RuneCountInString(collapsed) <= MaxSummaryStubRunes {
		return collapsed
	}
	runes := []rune(collapsed)
	return string(runes[:MaxSummaryStubRunes-3]) + "..."
}

// CommitProvisionalFallbackTx idempotently upserts truncated working rows onto
// topic_key = 'provisional:' || inbox_id, writes revisions/citations/audit decisions,
// transitions raw_inbox items to 'provisional', and bumps workspace manifest_revision.
// Caller must hold LockWorkspaceTx on tx.
func CommitProvisionalFallbackTx(ctx context.Context, tx pgx.Tx, workspaceID string, items []storage.InboxItem, reason string) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}
	if workspaceID == storage.WorkspaceGeneral {
		// Reserved 'general' workspace must never hold working or technical rows.
		ids := make([]uuid.UUID, len(items))
		for i, it := range items {
			ids[i] = it.InboxID
		}
		affected, err := storage.MarkInboxProvisional(ctx, tx, workspaceID, ids)
		if err != nil {
			return 0, err
		}
		if affected > 0 {
			if _, err := storage.InsertCurationDecisionTx(ctx, tx, storage.CurationDecision{
				WorkspaceID: workspaceID,
				InboxIDs:    ids,
				Action:      "provisional_fallback",
				Rationale:   fmt.Sprintf("%s (held provisional in inbox; working rows forbidden in 'general')", reason),
			}); err != nil {
				return 0, err
			}
		}
		return int(affected), nil
	}

	if strings.TrimSpace(reason) == "" {
		reason = "curator unavailable provisional fallback"
	}
	committedIDs := make([]uuid.UUID, 0, len(items))

	for _, item := range items {
		newID, err := uuid.NewV7()
		if err != nil {
			return 0, fmt.Errorf("generate provisional memory_id: %w", err)
		}
		topicKey := "provisional:" + item.InboxID.String()
		body, tokenCount := TruncateProvisionalBody(item.Content)
		stub := ExtractProvisionalSummary(item.Content)

		var mem storage.Memory
		err = tx.QueryRow(ctx, `
			INSERT INTO memory_manifest (
				memory_id, workspace_id, durability, topic_key, status, curation_state,
				current_version, summary_stub, body, token_count
			)
			VALUES ($1, $2, 'working', $3, 'active', 'provisional_heuristic', 1, $4, $5, $6)
			ON CONFLICT (workspace_id, topic_key) WHERE status = 'active'
			DO UPDATE SET
				current_version = memory_manifest.current_version + 1,
				summary_stub = EXCLUDED.summary_stub,
				body = EXCLUDED.body,
				token_count = EXCLUDED.token_count,
				updated_at = now()
			RETURNING
				memory_id, workspace_id, durability, topic_key, status, curation_state,
				current_version, summary_stub, body, token_count,
				COALESCE(embedding_space_id, ''), supersedes_memory_id, superseded_by_memory_id,
				last_reverted_version, false AS premise_drifted, created_at, updated_at
		`, newID, workspaceID, topicKey, stub, body, tokenCount).Scan(
			&mem.MemoryID, &mem.WorkspaceID, &mem.Durability, &mem.TopicKey, &mem.Status, &mem.CurationState,
			&mem.CurrentVersion, &mem.SummaryStub, &mem.Body, &mem.TokenCount,
			&mem.EmbeddingSpaceID, &mem.SupersedesMemoryID, &mem.SupersededByMemoryID,
			&mem.LastRevertedVersion, &mem.PremiseDrifted, &mem.CreatedAt, &mem.UpdatedAt,
		)
		if err != nil {
			return 0, fmt.Errorf("upsert provisional memory %q: %w", topicKey, err)
		}

		var agentPtr *string
		if strings.TrimSpace(item.AgentID) != "" {
			agent := item.AgentID
			agentPtr = &agent
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO memory_revisions (
				memory_id, version, workspace_id, durability, summary_stub, body, body_pruned,
				agent_id, source_authority, curation_state, change_reason
			)
			VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7, 'agent_observed', 'provisional_heuristic', $8)
		`, mem.MemoryID, mem.CurrentVersion, mem.WorkspaceID, mem.Durability, mem.SummaryStub, mem.Body, agentPtr, reason); err != nil {
			return 0, fmt.Errorf("insert provisional revision: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO memory_citations (memory_id, version, inbox_id, agent_id)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (memory_id, version, inbox_id) DO NOTHING
		`, mem.MemoryID, mem.CurrentVersion, item.InboxID, agentPtr); err != nil {
			return 0, fmt.Errorf("insert provisional citation: %w", err)
		}
		ver := mem.CurrentVersion
		if _, err := storage.InsertCurationDecisionTx(ctx, tx, storage.CurationDecision{
			WorkspaceID:    workspaceID,
			InboxIDs:       []uuid.UUID{item.InboxID},
			Action:         "provisional_fallback",
			TargetMemoryID: &mem.MemoryID,
			TargetVersion:  &ver,
			Rationale:      reason,
		}); err != nil {
			return 0, err
		}
		committedIDs = append(committedIDs, item.InboxID)
	}

	if _, err := storage.MarkInboxProvisional(ctx, tx, workspaceID, committedIDs); err != nil {
		return 0, err
	}
	if _, err := storage.BumpRevisionTx(ctx, tx, workspaceID, false); err != nil {
		return 0, err
	}
	return len(committedIDs), nil
}
