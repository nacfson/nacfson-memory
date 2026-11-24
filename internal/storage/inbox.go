package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// InboxItem represents a raw_inbox row claimed for curation.
type InboxItem struct {
	InboxID         uuid.UUID `json:"inbox_id"`
	WorkspaceID     string    `json:"workspace_id"`
	AgentID         string    `json:"agent_id,omitempty"`
	SourceAuthority string    `json:"source_authority"`
	RequestID       uuid.UUID `json:"request_id"`
	IdempotencyKey  string    `json:"idempotency_key,omitempty"`
	Content         string    `json:"content"`
	Status          string    `json:"status"`
	OccurredAt      time.Time `json:"occurred_at"`
	IngestedAt      time.Time `json:"ingested_at"`
}

// InsertInboxParams configures an append into raw_inbox + atomic River enqueue.
type InsertInboxParams struct {
	WorkspaceID    string
	AgentID        string
	IdempotencyKey string
	Content        string // must already be scrubbed before persistence
	OccurredAt     time.Time
}

// InsertInboxResult returns accepted inbox identifiers and current workspace watermark.
type InsertInboxResult struct {
	InboxID           uuid.UUID `json:"inbox_id"`
	RequestID         uuid.UUID `json:"request_id"`
	WorkspaceRevision int64     `json:"workspace_revision"`
	Replayed          bool      `json:"replayed"`
}

// InsertInboxAndEnqueueTx appends a scrubbed write to raw_inbox and atomically enqueues
// a per-workspace CurateWorkspaceJob unless an identical idempotency_key was already recorded.
func (s *Store) InsertInboxAndEnqueueTx(ctx context.Context, params InsertInboxParams) (InsertInboxResult, error) {
	if err := ValidateWorkspaceID(params.WorkspaceID); err != nil {
		return InsertInboxResult{}, err
	}
	if strings.TrimSpace(params.Content) == "" {
		return InsertInboxResult{}, fmt.Errorf("content must not be empty: %w", ErrInvalidInput)
	}

	inboxID, err := uuid.NewV7()
	if err != nil {
		return InsertInboxResult{}, fmt.Errorf("generate inbox_id UUIDv7: %w", err)
	}
	reqID, err := uuid.NewV7()
	if err != nil {
		return InsertInboxResult{}, fmt.Errorf("generate request_id UUIDv7: %w", err)
	}
	occurredAt := params.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}

	digest := sha256.Sum256([]byte(params.Content))

	var idemPtr *string
	if trimmed := strings.TrimSpace(params.IdempotencyKey); trimmed != "" {
		idemPtr = &trimmed
	}
	var agentPtr *string
	if trimmed := strings.TrimSpace(params.AgentID); trimmed != "" {
		agentPtr = &trimmed
	}

	tx, err := s.ServingPool.Begin(ctx)
	if err != nil {
		return InsertInboxResult{}, fmt.Errorf("begin inbox write tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := EnsureWorkspace(ctx, tx, params.WorkspaceID); err != nil {
		return InsertInboxResult{}, fmt.Errorf("ensure workspace %q: %w", params.WorkspaceID, err)
	}

	var (
		insertedID    uuid.UUID
		insertedReqID uuid.UUID
		replayed      bool
	)
	err = tx.QueryRow(ctx, `
		INSERT INTO raw_inbox (
			inbox_id, workspace_id, agent_id, source_authority,
			request_id, idempotency_key, content, content_sha256, status, occurred_at
		)
		VALUES ($1, $2, $3, 'agent_observed', $4, $5, $6, $7, 'pending', $8)
		ON CONFLICT (workspace_id, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING inbox_id, request_id
	`, inboxID, params.WorkspaceID, agentPtr, reqID, idemPtr, params.Content, digest[:], occurredAt).Scan(&insertedID, &insertedReqID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return InsertInboxResult{}, fmt.Errorf("insert raw_inbox: %w", err)
		}
		if idemPtr == nil {
			return InsertInboxResult{}, errors.New("unexpected insert conflict with null idempotency_key")
		}
		var existingSHA []byte
		err = tx.QueryRow(ctx, `
			SELECT inbox_id, request_id, content_sha256
			FROM raw_inbox
			WHERE workspace_id = $1 AND idempotency_key = $2
		`, params.WorkspaceID, *idemPtr).Scan(&insertedID, &insertedReqID, &existingSHA)
		if err != nil {
			return InsertInboxResult{}, fmt.Errorf("lookup idempotent inbox row: %w", err)
		}
		if !bytes.Equal(existingSHA, digest[:]) {
			return InsertInboxResult{}, ErrIdempotencyMismatch
		}
		replayed = true
	}

	if !replayed {
		if _, err := s.EnqueueCurateWorkspaceTx(ctx, tx, params.WorkspaceID); err != nil {
			return InsertInboxResult{}, err
		}
	}

	var manifestRev int64
	if err := tx.QueryRow(ctx, `SELECT manifest_revision FROM workspaces WHERE workspace_id = $1`, params.WorkspaceID).Scan(&manifestRev); err != nil {
		return InsertInboxResult{}, fmt.Errorf("read workspace manifest_revision: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return InsertInboxResult{}, fmt.Errorf("commit inbox write tx: %w", err)
	}
	return InsertInboxResult{
		InboxID:           insertedID,
		RequestID:         insertedReqID,
		WorkspaceRevision: manifestRev,
		Replayed:          replayed,
	}, nil
}

// ClaimPendingBatch fetches pending raw_inbox rows ordered chronologically up to maxItems
// and maxBytes (always allowing the first item through even if larger than maxBytes).
func ClaimPendingBatch(ctx context.Context, db DBTX, workspaceID string, maxItems, maxBytes int) ([]InboxItem, error) {
	if maxItems <= 0 {
		maxItems = 32
	}
	if maxBytes <= 0 {
		maxBytes = 32768
	}

	rows, err := db.Query(ctx, `
		SELECT
			inbox_id,
			workspace_id,
			COALESCE(agent_id, ''),
			source_authority,
			request_id,
			COALESCE(idempotency_key, ''),
			content,
			status,
			occurred_at,
			ingested_at
		FROM raw_inbox
		WHERE workspace_id = $1 AND status = 'pending'
		ORDER BY occurred_at ASC, ingested_at ASC, inbox_id ASC
		LIMIT $2
	`, workspaceID, maxItems)
	if err != nil {
		return nil, fmt.Errorf("claim pending inbox batch: %w", err)
	}
	defer rows.Close()

	var batch []InboxItem
	totalBytes := 0
	for rows.Next() {
		var item InboxItem
		if err := rows.Scan(
			&item.InboxID,
			&item.WorkspaceID,
			&item.AgentID,
			&item.SourceAuthority,
			&item.RequestID,
			&item.IdempotencyKey,
			&item.Content,
			&item.Status,
			&item.OccurredAt,
			&item.IngestedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending inbox row: %w", err)
		}
		itemBytes := len(item.Content)
		if len(batch) > 0 && totalBytes+itemBytes > maxBytes {
			break
		}
		totalBytes += itemBytes
		batch = append(batch, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending inbox rows: %w", err)
	}
	return batch, nil
}

// MarkInboxCurated transitions pending inbox items cleanly to 'curated'.
func MarkInboxCurated(ctx context.Context, tx pgx.Tx, workspaceID string, inboxIDs []uuid.UUID) (int64, error) {
	if len(inboxIDs) == 0 {
		return 0, nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE raw_inbox
		SET status = 'curated'
		WHERE workspace_id = $1
		  AND status = 'pending'
		  AND inbox_id = ANY($2)
	`, workspaceID, inboxIDs)
	if err != nil {
		return 0, fmt.Errorf("mark inbox curated: %w", err)
	}
	return tag.RowsAffected(), nil
}

// MarkInboxProvisional transitions pending inbox items to 'provisional'.
func MarkInboxProvisional(ctx context.Context, tx pgx.Tx, workspaceID string, inboxIDs []uuid.UUID) (int64, error) {
	if len(inboxIDs) == 0 {
		return 0, nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE raw_inbox
		SET status = 'provisional'
		WHERE workspace_id = $1
		  AND status = 'pending'
		  AND inbox_id = ANY($2)
	`, workspaceID, inboxIDs)
	if err != nil {
		return 0, fmt.Errorf("mark inbox provisional: %w", err)
	}
	return tag.RowsAffected(), nil
}

// MarkInboxDeadLetter isolates a poison-pill inbox item with its error description.
func MarkInboxDeadLetter(ctx context.Context, db DBTX, workspaceID string, inboxID uuid.UUID, lastErr string) error {
	tag, err := db.Exec(ctx, `
		UPDATE raw_inbox
		SET status = 'dead_letter', last_error = $3
		WHERE workspace_id = $1 AND inbox_id = $2
	`, workspaceID, inboxID, truncateRunes(lastErr, 2000))
	if err != nil {
		return fmt.Errorf("mark inbox dead_letter: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("inbox item %s %w", inboxID, ErrNotFound)
	}
	return nil
}

// ResetProvisionalToPending resets cited raw_inbox rows for the oldest active provisional
// manifest entries back to 'pending' and returns the affected workspace IDs and row count.
func ResetProvisionalToPending(ctx context.Context, tx pgx.Tx, limit int) ([]string, int64, error) {
	if limit <= 0 {
		limit = 10
	}

	rows, err := tx.Query(ctx, `
		WITH target_manifests AS (
			SELECT memory_id
			FROM memory_manifest
			WHERE curation_state = 'provisional_heuristic'
			  AND status = 'active'
			ORDER BY updated_at ASC
			LIMIT $1
		),
		target_inbox AS (
			SELECT DISTINCT mc.inbox_id
			FROM memory_citations mc
			JOIN target_manifests tm ON tm.memory_id = mc.memory_id
		)
		UPDATE raw_inbox r
		SET status = 'pending'
		FROM target_inbox ti
		WHERE r.inbox_id = ti.inbox_id
		  AND r.status = 'provisional'
		RETURNING r.workspace_id
	`, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("reset provisional inbox items to pending: %w", err)
	}
	defer rows.Close()

	seen := make(map[string]struct{})
	var workspaces []string
	var count int64
	for rows.Next() {
		var wsID string
		if err := rows.Scan(&wsID); err != nil {
			return nil, 0, fmt.Errorf("scan reset provisional workspace_id: %w", err)
		}
		count++
		if _, ok := seen[wsID]; !ok {
			seen[wsID] = struct{}{}
			workspaces = append(workspaces, wsID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate reset provisional rows: %w", err)
	}
	return workspaces, count, nil
}

// EnqueueCurateWorkspaceTx inserts a CurateWorkspaceJob unless one is already queued
// in ('available', 'pending', 'scheduled') for workspaceID. It intentionally ignores 'running'
// jobs so concurrent writes or in-worker follow-up enqueues are never dropped.
func (s *Store) EnqueueCurateWorkspaceTx(ctx context.Context, tx pgx.Tx, workspaceID string) (bool, error) {
	var queued bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM river_job
			WHERE kind = 'curate_workspace'
			  AND queue = 'curation'
			  AND state IN ('available', 'pending', 'scheduled')
			  AND args->>'workspace_id' = $1
		)
	`, workspaceID).Scan(&queued); err != nil {
		return false, fmt.Errorf("check queued curate_workspace job: %w", err)
	}
	if queued {
		return false, nil
	}
	if _, err := s.RiverInsert.InsertTx(ctx, tx, CurateWorkspaceJobArgs{WorkspaceID: workspaceID}, nil); err != nil {
		return false, fmt.Errorf("enqueue curate_workspace job: %w", err)
	}
	return true, nil
}

// EnqueueCurateWorkspace inserts a CurateWorkspaceJob outside an explicit transaction
// unless one is already queued for workspaceID.
func (s *Store) EnqueueCurateWorkspace(ctx context.Context, workspaceID string) (bool, error) {
	var queued bool
	if err := s.CurationPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM river_job
			WHERE kind = 'curate_workspace'
			  AND queue = 'curation'
			  AND state IN ('available', 'pending', 'scheduled')
			  AND args->>'workspace_id' = $1
		)
	`, workspaceID).Scan(&queued); err != nil {
		return false, fmt.Errorf("check queued curate_workspace job: %w", err)
	}
	if queued {
		return false, nil
	}
	if _, err := s.RiverInsert.Insert(ctx, CurateWorkspaceJobArgs{WorkspaceID: workspaceID}, nil); err != nil {
		return false, fmt.Errorf("enqueue curate_workspace job: %w", err)
	}
	return true, nil
}
