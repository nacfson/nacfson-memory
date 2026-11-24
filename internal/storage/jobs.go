package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/nacfson/nacfson-memory/internal/security"
)

const (
	QueueCuration    = "curation"
	QueueEmbedding   = "embedding"
	QueueMaintenance = "maintenance"
)

var (
	ErrMigrationRequired   = errors.New("database schema not migrated (run 'mem1 migrate')")
	ErrNotFound            = errors.New("not found")
	ErrConflict            = errors.New("conflict")
	ErrVersionConflict     = fmt.Errorf("version mismatch: %w", ErrConflict)
	ErrTopicConflict       = fmt.Errorf("active topic_key already exists: %w", ErrConflict)
	ErrRevisionPruned      = fmt.Errorf("revision body has been pruned: %w", ErrConflict)
	ErrInactiveMemory      = fmt.Errorf("memory is not active: %w", ErrConflict)
	ErrIdempotencyMismatch = errors.New("idempotency_key reused with different content")
	ErrInvalidWorkspaceID  = errors.New("invalid workspace_id")
	ErrInvalidInput        = errors.New("invalid input")
	ErrInvalidToken        = errors.New("invalid, expired, or already-consumed confirmation_token")
)

// Redaction aliases security.Redaction so callers share a single type without import cycles.
type Redaction = security.Redaction

// DBTX abstracts pgx pools, connections, and transactions for shared queries.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// CurateWorkspaceJobArgs triggers serialized curation for a single workspace.
type CurateWorkspaceJobArgs struct {
	WorkspaceID string `json:"workspace_id"`
}

func (CurateWorkspaceJobArgs) Kind() string { return "curate_workspace" }

func (CurateWorkspaceJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       QueueCuration,
		MaxAttempts: 3,
	}
}

// EmbedMemoryJobArgs asynchronously generates and persists the halfvec embedding
// for a committed manifest version.
type EmbedMemoryJobArgs struct {
	WorkspaceID string    `json:"workspace_id"`
	MemoryID    uuid.UUID `json:"memory_id"`
	Version     int       `json:"version"`
}

func (EmbedMemoryJobArgs) Kind() string { return "embed_memory" }

func (EmbedMemoryJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       QueueEmbedding,
		MaxAttempts: 5,
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
			ByState: []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRunning,
				rivertype.JobStateRetryable,
				rivertype.JobStateScheduled,
			},
		},
	}
}

// ProvisionalReplayJobArgs periodically re-queues provisional inbox entries once
// the curator recovers.
type ProvisionalReplayJobArgs struct{}

func (ProvisionalReplayJobArgs) Kind() string { return "provisional_replay" }

func (ProvisionalReplayJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       QueueMaintenance,
		MaxAttempts: 3,
	}
}
