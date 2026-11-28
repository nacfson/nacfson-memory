package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/nacfson/nacfson-memory/internal/config"
	"github.com/nacfson/nacfson-memory/migrations"
)

const migrationAdvisoryLockID int64 = 71042001

// Store owns split PostgreSQL pools (serving vs. curation/admin) and an insert-only River client.
type Store struct {
	ServingPool  *pgxpool.Pool
	CurationPool *pgxpool.Pool
	RiverInsert  *river.Client[pgx.Tx]
	cfg          config.Config
}

// PoolStatSnapshot captures runtime statistics for a single pgxpool.Pool.
type PoolStatSnapshot struct {
	TotalConns        int32 `json:"total_conns"`
	AcquiredConns     int32 `json:"acquired_conns"`
	IdleConns         int32 `json:"idle_conns"`
	ConstructingConns int32 `json:"constructing_conns"`
	MaxConns          int32 `json:"max_conns"`
	AcquireCount      int64 `json:"acquire_count"`
	EmptyAcquireCount int64 `json:"empty_acquire_count"`
}

// AllPoolStats reports snapshots of both pools.
type AllPoolStats struct {
	Serving  PoolStatSnapshot `json:"serving_pool"`
	Curation PoolStatSnapshot `json:"curation_pool"`
}

// InboxStatusCounts reports raw_inbox counts grouped by operational state.
type InboxStatusCounts struct {
	Pending           int64 `json:"pending"`
	Provisional       int64 `json:"provisional"`
	DeadLetter        int64 `json:"dead_letter"`
	ProvisionalActive int64 `json:"provisional_active"`
}

// RiverQueueStat reports River job counts by queue and state.
type RiverQueueStat struct {
	Queue string `json:"queue"`
	State string `json:"state"`
	Count int64  `json:"count"`
}

// DeadLetterInboxItem describes one dead-lettered raw_inbox record.
type DeadLetterInboxItem struct {
	InboxID        uuid.UUID `json:"inbox_id"`
	WorkspaceID    string    `json:"workspace_id"`
	LastError      string    `json:"last_error"`
	OccurredAt     time.Time `json:"occurred_at"`
	ContentPreview string    `json:"content_preview"`
}

// DiscardedRiverJob describes one discarded job in river_job. Errors mirrors River's
// jsonb[] errors column: one JSON object per failed attempt.
type DiscardedRiverJob struct {
	ID          int64             `json:"id"`
	Kind        string            `json:"kind"`
	Queue       string            `json:"queue"`
	Attempt     int               `json:"attempt"`
	Args        json.RawMessage   `json:"args"`
	Errors      []json.RawMessage `json:"errors,omitempty"`
	FinalizedAt *time.Time        `json:"finalized_at,omitempty"`
}

// DeadLettersReport combines dead-letter inbox items and discarded River jobs.
type DeadLettersReport struct {
	DeadLetters   []DeadLetterInboxItem `json:"dead_letters"`
	DiscardedJobs []DiscardedRiverJob   `json:"discarded_jobs"`
}

// RiverQueues returns the canonical worker queue concurrency settings for mem1 serve.
func RiverQueues() map[string]river.QueueConfig {
	return map[string]river.QueueConfig{
		QueueCuration:    {MaxWorkers: 4},
		QueueEmbedding:   {MaxWorkers: 4},
		QueueMaintenance: {MaxWorkers: 1},
	}
}

// Migrate runs schema and River migrations up (down=false) or rolls them back (down=true)
// under a dedicated PostgreSQL session advisory lock on a single pgx connection.
func Migrate(ctx context.Context, dsn string, down bool) error {
	if dsn == "" {
		return errors.New("database URL is required for migration")
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect for migration: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, migrationAdvisoryLockID)
	}()

	migrator, err := rivermigrate.New(riverpgxv5.New(nil), nil)
	if err != nil {
		return fmt.Errorf("create river migrator: %w", err)
	}

	if !down {
		upSQL, err := migrations.FS.ReadFile("0001_init.up.sql")
		if err != nil {
			return fmt.Errorf("read 0001_init.up.sql: %w", err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration tx: %w", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		if _, err := tx.Exec(ctx, string(upSQL)); err != nil {
			return fmt.Errorf("apply 0001_init.up.sql: %w", err)
		}
		if _, err := migrator.MigrateTx(ctx, tx, rivermigrate.DirectionUp, nil); err != nil {
			return fmt.Errorf("apply river up migrations: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit up migration: %w", err)
		}
		return nil
	}

	// Roll back River schema if present, then drop domain tables.
	var riverExists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass('public.river_migration') IS NOT NULL`).Scan(&riverExists); err != nil {
		return fmt.Errorf("check river_migration table: %w", err)
	}
	if riverExists {
		rtx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin river down tx: %w", err)
		}
		if _, err := migrator.MigrateTx(ctx, rtx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{TargetVersion: -1}); err != nil {
			_ = rtx.Rollback(ctx)
			return fmt.Errorf("rollback river migrations: %w", err)
		}
		if err := rtx.Commit(ctx); err != nil {
			return fmt.Errorf("commit river down migration: %w", err)
		}
	}

	downSQL, err := migrations.FS.ReadFile("0001_init.down.sql")
	if err != nil {
		return fmt.Errorf("read 0001_init.down.sql: %w", err)
	}
	dtx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin domain down tx: %w", err)
	}
	defer func() { _ = dtx.Rollback(ctx) }()

	if _, err := dtx.Exec(ctx, string(downSQL)); err != nil {
		return fmt.Errorf("apply 0001_init.down.sql: %w", err)
	}
	if err := dtx.Commit(ctx); err != nil {
		return fmt.Errorf("commit down migration: %w", err)
	}
	return nil
}

// OpenPools verifies schema migration state and opens isolated serving and curation pools
// together with an insert-only River client.
func OpenPools(ctx context.Context, cfg config.Config) (*Store, error) {
	if err := verifySchemaMigrated(ctx, cfg.DatabaseURL); err != nil {
		return nil, err
	}

	servingPool, err := openTypedPool(ctx, cfg.DatabaseURL, int32(cfg.EffectiveServingMaxConns()), "100ms")
	if err != nil {
		return nil, fmt.Errorf("open servingPool: %w", err)
	}

	curationPool, err := openTypedPool(ctx, cfg.DatabaseURL, int32(cfg.EffectiveCurationMaxConns()), "5000ms")
	if err != nil {
		servingPool.Close()
		return nil, fmt.Errorf("open curationPool: %w", err)
	}

	riverInsert, err := river.NewClient(riverpgxv5.New(curationPool), &river.Config{
		SkipUnknownJobCheck: true,
	})
	if err != nil {
		curationPool.Close()
		servingPool.Close()
		return nil, fmt.Errorf("init river insert client: %w", err)
	}

	return &Store{
		ServingPool:  servingPool,
		CurationPool: curationPool,
		RiverInsert:  riverInsert,
		cfg:          cfg,
	}, nil
}

func verifySchemaMigrated(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect for schema verification: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	var version int
	if err := conn.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		if isUndefinedTable(err) {
			return ErrMigrationRequired
		}
		return fmt.Errorf("check schema_migrations: %w", err)
	}
	if version < 1 {
		return ErrMigrationRequired
	}

	var riverJobExists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass('public.river_job') IS NOT NULL`).Scan(&riverJobExists); err != nil {
		return fmt.Errorf("check river_job table: %w", err)
	}
	if !riverJobExists {
		return ErrMigrationRequired
	}
	return nil
}

func openTypedPool(ctx context.Context, dsn string, maxConns int32, statementTimeout string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	if maxConns < 1 {
		maxConns = 1
	}
	poolCfg.MaxConns = maxConns
	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = statementTimeout
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return pgxvec.RegisterTypes(ctx, conn)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// Close closes both PostgreSQL pools.
func (s *Store) Close() {
	if s == nil {
		return
	}
	if s.ServingPool != nil {
		s.ServingPool.Close()
	}
	if s.CurationPool != nil {
		s.CurationPool.Close()
	}
}

// Ping verifies database connectivity on ServingPool.
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.ServingPool == nil {
		return errors.New("store not initialized")
	}
	return s.ServingPool.Ping(ctx)
}

// PoolStats returns runtime pool utilization metrics for servingPool and curationPool.
func (s *Store) PoolStats() AllPoolStats {
	return AllPoolStats{
		Serving:  snapshotPoolStat(s.ServingPool),
		Curation: snapshotPoolStat(s.CurationPool),
	}
}

func snapshotPoolStat(p *pgxpool.Pool) PoolStatSnapshot {
	if p == nil {
		return PoolStatSnapshot{}
	}
	st := p.Stat()
	return PoolStatSnapshot{
		TotalConns:        st.TotalConns(),
		AcquiredConns:     st.AcquiredConns(),
		IdleConns:         st.IdleConns(),
		ConstructingConns: st.ConstructingConns(),
		MaxConns:          st.MaxConns(),
		AcquireCount:      st.AcquireCount(),
		EmptyAcquireCount: st.EmptyAcquireCount(),
	}
}

// InboxCounts returns counts of pending, provisional, and dead-letter raw_inbox items
// plus active provisional manifest entries.
func (s *Store) InboxCounts(ctx context.Context) (InboxStatusCounts, error) {
	var counts InboxStatusCounts
	err := s.CurationPool.QueryRow(ctx, `
		SELECT
			COALESCE((SELECT count(*) FROM raw_inbox WHERE status = 'pending'), 0),
			COALESCE((SELECT count(*) FROM raw_inbox WHERE status = 'provisional'), 0),
			COALESCE((SELECT count(*) FROM raw_inbox WHERE status = 'dead_letter'), 0),
			COALESCE((SELECT count(*) FROM memory_manifest WHERE curation_state = 'provisional_heuristic' AND status = 'active'), 0)
	`).Scan(&counts.Pending, &counts.Provisional, &counts.DeadLetter, &counts.ProvisionalActive)
	if err != nil {
		return InboxStatusCounts{}, fmt.Errorf("query inbox counts: %w", err)
	}
	return counts, nil
}

// RiverQueueStats returns job counts grouped by queue and state.
func (s *Store) RiverQueueStats(ctx context.Context) ([]RiverQueueStat, error) {
	rows, err := s.CurationPool.Query(ctx, `
		SELECT queue, state::text, count(*)
		FROM river_job
		GROUP BY queue, state
		ORDER BY queue ASC, state ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query river queue stats: %w", err)
	}
	defer rows.Close()

	var stats []RiverQueueStat
	for rows.Next() {
		var st RiverQueueStat
		if err := rows.Scan(&st.Queue, &st.State, &st.Count); err != nil {
			return nil, fmt.Errorf("scan river queue stat: %w", err)
		}
		stats = append(stats, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate river queue stats: %w", err)
	}
	return stats, nil
}

// ListDeadLetters returns dead-lettered raw_inbox rows and discarded River jobs.
func (s *Store) ListDeadLetters(ctx context.Context, limit int) (DeadLettersReport, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	report := DeadLettersReport{
		DeadLetters:   []DeadLetterInboxItem{},
		DiscardedJobs: []DiscardedRiverJob{},
	}

	rows, err := s.CurationPool.Query(ctx, `
		SELECT inbox_id, workspace_id, COALESCE(last_error, ''), occurred_at, content
		FROM raw_inbox
		WHERE status = 'dead_letter'
		ORDER BY occurred_at DESC, inbox_id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return DeadLettersReport{}, fmt.Errorf("query dead-letter inbox: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item DeadLetterInboxItem
		var fullContent string
		if err := rows.Scan(&item.InboxID, &item.WorkspaceID, &item.LastError, &item.OccurredAt, &fullContent); err != nil {
			return DeadLettersReport{}, fmt.Errorf("scan dead-letter inbox: %w", err)
		}
		item.ContentPreview = truncateRunes(fullContent, 200)
		report.DeadLetters = append(report.DeadLetters, item)
	}
	if err := rows.Err(); err != nil {
		return DeadLettersReport{}, fmt.Errorf("iterate dead-letter inbox: %w", err)
	}

	jobRows, err := s.CurationPool.Query(ctx, `
		SELECT id, kind, queue, attempt, args, errors, finalized_at
		FROM river_job
		WHERE state = 'discarded'
		ORDER BY id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return DeadLettersReport{}, fmt.Errorf("query discarded river jobs: %w", err)
	}
	defer jobRows.Close()

	for jobRows.Next() {
		var job DiscardedRiverJob
		if err := jobRows.Scan(&job.ID, &job.Kind, &job.Queue, &job.Attempt, &job.Args, &job.Errors, &job.FinalizedAt); err != nil {
			return DeadLettersReport{}, fmt.Errorf("scan discarded river job: %w", err)
		}
		report.DiscardedJobs = append(report.DiscardedJobs, job)
	}
	if err := jobRows.Err(); err != nil {
		return DeadLettersReport{}, fmt.Errorf("iterate discarded river jobs: %w", err)
	}

	return report, nil
}

func truncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxRunes]) + "..."
}
