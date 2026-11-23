package storage

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	WorkspaceGeneral = "general"

	KindGeneral = "general"
	KindProject = "project"

	InheritConstitutionAndDurable = "constitution_and_durable"
	InheritConstitutionOnly       = "constitution_only"
	InheritNone                   = "none"
)

var workspaceIDRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// Workspace models a row in the workspaces table.
type Workspace struct {
	WorkspaceID          string    `json:"workspace_id"`
	Kind                 string    `json:"kind"`
	InheritGeneral       string    `json:"inherit_general"`
	ManifestRevision     int64     `json:"manifest_revision"`
	ConstitutionRevision int64     `json:"constitution_revision"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

// WorkspaceSummary extends Workspace with active memory counts for inspection routes.
type WorkspaceSummary struct {
	Workspace
	ActiveCount int64 `json:"active_count"`
}

// WorkspaceRevisions captures locked revision watermarks for atomic ETag transitions.
type WorkspaceRevisions struct {
	ManifestRevision     int64 `json:"manifest_revision"`
	ConstitutionRevision int64 `json:"constitution_revision"`
}

// ValidateWorkspaceID checks the PostgreSQL CHECK constraint pattern ^[a-z0-9][a-z0-9_-]{0,63}$.
func ValidateWorkspaceID(id string) error {
	if !workspaceIDRegex.MatchString(id) {
		return fmt.Errorf("%w: %q must match ^[a-z0-9][a-z0-9_-]{0,63}$", ErrInvalidWorkspaceID, id)
	}
	return nil
}

// IsValidInheritGeneral checks allowed workspace inheritance modes.
func IsValidInheritGeneral(mode string) bool {
	switch mode {
	case InheritConstitutionAndDurable, InheritConstitutionOnly, InheritNone:
		return true
	default:
		return false
	}
}

// EnsureWorkspace idempotently provisions a valid project workspace (or ensures 'general' exists).
func EnsureWorkspace(ctx context.Context, db DBTX, workspaceID string) error {
	if err := ValidateWorkspaceID(workspaceID); err != nil {
		return err
	}
	if workspaceID == WorkspaceGeneral {
		_, err := db.Exec(ctx, `
			INSERT INTO workspaces (workspace_id, kind, inherit_general, manifest_revision, constitution_revision)
			VALUES ('general', 'general', 'none', 0, 0)
			ON CONFLICT (workspace_id) DO NOTHING
		`)
		return err
	}
	_, err := db.Exec(ctx, `
		INSERT INTO workspaces (workspace_id, kind, inherit_general, manifest_revision, constitution_revision)
		VALUES ($1, 'project', 'constitution_and_durable', 0, 0)
		ON CONFLICT (workspace_id) DO NOTHING
	`, workspaceID)
	return err
}

// CreateWorkspace explicitly creates a new project workspace, returning ErrConflict on duplicates.
func CreateWorkspace(ctx context.Context, db DBTX, workspaceID, inheritGeneral string) (Workspace, error) {
	if err := ValidateWorkspaceID(workspaceID); err != nil {
		return Workspace{}, err
	}
	if workspaceID == WorkspaceGeneral {
		return Workspace{}, fmt.Errorf("reserved workspace 'general' already exists: %w", ErrConflict)
	}
	if inheritGeneral == "" {
		inheritGeneral = InheritConstitutionAndDurable
	}
	if !IsValidInheritGeneral(inheritGeneral) {
		return Workspace{}, fmt.Errorf("invalid inherit_general %q: %w", inheritGeneral, ErrInvalidInput)
	}

	var ws Workspace
	err := db.QueryRow(ctx, `
		INSERT INTO workspaces (workspace_id, kind, inherit_general)
		VALUES ($1, 'project', $2)
		RETURNING workspace_id, kind, inherit_general, manifest_revision, constitution_revision, created_at, updated_at
	`, workspaceID, inheritGeneral).Scan(
		&ws.WorkspaceID,
		&ws.Kind,
		&ws.InheritGeneral,
		&ws.ManifestRevision,
		&ws.ConstitutionRevision,
		&ws.CreatedAt,
		&ws.UpdatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return Workspace{}, fmt.Errorf("workspace %q already exists: %w", workspaceID, ErrConflict)
		}
		return Workspace{}, fmt.Errorf("create workspace: %w", err)
	}
	return ws, nil
}

// GetWorkspace fetches one workspace by ID.
func GetWorkspace(ctx context.Context, db DBTX, workspaceID string) (Workspace, error) {
	var ws Workspace
	err := db.QueryRow(ctx, `
		SELECT workspace_id, kind, inherit_general, manifest_revision, constitution_revision, created_at, updated_at
		FROM workspaces
		WHERE workspace_id = $1
	`, workspaceID).Scan(
		&ws.WorkspaceID,
		&ws.Kind,
		&ws.InheritGeneral,
		&ws.ManifestRevision,
		&ws.ConstitutionRevision,
		&ws.CreatedAt,
		&ws.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Workspace{}, fmt.Errorf("workspace %q %w", workspaceID, ErrNotFound)
		}
		return Workspace{}, fmt.Errorf("get workspace: %w", err)
	}
	return ws, nil
}

// ListWorkspaces returns all workspaces with active memory counts.
func ListWorkspaces(ctx context.Context, db DBTX) ([]WorkspaceSummary, error) {
	rows, err := db.Query(ctx, `
		SELECT
			w.workspace_id,
			w.kind,
			w.inherit_general,
			w.manifest_revision,
			w.constitution_revision,
			w.created_at,
			w.updated_at,
			COALESCE(COUNT(m.memory_id), 0) AS active_count
		FROM workspaces w
		LEFT JOIN memory_manifest m
		       ON m.workspace_id = w.workspace_id AND m.status = 'active'
		GROUP BY w.workspace_id
		ORDER BY (w.workspace_id <> 'general'), w.workspace_id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list workspaces: %w", err)
	}
	defer rows.Close()

	var result []WorkspaceSummary
	for rows.Next() {
		var ws WorkspaceSummary
		if err := rows.Scan(
			&ws.WorkspaceID,
			&ws.Kind,
			&ws.InheritGeneral,
			&ws.ManifestRevision,
			&ws.ConstitutionRevision,
			&ws.CreatedAt,
			&ws.UpdatedAt,
			&ws.ActiveCount,
		); err != nil {
			return nil, fmt.Errorf("scan workspace summary: %w", err)
		}
		result = append(result, ws)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspaces: %w", err)
	}
	return result, nil
}

// UpdateInheritGeneral updates the inherit_general policy for a workspace.
func UpdateInheritGeneral(ctx context.Context, db DBTX, workspaceID, inheritGeneral string) (Workspace, error) {
	if err := ValidateWorkspaceID(workspaceID); err != nil {
		return Workspace{}, err
	}
	if !IsValidInheritGeneral(inheritGeneral) {
		return Workspace{}, fmt.Errorf("invalid inherit_general %q: %w", inheritGeneral, ErrInvalidInput)
	}
	if workspaceID == WorkspaceGeneral && inheritGeneral != InheritNone {
		return Workspace{}, fmt.Errorf("workspace 'general' must retain inherit_general='none': %w", ErrInvalidInput)
	}

	var ws Workspace
	err := db.QueryRow(ctx, `
		UPDATE workspaces
		SET inherit_general = $2, updated_at = now()
		WHERE workspace_id = $1
		RETURNING workspace_id, kind, inherit_general, manifest_revision, constitution_revision, created_at, updated_at
	`, workspaceID, inheritGeneral).Scan(
		&ws.WorkspaceID,
		&ws.Kind,
		&ws.InheritGeneral,
		&ws.ManifestRevision,
		&ws.ConstitutionRevision,
		&ws.CreatedAt,
		&ws.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Workspace{}, fmt.Errorf("workspace %q %w", workspaceID, ErrNotFound)
		}
		return Workspace{}, fmt.Errorf("update workspace inherit_general: %w", err)
	}
	return ws, nil
}

// LockWorkspaceTx serializes commit-phase mutations on workspace_id with FOR UPDATE.
func LockWorkspaceTx(ctx context.Context, tx pgx.Tx, workspaceID string) (WorkspaceRevisions, error) {
	var rev WorkspaceRevisions
	err := tx.QueryRow(ctx, `
		SELECT manifest_revision, constitution_revision
		FROM workspaces
		WHERE workspace_id = $1
		FOR UPDATE
	`, workspaceID).Scan(&rev.ManifestRevision, &rev.ConstitutionRevision)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkspaceRevisions{}, fmt.Errorf("workspace %q %w", workspaceID, ErrNotFound)
		}
		return WorkspaceRevisions{}, fmt.Errorf("lock workspace %q: %w", workspaceID, err)
	}
	return rev, nil
}

// BumpRevisionTx increments manifest_revision and optionally constitution_revision.
func BumpRevisionTx(ctx context.Context, tx pgx.Tx, workspaceID string, bumpConstitution bool) (WorkspaceRevisions, error) {
	var rev WorkspaceRevisions
	err := tx.QueryRow(ctx, `
		UPDATE workspaces
		SET manifest_revision = manifest_revision + 1,
		    constitution_revision = constitution_revision + CASE WHEN $2 THEN 1 ELSE 0 END,
		    updated_at = now()
		WHERE workspace_id = $1
		RETURNING manifest_revision, constitution_revision
	`, workspaceID, bumpConstitution).Scan(&rev.ManifestRevision, &rev.ConstitutionRevision)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkspaceRevisions{}, fmt.Errorf("workspace %q %w", workspaceID, ErrNotFound)
		}
		return WorkspaceRevisions{}, fmt.Errorf("bump revision for %q: %w", workspaceID, err)
	}
	return rev, nil
}

// ComputeConstitutionETag formats a segmented weak ETag for merged constitution state.
func ComputeConstitutionETag(wsConstRev, genConstRev int64, inheritGeneral string) string {
	if inheritGeneral == InheritNone {
		return fmt.Sprintf(`W/"const:%d"`, wsConstRev)
	}
	return fmt.Sprintf(`W/"const:%d+gen:%d"`, wsConstRev, genConstRev)
}

// ComputeMetadataETag formats a segmented weak ETag for workspace recall metadata.
func ComputeMetadataETag(wsRev, genRev int64, inheritGeneral string) string {
	if inheritGeneral == InheritNone {
		return fmt.Sprintf(`W/"ws:%d"`, wsRev)
	}
	return fmt.Sprintf(`W/"ws:%d+gen:%d"`, wsRev, genRev)
}
