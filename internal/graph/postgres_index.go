package graph

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/nacfson/nacfson-memory/internal/storage"
)

const MaxActiveOutgoingEdges = 5

var (
	ErrSelfEdge           = errors.New("self-dependency edge is forbidden")
	ErrCrossWorkspaceEdge = errors.New("dependency target does not exist in workspace")
	ErrEdgeCycle          = errors.New("dependency graph cycle detected")
	ErrTooManyEdges       = fmt.Errorf("maximum %d active outgoing DEPENDS_ON edges exceeded", MaxActiveOutgoingEdges)
	ErrEmptyRationale     = errors.New("dependency edge rationale is required")
	ErrDuplicateEdge      = errors.New("duplicate dependency target in batch")
)

// EdgeSpec describes an outgoing DEPENDS_ON edge to upsert on a memory revision.
type EdgeSpec struct {
	TargetMemoryID uuid.UUID `json:"target_memory_id"`
	ExpectedToRev  int       `json:"expected_to_rev"`
	Rationale      string    `json:"rationale"`
}

// OutgoingEdge captures one active outgoing DEPENDS_ON edge along with computed
// read-time premise drift status and optional active replacement stub.
type OutgoingEdge struct {
	EdgeID                 uuid.UUID  `json:"edge_id"`
	FromMemoryID           uuid.UUID  `json:"from_memory_id"`
	FromRev                int        `json:"from_rev"`
	ToMemoryID             uuid.UUID  `json:"to_memory_id"`
	ExpectedToRev          int        `json:"expected_to_rev"`
	Rationale              string     `json:"rationale"`
	TargetTopicKey         string     `json:"target_topic_key"`
	TargetSummaryStub      string     `json:"target_summary_stub"`
	TargetStatus           string     `json:"target_status"`
	TargetCurrentRev       int        `json:"target_current_rev"`
	TargetLastRevertedRev  int        `json:"target_last_reverted_rev"`
	PremiseDrifted         bool       `json:"premise_drifted"`
	ReplacementMemoryID    *uuid.UUID `json:"replacement_memory_id,omitempty"`
	ReplacementTopicKey    string     `json:"replacement_topic_key,omitempty"`
	ReplacementSummaryStub string     `json:"replacement_summary_stub,omitempty"`
	ReplacementVersion     int        `json:"replacement_version,omitempty"`
}

// IncomingEdge describes an active reverse DEPENDS_ON link referencing a target memory.
type IncomingEdge struct {
	EdgeID          uuid.UUID `json:"edge_id"`
	FromMemoryID    uuid.UUID `json:"from_memory_id"`
	FromRev         int       `json:"from_rev"`
	FromTopicKey    string    `json:"from_topic_key"`
	FromSummaryStub string    `json:"from_summary_stub"`
	FromStatus      string    `json:"from_status"`
	ExpectedToRev   int       `json:"expected_to_rev"`
	Rationale       string    `json:"rationale"`
	PremiseDrifted  bool      `json:"premise_drifted"`
}

// DependencyGraph manages revision-pinned intra-workspace DEPENDS_ON links and
// read-time premise drift computation.
type DependencyGraph struct{}

// NewDependencyGraph returns a DependencyGraph helper.
func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{}
}

// LookupOutgoing resolves active outgoing DEPENDS_ON edges for fromMemoryIDs within
// workspaceID on the shared DBTX connection, computing premise_drifted and tracing
// superseded targets up to 10 hops to surface any active replacement stub.
func (g *DependencyGraph) LookupOutgoing(ctx context.Context, db storage.DBTX, workspaceID string, fromMemoryIDs []uuid.UUID) (map[uuid.UUID][]OutgoingEdge, error) {
	result := make(map[uuid.UUID][]OutgoingEdge)
	if len(fromMemoryIDs) == 0 {
		return result, nil
	}

	rows, err := db.Query(ctx, `
		WITH RECURSIVE replacement_chain AS (
			SELECT
				e.edge_id,
				tgt.memory_id AS cur_id,
				tgt.superseded_by_memory_id,
				tgt.status,
				tgt.topic_key,
				tgt.summary_stub,
				tgt.current_version,
				0 AS depth,
				ARRAY[tgt.memory_id] AS visited
			FROM memory_edges e
			JOIN memory_manifest tgt ON tgt.memory_id = e.to_memory_id
			WHERE e.workspace_id = $1
			  AND e.from_memory_id = ANY($2)
			  AND e.status = 'active'
			  AND tgt.status = 'superseded'
			  AND tgt.superseded_by_memory_id IS NOT NULL
			UNION ALL
			SELECT
				rc.edge_id,
				nxt.memory_id,
				nxt.superseded_by_memory_id,
				nxt.status,
				nxt.topic_key,
				nxt.summary_stub,
				nxt.current_version,
				rc.depth + 1,
				rc.visited || nxt.memory_id
			FROM replacement_chain rc
			JOIN memory_manifest nxt ON nxt.memory_id = rc.superseded_by_memory_id
			WHERE rc.depth < 10
			  AND rc.status = 'superseded'
			  AND NOT (nxt.memory_id = ANY(rc.visited))
		),
		active_replacement AS (
			SELECT DISTINCT ON (edge_id)
				edge_id,
				cur_id AS replacement_memory_id,
				topic_key AS replacement_topic_key,
				summary_stub AS replacement_summary_stub,
				current_version AS replacement_version
			FROM replacement_chain
			WHERE status = 'active'
			ORDER BY edge_id, depth ASC
		)
		SELECT
			e.edge_id,
			e.from_memory_id,
			e.from_rev,
			e.to_memory_id,
			e.expected_to_rev,
			e.rationale,
			tgt.topic_key,
			tgt.summary_stub,
			tgt.status,
			tgt.current_version,
			tgt.last_reverted_version,
			(tgt.status <> 'active' OR e.expected_to_rev < tgt.last_reverted_version) AS premise_drifted,
			ar.replacement_memory_id,
			COALESCE(ar.replacement_topic_key, ''),
			COALESCE(ar.replacement_summary_stub, ''),
			COALESCE(ar.replacement_version, 0)
		FROM memory_edges e
		JOIN memory_manifest tgt ON tgt.memory_id = e.to_memory_id
		LEFT JOIN active_replacement ar ON ar.edge_id = e.edge_id
		WHERE e.workspace_id = $1
		  AND e.from_memory_id = ANY($2)
		  AND e.status = 'active'
		ORDER BY e.from_memory_id ASC, e.created_at ASC, e.edge_id ASC
	`, workspaceID, fromMemoryIDs)
	if err != nil {
		return nil, fmt.Errorf("lookup outgoing edges: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var edge OutgoingEdge
		if err := rows.Scan(
			&edge.EdgeID,
			&edge.FromMemoryID,
			&edge.FromRev,
			&edge.ToMemoryID,
			&edge.ExpectedToRev,
			&edge.Rationale,
			&edge.TargetTopicKey,
			&edge.TargetSummaryStub,
			&edge.TargetStatus,
			&edge.TargetCurrentRev,
			&edge.TargetLastRevertedRev,
			&edge.PremiseDrifted,
			&edge.ReplacementMemoryID,
			&edge.ReplacementTopicKey,
			&edge.ReplacementSummaryStub,
			&edge.ReplacementVersion,
		); err != nil {
			return nil, fmt.Errorf("scan outgoing edge: %w", err)
		}
		result[edge.FromMemoryID] = append(result[edge.FromMemoryID], edge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outgoing edges: %w", err)
	}
	return result, nil
}

// LookupReverseDependents returns active incoming DEPENDS_ON links referencing toMemoryID.
func (g *DependencyGraph) LookupReverseDependents(ctx context.Context, db storage.DBTX, workspaceID string, toMemoryID uuid.UUID) ([]IncomingEdge, error) {
	rows, err := db.Query(ctx, `
		SELECT
			e.edge_id,
			e.from_memory_id,
			e.from_rev,
			src.topic_key,
			src.summary_stub,
			src.status,
			e.expected_to_rev,
			e.rationale,
			(tgt.status <> 'active' OR e.expected_to_rev < tgt.last_reverted_version) AS premise_drifted
		FROM memory_edges e
		JOIN memory_manifest src ON src.memory_id = e.from_memory_id
		JOIN memory_manifest tgt ON tgt.memory_id = e.to_memory_id
		WHERE e.workspace_id = $1
		  AND e.to_memory_id = $2
		  AND e.status = 'active'
		ORDER BY src.topic_key ASC, e.edge_id ASC
	`, workspaceID, toMemoryID)
	if err != nil {
		return nil, fmt.Errorf("lookup reverse dependents: %w", err)
	}
	defer rows.Close()

	var result []IncomingEdge
	for rows.Next() {
		var edge IncomingEdge
		if err := rows.Scan(
			&edge.EdgeID,
			&edge.FromMemoryID,
			&edge.FromRev,
			&edge.FromTopicKey,
			&edge.FromSummaryStub,
			&edge.FromStatus,
			&edge.ExpectedToRev,
			&edge.Rationale,
			&edge.PremiseDrifted,
		); err != nil {
			return nil, fmt.Errorf("scan incoming edge: %w", err)
		}
		result = append(result, edge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reverse dependents: %w", err)
	}
	return result, nil
}

// UpsertEdges updates outgoing active DEPENDS_ON edges for fromMemoryID.
// When dependsOn == nil, existing active edges are preserved unchanged.
// When dependsOn is non-nil (including empty slice), existing active edges are retired
// and replaced atomically after validating caps, workspace ownership, and DAG acyclicity.
func (g *DependencyGraph) UpsertEdges(ctx context.Context, tx pgx.Tx, workspaceID string, fromMemoryID uuid.UUID, fromRev int, dependsOn []EdgeSpec) error {
	if dependsOn == nil {
		return nil
	}
	if len(dependsOn) > MaxActiveOutgoingEdges {
		return fmt.Errorf("%w: got %d", ErrTooManyEdges, len(dependsOn))
	}

	targets := make([]uuid.UUID, 0, len(dependsOn))
	seenTargets := make(map[uuid.UUID]struct{}, len(dependsOn))
	for _, spec := range dependsOn {
		if spec.TargetMemoryID == uuid.Nil {
			return fmt.Errorf("%w: nil target_memory_id", ErrCrossWorkspaceEdge)
		}
		if spec.TargetMemoryID == fromMemoryID {
			return ErrSelfEdge
		}
		if strings.TrimSpace(spec.Rationale) == "" {
			return ErrEmptyRationale
		}
		if _, dup := seenTargets[spec.TargetMemoryID]; dup {
			return fmt.Errorf("%w: %s", ErrDuplicateEdge, spec.TargetMemoryID)
		}
		seenTargets[spec.TargetMemoryID] = struct{}{}
		targets = append(targets, spec.TargetMemoryID)
	}

	if len(targets) > 0 {
		var foundCount int
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM memory_manifest
			WHERE workspace_id = $1 AND memory_id = ANY($2)
		`, workspaceID, targets).Scan(&foundCount); err != nil {
			return fmt.Errorf("verify edge targets in workspace: %w", err)
		}
		if foundCount != len(targets) {
			return fmt.Errorf("%w: expected %d targets in workspace %q, found %d", ErrCrossWorkspaceEdge, len(targets), workspaceID, foundCount)
		}

		existing, err := loadWorkspaceEdges(ctx, tx, workspaceID)
		if err != nil {
			return err
		}
		// Simulate replacement of fromMemoryID's active outgoing edges.
		delete(existing, fromMemoryID)
		if err := ValidateBatchDAG(existing, map[uuid.UUID][]uuid.UUID{fromMemoryID: targets}); err != nil {
			return err
		}
	}

	if err := g.RetireOutgoingEdges(ctx, tx, workspaceID, fromMemoryID); err != nil {
		return err
	}

	for _, spec := range dependsOn {
		edgeID, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("generate edge_id UUIDv7: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO memory_edges (
				edge_id, workspace_id, from_memory_id, from_rev, to_memory_id,
				expected_to_rev, status, rationale
			)
			VALUES ($1, $2, $3, $4, $5, $6, 'active', $7)
			ON CONFLICT (from_memory_id, from_rev, to_memory_id)
			DO UPDATE SET
				expected_to_rev = EXCLUDED.expected_to_rev,
				status = 'active',
				rationale = EXCLUDED.rationale,
				updated_at = now()
		`, edgeID, workspaceID, fromMemoryID, fromRev, spec.TargetMemoryID, spec.ExpectedToRev, spec.Rationale); err != nil {
			return fmt.Errorf("upsert memory_edge (%s -> %s): %w", fromMemoryID, spec.TargetMemoryID, err)
		}
	}

	return nil
}

// RetireOutgoingEdges marks all active outgoing DEPENDS_ON edges from fromMemoryID retired.
func (g *DependencyGraph) RetireOutgoingEdges(ctx context.Context, tx pgx.Tx, workspaceID string, fromMemoryID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `
		UPDATE memory_edges
		SET status = 'retired', updated_at = now()
		WHERE workspace_id = $1 AND from_memory_id = $2 AND status = 'active'
	`, workspaceID, fromMemoryID); err != nil {
		return fmt.Errorf("retire outgoing edges for %s: %w", fromMemoryID, err)
	}
	return nil
}

// ValidateBatchDAG checks existing active workspace edges combined with candidate batch
// additions for cycles or self-loops using three-color DFS.
func ValidateBatchDAG(existing map[uuid.UUID][]uuid.UUID, batchAdditions map[uuid.UUID][]uuid.UUID) error {
	adj := make(map[uuid.UUID][]uuid.UUID, len(existing)+len(batchAdditions))
	nodes := make(map[uuid.UUID]struct{}, len(existing)+len(batchAdditions))

	addEdges := func(src map[uuid.UUID][]uuid.UUID) error {
		for from, tos := range src {
			nodes[from] = struct{}{}
			for _, to := range tos {
				if from == to {
					return ErrSelfEdge
				}
				nodes[to] = struct{}{}
				adj[from] = append(adj[from], to)
			}
		}
		return nil
	}

	if err := addEdges(existing); err != nil {
		return err
	}
	if err := addEdges(batchAdditions); err != nil {
		return err
	}

	const (
		unvisited uint8 = 0
		visiting  uint8 = 1
		visited   uint8 = 2
	)
	state := make(map[uuid.UUID]uint8, len(nodes))

	var dfs func(uuid.UUID) error
	dfs = func(u uuid.UUID) error {
		state[u] = visiting
		for _, v := range adj[u] {
			switch state[v] {
			case visiting:
				return fmt.Errorf("%w: %s -> %s", ErrEdgeCycle, u, v)
			case unvisited:
				if err := dfs(v); err != nil {
					return err
				}
			}
		}
		state[u] = visited
		return nil
	}

	for n := range nodes {
		if state[n] == unvisited {
			if err := dfs(n); err != nil {
				return err
			}
		}
	}
	return nil
}

func loadWorkspaceEdges(ctx context.Context, db storage.DBTX, workspaceID string) (map[uuid.UUID][]uuid.UUID, error) {
	rows, err := db.Query(ctx, `
		SELECT from_memory_id, to_memory_id
		FROM memory_edges
		WHERE workspace_id = $1 AND status = 'active'
	`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("load active workspace edges: %w", err)
	}
	defer rows.Close()

	edges := make(map[uuid.UUID][]uuid.UUID)
	for rows.Next() {
		var fromID, toID uuid.UUID
		if err := rows.Scan(&fromID, &toID); err != nil {
			return nil, fmt.Errorf("scan workspace edge: %w", err)
		}
		edges[fromID] = append(edges[fromID], toID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace edges: %w", err)
	}
	return edges, nil
}
