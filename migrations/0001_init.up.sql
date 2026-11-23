-- Extension bootstrap (executed before pgxpool registers pgvector types)
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 0. Migration version tracking
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INT         PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 1. Workspaces & commit-safe per-workspace revision watermarks (zero SEQUENCE objects)
CREATE TABLE IF NOT EXISTS workspaces (
    workspace_id          TEXT        PRIMARY KEY,
    kind                  TEXT        NOT NULL CHECK (kind IN ('general', 'project')),
    inherit_general       TEXT        NOT NULL DEFAULT 'constitution_and_durable'
                                      CHECK (inherit_general IN ('constitution_and_durable', 'constitution_only', 'none')),
    manifest_revision     BIGINT      NOT NULL DEFAULT 0,
    constitution_revision BIGINT      NOT NULL DEFAULT 0,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (workspace_id ~ '^[a-z0-9][a-z0-9_-]{0,63}$'),
    CHECK ((workspace_id = 'general') = (kind = 'general')),
    CHECK (kind <> 'general' OR inherit_general = 'none')
);

INSERT INTO workspaces (workspace_id, kind, inherit_general, manifest_revision, constitution_revision)
VALUES ('general', 'general', 'none', 0, 0)
ON CONFLICT (workspace_id) DO NOTHING;

-- 2. Unpartitioned append-only raw inbox (agent_observed turns; River owns worker lease state)
CREATE TABLE IF NOT EXISTS raw_inbox (
    inbox_id         UUID        PRIMARY KEY,
    workspace_id     TEXT        NOT NULL REFERENCES workspaces(workspace_id),
    agent_id         TEXT,
    source_authority TEXT        NOT NULL DEFAULT 'agent_observed'
                                 CHECK (source_authority = 'agent_observed'),
    request_id       UUID        NOT NULL,
    idempotency_key  TEXT,
    content          TEXT        NOT NULL,
    content_sha256   BYTEA       NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'pending'
                                 CHECK (status IN ('pending', 'curated', 'provisional', 'dead_letter')),
    last_error       TEXT,
    occurred_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE raw_inbox ALTER COLUMN content SET COMPRESSION lz4;

CREATE INDEX IF NOT EXISTS idx_raw_inbox_pending
    ON raw_inbox (workspace_id, occurred_at, ingested_at, inbox_id)
    WHERE status = 'pending';

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_inbox_idempotency
    ON raw_inbox (workspace_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- 3. Compacted active & retired per-memory manifest with inline halfvec(1536) embeddings
CREATE TABLE IF NOT EXISTS memory_manifest (
    memory_id               UUID         PRIMARY KEY,
    workspace_id            TEXT         NOT NULL REFERENCES workspaces(workspace_id),
    durability              TEXT         NOT NULL CHECK (durability IN ('constitution', 'durable', 'working')),
    topic_key               TEXT         NOT NULL,
    status                  TEXT         NOT NULL CHECK (status IN ('active', 'superseded', 'retired')),
    curation_state          TEXT         NOT NULL CHECK (curation_state IN ('curator_verified', 'provisional_heuristic', 'user_explicit')),
    current_version         INT          NOT NULL DEFAULT 1,
    summary_stub            TEXT         NOT NULL,
    body                    TEXT         NOT NULL,
    token_count             INT          NOT NULL CHECK (token_count >= 0 AND token_count <= 1500),
    embedding               HALFVEC(1536),
    embedding_space_id      TEXT,
    supersedes_memory_id    UUID         REFERENCES memory_manifest(memory_id) ON DELETE SET NULL,
    superseded_by_memory_id UUID         REFERENCES memory_manifest(memory_id) ON DELETE SET NULL,
    search_tsv              TSVECTOR     GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(topic_key, '') || ' ' || coalesce(summary_stub, '') || ' ' || coalesce(body, ''))
    ) STORED,
    last_reverted_version   INT          NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CHECK (durability <> 'constitution' OR curation_state = 'user_explicit'),
    CHECK (durability <> 'constitution' OR token_count <= 500)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_manifest_coupled_topic_active
    ON memory_manifest (workspace_id, topic_key)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_manifest_active_lookup
    ON memory_manifest (workspace_id, durability, status)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_manifest_embeddings_active_hnsw
    ON memory_manifest USING hnsw (embedding halfvec_cosine_ops)
    WHERE status = 'active' AND embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_manifest_tsv_active
    ON memory_manifest USING GIN (search_tsv)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_manifest_topic_trgm_active
    ON memory_manifest USING GIN (topic_key gin_trgm_ops)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_manifest_provisional
    ON memory_manifest (workspace_id, updated_at)
    WHERE curation_state = 'provisional_heuristic' AND status = 'active';

-- 4. Append-only per-memory revisions (metadata permanent until hard_purge; bodies pruned via history_retention_n)
CREATE TABLE IF NOT EXISTS memory_revisions (
    memory_id             UUID        NOT NULL REFERENCES memory_manifest(memory_id) ON DELETE CASCADE,
    version               INT         NOT NULL,
    workspace_id          TEXT        NOT NULL,
    durability            TEXT        NOT NULL,
    summary_stub          TEXT        NOT NULL,
    body                  TEXT,
    body_pruned           BOOLEAN     NOT NULL DEFAULT FALSE,
    restored_from_version INT,
    agent_id              TEXT,
    source_authority      TEXT        NOT NULL,
    curation_state        TEXT        NOT NULL,
    change_reason         TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (memory_id, version)
);

-- 5. Per-contributor & inbox citation provenance
CREATE TABLE IF NOT EXISTS memory_citations (
    memory_id  UUID        NOT NULL,
    version    INT         NOT NULL,
    inbox_id   UUID        NOT NULL REFERENCES raw_inbox(inbox_id) ON DELETE CASCADE,
    agent_id   TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (memory_id, version, inbox_id),
    FOREIGN KEY (memory_id, version) REFERENCES memory_revisions(memory_id, version) ON DELETE CASCADE
);

-- 6. Revision-pinned intra-workspace DEPENDS_ON edges (forward + reverse lookup)
CREATE TABLE IF NOT EXISTS memory_edges (
    edge_id         UUID        PRIMARY KEY,
    workspace_id    TEXT        NOT NULL REFERENCES workspaces(workspace_id),
    from_memory_id  UUID        NOT NULL REFERENCES memory_manifest(memory_id) ON DELETE CASCADE,
    from_rev        INT         NOT NULL,
    to_memory_id    UUID        NOT NULL REFERENCES memory_manifest(memory_id) ON DELETE CASCADE,
    expected_to_rev INT         NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'retired')),
    rationale       TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (from_memory_id <> to_memory_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_unique_rev_link
    ON memory_edges (from_memory_id, from_rev, to_memory_id);

CREATE INDEX IF NOT EXISTS idx_edges_forward_active
    ON memory_edges (workspace_id, from_memory_id)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_edges_reverse_active
    ON memory_edges (workspace_id, to_memory_id)
    WHERE status = 'active';

-- 7. Curation audit log (relational lineage across inbox_ids -> memory_id)
CREATE TABLE IF NOT EXISTS curation_decisions (
    decision_id       UUID        PRIMARY KEY,
    workspace_id      TEXT        NOT NULL,
    inbox_ids         UUID[]      NOT NULL DEFAULT '{}',
    action            TEXT        NOT NULL CHECK (action IN (
                          'create', 'revise', 'supersede', 'retire', 'noop',
                          'provisional_fallback', 'manual_edit', 'rollback', 'purge'
                      )),
    target_memory_id  UUID,
    target_version    INT,
    provider_name     TEXT,
    model_name        TEXT,
    prompt_tokens     INT         NOT NULL DEFAULT 0,
    completion_tokens INT         NOT NULL DEFAULT 0,
    rationale         TEXT        NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_curation_decisions_ws_time
    ON curation_decisions (workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_curation_decisions_target
    ON curation_decisions (target_memory_id)
    WHERE target_memory_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_curation_decisions_inbox_gin
    ON curation_decisions USING GIN (inbox_ids);

-- 8. Cross-process single-use confirmation tokens for privileged MCP mutations
CREATE TABLE IF NOT EXISTS confirmation_tokens (
    token_sha256     BYTEA       PRIMARY KEY,
    workspace_id     TEXT        NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    action           TEXT        NOT NULL CHECK (action IN ('create', 'edit', 'promote', 'revert', 'purge')),
    target_memory_id UUID,
    expires_at       TIMESTAMPTZ NOT NULL,
    consumed_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_confirm_tokens_expires
    ON confirmation_tokens (expires_at)
    WHERE consumed_at IS NULL;

INSERT INTO schema_migrations (version) VALUES (1) ON CONFLICT (version) DO NOTHING;
