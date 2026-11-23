# mem1 (`nacfson-memory` v1): Curated Cross-Agent Memory Backend

*   **Author:** nacfson
*   **Status:** Accepted
*   **Date:** 2026-11-23

--------------------------------------------------------------------------------

## Overview

`mem1` (to be built in the `nacfson-memory` repository and Go module `github.com/nacfson/nacfson-memory`) is a single-binary Go and PostgreSQL 17 backend that refines raw session context from heterogeneous AI agents—such as local coding CLIs (Claude Code, Cursor, Codex CLI) and web assistants (ChatGPT, Claude Web, Grok)—into structured long-term memory for a single-tenant personal or single-team deployment.

Agents interact over MCP (`modelcontextprotocol/go-sdk`, Streamable HTTP on `/mcp` and dedicated `mem1 mcp-stdio`) as the primary agent interface, while operators, scripts, and CLI wrappers inspect and manually curate memory state through a unified HTTP JSON API on port `8787` (loopback default, with no Web UI built in v1). Foreground agent writes append without `memory_manifest` row locks into an unpartitioned PostgreSQL `raw_inbox` table and enqueue durable `riverqueue/river` jobs; reads stay fail-open within a `<= 2,000` token cap. A serialized background curation worker per workspace distills noisy transcripts, couples updates on the same project topic into one shared memory, maintains read-time `DEPENDS_ON` drift flags, and retires outdated temporary notes explicitly without time-based expiration timers.

Implementation follows a two-layer sequence within this v1 design:

1.  **Priority 1 (Context-to-Memory Refinement & Storage Organization):** Deliver PostgreSQL 17 storage, unified workspace scoping, read-time `DEPENDS_ON` drift tracking, and the background `Curator` pipeline, validated against `LongMemEval_S` and the `MemoryAgentBench` Conflict Resolution slice under `<= 2,000` tokens.
2.  **Priority 2 (`>= 100` Concurrent Serving & Observability API):** Deliver the unified HTTP/MCP server (`8787`), `mem1 mcp-stdio`, split connection pools, safe degradation paths, and open-loop `>= 100` concurrent request load/chaos verification.

This document formalizes and updates the design settled in [concepts_brief.md](../../concepts_brief.md), supersedes the 10-service Topology B blueprint in [mem1-architecture-and-research-report.html](../../mem1-architecture-and-research-report.html), and incorporates the failure audit in [evaluation.md](../../evaluation.md).

## The Problem

Engineers who use multiple coding CLIs and web assistants across projects encounter five recurring memory failures:

1.  **Fragmented per-tool silos:** Context shared in Claude Code, Cursor, Codex CLI, ChatGPT, Claude Web, or Grok stays locked inside each vendor's product. Users repeat the same personal preferences and project architecture choices every time they switch tools.
2.  **Blocking writes and threshold-only deduplication errors:** Calling an LLM synchronously on the write path stalls active agent turns, while threshold-only cosine deduplication (`cos >= 0.92`, audited in [evaluation.md](../../evaluation.md)) merges contradictory numeric or configuration updates instead of replacing them.
3.  **Clock-driven expiration bugs:** Time-based TTL expiration deletes still-useful project working notes on arbitrary timers and creates clock-versus-`ETag` drift between cached reads and expired records.
4.  **Orphaned downstream assumptions:** When a core project decision changes (for example, switching from SQLite to PostgreSQL 17), downstream notes that relied on the old choice (such as SQLite locking flags or `.db` backup scripts) remain active and unflagged in flat vector stores until contradicted in a subsequent session.
5.  **Operational sprawl versus observability gaps:** Distributed multi-service stacks (Valkey, NATS, Qdrant, Temporal, Neo4j, S3) add multi-service operational overhead for a personal or single-team deployment, while embedded local stores (SQLite WAL) contend on concurrent writes (`SQLITE_BUSY` / WAL checkpoint stalls), run linear vector scans, and lack HTTP/MCP inspection APIs.

**Proposed Solution:** Run one Go binary (`mem1`) backed by one PostgreSQL 17 node. Keep foreground inbox appends separate from serialized per-workspace background curation so agents never wait on `memory_manifest` write locks. Separate personal profile context (reserved workspace `'general'`, never storing technical decisions) from project workspaces (e.g., `'payments'`, where contributors couple updates onto one shared topic memory), track independent per-memory versions and `DEPENDS_ON` links inside `memory_manifest`, enforce `<= 2,000` tokens on agent reads using 1-line overflow stubs, and expose a unified HTTP/MCP API plus CLI to inspect and manually manage memory state.

## Goals

*   **Single-Binary Go + PostgreSQL 17 Deployment:** Build one Go binary (`mem1`, module `github.com/nacfson/nacfson-memory`, Go `1.27` matching the host Go `1.27.3` toolchain) backed by stock PostgreSQL 17 (`pgvector >= 0.8.0` active-only `halfvec(1536)` HNSW + built-in `tsvector` GIN lexical search + `riverqueue/river`), compatible with local Docker and managed cloud PostgreSQL (AWS RDS, Google Cloud SQL, Neon) without AGPL-3.0 plugins.
*   **Non-Blocking Foreground Writes & Serialized Background Curation:** Accept foreground agent writes (`<= 64 KiB` per request) via inline secret scrubbing, unpartitioned `raw_inbox` `INSERT`s, and transactional River enqueues with zero `409 Conflict` retries and zero foreground LLM calls. Serialize curation, topic coupling (`v1 -> v2 -> v3`), contradiction resolution, and explicit stale-note retirement inside one active River worker per workspace.
*   **Verified Context-to-Memory Refinement (Priority 1 Exit Gate):** Implement a pluggable structured-output `Curator` interface with deterministic provisional fallback (`curation_state = 'provisional_heuristic'` inside `working`, never touching `constitution` or verified entries). Pass Priority 1 evaluation gates under `<= 2,000` tokens: `0` token-cap breaches, `0` automated `constitution` or `'general'` boundary violations, `>= 85%` on `MemoryAgentBench` Conflict Resolution, and `>= 75%` overall accuracy on `LongMemEval_S`.
*   **Unified Workspace Scoping & Coupled Topic Memories:** Reserve workspace `'general'` strictly for user profile, language, location, career, and background (never technical decisions). Configure per-project workspaces with three `inherit_general` modes (`'constitution_and_durable'` default so agent-learned personal facts flow automatically to projects, `'constitution_only'`, and `'none'`), with project entries overriding `'general'` conflicts. Couple multi-agent updates on the same project topic into one shared active memory with per-contributor citations.
*   **3 Durability Layers & Constitution Protection:** Enforce `constitution` (permanent rules, `<= 500` tokens per entry, `user_explicit`-only writes and promotions), `durable` (project decisions active until superseded), and `working` (short-lived task state retired explicitly by `Curator` or users).
*   **Independent Revisions, Read-Time `DEPENDS_ON` Drift & Token Cap:** Maintain independent per-memory versions inside compacted `memory_manifest`, append-only cherry-pick rollbacks (`v5` with `restored_from_version = 2`), `MEM1_HISTORY_RETENTION_N = 20` revision-body pruning (`0` = unbounded; metadata kept until purge), and `DEPENDS_ON` links pinned to `(from_rev, expected_to_rev)`. Compute `premise_drifted: true` without read locks strictly on target supersession, retirement, or revert (attaching active replacement stubs), and enforce `max_tokens <= 2,000` serialized output tokens on all agent context retrieval (`memory_recall`, `memory_expand`) and benchmark queries.
*   **Unified HTTP/MCP Surface (`8787`), Scoped Tokens & `mem1 mcp-stdio`:** Serve Streamable HTTP MCP (`/mcp`, authenticated via `MEM1_AGENT_TOKEN`) and HTTP JSON Observability & Manual Curation routes (`/v1/...`, always authenticated via `MEM1_ADMIN_TOKEN`) on one listener (`127.0.0.1:8787` default), gate privileged `user_explicit` MCP `memory_manage` actions with PostgreSQL-backed single-use operator `confirmation_token`s, and provide `mem1 mcp-stdio` for local coding CLIs without port or worker collisions.
*   **Deterministic Secret Scrubbing & Safe Degradation:** Strip known secret formats before PostgreSQL persistence or external LLM egress, require explicit zero-data-retention (ZDR) endpoint configuration, isolate external `Embedder` outages from curation dead-lettering, return `degraded: true` + `X-Memory-Degraded: true` + `Cache-Control: no-store` on passive recall failures (never `200 OK []`), and return `503` / JSON-RPC errors on explicit CRUD/admin failures.
*   **100+ Concurrent Serving (Priority 2 Exit Gate):** Sustain `>= 100` concurrent in-flight requests on one Go binary + single PG17 node with isolated `servingPool` (`24–32` conns, `100 ms` PostgreSQL/Go recall budget via 1 pipelined connection per recall) and `curationPool` (`4–8` conns, `5s`), passing `go test -race` chaos tests and uninstrumented `p95 <= 100 ms` PG/Go recall benchmarks.

## Non-Goals

*   **Multi-Tenant SaaS Isolation in v1:** v1 is strictly single-tenant (no `tenant_id` keys, PostgreSQL RLS, per-tenant scoped OAuth/RBAC, or per-tenant quotas). Hosted multi-tenant SaaS will be specified in a dedicated follow-up v2 design after Priority 1 quality passes.
*   **Visual Web UI / Frontend Process:** No web dashboard is built in v1; future UIs consume the `/v1/...` HTTP JSON API.
*   **Time-Based TTL Memory Expiration:** No clock-driven expiration timers; temporary notes are retired explicitly.
*   **Per-Agent Conflict Alerting (`memory_alerts`):** Deferred to Phase 2+ after Priority 1 context-to-memory refinement is validated (original author intent and re-entry trigger preserved in [concepts_brief.md](../../concepts_brief.md#L147-L150)).
*   **Two-Stage Zero-Output-Token Decision Models (`gemini-decision-1` / `TypeSafe Jev`):** `gemini-decision-1` (Google's `v1beta` preview closed-set decision API) and `TypeSafe Jev` (closed-set classifier lacking a Go SDK) cannot extract atomic claims from raw transcripts and still require a generative `Curator`. Deferred behind a future batched `DecisionProvider` interface once Phase 1 baselines pass.
*   **Async `ReverifyDependentsJob` Cascades & Unused Graph Edge Types (`WIKILINK`, `CONTRADICTS`, `SUPERSEDES`):** v1 restricts `memory_edges` to `DEPENDS_ON` and surfaces read-time `premise_drifted: true` + replacement stubs without launching background LLM cascade checks.
*   **Speculative Plumbing Cut Under Lean v1 Scope:** No monthly `raw_inbox` range partitioning, no `agent_private` / `visibility_scope` / `actor_id` split, no `last_accessed_at` / `AccessTouchFlusher`, no `runtime_controls` / `hourly_token_budget` DB control plane, no `purged_records` external tombstone table (single-PG atomic physical deletion removes source rows), no `go.opentelemetry.io/otel` / `traceparent` DB columns (use `log/slog` + relational lineage), no `max_concurrent_exports = 2` NDJSON streaming semaphore, no redundant `GET /v1/workspaces/{id}/memories` / `preview-context` / `graph` routes, no synchronous write locks/`409` retry loops, no automatic `ContentSlicer` (`> 64 KiB` chunking), no in-memory Go graph mirror, no external Topology B services (such as Valkey Read-Your-Own-Writes (`RYOW`) cache, NATS, Qdrant, Temporal, Neo4j, or S3 snapshots), and no per-record KMS sub-DEKs.

## Open Questions

None.

## Critical User Journeys (CUJs)

### CUJ 1: Non-Blocking Capture & Coupled Topic Refinement Across Agents

*   **Given:** A single-tenant `mem1` instance backed by PostgreSQL 17 (`mem1 serve`) with project workspace `'payments'`. Two coding CLIs (Claude Code via `mem1 mcp-stdio` and Cursor via `/mcp`) connect to `'payments'`.
*   **When:** Both agents send overlapping `memory_write` calls about `"payment retry backoff strategy"` within the same second while continuing foreground user work.
*   **Then:**
    *   Both writes scrub secrets inline, insert into unpartitioned `raw_inbox` (UUIDv7 `inbox_id`, unique `idempotency_key`), enqueue a River `CurateWorkspaceJob` atomically, and return `{accepted: true, inbox_id, request_id}` after DB commit without `memory_manifest` row locks or `409 Conflict` errors.
    *   One active River worker for `'payments'` batches both `'pending'` rows, calls `Curator.CurateBatch`, and updates one coupled active topic row in `memory_manifest` (recording both contributor `agent_id`s in `memory_revisions` and `memory_citations`, logging to `curation_decisions`, and enqueueing `EmbedMemoryJob`).

### CUJ 2: Upstream Decision Supersession & Read-Time Dependency Drift

*   **Given:** Project `'nacfson'` holds active `durable` memory `Mem A (v1: "Use SQLite for local prototype")` and two dependent memories `Mem B ("Enable SQLite WAL pragma")` and `Mem C ("Copy local .db file nightly")`, each linked via `DEPENDS_ON (from_rev=1, expected_to_rev=1)` to `Mem A`.
*   **When:** A session writes `"Switched primary database from SQLite to PostgreSQL 17"` and background `Curator` supersedes `Mem A` (`status = 'superseded'`, `superseded_by_memory_id = Mem A'`) with active `Mem A' ("Use PostgreSQL 17")`.
*   **Then:**
    *   Immediately on subsequent reads—with zero read locks—`Mem B` and `Mem C` surface `premise_drifted: true` alongside `Mem A'`'s active `summary_stub`, because `Mem A.status <> 'active'`.
    *   If `Mem A'` later receives a harmless additive revision (`v1 -> v2` on `Mem A'`), links pinned to `Mem A'` do not raise false drift warnings. Users or agents explicitly retire or update `Mem B` and `Mem C` when acting on the drift warning.

### CUJ 3: Unified Workspace Read Within the `<= 2,000` Token Cap

*   **Given:** Workspace `'general'` stores user profile rules in `constitution` (`"Respond in concise English"`) plus agent-learned `durable` personal entries (`"Lives in Seoul, primary language Go"`) and never technical decisions. Project `'payments'` uses `inherit_general = 'constitution_and_durable'` (default), has project `constitution` rules, 40 active `durable`/`working` memories, and 1-hop `DEPENDS_ON` links.
*   **When:** An agent calls `memory_recall` for `'payments'` with a task query.
*   **Then:**
    *   After optional query embedding outside the DB pool, one pipelined `BEGIN READ ONLY` `servingPool` transaction (`<= 100 ms` PostgreSQL/Go budget) loads merged `'payments'` + `'general'` `constitution`, runs hybrid vector + lexical (`tsvector` + `pg_trgm`) recall (including `'general'` `durable` entries with `'payments'` overriding matching `topic_key`s), packs `constitution` (`<= 500` tok) and ranked direct hits first, attaches 1-hop dependencies, and renders any overflow so total serialized JSON output stays `<= 2,000` tokens with 1-line `summary_stub` previews for `memory_expand`.
    *   `memory_recall` includes `manifest_etag` metadata (without `304` short-circuiting query-parameterized searches), while HTTP `GET /v1/workspaces/payments/constitution` supports conditional `If-None-Match` -> `304` checks. Switching `inherit_general = 'none'` excludes all `'general'` memories.

### CUJ 4: Observation, Cross-Workspace Querying, Manual Curation & Cherry-Pick Rollback

*   **Given:** An operator audits memories across all workspaces or repairs a mis-curated note without a Web UI.
*   **When:** The operator queries `GET /v1/memories` (with `MEM1_ADMIN_TOKEN`, cursor-paginated JSON up to 500 entries/page, optionally filtered by `workspace_id`, `durability`, `status`, or `premise_drifted`), inspects `GET /v1/memories/{id}` and `GET /v1/memories/{id}/explain`, manually promotes a rule to `constitution` (`PATCH /v1/memories/{id}`), or mints a PostgreSQL-backed `confirmation_token` (`mem1 confirm-token`) so an MCP agent (over `/mcp` or `mem1 mcp-stdio`) can invoke privileged `memory_manage` rollback (`revert`).
*   **Then:**
    *   `GET /v1/memories` runs on `curationPool` using UUIDv7 keyset pagination without affecting agent recall budgets.
    *   `explain` returns relational lineage from `raw_inbox` -> `memory_citations` -> `curation_decisions` -> current manifest head; `PATCH` and `revert` enforce `user_explicit` authority + CAS `expected_version` (`409` on stale version) and append monotonic revisions (`v5`, `restored_from_version = 2`, updating `last_reverted_version = 5`).
    *   `POST /v1/memories/{id}/purge` atomically removes the memory, revisions, citations, edges, and exclusively-cited raw inbox content while redacting `curation_decisions.rationale` inside one PostgreSQL transaction.

### CUJ 5: Outage Resilience & High-Concurrency Serving

*   **Given:** `>= 100` concurrent agent sessions are actively reading and writing against `mem1`.
*   **When:** (a) The external `Curator` LLM times out/rate-limits, (b) the external `Embedder` times out, or (c) PostgreSQL becomes temporarily unreachable.
*   **Then:**
    *   Under `Curator` outage, on the 3rd consecutive provider failure, the circuit breaker opens for `60s` and that attempt immediately commits truncated (`<= 1,500` tok) `working` fallback rows (`curation_state = 'provisional_heuristic'`, `topic_key = 'provisional:' || inbox_id`, never touching `constitution` or verified entries); once `Curator` recovers, a River `PeriodicJob` drains `'provisional'` items cleanly through `CurateWorkspaceJob` (`<= 10`/min) and atomically removes superseded `provisional:<inbox_id>` manifest rows.
    *   Under `Embedder` outage, `memory_recall` degrades gracefully to PostgreSQL lexical + `constitution` search (`degraded: true`), while `CurateWorkspaceJob` commits `memory_manifest` + `search_tsv` without failing and lets independent `EmbedMemoryJob` retries populate embeddings on recovery.
    *   Under PostgreSQL outage or `servingPool` saturation, passive `memory_recall` returns `degraded: true` (plus `X-Memory-Degraded: true` and `Cache-Control: no-store` over HTTP; never `200 OK []`), while writes, `memory_expand`, `memory_manage`, and HTTP operator routes return `503` / JSON-RPC errors.

## Technical Plan (Architecture)

### 1. Component Topology & Data Flow

`mem1` runs as one Go binary connected to one PostgreSQL 17 node. Foreground agent writes and pipelined reads never wait on `memory_manifest` write locks, and background `Curator`/`Embedder` HTTP calls (Phases A0 and B) hold zero database connections.

```text
+-----------------------------------------------------------------------------+
|  AI Agents (Claude Code, Cursor, Codex CLI, ChatGPT, Claude Web, Grok)      |
|  Operators / CLI (`mem1`) / Future UI Consumers                             |
+-------------------+---------------------------+-----------------------------+
                    | mem1 mcp-stdio            | HTTP :8787 (/mcp & /v1/*)
                    v (4+2 small pools)         v
+-----------------------------------------------------------------------------+
|                    mem1 Single Go Binary (nacfson-memory)                   |
|                                                                             |
|  Auth & Guards: MEM1_AGENT_TOKEN (/mcp), MEM1_ADMIN_TOKEN (/v1/* always),   |
|  Origin/Host Allowlist, Confirmation Tokens (PG), Max-Inflight (<=512)      |
|        |                               |                       |            |
|  [1] Write Path                 [2] Read Path           [3] Observability   |
|  Inline SecretScrubber          Query Embedder (2s)     & Manual Curation   |
|  (IngestWrite <=64 KiB)         + 1-Conn BEGIN READ     Keyset GET/Explain  |
|        |                        ONLY Recall (<=100ms)   Revert / Hard Purge |
|        v                               |                       |            |
|  servingPool (pgxpool: 24-32 conns, FIFO wait, statement_timeout=100ms)     |
|                                                                |            |
|                                                                v            |
|                                                 curationPool (4-8, 5s)      |
|                                                        ^       ^            |
|  [4] Background Workers (mem1 serve via riverqueue/river)      |            |
|  CurateWorkspaceJob (activeWorkspaces exclusion):      |       |            |
|   Phase A0: Pre-tx Embedder on pending inbox (0 DB conns)      |            |
|   Phase A : Short DB read tx (claim pending, snapshot state) --+            |
|   Phase B : External Curator HTTP + ValidatePlan (0 DB conns)               |
|   Phase C : FOR UPDATE commit, clear provisional, Enqueue EmbedMemoryJob    |
|  EmbedMemoryJob (independent retries) + Periodic ProvisionalReplayJob       |
+---------------------------------+-------------------------------------------+
                                  |
                                  v
+-----------------------------------------------------------------------------+
|                         PostgreSQL 17 (Single Node)                         |
|  workspaces | raw_inbox (unpartitioned, lz4)  | memory_manifest (halfvec    |
|  HNSW + tsvector GIN) | memory_revisions | memory_citations | memory_edges  |
|  (DEPENDS_ON) | curation_decisions | confirmation_tokens | schema_migrations|
+-----------------------------------------------------------------------------+
```

### 2. Concurrency Control, Request Guardrails & Durable Queuing

1.  **HTTP Admission Guard, Payload Cap & Split Token Auth (`internal/httpapi/middleware.go` + `MemoryService.IngestWrite`):**
    *   `MEM1_MAX_INFLIGHT` (default `512`) bounds concurrent HTTP handler execution on unified `:8787`. For `/v1/*`, saturated requests immediately return `503 Service Unavailable` with `Retry-After`. For `/mcp`, `middleware.go` passes an `inflightSaturated` context flag into the MCP dispatcher and buffers headers before flush: `memory_recall` returns HTTP `200` JSON-RPC `degraded: true` with `X-Memory-Degraded: true` and `Cache-Control: no-store`, while `memory_write`, `memory_expand`, and `memory_manage` fail with `-32053` service-unavailable errors (or `503` on transport rejection).
    *   `/mcp` HTTP middleware caps raw JSON-RPC request bodies at `80 KiB` (allowing JSON-RPC envelope headroom), while `MemoryService.IngestWrite` enforces `MEM1_MAX_WRITE_BYTES` (`65536` bytes) uniformly on decoded `content` across both HTTP and `mem1 mcp-stdio`. All `/v1/*` routes enforce `64 KiB` raw payload limits (`413 Payload Too Large`). Every write runs `SecretScrubber.Scrub` before persistence.
    *   **Auth & DNS-rebinding boundaries:**
        *   `GET /healthz`: unauthenticated.
        *   `Host` / `Origin` check: `Host` must match `MEM1_ALLOWED_HOSTS` (`127.0.0.1`, `localhost`, `[::1]` defaults); non-empty browser `Origin` headers must match allowed origins (`403 Forbidden` otherwise; empty `Origin` is permitted for non-browser CLI/MCP clients).
        *   `/v1/*`: **always** requires `Authorization: Bearer $MEM1_ADMIN_TOKEN` (even on `127.0.0.1`; `mem1 serve` fails startup if `MEM1_ADMIN_TOKEN` is empty).
        *   `/mcp`: requires `Authorization: Bearer $MEM1_AGENT_TOKEN` when bound non-loopback or whenever `MEM1_AGENT_TOKEN` is set (`mem1 serve` fails non-loopback startup if `MEM1_AGENT_TOKEN` is empty). MCP agents never receive `MEM1_ADMIN_TOKEN`; privileged `memory_manage` mutations additionally require a single-use PostgreSQL `confirmation_token`.
2.  **Isolated `pgxpool` Queuing, Single-Connection Read Pipelining & `mcp-stdio` Pool Sizing (`internal/storage`):**
    *   In `mem1 serve`, `servingPool` (`24–32` max conns, `statement_timeout = 100ms`) serves foreground writes and `memory_recall`, while `curationPool` (`4–8` max conns, `statement_timeout = 5s`) serves River workers, paginated `GET /v1/memories` inspection, and manual mutation routes calling `Embedder` pre-transaction.
    *   In `mem1 mcp-stdio`, pools default much smaller (`MEM1_STDIO_SERVING_MAX_CONNS = 4`, `MEM1_STDIO_CURATION_MAX_CONNS = 2`) so multiple IDE/CLI sessions alongside `mem1 serve` (`34` max conns) remain safely under PostgreSQL `max_connections = 100`.
    *   Each `memory_recall` acquires **1** `servingPool` connection, opens `BEGIN READ ONLY`, and executes Queries 1–4 before `COMMIT` and release.
3.  **Durable Queueing (`riverqueue/river`):**
    *   `pool.go` registers three River queues: `"curation"` (`MaxWorkers: 4`), `"embedding"` (`MaxWorkers: 4`), and `"maintenance"` (`MaxWorkers: 1`).
    *   Foreground writes execute `INSERT` into unpartitioned `raw_inbox` + atomic `river.InsertTx` using per-workspace `UniqueOpts` (`ByPeriod: 5s`, `ByArgs: true`, states `Available`, `Pending`, `Scheduled`). Inside `CurateWorkspaceWorker.Work`, an in-memory `activeWorkspaces sync.Map` enforces at most **1 active run per workspace** across the `"curation"` worker pool (returning `river.JobSnooze(2s)` if that workspace is already executing Phase A–C), preventing concurrent LLM/commit races on the same workspace while preserving parallelism across distinct workspaces.
    *   River manages worker leases, retries (`MaxAttempts = 3`), and `discarded` dead-letter transitions; single-item bisected failures at `job.Attempt >= MaxAttempts` set `raw_inbox.status = 'dead_letter'` + `last_error` so future workspace batches never stall on poison pills.

### 3. PostgreSQL 17 Storage Schema & Indexing (Migration `0001`)

PostgreSQL 17 lacks native `uuidv7()`, so Go generates all time-ordered UUIDv7 IDs via `github.com/google/uuid`. Migration `0001` creates the **9-table schema** (`schema_migrations` + 8 domain tables), records `version = 1` in `schema_migrations`, seeds workspace `'general'` idempotently, configures `lz4` compression, and declares cascading/nullifying foreign keys for clean single-transaction hard purges.

```sql
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
```

Key operational rules on this schema:

*   **Standalone Advisory-Locked Migration:** `storage.Migrate(ctx, dsn, down)` runs on a dedicated single `pgx.Conn` holding `pg_advisory_lock(71042001)` so concurrent `mem1 mcp-stdio` startups never race on DDL and `mem1 migrate --down` never applies `up` before rolling back. `OpenPools` verifies `version >= 1` exists without executing migration DDL.
*   **Row-level commit serialization:** Mutations acquire `SELECT manifest_revision, constitution_revision FROM workspaces WHERE workspace_id = $1 FOR UPDATE` inside their commit transaction, eliminating `SEQUENCE` visibility gaps and advisory-lock hash collisions.
*   **Segmented `ETag` watermarks:** `ComputeConstitutionETag(wsConstRev, genConstRev, inheritGeneral)` formats `W/"const:<ws_const_rev>+gen:<gen_const_rev>"` (or `W/"const:<ws_const_rev>"` when `'none'`) for HTTP `GET /v1/workspaces/{id}/constitution` (`304` support) and MCP `mem1://workspaces/{id}/constitution` payload metadata; `ComputeMetadataETag(wsRev, genRev, inheritGeneral)` formats `W/"ws:<ws_rev>+gen:<gen_rev>"` (or `W/"ws:<ws_rev>"`) as response metadata on `memory_recall`.
*   **`pgvector` lifecycle safety:** `AfterConnect` registers `pgxvec.RegisterTypes` after `vector` exists; `SET LOCAL hnsw.iterative_scan = relaxed_order` runs inside `BEGIN READ ONLY ... COMMIT` blocks (`pgvector >= 0.8.0`).
*   **Exact embedding input text:** All embedding paths (`EmbedMemoryWorker`, `ManualCreate`, `ManualEdit`, `RevertMemory`, `mem1 reindex`) pass `topic_key + "\n" + summary_stub + "\n" + body` to `Embedder.EmbedBatch`.
*   **Atomic physical hard purge (`HardPurgeTx`):** Acquire `LockWorkspaceTx` -> snapshot `durability` and all cited `inbox_id`s from `memory_citations WHERE memory_id = $1` *before* deleting rows -> delete `memory_manifest` (cascading to `memory_revisions`, `memory_citations`, `memory_edges`, and nullifying sibling `supersedes_*` columns via `ON DELETE SET NULL`) -> delete `raw_inbox` rows not referenced by other surviving `memory_citations` (setting `content = '[PURGED]'` on any shared multi-memory `raw_inbox` row) -> update `curation_decisions SET rationale = '[PURGED]' WHERE target_memory_id = $1 OR inbox_ids && $2` -> insert `'purge'` audit record -> bump `workspaces.manifest_revision` (plus `constitution_revision` if `durability == 'constitution'`).

### 4. Pluggable Provider Interfaces & Deterministic Contracts

Interfaces wrap only external LLM and embedding providers (`Curator` and `Embedder`). Single-implementation components (`SecretScrubber`, `LexicalSearch`, `DependencyGraph`, repository structs) are concrete Go types; shared read queries accept a `DBTX` interface (`Exec`, `Query`, `QueryRow` implemented by `*pgxpool.Pool` and `pgx.Tx`) so read queries pipeline cleanly on one `pgx.Tx` connection without leaking `*pgx.Tx` outside storage.

```go
type Curator interface {
    CurateBatch(ctx context.Context, req CurateRequest) (CuratePlan, error)
}

type Embedder interface {
    SpaceID() string
    Dimensions() int
    EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)
}

type InboxItemView struct {
    InboxID    uuid.UUID `json:"inbox_id"`
    AgentID    string    `json:"agent_id,omitempty"`
    OccurredAt time.Time `json:"occurred_at"`
    Content    string    `json:"content"`
}

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

type EdgeSpec struct {
    TargetMemoryID uuid.UUID `json:"target_memory_id"`
    ExpectedToRev  int       `json:"expected_to_rev"`
    Rationale      string    `json:"rationale"`
}

type CurateRequest struct {
    WorkspaceID     string          `json:"workspace_id"`
    WorkspaceKind   string          `json:"workspace_kind"`   // "general" | "project"
    SourceAuthority string          `json:"source_authority"` // "agent_observed"
    InboxItems      []InboxItemView `json:"inbox_items"`
    Candidates      []CandidateView `json:"candidates"`
}

type CuratePlan struct {
    ProviderName     string           `json:"provider_name"`
    ModelName        string           `json:"model_name"`
    PromptTokens     int              `json:"prompt_tokens"`
    CompletionTokens int              `json:"completion_tokens"`
    Mutations        []CurateMutation `json:"mutations"`
}

type CurateMutation struct {
    InboxIDs       []uuid.UUID `json:"inbox_ids"`
    Action         string      `json:"action"`   // "create" | "revise" | "supersede" | "retire" | "noop"
    Category       string      `json:"category"` // "personal_profile" | "personal_preference" | "project_technical" | "workflow_note"
    TargetMemoryID *uuid.UUID  `json:"target_memory_id,omitempty"`
    Durability     string      `json:"durability"`   // "durable" | "working" ("constitution" forbidden from Curator)
    TopicKey       string      `json:"topic_key"`    // ^[a-z0-9][a-z0-9-]{0,127}$
    SummaryStub    string      `json:"summary_stub"` // <= 180 runes, single line (no \n or \r)
    Body           string      `json:"body"`         // <= 1,500 tokens (cl100k_base)
    DependsOn      []EdgeSpec  `json:"depends_on,omitempty"` // max 5 active intra-workspace targets
    Rationale      string      `json:"rationale"`
}

type ValidationContext struct {
    WorkspaceID      string
    WorkspaceKind    string
    SourceAuthority  string
    ClaimedInboxIDs  []uuid.UUID
    ActiveCandidates map[uuid.UUID]CandidateView
    ExistingEdges    map[uuid.UUID][]uuid.UUID // from -> active to targets in workspace for cycle check
}

type Redaction struct {
    Type  string `json:"type"` // OPENAI_KEY|ANTHROPIC_KEY|GITHUB_TOKEN|AWS_KEY|JWT|PEM_KEY|BEARER_TOKEN|DSN_PASSWORD
    Count int    `json:"count"`
}
```

Deterministic policy rules enforced by `ValidatePlan(vctx ValidationContext, plan CuratePlan)`:

*   **Action rule (`revise` vs `supersede`):** Use `revise` for additive or clarifying updates preserving the premise (`current_version + 1` on same `memory_id`, keeping downstream `DEPENDS_ON` valid); use `supersede` when replacing or contradicting a premise (marks prior `'superseded'`, allocates new `memory_id`, flags downstream reads as drifted).
*   **Completeness & ownership:** Every UUID in `vctx.ClaimedInboxIDs` must appear in at least one `CurateMutation.InboxIDs` (including `noop`); `TargetMemoryID` is required for `revise`/`supersede`/`retire`, must belong to `vctx.WorkspaceID`, and all `DependsOn` targets must belong to `vctx.WorkspaceID`.
*   **Constitution & `'general'` protection:** Reject any `CurateMutation` with `Durability == "constitution"` or `'general'` writes with `Category` in `{"project_technical", "workflow_note"}` (or `Durability == "working"`). Manual writes/edits in `'general'` enforce the same `Category` rule.
*   **Slug, uniqueness, length & DAG limits:** Enforce `TopicKey` regex `^[a-z0-9][a-z0-9-]{0,127}$`, no duplicate `'create'` `TopicKey`s within one batch, single-line `SummaryStub <= 180` runes, `token_count <= 1,500` (and `<= 500` on manual `constitution` writes), `len(DependsOn) <= 5`, non-empty `Rationale`, zero self-edges, and combined existing-DB + batch `DEPENDS_ON` DAG acyclicity.

### 5. Background Curation Worker, Guardrails & Self-Healing

`CurateWorkspaceWorker` (queue `"curation"`), `EmbedMemoryWorker` (queue `"embedding"`), and `ProvisionalReplayJob` (`PeriodicJob`, `60s`, queue `"maintenance"`) run inside `mem1 serve` on `curationPool` and never hold DB connections across external HTTP calls:

1.  **Workspace Exclusion:** On entering `CurateWorkspaceWorker.Work`, check `activeWorkspaces.LoadOrStore(job.Args.WorkspaceID, struct{}{})`. If already active, return `river.JobSnooze(2 * time.Second)` so at most one worker runs Phase A–C per workspace without blocking other workspaces; clear with `defer activeWorkspaces.Delete(...)`.
2.  **Phase A0 — Pre-Transaction Candidate Query Embedding (Zero DB Conns Held):** Read up to `32` `'pending'` `raw_inbox` texts in a millisecond query and release immediately. If `Embedder` circuit breaker (`3` consecutive failures -> `60s` cooldown) is closed, call `Embedder.EmbedBatch`; otherwise skip vector candidate recall cleanly.
3.  **Phase A — Short Read & Snapshot Transaction (`curationPool`):** Claim up to `32` `'pending'` `raw_inbox` rows ordered by `(occurred_at, ingested_at, inbox_id)` up to `max(32 KiB, first single item <= 64 KiB)`. Rows remain `'pending'` across Phases A–B so process crashes never strand rows. Fetch top-10 active manifest candidates (hybrid vector + lexical or lexical-only fallback) plus prior revision snippets, snapshot `(memory_id, current_version, status)` for all candidates and workspace `ExistingEdges`, and release the DB connection.
4.  **Phase B — External `Curator` Call & Recursive Batch Bisection (Zero DB Conns Held):**
    *   If `MEM1_CURATION_PAUSED=true`, return `river.JobSnooze(30s)`. If `MEM1_FORCE_PROVISIONAL=true` or the `Curator` breaker (`3` consecutive provider failures -> `60s` cooldown, configurable `MEM1_CURATOR_TIMEOUT = 30s`) is open, run deterministic provisional fallback immediately.
    *   On transient provider failure (`timeout`/`429`/`5xx`), increment breaker count; on the **3rd consecutive failure**, open the breaker (`60s`) and **immediately execute provisional fallback inside the current attempt** (committing fallback and returning `nil` + re-enqueueing remaining `'pending'` rows rather than letting River discard Attempt 3).
    *   On `ValidatePlan` or malformed-JSON schema errors in a multi-item batch (`len > 1`), split the batch in half recursively and commit valid sub-batches cleanly while isolating any offending item to size `1`. If a single item fails validation at `job.Attempt >= MaxAttempts (3)`, update `raw_inbox SET status = 'dead_letter', last_error = $err WHERE inbox_id = $id` before returning the error so River records it in `discarded` and never blocks subsequent workspace items.
5.  **Phase C — Serialized Commit Transaction (`curationPool`):** Acquire `LockWorkspaceTx`. Re-verify candidate snapshots (`AND workspace_id = $ws`) and `raw_inbox.status = 'pending'`. If a concurrent manual mutation modified a target mid-flight or `create` hits `23505` on `idx_manifest_coupled_topic_active`, discard only that conflicted mutation cleanly and re-enqueue remaining work via `CurateWorkspaceJob`. Otherwise commit mutations (in `SupersedeMemoryTx`, update old row `status = 'superseded'` and retire its outgoing edges *before* inserting the replacement row, then link `superseded_by_memory_id`), insert `memory_revisions` + `memory_citations` + `memory_edges` + `curation_decisions`, **atomically delete any matching `'provisional:' || inbox_id` rows in `memory_manifest`** for all committed `inbox_id`s (including `noop`), prune revision bodies past `MEM1_HISTORY_RETENTION_N = 20`, mark committed `raw_inbox` rows `'curated'` (`rowsAffected == len(committed)`), increment `manifest_revision`, re-enqueue `CurateWorkspaceJob` if `'pending'` rows remain, and enqueue `EmbedMemoryJob` (River exponential backoff decouples `Embedder` outages from curation).

**Provisional Fallback & Periodic Replay (`fallback.go` + `ProvisionalReplayJob`):**
*   Provisional fallback truncates scrubbed inbox text deterministically to `<= 1,500` `cl100k_base` tokens (appending `"\n[truncated — pending full curation]"` when truncated), extracts a single-line `summary_stub` (`<= 180` runes), upserts idempotently onto `topic_key = 'provisional:' || inbox_id` (`ON CONFLICT (workspace_id, topic_key) WHERE status = 'active' DO UPDATE`), inserts `memory_revisions`, `memory_citations`, and `curation_decisions` (`action = 'provisional_fallback'`), and sets `raw_inbox.status = 'provisional'`.
*   Every `60s` when breaker is closed/half-open, `ProvisionalReplayJob` selects up to `10` active provisional items from `idx_manifest_provisional`, resets their cited `raw_inbox` rows back to `status = 'pending'`, and enqueues `CurateWorkspaceJob` so Phase C atomically replaces or deletes the `provisional:<inbox_id>` manifest row.

### 6. Retrieval, Drift Detection & `<= 2,000` Token Packing

Every `memory_recall` executes this deterministic pipeline:

1.  **Pre-DB Query Embedding (`MEM1_EMBED_TIMEOUT` = `2s` default):** If `query != ""` and `Embedder` circuit breaker (`3` failures -> `60s`) is closed, call `Embedder.EmbedBatch(ctx, [query])` with zero DB locks. On timeout or error (or open breaker), increment breaker, mark `degraded = true`, and continue with lexical + `constitution` recall.
2.  **Single-Connection Pipelined `BEGIN READ ONLY` Recall (`servingPool`, `<= 100 ms` PostgreSQL + Go budget):** Acquire **1** `servingPool` connection, begin `pgx.TxOptions{AccessMode: pgx.ReadOnly}`, pass the shared `pgx.Tx` across Queries 1–4, and `COMMIT` before release:
    *   *Query 1 (Constitution):* Load active `constitution` entries (`workspace_id = $ws`, plus `'general'` when `inherit_general <> 'none'`, project overriding `'general'` on `topic_key`).
    *   *Query 2 (Vector Recall, `durability <> 'constitution'`):* When query embedding succeeded, issue `SET LOCAL hnsw.iterative_scan = relaxed_order` and fetch `LIMIT 20` active rows for `$ws` (`AND embedding_space_id = $space`) and, when `inherit_general = 'constitution_and_durable'`, fetch `LIMIT 20` active `'durable'` rows from `'general'` separately so `'general'` entries never displace project rows before Go `topic_key` override dedup.
    *   *Query 3 (Lexical Recall, `durability <> 'constitution'`):* Using GIN-indexed `WHERE (search_tsv @@ $tsq OR topic_key % $q)`, rank top-20 per allowed scope via `0.7 * ts_rank_cd(search_tsv, $tsq) + 0.3 * similarity(topic_key, $q)`. First try `websearch_to_tsquery('english', $q)`; if `< 5` rows match, issue an `OR`-joined lexeme `to_tsquery` pass inside the same transaction so multi-word natural-language queries (e.g., `LongMemEval_S`) do not return empty results during `Embedder` outages. Empty `$q` orders by `updated_at DESC, memory_id DESC LIMIT 20`.
    *   *Fusion & Query 4 (1-Hop `DEPENDS_ON` & Multi-Hop Replacement Lookup):* Resolve project-over-`'general'` `topic_key` overrides in Go, merge Queries 2 and 3 via RRF (`k = 60`, multiplier `1.0` for `durable`, `0.85` for `working`, `0.45` for `provisional_heuristic`; `1.1x` project workspace boost), and call `DependencyGraph.LookupOutgoing` on the same `pgx.Tx`. Joins target `memory_manifest` + recursive `superseded_by_memory_id` resolution (`<= 10` hops to active replacement), setting `premise_drifted = (target.status <> 'active' OR edge.expected_to_rev < target.last_reverted_version)` with zero read locks.
3.  **Strict `<= 2,000` Serialized Token Packer (Offline `cl100k_base` via `tiktoken-go-loader`):**
    *   Default `max_tokens <= 0` to `2000`; clamp `budget = min(max_tokens, 2000)`, `constitution_cap = min(500, budget/2)`, `stub_headroom = min(200, budget/4)`.
    *   Pack `constitution` entries up to `constitution_cap`, direct hits up to `budget - stub_headroom`, 1-hop `DEPENDS_ON` links + replacement stubs, and remaining overflow as `summary_stub` previews (`expandable: true`).
    *   Validate total **serialized JSON response** token count in `cl100k_base`; if framing/UUID/edge tokens cause overshoot, evict deterministically in order until `serialized_tokens <= budget`: (1) drop lowest-ranked non-drifted dependency details, (2) convert lower-ranked direct hits from full `body` to `summary_stub`, (3) drop lowest-ranked overflow stubs, (4) convert excess `constitution` items to stubs.

### 7. Wire Contracts: Unified MCP + HTTP API + CLI

#### A. MCP Surface (`modelcontextprotocol/go-sdk`, Streamable HTTP on `/mcp` `:8787` + `mem1 mcp-stdio`)

Supports Streamable HTTP (`2026-07-28` stateless + `2025-11-25` fallback) on `/mcp` and `mem1 mcp-stdio`. Responses return structured JSON fields (`manifest_etag`, `workspace_revision`, `idempotency_key`, `degraded`); degraded HTTP `/mcp` calls buffer response headers before SSE/response flush to emit `X-Memory-Degraded: true` and `Cache-Control: no-store`. Validation/business/outage failures return MCP `CallToolResult{IsError: true}` / JSON-RPC errors (`-32602` invalid params, `-32013` oversized `> 64 KiB`, `-32022` unprocessable / idempotency hash mismatch, `-32053` service unavailable).

| MCP Tool / Resource | Input / Output Contract | Authority & Failure Semantics |
| :--- | :--- | :--- |
| `memory_write` | In: `{workspace_id, agent_id?, idempotency_key?, occurred_at?, content}` (`content <= 64 KiB`; `""` `idempotency_key` normalized to `NULL`)<br>Out: `{accepted, inbox_id, request_id, workspace_revision}` | Auto-provisions valid project workspaces (`EnsureWorkspace`), scrubs secrets, hashes post-scrub `content_sha256`, locks `source_authority = 'agent_observed'`. Replay returns stored `{inbox_id, request_id}`; hash mismatch -> `-32022`; `> 64 KiB` -> `-32013`; PG outage / saturation -> `-32053`. |
| `memory_recall` | In: `{workspace_id, query, max_tokens?}` (`<= 2,000`, default `2000`)<br>Out: `{memories, stubs, manifest_etag, workspace_revision, degraded}` | Read-only pipelined recall. Unknown workspace -> empty non-degraded result. On PG/pool/Embedder failure or `/mcp` saturation: `degraded: true` without `IsError: true` (HTTP adds `X-Memory-Degraded: true`, `Cache-Control: no-store`; never returns silent empty `degraded: false`). |
| `memory_expand` | In: `{workspace_id, memory_ids[], max_tokens?}` (`<= 2,000`, default `2000`)<br>Out: `{memories, edges, truncated, manifest_etag, workspace_revision}` | Validates each ID belongs to `workspace_id` or `'general'` when `inherit_general <> 'none'` (rejects foreign IDs with `-32602`), packs full bodies + edges within serialized `budget`, stubbing overflow (`truncated: true`). PG outage -> `-32053`. |
| `memory_manage` | In: `{workspace_id, action, target_memory_id?, durability?, category?, topic_key?, summary_stub?, body?, depends_on?, reason?, expected_version?, target_version?, confirmation_token?}` (`action`: `create\|edit\|promote\|retire\|revert\|purge`)<br>Out: `{memory_id, version, status, manifest_etag}` | `retire` on `working`/`durable` executes directly as `agent_observed`. Non-`constitution` `create`/`edit`/`promote` on `working`/`durable` is rejected (`-32602`: use `memory_write`). Privileged `user_explicit` actions (`constitution` `create`/`edit`/`promote`, `revert`, `purge`) require consuming a PostgreSQL-backed single-use `confirmation_token` (plus valid `MEM1_AGENT_TOKEN` on HTTP `/mcp`). Uses `curationPool`; `409` conflict / `-32053` PG outage. |
| `mem1://workspaces/{id}/constitution` | Read-only MCP Resource (`resources/read`) returning `{memories, manifest_etag, constitution_revision}` | Returns merged active `constitution` JSON in `ReadResourceResult` (`stdio` & HTTP `/mcp`; conditional HTTP `304` lives on `GET /v1/workspaces/{id}/constitution`). Explicit error on PG outage. |

#### B. HTTP JSON Observability & Manual Curation API (Unified `:8787`, No Web UI)

Served alongside `/mcp` on `MEM1_HTTP_ADDR` (`127.0.0.1:8787` default; rejects startup if `MEM1_ADMIN_TOKEN` is empty, validates `Host`/`Origin`). All `/v1/*` routes enforce `Authorization: Bearer $MEM1_ADMIN_TOKEN`, `64 KiB` raw payload cap (`413`), and canonical JSON errors `{error, code}` (`invalid_request` `400`, `unauthorized` `401`, `forbidden` `403`, `not_found` `404`, `conflict` `409`, `payload_too_large` `413`, `unprocessable_entity` `422`, `service_unavailable` `503`). Manual `POST`/`PATCH`/`revert` routes scrub secrets via `SecretScrubber`, validate `category` on `'general'` (`personal_profile | personal_preference`; rejects `working` on `'general'`), run `Embedder` pre-transaction on `curationPool` (committing `embedding = NULL` + `EmbedMemoryJob` if `Embedder` fails), and enforce CAS `expected_version`.

There are **18 total HTTP endpoints** (`17` `/v1/*` routes + `/healthz`):

| Method & Path | Request / Query Contract | Response & Guardrails |
| :--- | :--- | :--- |
| `GET /healthz` | None (unauthenticated) | `{status: "ok" \| "degraded", db_ok}` (`200` / `503`) |
| `GET /v1/workspaces` | None | `{workspaces: [{workspace_id, kind, inherit_general, active_count, manifest_revision, constitution_revision}]}` |
| `POST /v1/workspaces` | `{workspace_id, inherit_general?}` | `{workspace_id, kind, inherit_general, manifest_revision, constitution_revision}` (`201` / `409` duplicate / `400` invalid) |
| `PATCH /v1/workspaces/{id}` | `{inherit_general}` | `{workspace_id, inherit_general, updated_at}` |
| `GET /v1/workspaces/{id}/constitution` | `If-None-Match?` header | Merged active `constitution` + `ETag: ComputeConstitutionETag(...)` (`304 Not Modified` on match) |
| `GET /v1/memories` | Query: `workspace_id?`, `durability?`, `status?`, `curation_state?`, `premise_drifted?`, `include_revisions?`, `cursor?` (base64url UUIDv7 `memory_id`), `limit?` (`<= 500` default `100`, capped at `<= 50` if `include_revisions=true`) | `{items: [...], next_cursor}` ordered `memory_id DESC` on `curationPool`; exempt from `<= 2,000` token MCP cap |
| `GET /v1/memories/{id}` | Path UUID | `{memory: {...}, outgoing_edges: [...], incoming_edges: [...]}` (head, stub, `token_count`, drift flags) |
| `GET /v1/memories/{id}/revisions` | Path UUID | `{memory_id, revisions: [{version, durability, summary_stub, body, body_pruned, restored_from_version, agent_id, source_authority, change_reason, citations, created_at}]}` |
| `GET /v1/memories/{id}/explain` | Path UUID | `{memory_id, current_version, inbox_sources: [...], decisions: [...]}` from relational lineage |
| `POST /v1/workspaces/{id}/memories` | `{durability, category?, topic_key, body, summary_stub?, depends_on?, agent_id?}` (`category` required when `id == "general"`) | `{memory_id, version: 1, status: "active", manifest_etag}` (`201`; `409` active `topic_key` collision; `<= 500` tok for `constitution`, `<= 1,500` others) |
| `PATCH /v1/memories/{id}` | `{expected_version, category?, body?, durability?, summary_stub?, depends_on?, reason?}` (`category` required if target is in `"general"`) | `{memory_id, version, status, manifest_etag}`; `409` on version mismatch or editing inactive row; regenerates `summary_stub` if omitted and `body` changed; `depends_on == nil` preserves active outgoing edges |
| `POST /v1/memories/{id}/revert` | `{expected_version, target_version, reason?}` | `{memory_id, version, restored_from_version, status: "active", manifest_etag}`; updates `last_reverted_version = new_version`; clears `superseded_by_memory_id` if restoring `superseded`/`retired`; `409` if `body_pruned = TRUE` or active `topic_key` collision |
| `DELETE /v1/memories/{id}` | `{expected_version?, reason?}` | `{memory_id, status: "retired", manifest_etag}`; sets `status = 'retired'`, retires outgoing edges, triggers downstream read-time drift |
| `POST /v1/memories/{id}/purge` | `{confirm: true, reason?}` | `{purged: true, memory_id, deleted_inbox_count}` via atomic `HardPurgeTx` |
| `POST /v1/auth/confirm-token` | `{workspace_id, action, target_memory_id?}` | `{confirmation_token, expires_at}` — inserts SHA-256 hash into `confirmation_tokens` (5-min TTL, single-use) |
| `GET /v1/ops/status` | None | `{inflight, serving_pool, curation_pool, curator_breaker, embedder_breaker, pending_inbox, provisional_active, dead_letter_inbox, river_queues}` |
| `GET /v1/ops/curation-decisions` | Query: `workspace_id?`, `action?`, `since?`, `cursor?`, `limit?` (`<= 200`, cursor `created_at + decision_id`) | `{decisions: [...], next_cursor}` |
| `GET /v1/ops/dead-letters` | Query: `limit?` (`<= 100`, default `20`) | `{dead_letters: [{inbox_id, workspace_id, last_error, occurred_at, content_preview}], discarded_jobs: [...]}` |

#### C. CLI Surface (`mem1`)

Uses consistent flag parsing (`--id=<uuid>` / `--workspace=<id>`) so Go stdlib `flag` parses cleanly without positional-vs-flag ordering errors:

*   **Direct-Database Commands (read `MEM1_DATABASE_URL` directly; do not bind port `8787`):**
    *   `mem1 serve` — runs unified HTTP (`:8787` `/mcp` + `/v1/*`) + River `"curation"`, `"embedding"`, and `"maintenance"` workers (`SIGTERM`/`SIGINT` graceful shutdown).
    *   `mem1 mcp-stdio` — runs local MCP `stdio` server using small pools (`MEM1_STDIO_SERVING_MAX_CONNS = 4`, `MEM1_STDIO_CURATION_MAX_CONNS = 2`) without binding `:8787` or spawning River workers.
    *   `mem1 migrate [--down]` — advisory-locked standalone migrator (`0001_init.up.sql` / `--down` local reset + `rivermigrate`).
    *   `mem1 reindex [--batch-size=32]` — offline re-embedding backfill when changing `MEM1_EMBED_SPACE_ID` (updates `embedding`/`embedding_space_id` in place without incrementing `current_version`).
    *   `mem1 eval --suite=longmemeval_s,memoryagentbench_conflict --max-tokens=2000 [--dataset-dir=eval/datasets] [--limit=0] [--mock]` — runs Priority 1 benchmarks with embedded River workers.
*   **Operator HTTP Client Commands (call `MEM1_HTTP_ADDR` with `Authorization: Bearer $MEM1_ADMIN_TOKEN`):**
    *   `mem1 list [--workspace=<id>] [--durability=...] [--limit=100] [--json]` (`GET /v1/memories`)
    *   `mem1 explain --id=<memory_id>` (`GET /v1/memories/{id}/explain`)
    *   `mem1 revert --id=<memory_id> --to-version=<k> --expected-version=<n> [--reason=...]` (`POST /v1/memories/{id}/revert`)
    *   `mem1 purge --id=<memory_id> --yes [--reason=...]` (`POST /v1/memories/{id}/purge`)
    *   `mem1 confirm-token --workspace=<id> --action=<create|edit|promote|revert|purge> [--target=<id>]` (`POST /v1/auth/confirm-token`)
    *   `mem1 dead-letters [--limit=20]` (`GET /v1/ops/dead-letters`)

### 8. Failure Mode & Degradation Matrix

| Failure Scenario | System Behavior | Recovery Mechanism |
| :--- | :--- | :--- |
| External `Curator` LLM timeout / `429` | Breaker opens on 3rd failure (`60s`); same attempt commits truncated (`<= 1,500` tok) `provisional_heuristic` row inside `working` (`topic_key = 'provisional:' || inbox_id`, never touching `constitution` or verified entries) | River `PeriodicJob` resets up to `10`/min via `CurateWorkspaceJob`, which atomically deletes `provisional:<inbox_id>` manifest rows on Phase C commit |
| External `Embedder` outage | Foreground writes persist; `memory_recall` falls back to PG17 lexical + `constitution` (`degraded: true`); `CurateWorkspaceJob` commits `memory_manifest` + `search_tsv` and queues `EmbedMemoryJob` | Independent `EmbedMemoryJob` retries with River exponential backoff without dead-lettering `raw_inbox` |
| PostgreSQL 17 unreachable | Passive MCP `memory_recall` returns `degraded: true` (`X-Memory-Degraded: true`, `Cache-Control: no-store` over HTTP; never `200 OK []`); writes, `memory_expand`, `memory_manage`, and `/v1/*` fail cleanly (`503` / `-32053`) | `pgxpool` reconnects automatically; River resumes `'pending'` jobs without crash-stranded rows |
| HTTP `max_inflight` or `servingPool` saturation (`> 100 ms`) | Passive MCP `memory_recall` returns `200` `degraded: true` (`X-Memory-Degraded: true`, `Cache-Control: no-store`); writes and explicit calls return `503` + `Retry-After` / `-32053` | `pgxpool` FIFO wait list drains within `<= 100 ms` DB/Go budget |
| Oversized write payload (`> 64 KiB` content) | Returns `413 Payload Too Large` on HTTP `/v1/*` or `-32013` over MCP before DB write | Caller splits payload into smaller turns |
| Malformed / poison-pill inbox item | Recursive batch bisection isolates offending item to batch size `1`; on `Attempt >= 3`, marks `raw_inbox.status = 'dead_letter'` + `last_error` so workspace queue continues | Operator inspects via `GET /v1/ops/dead-letters` / `mem1 dead-letters` |
| Upstream decision replaced/retired/reverted | Immediate zero-lock `premise_drifted: true` + active replacement `summary_stub` (`<= 10` hops) on reads of dependent memories | Users or coding agents explicitly retire or update drifted downstream notes |

## Risks & Limits

*   **Single-Node Availability & Process Boundaries:** One PostgreSQL 17 node is the persistence single point of failure in v1. Background `CurateWorkspaceJob`, `EmbedMemoryJob`, and `ProvisionalReplayJob` execute only while `mem1 serve` is running (multiple `mem1 mcp-stdio` CLI processes accept writes and reads via direct PG pools, hold separate `Embedder` breakers, and leave writes `'pending'` if `mem1 serve` is offline).
*   **Background Curation Throughput & Eval Cost:** One active worker per workspace processes ~`1–3` external LLM round-trips/sec (`32` coalesced items max/batch), sustaining ~`6–15` turns/sec per shared workspace. Note that running the full 500-sample `LongMemEval_S` benchmark ingests ~`57.5M` raw tokens (~15k–20k sessions) plus Reader and 3-seed Judge calls; `mem1 eval --limit=N` supports bounded calibration runs before full-suite runs. Hot coupled topics that grow near `1,500` tokens rely on `Curator` splitting sub-topics cleanly via `topic_key` (repeated `ValidatePlan` size failures bisect to `dead_letter`).
*   **Fixed `HALFVEC(1536)` Dimensions, Skewed HNSW Scans & Reindex Window:** Migration `0001` pins `HALFVEC(1536)` (`MEM1_EMBED_DIM == 1536`) and `pgvector >= 0.8.0`. At `~3 KB` per active vector + HNSW graph overhead, `~100,000` active memories fit in `< 1 GiB` PG shared memory; under heavily skewed multi-workspace corpora (`< 1%` of rows in one workspace), `hnsw.iterative_scan` may reach `hnsw.max_scan_tuples` (`20,000`) or trigger `100 ms` fallback to lexical recall. Switching `MEM1_EMBED_SPACE_ID` requires an offline maintenance window running `mem1 reindex` before resuming vector recall.
*   **Payload & Token Ceilings:** Individual writes reject `content > 64 KiB`, `constitution` entries enforce `<= 500` tokens (`durable`/`working <= 1,500`), and agent retrieval (`memory_recall`, `memory_expand`) strictly caps serialized JSON output at `<= 2,000` tokens (`cl100k_base`).
*   **External Provider Zero-Data-Retention (ZDR) Boundary:** With `MEM1_ALLOW_NON_ZDR=false` (default), startup checks reject known public free/unauthenticated endpoints (e.g., `:free` model/path suffixes and unauthenticated trial URLs) and record operator ZDR attestation—because base URLs alone on OpenAI, Gemini, or OpenRouter cannot verify an API key's billing tier—unless explicitly overridden via `MEM1_ALLOW_NON_ZDR=true`.
*   **Pruned Revision Rollbacks & Storage Growth:** Rolling back past `MEM1_HISTORY_RETENTION_N = 20` (`body_pruned = TRUE`) returns `409 Conflict`. `raw_inbox`, `curation_decisions`, and `memory_revisions` metadata grow until manual purge or retention cleanup.

## Alternatives Considered

### Alternative 1: Python or TypeScript Primary Backend
*   **Pros:** Broad ecosystem of AI/LLM helper libraries and rapid prototyping speed.
*   **Cons:** Requires managing an interpreter/runtime environment, higher memory footprint under `>= 100` concurrent connections, and falls outside Go/Rust/C# compiled infrastructure targets.
*   **Decision & Rationale:** Rejected — Go delivers a static single binary, native `pgx/v5`, `riverqueue/river`, and `modelcontextprotocol/go-sdk` alignment, and lower complexity than Rust or C# NativeAOT.

### Alternative 2: SQLite (`sqlite-vec` + `FTS5`) as Primary Database
*   **Pros:** Embedded database with zero external daemon, single-file backups, WAL concurrent reads, and built-in `FTS5`.
*   **Cons:** Single-writer file lock stalls concurrent inbox writes against background curation commits (`SQLITE_BUSY`), `sqlite-vec` runs brute-force linear vector scans without HNSW, and SQLite lacks `SKIP LOCKED` transactional queues.
*   **Decision & Rationale:** Rejected — single-writer lock contention and linear vector scan limits fail the `>= 100` concurrent request target.

### Alternative 3: Multi-Service Distributed Stack (Topology B / Mem0 / Zep / Cognee / Supermemory Pattern)
*   **Pros:** Dedicated engines scale independently (e.g., Qdrant in Mem0, Neo4j/FalkorDB in Zep/Cognee, Cloudflare Workers/DO/Vectorize in Supermemory).
*   **Cons:** Multi-service operational overhead (3–6 stateful services, JVM RAM for Neo4j) and cross-database split-brain or partial-purge bugs (such as Valkey RYOW drift audited in [evaluation.md](../../evaluation.md)).
*   **Decision & Rationale:** Rejected — like Letta and LangMem (`PostgresStore`), `mem1` consolidates notes, revisions, inline `halfvec` embeddings, `DEPENDS_ON` edges, and River jobs inside one atomic PostgreSQL 17 node while keeping external `Curator` and `Embedder` providers pluggable via Go interfaces.

### Alternative 4: Coupled Whole-Corpus Snapshots
*   **Pros:** Atomic workspace-wide point-in-time rollbacks.
*   **Cons:** Every single-memory update re-versions unrelated memories and blocks per-memory cherry-pick edits and rollbacks.
*   **Decision & Rationale:** Rejected — replaced with independent per-memory versioning (`v1 -> v2 -> v3`) in compacted `memory_manifest` and append-only cherry-pick rollbacks.

### Alternative 5: Time-Based TTL Memory Expiration
*   **Pros:** Limits temporary table growth without LLM calls.
*   **Cons:** Deletes still-useful working notes on arbitrary clocks and creates clock-versus-`ETag` drift.
*   **Decision & Rationale:** Rejected — outdated temporary memories are retired explicitly by the background `Curator` or user/agent actions instead of TTL timers.

### Alternative 6: Synchronous Foreground Write Locks, Optimistic CAS (`409 Conflict`), and Global Write Ordering
*   **Pros:** Immediate read-your-own-writes consistency on `memory_manifest`.
*   **Cons:** Creates `409 Conflict` retry storms and write bottlenecks when `100+` concurrent agents write simultaneously.
*   **Decision & Rationale:** Rejected — foreground writes append without `memory_manifest` row locks into `raw_inbox` while one active River worker per workspace commits manifest revisions in the background.

### Alternative 7: Synchronous LLM Curation and Cosine (`>= 0.92`) Deduplication on Write
*   **Pros:** Zero visibility lag between write and read.
*   **Cons:** Stalls agent turns on multi-second LLM latency, and threshold-only cosine deduplication corrupts contradictory values ([evaluation.md](../../evaluation.md#L25-L29)).
*   **Decision & Rationale:** Rejected — violates non-blocking write guarantees and corrupts contradictory updates.

### Alternative 8: Combining All 6 Staleness Mechanisms in v1
*   **Pros:** Covers push notifications, IDE hooks, Git anchors, TTLs, piggybacked stamps, and `ETag`s simultaneously.
*   **Cons:** Overlapping invalidation paths add complexity before single-node bottlenecks are measured.
*   **Decision & Rationale:** Rejected — v1 scopes staleness handling to composite `ETag` metadata/HTTP `constitution` resource checks and explicit read-time drift flags; push notifications, IDE hooks, and Git anchors are deferred to future adapters.

### Alternative 9: In-Memory Go Dependency Graph Mirror
*   **Pros:** Sub-millisecond graph traversals in Go RAM.
*   **Cons:** Dual-source cache-coherency complexity when indexed PostgreSQL `memory_edges` handle 1-hop lookups inside the `100 ms` budget.
*   **Decision & Rationale:** Rejected — deferred until measured SQL latency exceeds budget.

### Alternative 10: Two-Stage Zero-Output-Token Decision Models (`gemini-decision-1` / `TypeSafe Jev`) in Phase 1
*   **Pros:** Zero output tokens for closed-set keep/revise/retire gates.
*   **Cons:** `gemini-decision-1` is `v1beta` preview, `TypeSafe Jev` lacks a Go SDK and caps at 1,200 RPM, and closed-set models still require a generative extractor for unstructured transcripts.
*   **Decision & Rationale:** Deferred — start v1 with one pluggable `Curator` interface plus deterministic fallback, keeping author intent (preserved in [concepts_brief.md](../../concepts_brief.md#L143-L146)) to add a batched `DecisionProvider` pre-filter once Phase 1 baselines pass.

### Alternative 11: 10,000-Token Read Cap
*   **Pros:** Injects more historical notes without stub truncation.
*   **Cons:** Causes attention dilution, prefill latency spikes, and non-comparable benchmark scores.
*   **Decision & Rationale:** Rejected — enforce `max_tokens <= 2,000` on all agent context retrieval and benchmark queries, rendering overflow as 1-line `summary_stub` previews.

### Alternative 12: Per-Agent Conflict Alerting (`memory_alerts`) in v1
*   **Pros:** Pushes immediate conflict alerts when concurrent agents touch the same topic.
*   **Cons:** Adds alert state and client polling workflows before core curation accuracy is verified.
*   **Decision & Rationale:** Deferred — validate context-to-memory refinement first; preserve `memory_alerts` intent for Phase 2+ ([concepts_brief.md](../../concepts_brief.md#L147-L150)).

### Alternative 13: Separate `user_global` Primitive or `constitution_only` Default
*   **Pros:** Strict manual gate before personal facts flow to project workspaces.
*   **Cons:** `constitution` writes require `user_explicit` authority, hiding agent-observed personal profile facts in `'general'` from every project workspace under `'constitution_only'`.
*   **Decision & Rationale:** Rejected — unify user context in reserved `'general'` workspace (strictly non-technical) and default projects to `'constitution_and_durable'` alongside `'constitution_only'` and `'none'` options.

### Alternative 14: Flat `supersedes_id` Without `DEPENDS_ON` vs. Cascading `ReverifyDependentsJob` in v1
*   **Pros:** Skipping edges removes schema work, whereas full cascades automate dependent rewrites.
*   **Cons:** Zero edges misses upstream breakages (SQLite -> PostgreSQL 17), while cascading async LLM re-verification on every minor revision triggers false alarms and hub storms before Priority 1 eval passes.
*   **Decision & Rationale:** Rejected both extremes — v1 stores revision-pinned intra-workspace `DEPENDS_ON` links only (dropping `WIKILINK`, `CONTRADICTS`, and duplicate `SUPERSEDES`) and computes read-time `premise_drifted: true` + active replacement stubs strictly on `supersede`/`retire`/`revert`, deferring async cascades to `Future Improvements`.

### Alternative 15: ParadeDB `pg_search` (Tantivy BM25) as Default Lexical Search
*   **Pros:** Built-in BM25 length normalization and typo matching in PostgreSQL.
*   **Cons:** AGPL-3.0 copyleft restrictions, unsupported on AWS RDS and Google Cloud SQL, retired on Neon, and upgrade `REINDEX` burden.
*   **Decision & Rationale:** Rejected — v1 uses stock PostgreSQL 17 `tsvector` + `pg_trgm` GIN indexes directly.

### Alternative 16: Multi-Tenant SaaS (`tenant_id` + RLS) in v1 vs. Separate v2 Design
*   **Pros:** Supports untrusted multi-organization SaaS hosting immediately.
*   **Cons:** Adds per-tenant scoped auth, PostgreSQL RLS session plumbing, per-tenant token quotas, and `tenant_id` boilerplate across every schema/interface before single-tenant Priority 1 curation quality is verified.
*   **Decision & Rationale:** Deferred — keep v1 strictly single-tenant and specify multi-tenant SaaS tenancy, RLS, and per-tenant auth in a separate v2 design after Priority 1 passes.

### Alternative 17: Bundled Web UI Process & Speculative v1 API/Telemetry Sprawl
*   **Pros:** Visual UI out of the box plus OTel Span Links, `runtime_controls`, `AccessTouchFlusher`, `purged_records`, and separate MCP/HTTP ports.
*   **Cons:** Duplicates `riverqueue/river` and PG17 capabilities, adds build/maintenance surface before Priority 1 evals pass, and splits MCP auth across two ports.
*   **Decision & Rationale:** Rejected — exclude Web UI and speculative v1 plumbing (`agent_private`, `last_accessed_at`, `runtime_controls`, OTel, `raw_inbox` range partitioning, redundant `GET /v1/workspaces/{id}/memories` / `preview-context` / `graph` routes, and NDJSON streaming semaphores); mount `/mcp` and lean `/v1/*` routes (`17` `/v1/*` + `/healthz`, retaining `POST /v1/workspaces/{id}/memories` for workspace-scoped manual creation) on one `:8787` listener with `mem1 mcp-stdio` and `log/slog`.

### Alternative 18: Custom In-Memory Back-Buffer Channel Queues & `ContentSlicer`
*   **Pros:** Micro-batches writes in RAM and slices `> 64 KiB` transcripts automatically.
*   **Cons:** Duplicates `pgxpool` native FIFO queuing and introduces partial-chunk reassembly bugs.
*   **Decision & Rationale:** Rejected — rely on `MEM1_MAX_INFLIGHT = 512`, `64 KiB` write limit (`413`), native `pgxpool` queuing, and `riverqueue/river`; defer chunk slicing to `Future Improvements`.

### Alternative 19: Atomic Fact Nodes + Read-Time Grouping vs. Coupled Topic Rows
*   **Pros:** Avoids per-memory `body` growth above `1,500` tokens when agents append repeatedly to one topic.
*   **Cons:** Requires read-time synthesis or clustering across fragmented atomic bullets, inflating `memory_recall` token overhead and complicating per-topic `v1 -> v2 -> v3` revision timelines.
*   **Decision & Rationale:** Rejected — use coupled shared-topic rows capped at `1,500` tokens (`token_count`), prompting `Curator` to split sub-topics into linked `topic_key` entries if a topic grows past threshold.

### Alternative 20: `PGMQ` or Custom `FOR UPDATE SKIP LOCKED` Queue vs. `riverqueue/river`
*   **Pros:** Fewer queue tables (custom `SKIP LOCKED`) or extension-level simplicity (`PGMQ`).
*   **Cons:** `PGMQ` is a Postgres extension unavailable on some managed PG hosts, and custom `SKIP LOCKED` reimplements backoff, unique job coalescing, snoozing, periodic jobs, and `discarded` dead-letter state.
*   **Decision & Rationale:** Rejected — adopt pure-Go + stock-SQL `riverqueue/river` (`riverpgxv5`) inside the single binary.

### Alternative 21: HTTP-Proxying `mem1 mcp-stdio` Through `mem1 serve` vs. Direct-PostgreSQL Pooling
*   **Pros:** One process manages PG pools and shared in-memory `Embedder` circuit breakers.
*   **Cons:** Breaks standalone local CLI startup whenever `mem1 serve` is stopped and forces `MEM1_AGENT_TOKEN` plumbing into local IDE `stdio` configs.
*   **Decision & Rationale:** Rejected — let `mem1 mcp-stdio` connect directly to PostgreSQL with bounded small pools (`4` serving + `2` curation conns) and share single-use `confirmation_tokens` via PostgreSQL rather than `mem1 serve` heap memory.

## Non-Functional Requirements

*   **Security & Privacy:**
    *   Deterministic regex `SecretScrubber` strips API keys, bearer tokens, JWTs, PEM blocks, and database credentials on both `raw_inbox` writes and manual `POST`/`PATCH` mutations before PostgreSQL persistence or external provider HTTP egress.
    *   Startup configuration rejects known public free/unauthenticated endpoints unless `MEM1_ALLOW_NON_ZDR=true`, paired with explicit operator ZDR attestation (`Risks & Limits`).
    *   Unified HTTP server (`/mcp` + `/v1/*`) binds to `127.0.0.1:8787` by default, validates `Host` and non-empty `Origin` headers, always requires `MEM1_ADMIN_TOKEN` on `/v1/*`, requires `MEM1_AGENT_TOKEN` on `/mcp` when non-loopback or set, and gates privileged `user_explicit` MCP `memory_manage` mutations with PostgreSQL-backed single-use `confirmation_token`s.
    *   Volume encryption at rest protects searchable PostgreSQL columns; `hard_purge` atomically removes memory, revision, citation, edge, and exclusively-cited raw inbox rows inside one PG transaction while redacting audit rationales.
    *   `constitution` writes require `source_authority = 'user_explicit'`, enforced in Go validation and PostgreSQL `CHECK` constraints.
*   **Performance, Latency & Resource Ceilings:**
    *   Concurrency target: `>= 100` simultaneous in-flight requests on one Go binary + single PG17 node.
    *   Foreground DB/Go recall SLA: `servingPool` enforces `statement_timeout = 100ms` (`24–32` max conns, 1 pipelined `BEGIN READ ONLY` connection per recall); `curationPool` runs isolated (`4–8` max conns, `statement_timeout = 5s`). External `Embedder` calls use a separate `MEM1_EMBED_TIMEOUT` (`2s` default).
    *   Agent read token cap: serialized JSON output `<= 2,000` tokens (`cl100k_base`) on `memory_recall`, `memory_expand`, and `mem1 eval` (`constitution <= 500` tok per entry; HTTP/CLI inspection endpoints exempt).
    *   Storage hygiene: active-only partial HNSW `halfvec(1536)` index, `lz4`-compressed unpartitioned `raw_inbox`, and configurable `MEM1_HISTORY_RETENTION_N = 20` (`0` = unbounded) body pruning (metadata rows retained until `hard_purge`).
*   **Operational Debuggability:**
    *   Structured Go `log/slog` JSON logs recording `request_id`, `inbox_id`, `workspace_id`, and latency metrics.
    *   Live queue, pool, breaker, and River `discarded` / `dead_letter` counts via `GET /v1/ops/status` and `GET /v1/ops/dead-letters` (`mem1 dead-letters`), plus SQL lineage via `GET /v1/memories/{id}/explain` (`mem1 explain --id=<id>`).
*   **Rollout & Schema Rollback:**
    *   Per-memory cherry-pick rollback (`POST /v1/memories/{id}/revert` / `mem1 revert`) appends monotonic revisions (`restored_from_version = k`, updating `last_reverted_version`) so watermarks never move backward.
    *   Schema changes run via embedded SQL migrations (`mem1 migrate`) plus `rivermigrate`; persistent deployments require a pre-migration database backup/snapshot, while `0001_init.down.sql` is restricted to local development resets.

## Additional Resources

*   Predecessor brainstorming brief: [concepts_brief.md](../../concepts_brief.md)
*   Architecture failure audit: [evaluation.md](../../evaluation.md)
*   Historical research report (superseded Topology B): [mem1-architecture-and-research-report.html](../../mem1-architecture-and-research-report.html)
*   Repository blueprint & design sitemap: [README.md](../../README.md), [docs/designs/SITEMAP.md](./SITEMAP.md)
*   Primary Go & PostgreSQL dependencies: `github.com/jackc/pgx/v5`, `github.com/google/uuid`, `github.com/riverqueue/river`, `github.com/pgvector/pgvector-go`, `github.com/modelcontextprotocol/go-sdk`, `github.com/pkoukk/tiktoken-go`
*   Evaluation benchmarks: `LongMemEval_S` (`arXiv:2410.10813`) and `MemoryAgentBench` (`Conflict Resolution` slice: sequential session streams testing overwrite/update accuracy when newer facts contradict earlier stored facts)

## Future Improvements

*   **Multi-Tenant SaaS (v2 Spec):** Dedicated follow-up design adding `tenant_id` partitioning, PostgreSQL RLS, per-tenant OAuth/scoped keys, and per-tenant rate/token quotas once Priority 1 quality passes.
*   **Async `ReverifyDependentsJob` & `premise_epoch`:** Add optional non-cascading or epoch-scoped background re-verification if Priority 1 evals show read-time drift flags plus replacement stubs are insufficient.
*   **Stage-1 Zero-Output-Token Decision Pre-Filter (`DecisionProvider`):** Add a batched pre-filter once Phase 1 `Curator` passes quality baselines and LLM spend justifies pre-filtering.
*   **Targeted Per-Agent Conflict Alerting (`memory_alerts`):** Re-introduce after Priority 1 validation and 100-agent overlap testing.
*   **`agent_private` / Visual Web Dashboard / `ContentSlicer` / Graph Mirror / Adapters:** Add `agent_private` visibility, a browser UI consuming `/v1/*` after Priority 2 APIs pass load gates, chunked `> 64 KiB` ingestion, an in-memory graph mirror if 1-hop SQL latency exceeds budget, ParadeDB `pg_search`, IDE hooks, and optional client-side SQLite caching.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

Planned file boundaries, component contracts, and verification gates across **Phase 1a**, **Phase 1b**, and **Phase 2** are specified below so each checkpoint compiles cleanly and passes its scoped tests.

### Target Files to Create/Modify

#### Phase 1a — Repository Bootstrap, Config, Security, Migrations, Storage, Search, Graph & Minimal CLI

##### 1. `go.mod` & `go.sum` (Create)
*   **Type / Scope:** Go module definition (`github.com/nacfson/nacfson-memory`, Go `1.27` matching host Go `1.27.3`).
*   **Proposed Changes:** Declare core dependencies: `github.com/jackc/pgx/v5`, `github.com/google/uuid`, `github.com/riverqueue/river`, `github.com/riverqueue/river/riverdriver/riverpgxv5`, `github.com/pgvector/pgvector-go`, `github.com/modelcontextprotocol/go-sdk` (`>= v1.7.0`), `github.com/pkoukk/tiktoken-go`, and `github.com/pkoukk/tiktoken-go-loader` (bundled offline `cl100k_base` BPE dictionaries). Populate exact checksums in `go.sum` via `go mod tidy` during Phase 1a bootstrap.

##### 2. `docker-compose.yml` & `.gitignore` (Create)
*   **Type / Scope:** Local PostgreSQL 17 integration infrastructure and workspace hygiene.
*   **Proposed Changes:** Configure `pgvector/pgvector:pg17` on `127.0.0.1:5432:5432` with `POSTGRES_USER=mem1`, `POSTGRES_PASSWORD=mem1`, `POSTGRES_DB=mem1`, local DSN `postgres://mem1:mem1@127.0.0.1:5432/mem1?sslmode=disable`, `pg_isready` healthcheck, persistent volume, `max_connections=100`, and `shared_buffers=256MB`. In `.gitignore`, ignore `bin/`, `coverage.out`, and downloaded `eval/datasets/*.jsonl`, `eval/datasets/*.json`, and `eval/datasets/*.parquet`.

##### 3. `Makefile` (Create)
*   **Type / Scope:** Build and verification targets.
*   **Proposed Changes:** Define targets:
    *   `build`: `CGO_ENABLED=0 go build -o bin/mem1 ./cmd/mem1`
    *   `test`: `go test -race ./...`
    *   `test-integration`: `go test -tags=integration -race ./...`
    *   `lint`: `go vet ./... && golangci-lint run ./...`
    *   `migrate`: `./bin/mem1 migrate`
    *   `reindex`: `./bin/mem1 reindex --batch-size=32`
    *   `eval`: `./bin/mem1 eval --suite=longmemeval_s,memoryagentbench_conflict --max-tokens=2000`
    *   `loadtest`: `go test -tags="integration load" -race ./tests/load/... && go test -tags="integration load" -run TestRecallLatencySLO ./tests/load/...`

##### 4. `internal/config/config.go` & `internal/config/config_test.go` (Create)
*   **Type / Scope:** Mode-scoped configuration loader, `Config` struct, startup validator, and unit tests.
*   **Proposed Changes:** Define `Config` struct and `Load(mode string) (Config, error)` (`mode`: `"serve"|"mcp-stdio"|"migrate"|"reindex"|"eval"`) so administrative/offline commands only validate required env vars:
    *   DB & HTTP (`serve` requires all): `MEM1_DATABASE_URL` (required in all modes), `MEM1_HTTP_ADDR` (`127.0.0.1:8787`), `MEM1_ALLOWED_HOSTS` (`127.0.0.1,localhost,[::1]`), `MEM1_ADMIN_TOKEN` (required non-empty in `"serve"`), `MEM1_AGENT_TOKEN` (required non-empty if `MEM1_HTTP_ADDR` is non-loopback).
    *   Pools & limits: `MEM1_SERVING_MAX_CONNS` (`28`), `MEM1_CURATION_MAX_CONNS` (`6`), `MEM1_STDIO_SERVING_MAX_CONNS` (`4`), `MEM1_STDIO_CURATION_MAX_CONNS` (`2`), `MEM1_MAX_INFLIGHT` (`512`), `MEM1_MAX_WRITE_BYTES` (`65536`), `MEM1_HISTORY_RETENTION_N` (`20`, `0` = unbounded), `MEM1_CURATION_PAUSED` (`false`), `MEM1_FORCE_PROVISIONAL` (`false`).
    *   Providers (`serve`, `mcp-stdio`, `reindex`, `eval`): `MEM1_CURATOR_PROVIDER` (`openai_compat` default | `mock`), `MEM1_CURATOR_BASE_URL`, `MEM1_CURATOR_MODEL`, `MEM1_CURATOR_API_KEY`, `MEM1_CURATOR_TIMEOUT` (`30s`), `MEM1_EMBED_PROVIDER` (`openai_compat` default | `mock`), `MEM1_EMBED_BASE_URL`, `MEM1_EMBED_MODEL` (`text-embedding-3-small`), `MEM1_EMBED_API_KEY`, `MEM1_EMBED_SPACE_ID` (`openai/text-embedding-3-small:1536`), `MEM1_EMBED_DIM` (`1536`; reject `!= 1536`), `MEM1_EMBED_TIMEOUT` (`2s`), `MEM1_ALLOW_NON_ZDR` (`false`).
    *   Eval (`eval` without `--mock`): `MEM1_EVAL_READER_BASE_URL`, `MEM1_EVAL_READER_MODEL`, `MEM1_EVAL_READER_API_KEY`, `MEM1_EVAL_JUDGE_BASE_URL`, `MEM1_EVAL_JUDGE_MODEL`, `MEM1_EVAL_JUDGE_API_KEY`.
    *   `config_test.go` (unit): verify missing `MEM1_ADMIN_TOKEN` failure in `"serve"`, non-loopback `MEM1_AGENT_TOKEN` failure, `MEM1_EMBED_DIM != 1536`, mode-scoped validation (`"migrate"` passes without LLM keys), and ZDR endpoint validation.

##### 5. `internal/security/scrubber.go` & `internal/security/scrubber_test.go` (Create)
*   **Type / Scope:** Deterministic regex secret scrubber, endpoint ZDR policy validator, and unit tests.
*   **Proposed Changes:** Implement concrete `SecretScrubber` with `Scrub(input string) (string, []storage.Redaction)` applying ordered rules (matching `ANTHROPIC_KEY` `sk-ant-[A-Za-z0-9_-]{20,}` before `OPENAI_KEY` `sk-(?:proj-)?[A-Za-z0-9_-]{20,}`, `GITHUB_TOKEN` `(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})`, `AWS_KEY` `AKIA[0-9A-Z]{16}`, `JWT` `eyJ...`, `PEM_KEY` `-----BEGIN ... PRIVATE KEY-----...-----END ... PRIVATE KEY-----`, `BEARER_TOKEN` replacing only the bearer token capture after `Authorization:\s*Bearer\s+`, and `DSN_PASSWORD` replacing only the credential component inside `postgres(?:ql)?://[^:]+:([^@]+)@`) with `[REDACTED:<TYPE>]`, leaving Git SHAs and UUIDs intact. Implement `ValidateProviderEndpoint(baseURL, model string, allowNonZDR bool) error` blocking known `:free` model suffixes, `openrouter.ai` free endpoints without ZDR path/override, and known unauthenticated public trial hosts when `allowNonZDR == false`. Cover all rules in `scrubber_test.go` (unit).

##### 6. `migrations/embed.go`, `migrations/0001_init.up.sql`, & `migrations/0001_init.down.sql` (Create)
*   **Type / Scope:** Embedded SQL migrations (`//go:embed *.sql` inside package `migrations` so `internal/storage` imports `migrations.FS`).
*   **Proposed Changes:** Implement the **9-table schema** (`schema_migrations` seeded with `version = 1` + 8 domain tables: `workspaces` seeded with `'general'`, unpartitioned `lz4`-compressed `raw_inbox` with `occurred_at` and `'dead_letter'`/`last_error`, `memory_manifest` with inline `HALFVEC(1536)` + partial HNSW + `last_reverted_version` + `constitution <= 500` tok check, `memory_revisions`, `memory_citations`, `memory_edges` `DEPENDS_ON`-only, `curation_decisions` with `target_memory_id` and `GIN (inbox_ids)` indexes, and `confirmation_tokens`), plus `0001_init.down.sql` dropping those tables in reverse dependency order alongside `rivermigrate` down for local resets.

##### 7. `internal/storage/pool.go`, `internal/storage/jobs.go`, `internal/storage/workspaces.go`, `internal/storage/inbox.go`, `internal/storage/manifest.go`, `internal/storage/tokens.go`, & `internal/storage/integration_test.go` (Create)
*   **Type / Scope:** PG17 migrator/pools, River job arg types, token store, repositories, and PG integration tests (`//go:build integration`).
*   **Proposed Changes:**
    *   `pool.go`: Expose standalone advisory-locked `Migrate(ctx, dsn, down)` and separate `OpenPools(ctx, cfg)` (verifying `schema_migrations` version `>= 1`, opening `servingPool` + `curationPool` with `AfterConnect` registering `pgxvec.RegisterTypes`, configuring River queues `"curation": 4`, `"embedding": 4`, `"maintenance": 1`). Expose pool metrics, pending/provisional/dead-letter inbox counts, River queue stats, and `ListDeadLetters(ctx, limit)`.
    *   `jobs.go`: Define `CurateWorkspaceJobArgs`, `EmbedMemoryJobArgs`, and `ProvisionalReplayJobArgs` with `Kind()` and `InsertOpts()` cleanly in `internal/storage` (zero circular dependencies).
    *   `tokens.go`: Implement `IssueConfirmToken` (random 32-byte token, SHA-256 insert + expired cleanup) and `ConsumeConfirmToken` (atomic single-use `UPDATE ... WHERE ... RETURNING`).
    *   `workspaces.go`: Implement `EnsureWorkspace`, `CreateWorkspace` (`409` on duplicate), `GetWorkspace`, `ListWorkspaces`, `UpdateInheritGeneral`, `LockWorkspaceTx` (`FOR UPDATE`), `BumpRevisionTx`, `ComputeConstitutionETag`, and `ComputeMetadataETag`.
    *   `inbox.go`: Implement `InsertInboxAndEnqueueTx` (UUIDv7 `inbox_id`, post-scrub SHA-256, `ON CONFLICT (workspace_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING` + conflict select returning `{inbox_id, request_id}` or `ErrIdempotencyMismatch`, atomic `river.InsertTx`), `ClaimPendingBatch` (`max(32 KiB, first single item <= 64 KiB)` ordered by `occurred_at, ingested_at, inbox_id`), `MarkInboxCurated`, `MarkInboxProvisional`, `MarkInboxDeadLetter`, and `ResetProvisionalToPending`.
    *   `manifest.go`: Accept `DBTX` where shared read-tx queries run (`GetConstitution`, `SearchVectorTopK`, `GetWorkspace`). Implement `CreateMemoryTx`, `ReviseCoupledTopicTx`, `EditMemoryTx` (partial update, CAS on active rows, optional stub regeneration), `SupersedeMemoryTx` (retire old row + outgoing edges BEFORE inserting replacement, then set `superseded_by_memory_id`), `SoftRetireTx` (set `'retired'` + retire outgoing edges), `RevertToVersionTx` (handles `superseded`/`retired` restoration, sets `last_reverted_version`, returns sentinel error on `body_pruned` or `topic_key` collision), `HardPurgeTx` (locks workspace -> snapshots cited `inbox_id`s BEFORE deleting manifest -> cleans `raw_inbox` -> redacts `curation_decisions` -> writes `'purge'` decision -> bumps revisions), `PruneRevisionBodiesTx`, `DeleteProvisionalManifestByInboxTx`, `ListAllMemoriesKeyset` (`ORDER BY memory_id DESC`, `premise_drifted` join, `include_revisions`), `ListMemoriesForReindexKeyset`, `UpdateMemoryEmbeddingTx`, `GetMemory`, `GetRevisions`, `ExplainLineage`, `InsertCurationDecisionTx`, and `ListCurationDecisions`.
    *   `integration_test.go` (`//go:build integration`): Test Migration `0001` idempotency + `--down`, `confirmation_tokens` single-use atomicity, `idempotency_key` replay & `422` mismatch, coupled topics, `EditMemoryTx`, `SupersedeMemoryTx`, pruned/conflicted `RevertToVersionTx`, `HardPurgeTx`, `ListAllMemoriesKeyset`, and `ListDeadLetters`.

##### 8. `internal/testutil/pg.go` (Create)
*   **Type / Scope:** Shared PostgreSQL 17 integration test harness importable across all test packages.
*   **Proposed Changes:** Provide `OpenTestDB(t *testing.T)` (runs `storage.Migrate` once, opens test pools, truncates domain + River tables and re-seeds `'general'` cleanly between tests).

##### 9. `internal/search/lexical.go` & `internal/search/lexical_test.go` (Create)
*   **Type / Scope:** Concrete `LexicalSearch` (`DBTX`) component and integration tests (`//go:build integration`).
*   **Proposed Changes:** Run GIN-indexed `WHERE (search_tsv @@ $tsq OR topic_key % $q)` per allowed scope (`durability <> 'constitution'`) using `websearch_to_tsquery('english', $q)` with `OR`-lexeme `to_tsquery` fallback when `< 5` hits match (and `updated_at DESC, memory_id DESC LIMIT 20` on `$q == ""`). Verify ranking, OR-fallback, and `'general'` merge in `lexical_test.go`.

##### 10. `internal/graph/postgres_index.go` & `internal/graph/postgres_index_test.go` (Create)
*   **Type / Scope:** Concrete `DependencyGraph` (`DBTX`) component and integration tests (`//go:build integration`).
*   **Proposed Changes:** Implement `LookupOutgoing` (accepts `DBTX`, joins target manifest + recursive active `superseded_by_memory_id` `<= 10` hops, computes `premise_drifted` using `target.status <> 'active' OR edge.expected_to_rev < target.last_reverted_version`), `LookupReverseDependents` (queries `idx_edges_reverse_active` for incoming edges on `GET /v1/memories/{id}`), and `UpsertEdges` (rejects self-edges, cross-workspace targets, intra-batch/DB cycles, and `> 5` active edges; supports preserving edges when `DependsOn == nil`, replacing on explicit slice, and retiring on `supersede`/`retire`).

##### 11. `cmd/mem1/main.go` (Create — Phase 1a Bootstrap)
*   **Type / Scope:** Binary entry point (compiles cleanly with Phase 1a packages only).
*   **Proposed Changes:** Wire `mem1 migrate [--down]` using `config.Load("migrate")` and `storage.Migrate`.

#### Phase 1b — Embeddings, Curation Worker, Retrieval Packer, Service Facade, Eval Runner & Unit/Integration Tests

##### 12. `internal/embedding/openai_embedder.go` & `internal/embedding/openai_embedder_test.go` (Create)
*   **Type / Scope:** `Embedder` interface, `/v1/embeddings` HTTP client, `MockEmbedder`, and unit tests.
*   **Proposed Changes:** Implement batched `/v1/embeddings` calls with `MEM1_EMBED_TIMEOUT`, dimension assertions (`1536`), and deterministic `MockEmbedder`. Cover HTTP decoding, timeout, dimension validation, and `MockEmbedder` in `openai_embedder_test.go` (unit).

##### 13. `internal/curation/interfaces.go`, `internal/curation/openai_curator.go`, `internal/curation/fallback.go`, `internal/curation/worker.go`, `internal/curation/worker_test.go`, & `internal/curation/worker_integration_test.go` (Create)
*   **Type / Scope:** Curation DTOs, `ValidatePlan`, OpenAI `/chat/completions` client + `MockCurator`, breaker + truncated provisional fallback, River workers, and unit/integration tests.
*   **Proposed Changes:**
    *   `interfaces.go`: Define `CurateRequest`, `CuratePlan`, `CurateMutation`, `InboxItemView`, `CandidateView`, `EdgeSpec`, `ValidationContext`, and `ValidatePlan` (100% inbox coverage, `'general'` category guard, `<= 5` intra-workspace DAG acyclicity, `TopicKey` slug regex).
    *   `openai_curator.go`: OpenAI `/chat/completions` structured JSON-schema client (`MEM1_CURATOR_TIMEOUT`), token usage tracking, and deterministic `MockCurator`.
    *   `fallback.go`: Thread-safe `Breaker` (`3` failures -> `60s` cooldown) shared cleanly + truncated `<= 1,500`-tok / `180`-char fallback upsert (`provisional:<inbox_id>`) writing revisions, citations, and audit log.
    *   `worker.go`: Implement `CurateWorkspaceWorker` (`activeWorkspaces` snooze, Phase A0 -> Phase A -> Phase B bisection + Attempt-3 fallback -> Phase C atomic snapshot check + provisional row delete), `EmbedMemoryWorker` (`topic_key + "\n" + summary_stub + "\n" + body`), and `ProvisionalReplayJob` (`<= 10`/min).
    *   `worker_test.go` (unit) & `worker_integration_test.go` (`//go:build integration`): Cover breaker, bisection, `ValidatePlan`, truncation, concurrency exclusion snooze, Attempt-3 fallback, provisional delete on replay, and poison-pill `'dead_letter'`.

##### 14. `internal/retrieval/packer.go`, `internal/retrieval/packer_test.go`, & `internal/retrieval/packer_integration_test.go` (Create)
*   **Type / Scope:** Query embedding (`MEM1_EMBED_TIMEOUT` + shared `Embedder` breaker), 1-connection `BEGIN READ ONLY` pipelined recall, and serialized `<= 2,000` token packer + unit/integration tests.
*   **Proposed Changes:** Implement `RecallContext` and `ExpandMemories` enforcing `max_tokens <= 2000` (default `2000`, serialized JSON counting via bundled `cl100k_base` with deterministic 4-tier eviction order, `memory_expand` workspace authorization and `truncated: true`). Add unit packing/eviction tests in `packer_test.go` and PG recall tests in `packer_integration_test.go` (`//go:build integration`).

##### 15. `internal/service/memory_service.go` & `internal/service/memory_service_test.go` (Create)
*   **Type / Scope:** Core application facade and unit tests.
*   **Proposed Changes:** Expose `HealthCheck`, `IngestWrite`, `RecallContext`, `ExpandMemories`, `GetConstitution`, `ListWorkspaces`, `CreateWorkspace`, `UpdateWorkspace`, `ListMemoriesKeyset`, `GetMemory`, `GetRevisions`, `ExplainLineage`, `ManualCreate`, `ManualEdit`, `RevertMemory`, `SoftRetire`, `HardPurge`, `OpsStatus`, `ListCurationDecisions`, `ListDeadLetters`, `IssueConfirmToken`, `ConsumeConfirmToken`, and `ReindexEmbeddings` (CLI helper updating inline embeddings in batches of 32 without incrementing `current_version`). Test validation, category enforcement, `IngestWrite` byte limits, and confirmation token orchestration in `memory_service_test.go` (unit with fakes).

##### 16. `eval/runner.go`, `eval/runner_test.go`, & `eval/datasets/README.md` (Create)
*   **Type / Scope:** Priority 1 standalone quality evaluation harness (`mem1 eval`).
*   **Proposed Changes:** Specify exact JSONL input schema (`{sample_id, dataset, sessions: [{session_id, occurred_at, turns: [{role, content}]}], question, expected_answer, category}`), `--dataset-dir` (default `eval/datasets`), `--limit` flag, Reader prompt (strict context-only answering at `temp = 0.0`), and Judge prompt (`correct | incorrect`, 3-seed majority vote per [evaluation.md](../../evaluation.md#L19)). Instantiate an embedded in-process River client on `curationPool` inside `eval/runner.go` (so Phase 1b `mem1 eval` executes jobs without `mem1 serve`), isolate each sample in its own project workspace (`+ 'general'` facts when tested), ingest turns preserving `occurred_at`, poll until River `"curation"` and `"embedding"` queues have 0 active/available rows, query `RecallContext` with `max_tokens <= 2000`, and verify `0` boundary/cap violations (`--mock` runs an embedded synthetic 4-sample fixture with deterministic mock Reader/Judge for offline smoke tests). Cover queue-drain waiting and judge scoring in `eval/runner_test.go` (unit).

##### 17. `cmd/mem1/main.go` (Modify — Phase 1b Gate)
*   **Type / Scope:** Register `reindex [--batch-size=32]` and `eval [--suite=...] [--max-tokens=2000] [--dataset-dir=...] [--limit=0] [--mock]` subcommands using Phase 1b packages.

#### Phase 2 — Unified HTTP + MCP Server, `mem1 mcp-stdio`, Operator CLI & Concurrency/Chaos Suites

##### 18. `internal/httpapi/middleware.go`, `internal/httpapi/server.go`, & `internal/httpapi/handlers.go` (Create)
*   **Type / Scope:** Unified `:8787` `net/http` mux serving `/mcp` and all **18** HTTP routes (`/healthz` + 17 `/v1/*` routes) with `log/slog` logging.
*   **Proposed Changes:** Implement split `MEM1_AGENT_TOKEN` (`/mcp`) vs `MEM1_ADMIN_TOKEN` (`/v1/*` mandatory), `Host`/`Origin` validator, route-aware `MEM1_MAX_INFLIGHT = 512` (returning `503 + Retry-After` on `/v1/*` and passing `inflightSaturated` context flag + buffered `X-Memory-Degraded`/`Cache-Control` headers on `/mcp`), `80 KiB` `/mcp` envelope vs `64 KiB` `/v1/*` body caps, keyset pagination + `GET /v1/workspaces/{id}/constitution` (`304`) + `GET /v1/ops/dead-letters`, and canonical `{error, code}` HTTP status mapping.

##### 19. `internal/mcpserver/server.go` (Create)
*   **Type / Scope:** Streamable HTTP handler mounted on `/mcp` and `stdio` MCP runner (`modelcontextprotocol/go-sdk`).
*   **Proposed Changes:** Register `memory_write` (`agent_observed`), `memory_recall` (fail-open `degraded: true`, never `IsError: true`), `memory_expand`, `memory_manage` (strict `retire` `working`/`durable` OR privileged `constitution`/`revert`/`purge` consuming PostgreSQL `confirmation_token`), and `mem1://workspaces/{id}/constitution` JSON-RPC resource.

##### 20. `cmd/mem1/main.go` & `cmd/mem1/main_test.go` (Modify / Create — Phase 2 Full CLI)
*   **Type / Scope:** Add `serve`, `mcp-stdio`, `list`, `explain`, `revert`, `purge`, `confirm-token`, and `dead-letters` (`--id=<uuid>` flags via stdlib `flag`); add `main_test.go` (unit) covering flag parsing and command dispatch.

##### 21. Phase 2 Wire, Load & Chaos Test Suites (Create)
*   `internal/httpapi/api_test.go` (unit/`httptest`) & `internal/httpapi/api_integration_test.go` (`//go:build integration`) — wire tests across all 18 HTTP endpoints (`GET /v1/workspaces/{id}/constitution` `304`, `GET /v1/ops/dead-letters`, `GET /v1/memories` `include_revisions` limit cap `<= 50`), split token auth (`MEM1_AGENT_TOKEN` cannot access `/v1/*`; unauthenticated `/v1/*` rejected on loopback), `Origin`/`Host`, `413`/`409`/`422`/`503`.
*   `internal/mcpserver/mcp_test.go` (`//go:build integration`) — wire tests over Streamable HTTP (`/mcp`) and `stdio` subprocess IPC verifying cross-process `confirmation_tokens` minted via `/v1/auth/confirm-token` and consumed inside `mem1 mcp-stdio`, `memory_write` replay/`422`, `memory_manage` `invalid_action` on `working` edits, and saturated/outage `memory_recall` returning `degraded: true` + `X-Memory-Degraded: true` + `Cache-Control: no-store`.
*   `tests/load/concurrency_test.go` (`//go:build integration && load`) — TCP-relay `FailingPoolProxy` fault-injection proxy + `FailingCurator` running `100` concurrent workers for `60s` at `150 RPS` (`70%` recall / `25%` write / `5%` disjoint-ID manage): `TestConcurrencyAndChaos` (`-race`, verifies `0` `409` on writes, `0` `200 OK []` during Postgres outage, and provisional replay cleanup on `Curator` outage) and `TestRecallLatencySLO` (uninstrumented, `MockEmbedder`, verifies `p95 <= 100 ms` PostgreSQL/Go recall).

##### 22. Repository Documentation Updates (Modify)
*   `README.md` (`Modify`) — replace greenfield placeholders with `mem1` setup, Docker PG17 bootstrap, test, and eval commands once implementation exists.
*   `docs/designs/mem1-v1-design.md` (`Modify`) — transition `Status: Proposed` -> `Accepted` upon approval.
*   `_agents/skills/nacfson-memory-elephant/SKILL.md` (`Modify`) — document implemented Go package layout once built.

### Verification Plan

*   **Compilation / Static Binary Check (Phases 1a, 1b, 2):**
    `CGO_ENABLED=0 go build -o bin/mem1 ./cmd/mem1` must exit `0` at the end of each phase and produce a static ELF binary (verified with `ldd bin/mem1` reporting `"not a dynamic executable"` or `"statically linked"`).
*   **Offline Unit & Race Checks:**
    *   Phase 1a: `go test -race ./internal/... ./cmd/...` must exit `0`.
    *   Phase 1b & Phase 2: `go test -race ./internal/... ./cmd/... ./eval/...` must exit `0` with zero races and without requiring PostgreSQL.
*   **Static Analysis & Linting:**
    `go vet ./... && golangci-lint run ./...` (default ruleset) must exit `0` with zero findings.
*   **Per-Package Statement Coverage Gate (`>= 80%`):**
    ```bash
    docker compose up -d postgres \
      && go build -o bin/mem1 ./cmd/mem1 && ./bin/mem1 migrate \
      && for pkg in ./internal/config ./internal/security ./internal/storage ./internal/search ./internal/graph ./internal/embedding ./internal/curation ./internal/retrieval ./internal/service; do \
           go test -tags=integration -cover "$pkg"; \
         done
    ```
    Every listed package must pass and report `>= 80.0%` statement coverage.
*   **Priority 1 Integration & Quality Eval Gate (End of Phase 1b):**
    Requires live `MEM1_CURATOR_*`, `MEM1_EMBED_*`, `MEM1_EVAL_READER_*`, and `MEM1_EVAL_JUDGE_*` endpoints configured (`./bin/mem1 eval --mock` verifies plumbing offline in CI without accuracy gates):
    ```bash
    docker compose up -d postgres \
      && CGO_ENABLED=0 go build -o bin/mem1 ./cmd/mem1 \
      && ./bin/mem1 migrate \
      && go test -tags=integration -race ./internal/... ./eval/... \
      && ./bin/mem1 eval --suite=longmemeval_s,memoryagentbench_conflict --max-tokens=2000
    ```
    Must verify `0` token-cap breaches, `0` automated `constitution` or `'general'` boundary violations, `>= 85%` on `MemoryAgentBench` Conflict Resolution, and `>= 75%` on `LongMemEval_S`.
*   **Priority 2 Concurrency & Chaos Gates (End of Phase 2):**
    1. Wire + functional race/chaos gate:
       ```bash
       go test -tags="integration load" -race -run TestConcurrencyAndChaos ./internal/httpapi/... ./internal/mcpserver/... ./tests/load/...
       ```
       Must sustain `100` concurrent workers (`150 RPS` for `60s`) with `0` write `409 Conflict`s, `0` `200 OK []` responses under induced Postgres outage, and verified `provisional:<inbox_id>` cleanup on `Curator` recovery.
    2. Uninstrumented tail-latency gate (run without `-race` overhead):
       ```bash
       go test -tags="integration load" -run TestRecallLatencySLO ./tests/load/...
       ```
       Must sustain `>= 100` concurrent callers with `p95 <= 100 ms` PostgreSQL + Go recall latency (`servingPool`).

--------------------------------------------------------------------------------

*Drafted with EGM*
