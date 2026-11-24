# mem1 (`nacfson-memory`) Blueprint

## System Overview

`mem1` is a single-binary Go + PostgreSQL 17 (`pgvector` `HALFVEC(1536)` + River) cross-agent memory backend designed for single-tenant personal or single-team deployments. Agents connect over MCP (stateless Streamable HTTP on `/mcp` or local `mem1 mcp-stdio`), while operators and scripts inspect and manage memory state via a unified `:8787` HTTP JSON API (`/healthz` + 17 `/v1/*` endpoints) and operator CLI subcommands.

Foreground writes append lock-free into `raw_inbox` (`source_authority = 'agent_observed'`) and enqueue durable River background jobs (`curate_workspace`, `embed_memory`, periodic `provisional_replay`), while recalls execute a single `BEGIN READ ONLY` hybrid vector + GIN lexical + 1-hop `DEPENDS_ON` query capped at `<= 2,000` serialized JSON tokens (`cl100k_base`).

- **Design Specification:** [docs/designs/mem1-v1-design.md](./docs/designs/mem1-v1-design.md)
- **Design Sitemap:** [docs/designs/SITEMAP.md](./docs/designs/SITEMAP.md)
- **Elephant Skill:** [\_agents/skills/nacfson-memory-elephant/SKILL.md](./_agents/skills/nacfson-memory-elephant/SKILL.md)

## Subdirectories & System Boundaries

- `cmd/mem1/` — Static Go CLI binary:
  - Direct-DB commands: `serve`, `mcp-stdio`, `migrate [--down]`, `reindex [--batch-size=32]`, `eval [--mock]`
  - Operator HTTP commands (`MEM1_HTTP_ADDR` + `MEM1_ADMIN_TOKEN`): `list`, `explain`, `revert`, `purge`, `confirm-token`, `dead-letters`
- `migrations/` — Embedded PostgreSQL 17 SQL migrations (`0001_init.up.sql` / `0001_init.down.sql`) + River queue migrations.
- `internal/config/` — Mode-specific environment loading, ZDR endpoint checks, and serving/curation pool sizing.
- `internal/security/` — Secret scrubber (`SecretScrubber`), ZDR endpoint validator, loopback host helpers, and `topic_key` slug rules.
- `internal/storage/` — Dual `pgxpool` pools (`ServingPool`, `CurationPool`), `raw_inbox`, `memory_manifest`, `memory_revisions`, `memory_edges`, `confirmation_tokens`, keyset pagination, and ops queries.
- `internal/curation/` — Per-workspace serialized River curation/embedding/provisional-replay workers, circuit breakers, plan validation/bisection, and OpenAI-compatible / mock curators.
- `internal/embedding/` — OpenAI-compatible & deterministic mock `1536`-dim vector embedders.
- `internal/search/` & `internal/graph/` — PG17 GIN + `OR`-lexeme lexical search and intra-workspace `DEPENDS_ON` BFS cycle/drift detection.
- `internal/retrieval/` — Pipelined read-only `Packer`, RRF ranking, `premise_drifted` resolution, and 4-tier `<= 2,000` token packer.
- `internal/service/` — Unified application service (`MemoryService`) backing MCP and HTTP handlers.
- `internal/mcpserver/` — Stateless MCP server exposing `memory_write`, `memory_recall`, `memory_expand`, `memory_manage`, and `mem1://workspaces/{id}/constitution`.
- `internal/httpapi/` — Unified `:8787` mux + middleware (`Host`/`Origin` check, split bearer tokens, `MEM1_MAX_INFLIGHT` saturation guard, `64 KiB` / `80 KiB` body caps, buffered degraded headers).
- `eval/` — Benchmark harness for `LongMemEval_S` and `MemoryAgentBench` Conflict Resolution (`eval/datasets/`).
- `tests/load/` — Priority 2 `100`-worker concurrency, PG/Curator outage chaos proxy, and `p95 <= 100 ms` recall latency SLO tests.

## Quickstart

### 1. Start PostgreSQL 17 (`pgvector:pg17`) and Build

```bash
podman compose up -d   # or: docker compose up -d
make build             # produces static bin/mem1 (CGO_ENABLED=0)
MEM1_DATABASE_URL="postgres://mem1:mem1@127.0.0.1:5432/mem1?sslmode=disable" ./bin/mem1 migrate
```

### 2. Run Unified Server (`serve`)

Mock mode (local testing with zero external API keys):

```bash
MEM1_DATABASE_URL="postgres://mem1:mem1@127.0.0.1:5432/mem1?sslmode=disable" \
MEM1_ADMIN_TOKEN="dev-admin-local" \
MEM1_CURATOR_PROVIDER="mock" \
MEM1_EMBED_PROVIDER="mock" \
./bin/mem1 serve
```

Live OpenAI-compatible provider mode requires `MEM1_CURATOR_BASE_URL`, `MEM1_CURATOR_MODEL`, `MEM1_CURATOR_API_KEY`, `MEM1_EMBED_BASE_URL`, `MEM1_EMBED_MODEL`, `MEM1_EMBED_API_KEY`, and `MEM1_EMBED_SPACE_ID` (set `MEM1_ALLOW_NON_ZDR=true` only if using an explicitly opted-in non-ZDR endpoint).

### 3. Connect MCP Clients

- **Streamable HTTP (`/mcp` on `:8787`, no idle SSE streams):** point MCP clients to `http://127.0.0.1:8787/mcp` (and pass `Authorization: Bearer $MEM1_AGENT_TOKEN` if `MEM1_AGENT_TOKEN` is set).
- **Local stdio (`mcp-stdio`, no background workers spawned):** invoke `./bin/mem1 mcp-stdio` with `MEM1_DATABASE_URL` and `MEM1_EMBED_*` env vars while `mem1 serve` runs background curation in parallel.

### 4. Operator CLI Examples

```bash
export MEM1_ADMIN_TOKEN="dev-admin-local"
curl -s http://127.0.0.1:8787/healthz
./bin/mem1 list --workspace=my-project
./bin/mem1 explain --id=<memory_uuid>
./bin/mem1 confirm-token --workspace=my-project --action=create
./bin/mem1 dead-letters --limit=20
```

## Global Invariants

- **Bounded Agent Recall (`<= 2,000` tokens):** Both `memory_recall` and `memory_expand` strictly pack serialized JSON within `<= 2,000` `cl100k_base` tokens; unlimited inspection lives on `/v1/memories*` (`MEM1_ADMIN_TOKEN` required).
- **Split Pools & Fail-Open Reads:** `ServingPool` uses a `100 ms` statement timeout (`serving_max_conns=28`), isolated from `CurationPool` (`5000 ms`, `curation_max_conns=6`). Under DB/Embedder failure or `/mcp` saturation, `memory_recall` degrades cleanly (`degraded: true`, HTTP `X-Memory-Degraded: true`, `Cache-Control: no-store`) and never fabricates empty non-degraded results (`200 OK []`).
- **Workspace & Authority Isolation:** `'general'` forbids `working` durability and restricts manual entries to `personal_profile` / `personal_preference`; `constitution` mutations and `revert`/`purge` over MCP require a single-use PostgreSQL `confirmation_token` minted by an admin.
- **Static Zero-SSE MCP Capabilities:** `mcp.NewServer` explicitly sets `Tools.ListChanged = false`, `Resources.ListChanged = false`, and `Resources.Subscribe = false` so HTTP MCP clients use stateless `POST /mcp` JSON without opening persistent `subscriptions/listen` streams.
- **Test Database Isolation:** Integration suites (`-tags=integration` / `-tags="integration load"`) reset tables on `MEM1_TEST_DATABASE_URL` (falling back to `MEM1_DATABASE_URL`). Set `MEM1_TEST_DATABASE_URL` to a separate scratch database before running tests alongside a live `mem1` database.

## System Verification

- Static build check: `make build && ldd bin/mem1` (`not a dynamic executable`)
- Static analysis & offline unit tests (`-race`): `go vet ./... && make test`
- PostgreSQL 17 integration suite (`-race`): `make test-integration`
- Offline eval harness smoke check: `MEM1_DATABASE_URL="postgres://mem1:mem1@127.0.0.1:5432/mem1?sslmode=disable" ./bin/mem1 eval --mock`
- Priority 2 100-worker concurrency/chaos + tail-latency gates: `make loadtest`
