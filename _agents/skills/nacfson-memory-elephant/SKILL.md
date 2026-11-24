---
name: nacfson-memory-elephant
description: >-
  Load project context, design history, and architectural boundaries for the
  nacfson-memory repository before brainstorming, drafting, or implementing
  changes.
---

# nacfson-memory Elephant Context

Load these entry points when starting an Elephant session in `nacfson-memory`:

1. [Repository Blueprint](../../../README.md) — Top-level system boundaries, quickstart, global invariants, and verification commands.
2. [Design Specifications Sitemap](../../../docs/designs/SITEMAP.md) — Index of approved designs and RFCs.
3. [mem1 v1 Design Specification](../../../docs/designs/mem1-v1-design.md) — Full schema, curation, retrieval, HTTP/MCP/CLI wire contracts, and verification gates.

## Key Code Paths

- `cmd/mem1/main.go` — Static Go binary entrypoint (`serve`, `mcp-stdio`, `migrate`, `reindex`, `eval`, `list`, `explain`, `revert`, `purge`, `confirm-token`, `dead-letters`).
- `migrations/0001_init.up.sql` & `0001_init.down.sql` — PostgreSQL 17 + `pgvector` `HALFVEC(1536)` schema, GIN/HNSW indexes, and River job queues.
- `internal/config/config.go` — Mode-aware configuration validation, split pool caps, ZDR provider enforcement, and loopback auth guards.
- `internal/security/` — Pre-DB secret scrubbing (`SecretScrubber`), host/ZDR URL validation, and `topic_key` slug checks.
- `internal/storage/` — Dual `pgxpool` pools (`ServingPool` 100ms timeout, `CurationPool` 5s timeout), inbox ingestion, manifest/revision/edge persistence, confirmation tokens, and audit queries.
- `internal/curation/` — Serialized per-workspace River background workers (`CurateWorkspaceWorker`, `EmbedMemoryWorker`, `ProvisionalReplayWorker`), circuit breakers, `ValidatePlan`, and provisional fallback.
- `internal/retrieval/` — Single read-only tx pipelined recall (`Packer`), RRF fusion, `DEPENDS_ON` drift checks, and `cl100k_base` `<= 2,000` serialized token packer.
- `internal/search/` & `internal/graph/` — PostgreSQL GIN + `OR`-lexeme lexical search and intra-workspace `DEPENDS_ON` BFS cycle/drift resolution.
- `internal/embedding/` — OpenAI-compatible & mock `HALFVEC(1536)` embedders.
- `internal/service/memory_service.go` — Core application facade unifying validation, scrubbing, manual CAS mutations, lineage, and ops introspection.
- `internal/mcpserver/server.go` — Stateless MCP server (`memory_write`, `memory_recall`, `memory_expand`, `memory_manage`, `mem1://workspaces/{id}/constitution`) over HTTP `/mcp` and `mem1 mcp-stdio`.
- `internal/httpapi/` — Unified `:8787` HTTP JSON mux (`/healthz` + 17 `/v1/*` routes + `/mcp`), split token auth, `Host`/`Origin` validator, `MEM1_MAX_INFLIGHT` saturation guard, and buffered degraded headers.
- `eval/` — Priority 1 `LongMemEval_S` / `MemoryAgentBench` evaluation runner + offline `--mock` smoke harness.
- `tests/load/concurrency_test.go` — Priority 2 `100`-worker chaos proxy + `p95 <= 100 ms` recall SLO suite (`make loadtest`).
