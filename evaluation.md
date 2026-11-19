# mem1 Architecture & Research Report Evaluation

- **Target:** [mem1-architecture-and-research-report.html](./mem1-architecture-and-research-report.html)
- **Date:** 2026-11-19

## Overall Verdict

- **Research grounding is unusually strong.** The external references are real and accurately characterized (Perplexity's `sessions/` -> `notes/` -> `knowledge/` Brain/Dream wiki, the stateless `2026-07-28` MCP spec with `Mcp-Method` + `2025-11-25` fallback, `LongMemEval-V2` / `arXiv:2605.12493`, `grok.com/connectors` + `grok-build`, and Meta Muse's Secure VM / Sentinel constraints).
- **Architecture is over-built on distributed plumbing and under-specified on AI memory quality.** Roughly 80% of the design optimizes for multi-region SaaS scale (10+ stateful components, custom Valkey RYOW watermarks, 4 access surfaces, 4-language SDKs, SSE reverse proxy), while containing **four concrete correctness/SLO contradictions** in concurrency, cryptography, and Dream verification — and leaving the hardest cross-agent semantic problems unsolved.
- **Missing backing specs:** The report references `docs/01–06` (e.g., `~/Projects/Nacfson/mem1`), which are not in this repo yet, so full DDL/Lua/schema details cannot be audited directly here.

---

## What the Architecture Gets Right (Keep)

1. **Asymmetric Workload Separation ([Sec 5](./mem1-architecture-and-research-report.html#sec-5)):** Correctly diagnoses that `Extract`/`Dream` are 90–98% LLM I/O bound and must never sit on synchronous serving paths, and rightly warns against optimizing gateway languages before fixing remote embedding latency.
2. **Deterministic Hooks + Cold-Start Briefs ([Sec 2.1](./mem1-architecture-and-research-report.html#sec-2)):** Recognizing that voluntary LLM tool-calling misses ~20–40% of relevant recalls — and pairing `SessionStart`/`Stop`/`PostToolUse` hooks with compiled `memory://workspace/{id}/brief` resources — is the highest-leverage UX choice in the document.
3. **Provider Agnosticism & Practical Details ([Sec 1.1](./mem1-architecture-and-research-report.html#sec-1), [Sec 2](./mem1-architecture-and-research-report.html#sec-2)):** Pinning `embedding_space_id` with 4-phase migration prevents silent cosine corruption; appending injected memory *after* the static system prompt preserves provider KV-caching.
4. **Evaluation Rigor ([Sec 6](./mem1-architecture-and-research-report.html#sec-6)):** Avoiding non-commercial LoCoMo in favor of MIT/Apache-2.0 `LongMemEval` + `LongMemEval-V2` + `MemoryAgentBench`, enforcing a strict `<= 2,000` token cap, 3-seed majority voting with paired McNemar's test, and open-loop load testing (`k6`/`ghz`) is strong engineering discipline.

---

## Critical Engineering Flaws & Contradictions (Fix Before Building)

### 1. Synchronous Semantic Dedup (`cos >= 0.92`) Breaks Both Correctness and the `Store` SLO ([Sec 3.1](./mem1-architecture-and-research-report.html#sec-3), Sec 3.2 #4, Sec 3.3)
- **Silent data corruption:** Dense embeddings routinely assign `cos >= 0.93–0.97` to **opposite or updated technical facts** (e.g., `"Postgres max_connections is 200"` vs. `"500"`, `"prefer dark mode"` vs. `"light mode"`, `"never auto-migrate prod"` vs. `"always auto-migrate prod"`). Auto-merging at `cos >= 0.92` on `Store` will silently destroy updates and mask contradictions.
- **SLO contradiction:** `Store` defaults to `dedup_mode="semantic"` while holding a **single-writer namespace `dedup_lease`** across both foreground `Store` and background `Extract`, running Embed + Qdrant ANN + Valkey `pending_emb` + Postgres unprojected scans before `COMMIT`. At 25–50 ms per write (or 100–300 ms with remote embeddings), a single shared workspace caps at **~20–40 writes/s**; 20 concurrent agents (or an `Extract` burst) will blow the `80–90 ms` p99 SLO by 5–10x.
- **Fencing race:** Checking a Valkey lease in Step 4 Lua *before* opening the Postgres TX does not protect against a GC/scheduler/network stall between Lua and `COMMIT` unless Postgres itself enforces a monotonic fencing token inside the `COMMIT` transaction.
- **Recommendation:** Make synchronous `Store` fast and lock-free with **exact/canonical-hash dedup only**. Move semantic dedup and contradiction detection (using claim comparison / LLM reconciliation, not bare cosine) entirely into async `Extract`/`Dream`.

### 2. Custom Valkey RYOW Overlay Is Fragile Plumbing Solving a Solved Postgres Problem ([Sec 3.2 #3–4](./mem1-architecture-and-research-report.html#sec-3), [Sec 7](./mem1-architecture-and-research-report.html#sec-7))
- The 53-bit `(leader_epoch << 40) | seq_id` bit-packed Valkey overlay (`RYOW`, `pending_emb`, `tombstones`, Step 5 Lua scripts) already surfaced two **resurrection/consistency bugs** in the report's own "Non-Blocking Implementation Notes" (`hard_purge` revival if Step 5 stalls; `COLD_MISS` on Valkey restart between Postgres `COMMIT` and Step 5).
- **Recommendation:** Drop the Valkey RYOW overlay. Unprojected writes for a namespace (`WHERE seq_id > $projected_seq`) are a tiny hot tail in Postgres `shared_buffers`. Reading them directly from an indexed `(tenant_id, memory_namespace, seq_id)` query takes 1–3 ms, gives true ACID RYOW across failovers and restarts, and eliminates an entire class of distributed cache-coherency bugs. Keep Valkey strictly for stateless caches (embeddings, briefs) and rate limits.

### 3. Encryption-at-Rest vs. Search & GDPR Crypto-Shredding Contradictions ([Sec 1.1](./mem1-architecture-and-research-report.html#sec-1), [Sec 4.2](./mem1-architecture-and-research-report.html#sec-4))
- **Asymmetric protection:** Encrypting Postgres `BYTEA` + NATS outbox while projection workers unwrap DEKs into unencrypted BM25/Tantivy inverted indexes, invertible dense vectors in Qdrant, and plaintext SQL `memory_edges` protects against raw Postgres dump exfiltration, not compromise of the search plane or worker KMS role — while paying the full operational tax of per-record sub-DEKs and KMS fan-out (50 search hits = up to 50 sub-DEKs to unwrap or cache-invalidate on purge).
- **`version` in GCM AAD (`tenant_id || memory_id || version`):** Forces decrypt + re-encrypt of `ciphertext_payload` on every metadata-only `PATCH` or citation `MERGE` that increments `version`.
- **GDPR `hard_purge` vs. Git-compatible S3 snapshots:** Filtering `purged_memories` on `/revert` leaves erased PII physically present inside immutable Merkle-chained Git objects in S3. And once User A's turn is synthesized into a shared workspace `knowledge/` page, crypto-shredding User A's sub-DEK cannot erase that derived text without rewriting the shared snapshot.
- **Recommendation:** Separate the threat models cleanly: use **namespace-level KMS DEKs** (not per-record sub-DEKs), drop `version` from payload AAD (bind AAD to immutable `content_id`), and handle GDPR erasure on compiled snapshots via **snapshot rewriting + retention GC** rather than pretending revert-time filtering equals physical erasure.

### 4. Dream `4-Check Verify Gate` Brittleness & Quarantine Starvation ([Sec 4.2](./mem1-architecture-and-research-report.html#sec-4))
- **Local NLI mismatch:** Local NLI models (trained on short MNLI sentence pairs) perform poorly on 65 KB synthesized Markdown diffs, temporal progressions (`"v1 used X, v2 moved to Y"`), and subtle numeric/config flips (`timeout=30s` -> `3s`).
- **Citation existence != grounding:** Checking that `[cite:<event_uuidv7>]` exists in `memory_events` does not prevent an LLM from attaching a real event ID to a hallucinated or misattributed claim.
- **Quarantine deadlock:** All-or-nothing `"Abort & Quarantine"` on any check failure risks permanently wedging a namespace's Dream loop whenever one poison-pill turn repeatedly triggers `unconsolidated >= 5`.
- **Recommendation:** Commit Dream updates at **per-page or per-claim granularity** with poison-pill dead-lettering, and replace whole-page NLI with claim-level entailment against the exact cited span.

---

## Under-Specified AI Memory Problems

Even if all infrastructure works flawlessly, four memory-quality gaps will decide whether the system feels magical or noisy across agents:

1. **Cross-Agent Trust & Context Pollution:** In a shared `workspace:shared` namespace, a speculative ChatGPT/Grok web brainstorm carries the same structural weight as a verified `claude-code` tool execution or merged commit. You need an explicit **authority/provenance hierarchy** (`user_explicit` / `verified_tool_outcome` > `agent_inference`) and a low-friction **correction/downvote loop** when recalled memory is wrong.
2. **Codebase Staleness Invalidation:** Binding `workspace_id` to Git remote + branch isn't enough — code facts rot when files are refactored. Code-origin memories should carry optional `(repo_path, symbol, commit_sha)` anchors so hooks or Dream can auto-demote/re-verify memories whose referenced files changed.
3. **Salience Decay Forgetting Rare Critical Rules:** Time/access decay (`S(t) < 0.15`) will prune rarely-accessed institutional invariants (security policies, compliance rules, `"never run X in prod"`). Add a first-class **`pinned` / `decay_exempt`** class (constraints, preferences, architecture invariants).
4. **Files vs. MCP Search Trade-Off (from `LongMemEval-V2` itself):** Notably, the `LongMemEval-V2` paper cited in Sec 6 found that file-based coding-agent exploration (`AgentRunbook-C`) outperformed pure RAG on complex agent trajectories. For local coding agents (`Claude Code`, `Cursor`, `Codex`), syncing compiled `knowledge/` Markdown locally for native `rg`/read access alongside lightweight MCP lookup may outperform routing everything through 8 MCP tools + hybrid vector/BM25/graph plumbing.

---

## Scope & Buildability Critique

Although [Sec 5.2](./mem1-architecture-and-research-report.html#sec-5) rightly lists **Topology A (Postgres monolith)** for 1–5 engineers, the rest of the brief designs around **Topology B** (~10 stateful services) plus **4 full access surfaces**:
- **Cut the AI Reverse Proxy (Surface 4) from v1/v2:** It cannot sit in front of consumer web chats (ChatGPT, Claude Web, Grok.com, Meta Muse), and intercepting streaming SSE tool turns couples you to fast-changing provider wire formats (thinking blocks, server tools, cache breakpoints) for narrow API-only upside.
- **Start with 2 surfaces, 1 database:** Ship **Postgres 17 (`pgvector` + `pg_search` + SQL `memory_edges` + Postgres queue)** behind **REST + MCP (`2026-07-28` / `2025-11-25`)** plus a tiny local CLI hook daemon for coding agents. Defer gRPC, multi-language offline-WAL SDKs, Qdrant, NATS, Valkey RYOW, and Temporal until measured traffic or benchmark ceilings prove you need them.
- **Prove memory quality before distributed scale:** Run a minimal Topology A prototype + Dream loop against `LongMemEval_S` and a slice of `LongMemEval-V2` first. If compiled wiki Dream beats baseline RAG within your token budget, scaling the storage plane later is straightforward because the wire contract stays stable.

---

## Open Scoping Questions

Before turning this into a formal specification in [docs/designs/SITEMAP.md](./docs/designs/SITEMAP.md), three scoping decisions will determine ~70% of the architecture:

1. **Target deployment first:** Are you building this primarily for **personal / small-team self-hosted cross-agent memory** (Topology A/D), or a **multi-tenant commercial SaaS** from day one?
2. **Backing specs:** Do `docs/01–06` already exist in another repo (`~/Projects/Nacfson/mem1`) that you plan to bring in and revise, or is this HTML report the starting input for drafting clean specs here?
3. **Primary v1 agents:** Which 2–3 agents do you actually want connected on Day 1 (e.g., Claude Code / Codex / Cursor + ChatGPT / Claude Web via MCP)?
