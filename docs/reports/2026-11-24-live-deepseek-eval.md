# Live DeepSeek evaluation and failure analysis

**Date.** 2026-11-24
**Baseline commit.** `c1c7e26` plus local `.env` and `--env-file` support
**Runtime stack.** `mem1 serve` (`:8787`), PostgreSQL 17, `deepseek-chat` (`https://api.deepseek.com/v1`), and `MEM1_EMBED_PROVIDER=mock`

This report records a live evaluation run executed through Orca's native `mem1_mem1` MCP integration (`http://127.0.0.1:8787/mcp`). Across 3 test scenarios and 11 `memory_write` calls (`1,164` raw characters), `mem1` passed `6` of `9` rubric checks, produced `1` partial behavioral divergence, and surfaced `2` concrete bugs in curation schema guidance and dead-letter inspection.

## Test configuration

- **Provider split.** `MEM1_CURATOR_PROVIDER=openai_compat` using `deepseek-chat` with `MEM1_EMBED_PROVIDER=mock` (lexical GIN search only during recall).
- **Workspaces tested.** `orca-eval-agg` (aggregation, filler filtering, working TODO retirement), `orca-eval-drift` (conflict supersession and `DEPENDS_ON` drift), `general` and `orca-eval-iso` (personal boundary isolation and cross-workspace inheritance).
- **Audit method.** All timing, token counts, manifest rows, and error payloads were queried directly from PostgreSQL (`raw_inbox`, `curation_decisions`, `memory_manifest`, `memory_revisions`, `memory_edges`, and `river_job`).

## Rubric scorecard

| Scenario | Criterion | Result | Observed outcome |
| :--- | :--- | :--- | :--- |
| `orca-eval-agg` | `W1` and `W2` merge into one coupled topic | Pass | Merged into `billing-api-stack-and-latency-target` (`v2`, `64` tokens) |
| `orca-eval-agg` | `W3` chitchat dropped (`noop`) | Pass | Logged `noop` with no manifest entry created |
| `orca-eval-agg` | `W4` saved as `working` and `W5` retired | Fail | `W4` dead-lettered after 3 retries (`river_job` #57); `W5` logged `noop` |
| `orca-eval-drift` | `D3` supersedes REST JSON with gRPC | Pass | `internal-rpc-grpc-http2` active; `internal-rpc-rest-json` marked `superseded` |
| `orca-eval-drift` | Recall drops superseded REST decision | Pass | Returned only `internal-rpc-grpc-http2` plus inherited `general` rule |
| `orca-eval-drift` | `D2` adds `DEPENDS_ON` edge and triggers `premise_drifted` | Partial | DeepSeek revised `D1` (`v1` to `v2`) instead of splitting a topic (`0` edges) |
| `general` + `iso` | `G1` personal preference stored in `general` | Pass | Created `go-coding-style-preference` (`31` tokens) in `general` |
| `general` + `iso` | `G2` bug-fix note blocked from `general` | Pass | Rejected via `noop` citing reserved `general` workspace rules |
| `general` + `iso` | Project recall inherits `general` preference | Pass | `orca-eval-iso` recall returned both project stack and `general` rule |
| Operations | Zero dead letters and working `mem1 dead-letters` | Fail | `1` dead letter (`W4`), and `mem1 dead-letters` returned HTTP `503` |

## Predicted vs. reality failure analysis

### 1. `W4` dead-lettered under `json_object` fallback (Bug)

- **Predicted.** For `W4` (`"WIP TODO: add a benchmark for /v1/charge retries before Friday's release."`), `deepseek-chat` emits `depends_on: []` or an integer `expected_to_rev`, saving a `working` memory that `W5` later retires.
- **Reality.** Because DeepSeek falls back from strict `json_schema` to `json_object` mode, `curatorSystemPrompt` in `internal/curation/openai_curator.go` (lines 101-104) describes `depends_on` only in prose without a typed JSON example. DeepSeek emitted `"expected_to_rev": true` (boolean instead of integer), causing `CompileRawPlan` to fail across all 3 River attempts (`river_job` id `57` discarded with `json: cannot unmarshal bool into Go struct field rawPlanPayload.mutations.0.depends_on.0.expected_to_rev of type int`). `W4` moved to `dead_letter`, and `W5` later found no matching TODO candidate and logged `noop`.
- **Fix.** Include an explicit typed `depends_on` object example (`{"target_memory_id":"M1","expected_to_rev":1,"rationale":"..."}`) inside `curatorSystemPrompt`.

### 2. `mem1 dead-letters` crashes with HTTP `503` on discarded River jobs (Bug)

- **Predicted.** Running `./bin/mem1 dead-letters` or calling `GET /v1/ops/dead-letters` returns both dead-lettered `raw_inbox` items and discarded `river_job` rows.
- **Reality.** With real discarded River job #57 present, `./bin/mem1 dead-letters` failed with HTTP `503` (`scan discarded river job: can't scan into dest[5] (col: errors): failed to scan array element 0: json: cannot unmarshal object into Go value of type uint8`). In `internal/storage/pool.go` (lines 81 and 428), `DiscardedRiverJob.Errors` uses `json.RawMessage` (`[]uint8`), while River stores `river_job.errors` as `jsonb[]`. Prior test suites missed the bug because they contained `0` discarded River jobs.
- **Fix.** Change `DiscardedRiverJob.Errors` to `[]json.RawMessage` (or select `COALESCE(to_jsonb(errors), '[]'::jsonb)`) and add an integration test with a real discarded `river_job` row.

### 3. `curation_decisions` duplicates batch token counts and omits failed retries (Telemetry)

- **Predicted.** Summing `prompt_tokens` and `completion_tokens` across `curation_decisions` reports exact API token consumption per curation batch.
- **Reality.** In `internal/curation/worker.go` (lines 559-712), every mutation emitted in one batch records the full API call's `prompt_tokens` and `completion_tokens`. In `orca-eval-agg`, the `W2`+`W3` batch (`790` prompt and `256` completion tokens) stamped both the `revise` row and the `noop` row, inflating the raw table sum to `9,070` tokens (`7,363` prompt + `1,707` completion) vs. `8,024` actual tokens (`6,573` prompt + `1,451` completion). Meanwhile, `W4`'s 3 failed attempts consumed unlogged DeepSeek tokens because only committed batches write to `curation_decisions`.
- **Fix.** Record call-level tokens once per batch (or split proportionally across batch rows) and log failed attempt usage before retry or dead-lettering.

### 4. `D2` coupled revision skipped `DEPENDS_ON` edges (Behavior divergence)

- **Predicted.** `D2` (gateway middleware validating JSON schemas and injecting `X-Trace-Id` because internal RPCs use REST JSON) creates a separate topic linked to `D1` via `DEPENDS_ON`, so `D3` triggers `premise_drifted: true`.
- **Reality.** DeepSeek treated `D2` as an additive revision (`revise` `v1` to `v2`) on `internal-rpc-rest-json`, leaving `memory_edges` at `0` rows. `D3` still superseded `internal-rpc-rest-json` cleanly with `internal-rpc-grpc-http2` and noted the gateway middleware impact in prose.
- **Fix.** Add guidance and a few-shot rule in `curatorSystemPrompt` specifying when cross-component assumptions should be stored as separate linked topics instead of revising the upstream topic.

## Process latency and token economics

Nine committed DeepSeek calls processed `10` inbox items (`W4` failed on 3 unlogged attempts). Average end-to-end curation latency (`curation_decisions.created_at - min(raw_inbox.ingested_at)`) was `1.85 s` (min `1.062 s`, max `4.468 s`).

| Workspace | Batch | Actions | Items | Prompt tok | Completion tok | Latency (s) |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: |
| `orca-eval-agg` | `W1` | `create` | 1 | 646 | 147 | 1.249 |
| `orca-eval-agg` | `W2+W3` | `revise`, `noop` | 2 | 790 | 256 | 4.468 |
| `orca-eval-drift` | `D1` | `create` | 1 | 644 | 142 | 1.559 |
| `general` | `G1` | `create` | 1 | 632 | 126 | 1.159 |
| `general` | `G2` | `noop` | 1 | 715 | 87 | 1.062 |
| `orca-eval-iso` | `P1` | `create` | 1 | 639 | 143 | 1.827 |
| `orca-eval-agg` | `W5` | `noop` | 1 | 855 | 81 | 1.146 |
| `orca-eval-drift` | `D2` | `revise` | 1 | 757 | 220 | 1.584 |
| `orca-eval-drift` | `D3` | `supersede` | 1 | 895 | 249 | 2.548 |
| **Total (deduplicated)** | **9 calls** | **10 rows** | **10** | **6,573** | **1,451** | **1.845 avg** |

### Recall headroom and batch amortization

- **Bounded recall payloads.** Active plus inherited content tokens during `memory_recall` stayed at `95` (`orca-eval-agg`, `W/"ws:2+gen:1"`), `133` (`orca-eval-drift`, `W/"ws:3+gen:1"`), and `74` (`orca-eval-iso`, `W/"ws:1+gen:1"`) against the `2,000`-token ceiling, with `0` degraded responses.
- **Batch amortization.** Single-item batches averaged `723` prompt tokens per write (`5,783 / 8`). The 2-item batch (`W2+W3`) used `790` prompt tokens total (`395` per write, `46%` lower prompt overhead) because system prompt and candidate overhead (~`600` tokens) is paid once per batch.

### Estimated DeepSeek cost

Using published `deepseek-flash` / `deepseek-chat` API pricing (Off-Peak: `$0.15/1M` cache-miss input, `$0.003/1M` cache-hit input, `$0.60/1M` output; Peak: `$0.30/1M` miss, `$0.006/1M` hit, `$1.20/1M` output):

- **9 logged calls (`6,573` input, `1,451` output).** `$0.00186` (`0%` cache hit) to `$0.00118` (`70%` cache hit) Off-Peak, and `$0.00371` to `$0.00236` Peak (plus 3 unlogged failed retries on `W4`).
- **Projected `100` writes/day (`2` writes/batch, `70%` prefix cache hit).** `~$0.26/month` Off-Peak and `~$0.52/month` Peak for background curation, with `$0` LLM spend on `memory_recall`.

## Next fixes before production

1. Add an explicit typed `depends_on` object example to `curatorSystemPrompt` in `internal/curation/openai_curator.go`.
2. Fix `DiscardedRiverJob.Errors` scanning (`[]json.RawMessage` or `to_jsonb(errors)`) in `internal/storage/pool.go` and add a discarded-job integration test.
3. Deduplicate batch token reporting across `curation_decisions` and log failed retry usage in `internal/curation/worker.go`.
4. Pair `deepseek-chat` with a `1536`-dim embedding endpoint (`MEM1_EMBED_*`) and run `./bin/mem1 reindex` before production deployment.
