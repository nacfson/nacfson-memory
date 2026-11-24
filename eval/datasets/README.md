# `mem1` Priority 1 Evaluation Datasets

Put downloaded evaluation benchmark files in this directory (`*.jsonl`, `*.json`, and `*.parquet` are gitignored via [.gitignore](../../.gitignore)).

## Supported Files

- `eval/datasets/longmemeval_s.jsonl` (`LongMemEval_S`, `>= 75%` gate)
- `eval/datasets/memoryagentbench_conflict.jsonl` (`MemoryAgentBench` Conflict Resolution slice, `>= 85%` gate)

## JSONL Line Schema

Each line in a dataset JSONL file must match:

```json
{
  "sample_id": "sample-001",
  "dataset": "memoryagentbench_conflict",
  "category": "conflict_resolution",
  "sessions": [
    {
      "session_id": "s1",
      "occurred_at": "2026-10-01T10:00:00Z",
      "turns": [
        {"role": "user", "content": "Primary database is SQLite."}
      ]
    }
  ],
  "question": "What is our primary database?",
  "expected_answer": "PostgreSQL 17"
}
```

## Running Evaluations

Offline pipeline smoke test (no external LLM/embedding keys needed, uses 4 embedded synthetic samples):

```bash
export MEM1_DATABASE_URL="postgres://mem1:mem1@127.0.0.1:5432/mem1?sslmode=disable"
./bin/mem1 migrate
./bin/mem1 eval --mock
```

Live calibrated or full benchmark run:

```bash
./bin/mem1 eval \
  --suite=longmemeval_s,memoryagentbench_conflict \
  --max-tokens=2000 \
  --dataset-dir=eval/datasets \
  --limit=25
```
