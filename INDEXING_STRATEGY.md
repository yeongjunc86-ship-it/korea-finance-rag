# Indexing Strategy (Phase 1 Step 4)

Version: v1.0
Last Updated: 2026-02-15

## 1. Commands
### Full Rebuild (authoritative)
```bash
cd /home/aidome/workspace/korea-finance-rag
source .venv/bin/activate
./scripts/run_index_full.sh
```

- Uses `scripts/build_index.py`
- Rebuilds `data/index/chunks.jsonl` from all `data/raw/*.json`
- Writes state snapshot to `data/index/index_state.json`

### Incremental Update (fast path)
```bash
cd /home/aidome/workspace/korea-finance-rag
source .venv/bin/activate
./scripts/run_index_incremental.sh
```

- Uses `scripts/build_index_incremental.py`
- Detects changed/removed raw files via `index_state.json`
- Rebuilds only changed docs and merges into existing index

Dry-run change check:
```bash
python scripts/build_index_incremental.py --dry-run
```

One-time state sync (when existing `chunks.jsonl` exists but `index_state.json` is missing):
```bash
python scripts/sync_index_state.py
```

## 2. Change Detection Rule
A raw file is considered changed when either differs from previous snapshot:
- `mtime_ns`
- `size`

Removed raw files are also detected and corresponding index rows are deleted.

## 3. Retry Policy
- Wrapper scripts retry up to `MAX_RETRY` times (default: 2)
- Set override: `MAX_RETRY=3 ./scripts/run_index_incremental.sh`

## 4. Rollback Policy
### Full rebuild rollback
- Before full rebuild, backup is created in `data/index/backups/`
- If rebuild fails after retries, previous `chunks.jsonl` and `index_state.json` are restored automatically

### Incremental rollback
- Incremental writer uses atomic replace (`*.tmp` -> `chunks.jsonl`)
- On failure, previous index file remains intact

## 5. Operational Rules
- First bootstrap must run full rebuild once
- Recommended schedule:
  - nightly full rebuild (stability)
  - hourly incremental update (freshness)
- If embed model changes, run full rebuild mandatory

## 6. Acceptance Criteria (Step 4)
- Full and incremental commands are separated
- State-based incremental indexing works
- Retry and rollback behavior is documented and executable
