# Ops Minimum Spec (Phase 1 Step 6)

Version: v1.0
Last Updated: 2026-02-15

## 1. Goal
운영 관측성을 확보하기 위해 로그 형식과 헬스체크 항목을 최소 표준으로 고정한다.

## 2. Unified Log Format
Top-level runner scripts use the same format:
`<UTC_ISO> | <component> | <level> | <message>`

Example:
`2026-02-15T00:15:00Z | collector | INFO | step=1/2 task=yahoo_collection status=start`

Applied scripts:
- `scripts/run_full_collection.sh`
- `scripts/run_index_full.sh`
- `scripts/run_index_incremental.sh`

## 3. Health Contract
`GET /api/health` must include:
- `ok`
- `index_loaded`
- `chunk_count`
- `index_version`
- `indexed_doc_count`
- `index_path`, `index_exists`, `index_size_bytes`, `index_updated_at`
- `state_path`, `state_exists`, `state_updated_at`

## 4. Operational Usage
```bash
# service health
curl -s http://127.0.0.1:8000/api/health

# collection run with unified logs
./scripts/run_full_collection.sh

# full index run with retry/rollback logs
./scripts/run_index_full.sh

# incremental index run with unified logs
./scripts/run_index_incremental.sh
```

## 5. Acceptance Criteria (Step 6)
- Top-level collection/index scripts emit unified logs
- `/api/health` includes index version and indexed document count
- API contract and README are updated to reflect operational minimum
