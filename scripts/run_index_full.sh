#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
LOG_COMPONENT="index-full"
source "$ROOT_DIR/scripts/lib/logging.sh"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
INDEX_PATH="${INDEX_PATH:-data/index/chunks.jsonl}"
STATE_PATH="${STATE_PATH:-data/index/index_state.json}"
BACKUP_DIR="${BACKUP_DIR:-data/index/backups}"
MAX_RETRY="${MAX_RETRY:-2}"

mkdir -p "$(dirname "$INDEX_PATH")" "$BACKUP_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  log_error "python not found: $PYTHON_BIN"
  exit 1
fi

TS="$(date +%Y%m%d_%H%M%S)"
INDEX_BAK=""
STATE_BAK=""

if [[ -f "$INDEX_PATH" ]]; then
  INDEX_BAK="$BACKUP_DIR/chunks_${TS}.jsonl"
  cp "$INDEX_PATH" "$INDEX_BAK"
fi

if [[ -f "$STATE_PATH" ]]; then
  STATE_BAK="$BACKUP_DIR/state_${TS}.json"
  cp "$STATE_PATH" "$STATE_BAK"
fi

log_info "run_start index_path=$INDEX_PATH state_path=$STATE_PATH backup_dir=$BACKUP_DIR max_retry=$MAX_RETRY"

ok=0
for attempt in $(seq 1 "$MAX_RETRY"); do
  log_info "attempt=$attempt/$MAX_RETRY status=start"
  if "$PYTHON_BIN" scripts/build_index.py; then
    ok=1
    log_info "attempt=$attempt/$MAX_RETRY status=success"
    break
  fi
  log_warn "attempt=$attempt/$MAX_RETRY status=failed"
  sleep 1
done

if [[ "$ok" -ne 1 ]]; then
  log_error "failed_after_retries"
  if [[ -n "$INDEX_BAK" && -f "$INDEX_BAK" ]]; then
    cp "$INDEX_BAK" "$INDEX_PATH"
    log_warn "restored_index_backup path=$INDEX_BAK"
  fi
  if [[ -n "$STATE_BAK" && -f "$STATE_BAK" ]]; then
    cp "$STATE_BAK" "$STATE_PATH"
    log_warn "restored_state_backup path=$STATE_BAK"
  fi
  exit 1
fi

log_info "run_done"
