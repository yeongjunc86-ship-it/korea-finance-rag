#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
LOG_COMPONENT="index-incremental"
source "$ROOT_DIR/scripts/lib/logging.sh"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
MAX_RETRY="${MAX_RETRY:-2}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  log_error "python not found: $PYTHON_BIN"
  exit 1
fi

log_info "run_start max_retry=$MAX_RETRY"

ok=0
for attempt in $(seq 1 "$MAX_RETRY"); do
  log_info "attempt=$attempt/$MAX_RETRY status=start"
  if "$PYTHON_BIN" scripts/build_index_incremental.py; then
    ok=1
    log_info "attempt=$attempt/$MAX_RETRY status=success"
    break
  fi
  log_warn "attempt=$attempt/$MAX_RETRY status=failed"
  sleep 1
done

if [[ "$ok" -ne 1 ]]; then
  log_error "failed_after_retries"
  exit 1
fi

log_info "run_done"
