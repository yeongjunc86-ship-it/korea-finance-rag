#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   LOG_COMPONENT="collector"; source scripts/lib/logging.sh
#   log_info "message"
#   log_error "message"

_log_ts() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

_log_line() {
  local level="$1"
  shift
  local msg="$*"
  printf '%s | %s | %s | %s\n' "$(_log_ts)" "${LOG_COMPONENT:-app}" "$level" "$msg"
}

log_info() {
  _log_line "INFO" "$*"
}

log_warn() {
  _log_line "WARN" "$*"
}

log_error() {
  _log_line "ERROR" "$*"
}
