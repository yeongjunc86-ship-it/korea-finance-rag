#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
LOG_COMPONENT="external_enrichment"
source "$ROOT_DIR/scripts/lib/logging.sh"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
EXTERNAL_DIR="${EXTERNAL_DIR:-data/external}"
RESUME="${RESUME:-1}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  log_error "python not found: $PYTHON_BIN"
  exit 1
fi

RESUME_FLAG=""
if [[ "$RESUME" == "1" ]]; then
  RESUME_FLAG="--resume"
fi

mkdir -p "$EXTERNAL_DIR"
log_info "external_enrichment_start dir=$EXTERNAL_DIR"

if [[ -f "$EXTERNAL_DIR/market_share.csv" ]]; then
  "$PYTHON_BIN" scripts/import_market_share_external.py --input-csv "$EXTERNAL_DIR/market_share.csv" $RESUME_FLAG
else
  log_warn "skip market_share import: file not found ($EXTERNAL_DIR/market_share.csv)"
fi

if [[ -f "$EXTERNAL_DIR/mna_comps.csv" ]]; then
  "$PYTHON_BIN" scripts/import_mna_comps_external.py --input-csv "$EXTERNAL_DIR/mna_comps.csv" $RESUME_FLAG
else
  log_warn "skip mna comps import: file not found ($EXTERNAL_DIR/mna_comps.csv)"
fi

if [[ -f "$EXTERNAL_DIR/patents.csv" ]]; then
  "$PYTHON_BIN" scripts/import_patent_external.py --input-csv "$EXTERNAL_DIR/patents.csv" $RESUME_FLAG
else
  log_warn "skip patent import: file not found ($EXTERNAL_DIR/patents.csv)"
fi

if [[ -f "$EXTERNAL_DIR/esg_scores.csv" ]]; then
  "$PYTHON_BIN" scripts/import_esg_external.py --input-csv "$EXTERNAL_DIR/esg_scores.csv" $RESUME_FLAG
else
  log_warn "skip esg import: file not found ($EXTERNAL_DIR/esg_scores.csv)"
fi

if [[ -f "$EXTERNAL_DIR/customer_dependency.csv" ]]; then
  "$PYTHON_BIN" scripts/import_customer_dependency_external.py --input-csv "$EXTERNAL_DIR/customer_dependency.csv" $RESUME_FLAG
else
  log_warn "skip customer dependency import: file not found ($EXTERNAL_DIR/customer_dependency.csv)"
fi

if [[ -d "$EXTERNAL_DIR/customer_reports" ]]; then
  "$PYTHON_BIN" scripts/import_customer_dependency_reports.py --input-dir "$EXTERNAL_DIR/customer_reports" $RESUME_FLAG || true
else
  log_warn "skip customer dependency report import: dir not found ($EXTERNAL_DIR/customer_reports)"
fi

log_info "external_enrichment_done"
