#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
LOG_COMPONENT="industry_pipeline"
source "$ROOT_DIR/scripts/lib/logging.sh"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
INDUSTRIES="${INDUSTRIES:-반도체,바이오,2차전지,자동차,방산,조선,클라우드,에너지}"
MIN_SAMPLES="${MIN_SAMPLES:-3}"
TAM_MULTIPLIER="${TAM_MULTIPLIER:-2.0}"
SAM_RATIO="${SAM_RATIO:-0.35}"
SOM_RATIO="${SOM_RATIO:-0.10}"
COMMODITY_FILE="${COMMODITY_FILE:-}"
RESUME="${RESUME:-1}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  log_error "python not found: $PYTHON_BIN"
  exit 1
fi

RESUME_FLAG=""
if [[ "$RESUME" == "1" ]]; then
  RESUME_FLAG="--resume"
fi

log_info "industry_special_start industries=$INDUSTRIES min_samples=$MIN_SAMPLES"
"$PYTHON_BIN" scripts/fetch_industry_valuation.py \
  --industries "$INDUSTRIES" \
  --min-samples "$MIN_SAMPLES" \
  $RESUME_FLAG

"$PYTHON_BIN" scripts/fetch_industry_tamsam.py \
  --industries "$INDUSTRIES" \
  --tam-multiplier "$TAM_MULTIPLIER" \
  --sam-ratio "$SAM_RATIO" \
  --som-ratio "$SOM_RATIO" \
  --min-samples "$MIN_SAMPLES" \
  $RESUME_FLAG

COMMODITY_ARGS=()
if [[ -n "$COMMODITY_FILE" ]]; then
  COMMODITY_ARGS+=(--commodity-file "$COMMODITY_FILE")
fi
"$PYTHON_BIN" scripts/fetch_industry_commodity_sensitivity.py \
  --industries "$INDUSTRIES" \
  "${COMMODITY_ARGS[@]}" \
  $RESUME_FLAG

log_info "industry_special_done"

