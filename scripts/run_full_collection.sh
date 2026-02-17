#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
LOG_COMPONENT="collector"
source "$ROOT_DIR/scripts/lib/logging.sh"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
TICKERS_FILE="${TICKERS_FILE:-data/processed/korea_tickers_all.txt}"
YAHOO_SLEEP="${YAHOO_SLEEP:-0.15}"
DART_SLEEP="${DART_SLEEP:-0.25}"
NEWS_SLEEP="${NEWS_SLEEP:-0.2}"
NEWS_LIMIT_COMPANY="${NEWS_LIMIT_COMPANY:-200}"
NEWS_PER_COMPANY="${NEWS_PER_COMPANY:-2}"
NEWS_ENABLED="${NEWS_ENABLED:-1}"
DART_NOTES_ENABLED="${DART_NOTES_ENABLED:-1}"
INDUSTRY_SPECIAL_ENABLED="${INDUSTRY_SPECIAL_ENABLED:-1}"
VALUATION_CASE_ENABLED="${VALUATION_CASE_ENABLED:-1}"
SYNERGY_CASE_ENABLED="${SYNERGY_CASE_ENABLED:-1}"
DUE_DILIGENCE_CASE_ENABLED="${DUE_DILIGENCE_CASE_ENABLED:-1}"
STRATEGIC_CASE_ENABLED="${STRATEGIC_CASE_ENABLED:-1}"
EXTERNAL_ENRICH_ENABLED="${EXTERNAL_ENRICH_ENABLED:-0}"
COMPANY_MASTER_ENABLED="${COMPANY_MASTER_ENABLED:-1}"
DART_FINANCIALS_ENABLED="${DART_FINANCIALS_ENABLED:-1}"
FINANCIALS_5Y_ENABLED="${FINANCIALS_5Y_ENABLED:-1}"
DART_FINANCIALS_YEARS="${DART_FINANCIALS_YEARS:-5}"
CUSTOMER_DEPENDENCY_ENABLED="${CUSTOMER_DEPENDENCY_ENABLED:-1}"
CUSTOMER_DEPENDENCY_LLM_ENABLED="${CUSTOMER_DEPENDENCY_LLM_ENABLED:-0}"
CUSTOMER_DEPENDENCY_LLM_PROVIDER="${CUSTOMER_DEPENDENCY_LLM_PROVIDER:-openai}"
CUSTOMER_DEPENDENCY_LLM_MODEL="${CUSTOMER_DEPENDENCY_LLM_MODEL:-}"
CUSTOMER_DEPENDENCY_LLM_LIMIT="${CUSTOMER_DEPENDENCY_LLM_LIMIT:-100}"
CUSTOMER_DEPENDENCY_LLM_MIN_CONFIDENCE="${CUSTOMER_DEPENDENCY_LLM_MIN_CONFIDENCE:-0.3}"
CUSTOMER_DEPENDENCY_LLM_TIMEOUT="${CUSTOMER_DEPENDENCY_LLM_TIMEOUT:-60}"
CUSTOMER_DEPENDENCY_LLM_ALLOW_EMPTY_CONTEXT="${CUSTOMER_DEPENDENCY_LLM_ALLOW_EMPTY_CONTEXT:-1}"
EXTERNAL_DIR="${EXTERNAL_DIR:-data/external}"
INDUSTRIES="${INDUSTRIES:-반도체,바이오,2차전지,자동차,방산,조선,클라우드,에너지}"
MIN_SAMPLES="${MIN_SAMPLES:-3}"
TAM_MULTIPLIER="${TAM_MULTIPLIER:-2.0}"
SAM_RATIO="${SAM_RATIO:-0.35}"
SOM_RATIO="${SOM_RATIO:-0.10}"
COMMODITY_FILE="${COMMODITY_FILE:-}"
INDEX_ENABLED="${INDEX_ENABLED:-1}"
INDEX_ALLOW_BOOTSTRAP="${INDEX_ALLOW_BOOTSTRAP:-0}"
LOG_DIR="${LOG_DIR:-logs}"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/full_collection_$RUN_TS.log"

mkdir -p "$LOG_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  log_error "python not found: $PYTHON_BIN"
  log_error "set PYTHON_BIN or create .venv first."
  exit 1
fi

if [[ ! -f "$TICKERS_FILE" ]]; then
  log_error "tickers file not found: $TICKERS_FILE"
  exit 1
fi

if [[ -z "${DART_API_KEY:-}" ]]; then
  log_error "DART_API_KEY is not set. export it or configure .env first."
  exit 1
fi

{
  log_info "run_start root=$ROOT_DIR python=$PYTHON_BIN tickers_file=$TICKERS_FILE yahoo_sleep=$YAHOO_SLEEP dart_sleep=$DART_SLEEP log=$LOG_FILE"
  log_info "step=1/15 task=yahoo_collection status=start"
  "$PYTHON_BIN" scripts/fetch_yahoo.py \
    --tickers-file "$TICKERS_FILE" \
    --resume \
    --sleep "$YAHOO_SLEEP"
  log_info "step=1/15 task=yahoo_collection status=done"

  log_info "step=2/15 task=dart_collection status=start"
  "$PYTHON_BIN" scripts/fetch_dart_bulk.py \
    --resume \
    --sleep "$DART_SLEEP"
  log_info "step=2/15 task=dart_collection status=done"

  if [[ "$DART_NOTES_ENABLED" == "1" ]]; then
    log_info "step=3/15 task=dart_notes_parse status=start"
    "$PYTHON_BIN" scripts/parse_dart_notes.py --resume
    log_info "step=3/15 task=dart_notes_parse status=done"
  else
    log_warn "step=3/15 task=dart_notes_parse status=skipped reason=DART_NOTES_ENABLED=$DART_NOTES_ENABLED"
  fi

  if [[ "$DART_FINANCIALS_ENABLED" == "1" ]]; then
    log_info "step=4/15 task=dart_financials_fetch status=start years=$DART_FINANCIALS_YEARS"
    "$PYTHON_BIN" scripts/fetch_dart_financials.py --years "$DART_FINANCIALS_YEARS" --resume --sleep "$DART_SLEEP"
    log_info "step=4/15 task=dart_financials_fetch status=done"
  else
    log_warn "step=4/15 task=dart_financials_fetch status=skipped reason=DART_FINANCIALS_ENABLED=$DART_FINANCIALS_ENABLED"
  fi

  if [[ "$FINANCIALS_5Y_ENABLED" == "1" ]]; then
    log_info "step=5/15 task=financials_5y_build status=start"
    "$PYTHON_BIN" scripts/build_company_financials_5y.py --write-raw
    log_info "step=5/15 task=financials_5y_build status=done"
  else
    log_warn "step=5/15 task=financials_5y_build status=skipped reason=FINANCIALS_5Y_ENABLED=$FINANCIALS_5Y_ENABLED"
  fi

  if [[ "$NEWS_ENABLED" == "1" ]]; then
    log_info "step=6/15 task=news_collection status=start news_limit_company=$NEWS_LIMIT_COMPANY news_per_company=$NEWS_PER_COMPANY news_sleep=$NEWS_SLEEP"
    "$PYTHON_BIN" scripts/fetch_news.py \
      --universe-file data/processed/korea_universe.json \
      --limit-company "$NEWS_LIMIT_COMPANY" \
      --per-company "$NEWS_PER_COMPANY" \
      --resume \
      --sleep "$NEWS_SLEEP"
    log_info "step=6/15 task=news_collection status=done"
  else
    log_warn "step=6/15 task=news_collection status=skipped reason=NEWS_ENABLED=$NEWS_ENABLED"
  fi

  if [[ "$CUSTOMER_DEPENDENCY_LLM_ENABLED" == "1" ]]; then
    log_info "step=7/15 task=customer_dependency_llm_extract status=start provider=$CUSTOMER_DEPENDENCY_LLM_PROVIDER limit=$CUSTOMER_DEPENDENCY_LLM_LIMIT"
    LLM_CMD=(
      "$PYTHON_BIN" scripts/extract_customer_dependency_llm.py
      --provider "$CUSTOMER_DEPENDENCY_LLM_PROVIDER"
      --limit "$CUSTOMER_DEPENDENCY_LLM_LIMIT"
      --min-confidence "$CUSTOMER_DEPENDENCY_LLM_MIN_CONFIDENCE"
      --timeout "$CUSTOMER_DEPENDENCY_LLM_TIMEOUT"
      --resume
    )
    if [[ -n "$CUSTOMER_DEPENDENCY_LLM_MODEL" ]]; then
      LLM_CMD+=(--model "$CUSTOMER_DEPENDENCY_LLM_MODEL")
    fi
    if [[ "$CUSTOMER_DEPENDENCY_LLM_ALLOW_EMPTY_CONTEXT" == "1" ]]; then
      LLM_CMD+=(--allow-empty-context)
    fi
    "${LLM_CMD[@]}"
    log_info "step=7/15 task=customer_dependency_llm_extract status=done"
  else
    log_warn "step=7/15 task=customer_dependency_llm_extract status=skipped reason=CUSTOMER_DEPENDENCY_LLM_ENABLED=$CUSTOMER_DEPENDENCY_LLM_ENABLED"
  fi

  if [[ "$CUSTOMER_DEPENDENCY_ENABLED" == "1" ]]; then
    log_info "step=7/15 task=customer_dependency_build status=start"
    "$PYTHON_BIN" scripts/build_customer_dependency.py --write-raw
    log_info "step=7/15 task=customer_dependency_build status=done"
  else
    log_warn "step=7/15 task=customer_dependency_build status=skipped reason=CUSTOMER_DEPENDENCY_ENABLED=$CUSTOMER_DEPENDENCY_ENABLED"
  fi

  if [[ "$INDUSTRY_SPECIAL_ENABLED" == "1" ]]; then
    log_info "step=8/15 task=industry_special status=start industries=$INDUSTRIES"
    RESUME=1 \
    INDUSTRIES="$INDUSTRIES" \
    MIN_SAMPLES="$MIN_SAMPLES" \
    TAM_MULTIPLIER="$TAM_MULTIPLIER" \
    SAM_RATIO="$SAM_RATIO" \
    SOM_RATIO="$SOM_RATIO" \
    COMMODITY_FILE="$COMMODITY_FILE" \
    PYTHON_BIN="$PYTHON_BIN" \
    ./scripts/run_industry_special_pipeline.sh
    log_info "step=8/15 task=industry_special status=done"
  else
    log_warn "step=8/15 task=industry_special status=skipped reason=INDUSTRY_SPECIAL_ENABLED=$INDUSTRY_SPECIAL_ENABLED"
  fi

  if [[ "$VALUATION_CASE_ENABLED" == "1" ]]; then
    log_info "step=9/15 task=valuation_case_build status=start"
    "$PYTHON_BIN" scripts/build_valuation_cases.py --resume
    log_info "step=9/15 task=valuation_case_build status=done"
  else
    log_warn "step=9/15 task=valuation_case_build status=skipped reason=VALUATION_CASE_ENABLED=$VALUATION_CASE_ENABLED"
  fi

  if [[ "$SYNERGY_CASE_ENABLED" == "1" ]]; then
    log_info "step=10/15 task=synergy_case_build status=start"
    "$PYTHON_BIN" scripts/build_synergy_cases.py --resume
    log_info "step=10/15 task=synergy_case_build status=done"
  else
    log_warn "step=10/15 task=synergy_case_build status=skipped reason=SYNERGY_CASE_ENABLED=$SYNERGY_CASE_ENABLED"
  fi

  if [[ "$DUE_DILIGENCE_CASE_ENABLED" == "1" ]]; then
    log_info "step=11/15 task=due_diligence_case_build status=start"
    "$PYTHON_BIN" scripts/build_due_diligence_cases.py --resume
    log_info "step=11/15 task=due_diligence_case_build status=done"
  else
    log_warn "step=11/15 task=due_diligence_case_build status=skipped reason=DUE_DILIGENCE_CASE_ENABLED=$DUE_DILIGENCE_CASE_ENABLED"
  fi

  if [[ "$STRATEGIC_CASE_ENABLED" == "1" ]]; then
    log_info "step=12/15 task=strategic_case_build status=start"
    "$PYTHON_BIN" scripts/build_strategic_cases.py --resume
    log_info "step=12/15 task=strategic_case_build status=done"
  else
    log_warn "step=12/15 task=strategic_case_build status=skipped reason=STRATEGIC_CASE_ENABLED=$STRATEGIC_CASE_ENABLED"
  fi

  if [[ "$EXTERNAL_ENRICH_ENABLED" == "1" ]]; then
    log_info "step=13/15 task=external_enrichment status=start external_dir=$EXTERNAL_DIR"
    RESUME=1 \
    EXTERNAL_DIR="$EXTERNAL_DIR" \
    PYTHON_BIN="$PYTHON_BIN" \
    ./scripts/run_external_enrichment.sh
    log_info "step=13/15 task=external_enrichment status=done"
    if [[ "$CUSTOMER_DEPENDENCY_ENABLED" == "1" ]]; then
      log_info "step=13/15 task=customer_dependency_rebuild_after_external status=start"
      "$PYTHON_BIN" scripts/build_customer_dependency.py --write-raw
      log_info "step=13/15 task=customer_dependency_rebuild_after_external status=done"
    fi
  else
    log_warn "step=13/15 task=external_enrichment status=skipped reason=EXTERNAL_ENRICH_ENABLED=$EXTERNAL_ENRICH_ENABLED"
  fi

  if [[ "$COMPANY_MASTER_ENABLED" == "1" ]]; then
    log_info "step=14/15 task=company_master_build status=start"
    "$PYTHON_BIN" scripts/build_company_master.py
    log_info "step=14/15 task=company_master_build status=done"
  else
    log_warn "step=14/15 task=company_master_build status=skipped reason=COMPANY_MASTER_ENABLED=$COMPANY_MASTER_ENABLED"
  fi

  if [[ "$INDEX_ENABLED" == "1" ]]; then
    log_info "step=15/15 task=index_incremental status=start"
    INDEX_CMD=("$PYTHON_BIN" scripts/build_index_incremental.py)
    if [[ "$INDEX_ALLOW_BOOTSTRAP" == "1" ]]; then
      INDEX_CMD+=("--allow-bootstrap")
    fi
    "${INDEX_CMD[@]}"
    log_info "step=15/15 task=index_incremental status=done"
  else
    log_warn "step=15/15 task=index_incremental status=skipped reason=INDEX_ENABLED=$INDEX_ENABLED"
  fi
  log_info "run_done"
} 2>&1 | tee -a "$LOG_FILE"
