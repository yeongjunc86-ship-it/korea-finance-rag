#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
YAHOO_SLEEP="${YAHOO_SLEEP:-0.15}"
DART_SLEEP="${DART_SLEEP:-0.25}"
YAHOO_LIMIT="${YAHOO_LIMIT:-0}"
RESUME="${RESUME:-1}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "python not found: $PYTHON_BIN"
  exit 1
fi

echo "[1/3] build universe..."
"$PYTHON_BIN" scripts/build_universe.py

echo "[2/3] fetch yahoo..."
Y_CMD=(
  "$PYTHON_BIN" scripts/fetch_yahoo.py
  --tickers-file data/processed/korea_tickers_all.txt
  --sleep "$YAHOO_SLEEP"
)
if [[ "$YAHOO_LIMIT" != "0" ]]; then
  Y_CMD+=(--limit "$YAHOO_LIMIT")
fi
if [[ "$RESUME" == "1" ]]; then
  Y_CMD+=(--resume)
fi
"${Y_CMD[@]}"

echo "[3/3] fetch dart bulk..."
D_CMD=(
  "$PYTHON_BIN" scripts/fetch_dart_bulk.py
  --sleep "$DART_SLEEP"
)
if [[ "$RESUME" == "1" ]]; then
  D_CMD+=(--resume)
fi
"${D_CMD[@]}"

echo "[4/4] build vector index (incremental with bootstrap fallback)..."
I_CMD=(
  "$PYTHON_BIN" scripts/build_index_incremental.py
)
if [[ "$RESUME" == "1" ]]; then
  I_CMD+=(--allow-bootstrap)
fi
"${I_CMD[@]}"

echo "done: disclosure bulk collection + vector indexing completed"
