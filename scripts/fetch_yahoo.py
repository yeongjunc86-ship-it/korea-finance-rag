#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Iterable

import yfinance as yf


RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def to_json_safe(value):
    """Convert pandas/numpy/datetime-like values to JSON-serializable values."""
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            pass
    return value


def read_tickers_file(path: Path) -> list[str]:
    rows = path.read_text(encoding="utf-8").splitlines()
    tickers: list[str] = []
    for row in rows:
        t = row.strip()
        if not t or t.startswith("#"):
            continue
        tickers.append(t)
    return tickers


def fetch_one_ticker(ticker: str) -> dict:
    t = yf.Ticker(ticker)
    info = t.info
    hist = t.history(period="1mo", interval="1d").reset_index().to_dict(orient="records")

    market = "KOSPI" if ticker.endswith(".KS") else "KOSDAQ" if ticker.endswith(".KQ") else "OTHER"
    payload = {
        "company": info.get("longName") or ticker,
        "ticker": ticker,
        "market": market,
        "source": "yahoo_finance",
        "profile": {
            "industry": info.get("industry"),
            "sector": info.get("sector"),
            "market_cap": info.get("marketCap"),
            "revenue": info.get("totalRevenue"),
            "operating_margins": info.get("operatingMargins"),
        },
        "price_history_1m": hist,
    }
    return payload


def save_payload(ticker: str, payload: dict) -> Path:
    out = RAW_DIR / f"yahoo_{ticker.replace('.', '_')}.json"
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=to_json_safe),
        encoding="utf-8",
    )
    return out


def iterate_tickers(items: Iterable[str], limit: int | None = None) -> list[str]:
    out = list(items)
    if limit is not None and limit > 0:
        return out[:limit]
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", nargs="*", default=[], help="예: 005930.KS 000660.KS")
    parser.add_argument("--tickers-file", help="줄 단위 티커 파일(.txt). 예: data/processed/korea_tickers_all.txt")
    parser.add_argument("--limit", type=int, default=0, help="상위 N개만 수집 (0은 전체)")
    parser.add_argument("--sleep", type=float, default=0.2, help="요청 간 대기(초)")
    parser.add_argument("--resume", action="store_true", help="기존 파일이 있으면 건너뛰기")
    args = parser.parse_args()

    tickers: list[str] = list(args.tickers)
    if args.tickers_file:
        tickers.extend(read_tickers_file(Path(args.tickers_file)))

    dedup: list[str] = []
    seen: set[str] = set()
    for t in tickers:
        if t not in seen:
            seen.add(t)
            dedup.append(t)
    tickers = iterate_tickers(dedup, args.limit if args.limit > 0 else None)

    if not tickers:
        raise SystemExit("수집할 티커가 없습니다. --tickers 또는 --tickers-file을 사용하세요.")

    ok = 0
    fail = 0
    for idx, ticker in enumerate(tickers, start=1):
        out = RAW_DIR / f"yahoo_{ticker.replace('.', '_')}.json"
        if args.resume and out.exists():
            print(f"[{idx}/{len(tickers)}] skip (exists): {out}")
            continue

        try:
            payload = fetch_one_ticker(ticker)
            saved = save_payload(ticker, payload)
            ok += 1
            print(f"[{idx}/{len(tickers)}] saved: {saved}")
        except Exception as e:  # noqa: BLE001
            fail += 1
            print(f"[{idx}/{len(tickers)}] fail: {ticker} ({e})")
        time.sleep(max(0.0, args.sleep))

    print(f"done. success={ok}, fail={fail}, total={len(tickers)}")


if __name__ == "__main__":
    main()
