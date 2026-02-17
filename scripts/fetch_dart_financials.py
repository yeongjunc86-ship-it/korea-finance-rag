#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

DART_FIN_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"


def load_targets() -> list[dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for p in sorted(RAW_DIR.glob("dart_*.json")):
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        corp_code = str(payload.get("corp_code") or "").strip()
        if not corp_code:
            dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
            corp_code = str(dart.get("corp_code") or "").strip()
        if not corp_code:
            continue
        rows[corp_code] = {
            "corp_code": corp_code,
            "company": str(payload.get("company") or "").strip() or corp_code,
            "ticker": str(payload.get("ticker") or "").strip(),
            "market": str(payload.get("market") or "OTHER").strip() or "OTHER",
        }
    return sorted(rows.values(), key=lambda x: x["corp_code"])


def fetch_one(api_key: str, corp_code: str, year: int, reprt_code: str, fs_div: str) -> dict[str, Any]:
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": str(year),
        "reprt_code": reprt_code,
        "fs_div": fs_div,
    }
    resp = requests.get(DART_FIN_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch DART single account financials by company/year")
    parser.add_argument("--years", type=int, default=5, help="최근 N개년 수집")
    parser.add_argument("--reprt-code", default="11011", help="보고서 코드(기본: 사업보고서 11011)")
    parser.add_argument("--fs-div", default="CFS", choices=["CFS", "OFS"], help="연결/별도 구분")
    parser.add_argument("--sleep", type=float, default=0.2, help="요청 간 대기(초)")
    parser.add_argument("--limit", type=int, default=0, help="상위 N개 회사만")
    parser.add_argument("--resume", action="store_true", help="기존 파일 건너뛰기")
    parser.add_argument("--corp-codes", nargs="*", default=[], help="특정 corp_code만 수집")
    parser.add_argument("--tickers", nargs="*", default=[], help="특정 ticker만 수집")
    args = parser.parse_args()

    api_key = os.getenv("DART_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("DART_API_KEY가 없습니다. .env에 설정하세요.")

    targets = load_targets()
    if args.corp_codes:
        code_set = {str(x).strip() for x in args.corp_codes if str(x).strip()}
        targets = [t for t in targets if str(t.get("corp_code") or "").strip() in code_set]
    if args.tickers:
        ticker_set = {str(x).strip().lower() for x in args.tickers if str(x).strip()}
        targets = [t for t in targets if str(t.get("ticker") or "").strip().lower() in ticker_set]
    if args.limit > 0:
        targets = targets[: args.limit]
    if not targets:
        raise SystemExit("대상 기업이 없습니다. 먼저 fetch_dart_bulk.py를 실행하세요.")

    this_year = datetime.now().year
    years = [this_year - i for i in range(1, max(1, args.years) + 1)]

    ok = 0
    skip = 0
    fail = 0
    for i, t in enumerate(targets, start=1):
        corp_code = t["corp_code"]
        for y in years:
            out = RAW_DIR / f"dart_financials_{corp_code}_{y}_{args.fs_div}.json"
            if args.resume and out.exists():
                skip += 1
                continue
            try:
                data = fetch_one(
                    api_key=api_key,
                    corp_code=corp_code,
                    year=y,
                    reprt_code=args.reprt_code,
                    fs_div=args.fs_div,
                )
                payload = {
                    "company": t["company"],
                    "ticker": t["ticker"],
                    "market": t["market"],
                    "source": "opendart_fnlttSinglAcntAll",
                    "corp_code": corp_code,
                    "bsns_year": y,
                    "reprt_code": args.reprt_code,
                    "fs_div": args.fs_div,
                    "dart": data,
                }
                out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                ok += 1
            except Exception as e:  # noqa: BLE001
                fail += 1
                print(f"[{i}/{len(targets)}] fail corp_code={corp_code} year={y} ({e})")
            time.sleep(max(0.0, args.sleep))

        if i % 100 == 0:
            print(f"progress companies={i}/{len(targets)} ok={ok} skip={skip} fail={fail}")

    print(f"done. companies={len(targets)} years={len(years)} success={ok} skip={skip} fail={fail}")


if __name__ == "__main__":
    main()
