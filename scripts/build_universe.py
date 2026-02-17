#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import os
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import requests
from pykrx import stock
from dotenv import load_dotenv

load_dotenv()

OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def find_recent_trading_day(max_lookback_days: int = 14) -> str:
    today = dt.date.today()
    for i in range(max_lookback_days + 1):
        d = (today - dt.timedelta(days=i)).strftime("%Y%m%d")
        tickers = stock.get_market_ticker_list(d, market="KOSPI")
        if tickers:
            return d
    raise RuntimeError("최근 영업일을 찾지 못했습니다. 네트워크/pykrx 상태를 확인하세요.")


def build_market(base_date: str, market: str, suffix: str) -> list[dict[str, str]]:
    tickers = stock.get_market_ticker_list(base_date, market=market)
    rows: list[dict[str, str]] = []
    for t in tickers:
        name = stock.get_market_ticker_name(t)
        rows.append(
            {
                "market": market,
                "krx_ticker": t,
                "yahoo_ticker": f"{t}.{suffix}",
                "name": name,
            }
        )
    return rows


def build_from_dart(api_key: str) -> list[dict[str, str]]:
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    resp = requests.get(url, params={"crtfc_key": api_key}, timeout=60)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        xml_name = zf.namelist()[0]
        xml_bytes = zf.read(xml_name)

    root = ET.fromstring(xml_bytes)
    rows: list[dict[str, str]] = []
    for item in root.findall("list"):
        code = (item.findtext("stock_code") or "").strip()
        name = (item.findtext("corp_name") or "").strip()
        if not code:
            continue
        # DART corpCode.xml에는 시장(KOSPI/KOSDAQ) 구분값이 없으므로 후보 2종을 생성한다.
        rows.append({"market": "UNKNOWN", "krx_ticker": code, "yahoo_ticker": f"{code}.KS", "name": name})
        rows.append({"market": "UNKNOWN", "krx_ticker": code, "yahoo_ticker": f"{code}.KQ", "name": name})
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-konex", action="store_true", help="KONEX 포함 여부")
    args = parser.parse_args()

    rows: list[dict[str, str]] = []
    base_date = ""
    try:
        base_date = find_recent_trading_day()
        rows.extend(build_market(base_date, "KOSPI", "KS"))
        rows.extend(build_market(base_date, "KOSDAQ", "KQ"))
        if args.include_konex:
            # Yahoo에서 KONEX 지원이 제한적일 수 있어 기본값은 제외.
            rows.extend(build_market(base_date, "KONEX", "KQ"))
    except Exception:
        rows = []

    if not rows:
        api_key = os.getenv("DART_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError(
                "pykrx로 유니버스 생성 실패 + DART_API_KEY 없음. .env에 DART_API_KEY를 설정하세요."
            )
        rows = build_from_dart(api_key)
        base_date = "DART_FALLBACK"

    rows.sort(key=lambda x: (x["market"], x["krx_ticker"]))

    json_path = OUT_DIR / "korea_universe.json"
    txt_path = OUT_DIR / "korea_tickers_all.txt"

    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    txt_path.write_text("\n".join(r["yahoo_ticker"] for r in rows) + "\n", encoding="utf-8")

    print(f"saved: {json_path}")
    print(f"saved: {txt_path}")
    print(f"base_date: {base_date}")
    print(f"done. total={len(rows)}")


if __name__ == "__main__":
    main()
