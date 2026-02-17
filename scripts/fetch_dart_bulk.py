#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import os
import time
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

DART_CORPCODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
DART_COMPANY_URL = "https://opendart.fss.or.kr/api/company.json"


def download_corp_code_table(api_key: str) -> list[dict[str, str]]:
    resp = requests.get(DART_CORPCODE_URL, params={"crtfc_key": api_key}, timeout=60)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        name = zf.namelist()[0]
        xml_bytes = zf.read(name)

    root = ET.fromstring(xml_bytes)
    rows: list[dict[str, str]] = []
    for item in root.findall("list"):
        corp_code = (item.findtext("corp_code") or "").strip()
        corp_name = (item.findtext("corp_name") or "").strip()
        stock_code = (item.findtext("stock_code") or "").strip()
        modify_date = (item.findtext("modify_date") or "").strip()
        if not corp_code:
            continue
        rows.append(
            {
                "corp_code": corp_code,
                "corp_name": corp_name,
                "stock_code": stock_code,
                "modify_date": modify_date,
            }
        )
    return rows


def fetch_company(api_key: str, corp_code: str) -> dict:
    params = {"crtfc_key": api_key, "corp_code": corp_code}
    resp = requests.get(DART_COMPANY_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def market_of(stock_code: str) -> str:
    if not stock_code:
        return "OTHER"
    # Exchange detail is not returned here; keep OTHER and let merge stage enrich.
    return "OTHER"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="상위 N개만 수집 (0은 전체)")
    parser.add_argument("--sleep", type=float, default=0.25, help="요청 간 대기(초)")
    parser.add_argument("--resume", action="store_true", help="기존 파일이 있으면 건너뛰기")
    parser.add_argument("--corp-codes", nargs="*", default=[], help="특정 corp_code만 수집")
    args = parser.parse_args()

    api_key = os.getenv("DART_API_KEY", "")
    if not api_key:
        raise SystemExit("DART_API_KEY가 없습니다. .env에 설정하세요.")

    table = download_corp_code_table(api_key)
    listed = [r for r in table if r.get("stock_code")]
    listed.sort(key=lambda x: x["stock_code"])

    table_out = PROC_DIR / "dart_corp_codes_listed.json"
    table_out.write_text(json.dumps(listed, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"saved: {table_out} ({len(listed)})")

    if args.corp_codes:
        code_set = {str(x).strip() for x in args.corp_codes if str(x).strip()}
        listed = [r for r in listed if str(r.get("corp_code") or "").strip() in code_set]

    targets = listed[: args.limit] if args.limit > 0 else listed

    ok = 0
    fail = 0
    for idx, row in enumerate(targets, start=1):
        corp_code = row["corp_code"]
        stock_code = row["stock_code"]
        out = RAW_DIR / f"dart_{corp_code}.json"
        if args.resume and out.exists():
            print(f"[{idx}/{len(targets)}] skip (exists): {out}")
            continue

        try:
            data = fetch_company(api_key, corp_code)
            payload = {
                "company": data.get("corp_name") or row.get("corp_name") or corp_code,
                "ticker": f"{stock_code}.KS" if stock_code else stock_code,
                "market": market_of(stock_code),
                "source": "opendart_company",
                "corp_code": corp_code,
                "stock_code": stock_code,
                "dart": data,
            }
            out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            ok += 1
            print(f"[{idx}/{len(targets)}] saved: {out}")
        except Exception as e:  # noqa: BLE001
            fail += 1
            print(f"[{idx}/{len(targets)}] fail: corp_code={corp_code}, stock={stock_code} ({e})")

        time.sleep(max(0.0, args.sleep))

    print(f"done. success={ok}, fail={fail}, total={len(targets)}")


if __name__ == "__main__":
    main()
