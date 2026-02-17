#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


# OpenDART company overview endpoint example.
DART_COMPANY_URL = "https://opendart.fss.or.kr/api/company.json"


def fetch_company(api_key: str, corp_code: str) -> dict:
    params = {"crtfc_key": api_key, "corp_code": corp_code}
    resp = requests.get(DART_COMPANY_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corp-codes", nargs="+", required=True, help="DART corp_code 리스트")
    args = parser.parse_args()

    api_key = os.getenv("DART_API_KEY", "")
    if not api_key:
        raise SystemExit("DART_API_KEY가 없습니다. .env에 설정하세요.")

    for code in args.corp_codes:
        data = fetch_company(api_key, code)
        out = RAW_DIR / f"dart_{code}.json"
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"saved: {out}")


if __name__ == "__main__":
    main()
