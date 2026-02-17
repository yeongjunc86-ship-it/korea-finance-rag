#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def slug(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def main() -> None:
    parser = argparse.ArgumentParser(description="Import external patent dataset to raw json")
    parser.add_argument("--input-csv", default="data/external/patents.csv")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    csv_path = Path(args.input_csv)
    if not csv_path.exists():
        raise SystemExit(f"input not found: {csv_path}")

    rows = read_rows(csv_path)
    if not rows:
        raise SystemExit("input csv is empty")

    by_company: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        company = str(r.get("company") or "").strip()
        if not company:
            continue
        by_company[company].append(r)

    ok = 0
    skip = 0
    for company, items in by_company.items():
        out = RAW_DIR / f"patent_{slug(company)}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"skip (exists): {out}")
            continue

        patents = []
        for it in items:
            patents.append(
                {
                    "patent_id": str(it.get("patent_id") or "").strip() or None,
                    "title": str(it.get("title") or "").strip() or None,
                    "tech_domain": str(it.get("tech_domain") or "").strip() or None,
                    "filed_date": str(it.get("filed_date") or "").strip() or None,
                    "country": str(it.get("country") or "").strip() or None,
                    "status": str(it.get("status") or "").strip() or None,
                }
            )
        source_url = str(items[0].get("source_url") or "").strip() or None
        ticker = str(items[0].get("ticker") or "").strip() or None
        market = str(items[0].get("market") or "OTHER").strip() or "OTHER"
        collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        summary = f"{company} 특허 데이터 {len(patents)}건이 수집되었습니다."

        payload = {
            "company": company,
            "ticker": ticker,
            "market": market,
            "source": "external_patent",
            "title": f"{company} 핵심 특허",
            "summary": summary,
            "content": summary,
            "published_at": None,
            "collected_at": collected_at,
            "source_url": source_url,
            "patents": patents,
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        ok += 1
        print(f"saved: {out}")

    print(f"done. success={ok}, skip={skip}, companies={len(by_company)}")


if __name__ == "__main__":
    main()

