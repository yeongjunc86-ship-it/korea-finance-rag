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


def to_float(v: str):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def main() -> None:
    parser = argparse.ArgumentParser(description="Import M&A comparable deals CSV into raw json")
    parser.add_argument("--input-csv", default="data/external/mna_comps.csv")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    csv_path = Path(args.input_csv)
    if not csv_path.exists():
        raise SystemExit(f"input not found: {csv_path}")

    rows = read_rows(csv_path)
    if not rows:
        raise SystemExit("input csv is empty")

    by_industry: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        industry = str(r.get("industry") or "").strip()
        target = str(r.get("target_company") or "").strip()
        if not industry or not target:
            continue
        by_industry[industry].append(r)

    ok = 0
    skip = 0
    for industry, items in by_industry.items():
        out = RAW_DIR / f"mna_{slug(industry)}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"skip (exists): {out}")
            continue

        deals = []
        for it in items:
            deals.append(
                {
                    "target_company": str(it.get("target_company") or "").strip() or None,
                    "acquirer": str(it.get("acquirer") or "").strip() or None,
                    "announce_date": str(it.get("announce_date") or "").strip() or None,
                    "deal_value": to_float(str(it.get("deal_value") or "").strip()),
                    "currency": str(it.get("currency") or "").strip() or None,
                    "ev_ebitda": to_float(str(it.get("ev_ebitda") or "").strip()),
                    "ev_sales": to_float(str(it.get("ev_sales") or "").strip()),
                    "country": str(it.get("country") or "").strip() or None,
                    "source_url": str(it.get("source_url") or "").strip() or None,
                }
            )

        ev_values = [d["ev_ebitda"] for d in deals if isinstance(d.get("ev_ebitda"), float)]
        avg_ev_ebitda = (sum(ev_values) / len(ev_values)) if ev_values else None
        as_of = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        summary = (
            f"{industry} 산업 최근 유사 거래 {len(deals)}건입니다. "
            f"평균 EV/EBITDA는 {avg_ev_ebitda:.2f}배입니다."
            if avg_ev_ebitda is not None
            else f"{industry} 산업 최근 유사 거래 {len(deals)}건입니다."
        )

        payload = {
            "company": f"{industry} 산업",
            "ticker": None,
            "market": "OTHER",
            "source": "external_mna_comps",
            "industry_name": industry,
            "title": f"{industry} 유사 거래 사례",
            "summary": summary,
            "content": summary,
            "published_at": as_of,
            "collected_at": as_of,
            "mna_comps": {
                "industry": industry,
                "deal_count": len(deals),
                "avg_ev_ebitda": avg_ev_ebitda,
                "deals": deals,
            },
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        ok += 1
        print(f"saved: {out}")

    print(f"done. success={ok}, skip={skip}, industries={len(by_industry)}")


if __name__ == "__main__":
    main()

