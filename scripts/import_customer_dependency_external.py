#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def slug(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def to_float(v: str | None) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace("%", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def norm_market(v: str | None) -> str:
    s = str(v or "").strip().upper()
    if s in {"KOSPI", "KOSDAQ", "KONEX", "NYSE", "NASDAQ"}:
        return s
    return "OTHER"


def main() -> None:
    parser = argparse.ArgumentParser(description="Import external customer dependency dataset to raw json")
    parser.add_argument("--input-csv", default="data/external/customer_dependency.csv")
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
        customer = str(r.get("customer_name") or "").strip()
        if not company or not customer:
            continue
        by_company[company].append(r)

    ok = 0
    skip = 0
    for company, items in by_company.items():
        ticker = str(items[0].get("ticker") or "").strip() or None
        key = (ticker or slug(company)).replace(".", "_")
        out = RAW_DIR / f"customer_dependency_external_{key}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"skip (exists): {out}")
            continue

        top_customers: list[dict[str, Any]] = []
        for it in items:
            share = to_float(it.get("revenue_share_pct"))
            confidence = to_float(it.get("confidence"))
            top_customers.append(
                {
                    "name": str(it.get("customer_name") or "").strip(),
                    "revenue_share_pct": share,
                    "fiscal_year": str(it.get("fiscal_year") or "").strip() or None,
                    "source_type": str(it.get("source_type") or "external").strip() or "external",
                    "source_url": str(it.get("source_url") or "").strip() or None,
                    "confidence": confidence if confidence is not None else 0.9,
                    "note": str(it.get("note") or "").strip() or None,
                }
            )

        top_customers.sort(
            key=lambda x: float(x.get("revenue_share_pct") or 0),
            reverse=True,
        )

        top1 = next((float(x["revenue_share_pct"]) for x in top_customers if isinstance(x.get("revenue_share_pct"), (int, float))), None)
        top3_vals = [float(x["revenue_share_pct"]) for x in top_customers[:3] if isinstance(x.get("revenue_share_pct"), (int, float))]
        top3 = sum(top3_vals) if top3_vals else None
        collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        market = norm_market(items[0].get("market"))

        summary = (
            f"{company} 주요 매출 고객 데이터(외부 입력)입니다. "
            f"Top1 의존도 {top1:.1f}%."
            if top1 is not None
            else f"{company} 주요 매출 고객 데이터(외부 입력)입니다."
        )
        payload = {
            "company": company,
            "ticker": ticker,
            "market": market,
            "source": "external_customer_dependency",
            "title": f"{company} 주요 매출 고객/의존도(외부)",
            "summary": summary,
            "content": summary,
            "published_at": None,
            "collected_at": collected_at,
            "customer_dependency": {
                "coverage_status": "external_input",
                "top_customers": top_customers[:20],
                "metrics": {
                    "top1_share_pct": top1,
                    "top3_share_pct": top3,
                    "customer_count": len(top_customers),
                },
            },
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        ok += 1
        print(f"saved: {out}")

    print(f"done. success={ok}, skip={skip}, companies={len(by_company)}")


if __name__ == "__main__":
    main()

