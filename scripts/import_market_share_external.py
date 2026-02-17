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


def to_float(v: str) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Import external market share dataset to raw json")
    parser.add_argument("--input-csv", default="data/external/market_share.csv")
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
        company = str(r.get("company") or "").strip()
        if not industry or not company:
            continue
        by_industry[industry].append(r)

    ok = 0
    skip = 0
    for industry, items in by_industry.items():
        out = RAW_DIR / f"market_share_{slug(industry)}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"skip (exists): {out}")
            continue

        players: list[dict[str, object]] = []
        for it in items:
            share = to_float(str(it.get("share_pct") or "").strip())
            players.append(
                {
                    "company": str(it.get("company") or "").strip(),
                    "market": str(it.get("market") or "OTHER").strip() or "OTHER",
                    "share_pct": share,
                    "country": str(it.get("country") or "").strip() or None,
                }
            )
        players.sort(key=lambda x: float(x.get("share_pct") or 0), reverse=True)
        as_of = str(items[0].get("as_of") or "").strip() or None
        source_url = str(items[0].get("source_url") or "").strip() or None
        collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

        top3 = [p["company"] for p in players[:3] if p.get("company")]
        summary = f"{industry} 산업 시장점유율 데이터입니다. 상위 기업: {', '.join(top3) if top3 else '정보 부족'}."

        payload = {
            "company": f"{industry} 산업",
            "ticker": None,
            "market": "OTHER",
            "source": "external_market_share",
            "industry_name": industry,
            "title": f"{industry} 산업 시장점유율",
            "summary": summary,
            "content": summary,
            "published_at": as_of,
            "collected_at": collected_at,
            "source_url": source_url,
            "market_share": {
                "industry": industry,
                "as_of": as_of,
                "players": players,
            },
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        ok += 1
        print(f"saved: {out}")

    print(f"done. success={ok}, skip={skip}, industries={len(by_industry)}")


if __name__ == "__main__":
    main()

