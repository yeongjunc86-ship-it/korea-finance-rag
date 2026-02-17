#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
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


def to_float(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Import external ESG dataset to raw json")
    parser.add_argument("--input-csv", default="data/external/esg_scores.csv")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    csv_path = Path(args.input_csv)
    if not csv_path.exists():
        raise SystemExit(f"input not found: {csv_path}")

    rows = read_rows(csv_path)
    if not rows:
        raise SystemExit("input csv is empty")

    ok = 0
    skip = 0
    for row in rows:
        company = str(row.get("company") or "").strip()
        if not company:
            continue
        out = RAW_DIR / f"esg_{slug(company)}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"skip (exists): {out}")
            continue

        ticker = str(row.get("ticker") or "").strip() or None
        market = str(row.get("market") or "OTHER").strip() or "OTHER"
        esg_score = to_float(row.get("esg_score"))
        e_score = to_float(row.get("e_score"))
        s_score = to_float(row.get("s_score"))
        g_score = to_float(row.get("g_score"))
        risk_flags = [x.strip() for x in str(row.get("risk_flags") or "").split(";") if x.strip()]
        as_of = str(row.get("as_of") or "").strip() or None
        provider = str(row.get("provider") or "").strip() or "external_esg_provider"
        source_url = str(row.get("source_url") or "").strip() or None
        collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

        summary = (
            f"{company} ESG 점수는 {esg_score if esg_score is not None else '정보 부족'}이며, "
            f"E/S/G는 {e_score if e_score is not None else '정보 부족'}/"
            f"{s_score if s_score is not None else '정보 부족'}/"
            f"{g_score if g_score is not None else '정보 부족'}입니다."
        )
        if risk_flags:
            summary += f" 주요 리스크: {', '.join(risk_flags[:5])}."

        payload = {
            "company": company,
            "ticker": ticker,
            "market": market,
            "source": "external_esg",
            "title": f"{company} ESG 평가",
            "summary": summary,
            "content": summary,
            "published_at": as_of,
            "collected_at": collected_at,
            "source_url": source_url,
            "esg": {
                "provider": provider,
                "as_of": as_of,
                "esg_score": esg_score,
                "e_score": e_score,
                "s_score": s_score,
                "g_score": g_score,
                "risk_flags": risk_flags,
            },
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        ok += 1
        print(f"saved: {out}")

    print(f"done. success={ok}, skip={skip}, rows={len(rows)}")


if __name__ == "__main__":
    main()

