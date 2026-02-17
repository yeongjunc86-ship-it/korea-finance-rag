#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

INDUSTRY_ALIASES: dict[str, list[str]] = {
    "반도체": ["semiconductor", "semiconductors", "chip", "memory"],
    "2차전지": ["battery", "batteries", "lithium"],
    "바이오": ["biotech", "biotechnology", "pharma", "pharmaceutical"],
    "자동차": ["auto", "automobile", "automotive", "vehicle"],
    "조선": ["ship", "shipping", "shipbuilding", "marine"],
    "방산": ["defense", "aerospace", "military"],
    "클라우드": ["cloud", "saas", "infrastructure software", "data center"],
    "에너지": ["energy", "oil", "gas", "utility", "renewable"],
}


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def industry_slug(name: str) -> str:
    return hashlib.sha1(name.encode("utf-8")).hexdigest()[:12]


def parse_industries(raw: str) -> list[str]:
    out: list[str] = []
    for token in raw.split(","):
        t = token.strip()
        if t:
            out.append(t)
    dedup: list[str] = []
    seen: set[str] = set()
    for t in out:
        if t in seen:
            continue
        seen.add(t)
        dedup.append(t)
    return dedup


def to_float(v: Any) -> float | None:
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def match_industry(payload: dict[str, Any], keyword: str) -> bool:
    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
    texts = [
        str(payload.get("company") or ""),
        str(payload.get("ticker") or ""),
        str(profile.get("industry") or ""),
        str(profile.get("sector") or ""),
    ]
    haystack = " ".join(texts).lower()
    terms = [keyword.lower(), *INDUSTRY_ALIASES.get(keyword, [])]
    return any(t and t in haystack for t in terms)


def summarize_for_industry(keyword: str, files: list[Path], min_samples: int) -> dict[str, Any] | None:
    ps_values: list[float] = []
    opm_values: list[float] = []
    sample_tickers: list[str] = []

    for p in files:
        payload = load_json(p)
        if not payload or not match_industry(payload, keyword):
            continue
        profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}

        market_cap = to_float(profile.get("market_cap"))
        revenue = to_float(profile.get("revenue"))
        op_margin = to_float(profile.get("operating_margins"))
        ticker = str(payload.get("ticker") or "").strip()
        if ticker:
            sample_tickers.append(ticker)
        if market_cap and revenue and revenue > 0:
            ps_values.append(market_cap / revenue)
        if op_margin is not None:
            opm_values.append(op_margin)

    if len(ps_values) < min_samples:
        return None

    ps_avg = sum(ps_values) / len(ps_values)
    ps_med = median(ps_values)
    opm_avg = (sum(opm_values) / len(opm_values)) if opm_values else None

    collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    summary = (
        f"{keyword} 산업의 표본 {len(ps_values)}개 기업 기준 평균 PS 멀티플은 {ps_avg:.2f}배, "
        f"중앙값은 {ps_med:.2f}배입니다."
    )
    if opm_avg is not None:
        summary += f" 평균 영업이익률은 {opm_avg * 100:.1f}%입니다."

    payload = {
        "company": f"{keyword} 산업",
        "ticker": None,
        "market": "OTHER",
        "source": "industry_valuation_aggregator",
        "industry_name": keyword,
        "profile": {
            "industry": keyword,
            "sector": "Industry",
            "market_cap": None,
            "revenue": None,
            "operating_margins": opm_avg,
        },
        "title": f"{keyword} 산업 밸류에이션 멀티플 추정",
        "summary": summary,
        "content": summary,
        "published_at": collected_at,
        "collected_at": collected_at,
        "valuation": {
            "metric": "PS",
            "sample_count": len(ps_values),
            "average": round(ps_avg, 4),
            "median": round(ps_med, 4),
            "op_margin_avg": round(opm_avg, 6) if opm_avg is not None else None,
            "sample_tickers": sorted(set(sample_tickers))[:100],
        },
    }
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build industry valuation snapshots from yahoo raw data")
    parser.add_argument(
        "--industries",
        default="반도체,바이오,2차전지,자동차,방산,조선,클라우드,에너지",
        help="쉼표 구분 산업 키워드",
    )
    parser.add_argument("--min-samples", type=int, default=3, help="산업별 최소 표본 수")
    parser.add_argument("--resume", action="store_true", help="기존 파일이 있으면 skip")
    args = parser.parse_args()

    yahoo_files = sorted(RAW_DIR.glob("yahoo_*.json"))
    if not yahoo_files:
        raise SystemExit("yahoo raw 파일이 없습니다. 먼저 fetch_yahoo.py를 실행하세요.")

    industries = parse_industries(args.industries)
    if not industries:
        raise SystemExit("산업 목록이 비어 있습니다.")

    ok = 0
    skip = 0
    miss = 0
    for idx, industry in enumerate(industries, start=1):
        out = RAW_DIR / f"valuation_{industry_slug(industry)}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"[{idx}/{len(industries)}] skip (exists): {out}")
            continue

        payload = summarize_for_industry(industry, yahoo_files, max(1, args.min_samples))
        if not payload:
            miss += 1
            print(f"[{idx}/{len(industries)}] no-data: {industry} (min_samples={args.min_samples})")
            continue

        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        ok += 1
        print(f"[{idx}/{len(industries)}] saved: {out}")

    print(f"done. success={ok}, skip={skip}, no_data={miss}, total={len(industries)}")


if __name__ == "__main__":
    main()
