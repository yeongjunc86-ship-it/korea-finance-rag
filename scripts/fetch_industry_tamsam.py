#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
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


def to_float(v: Any) -> float | None:
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def industry_slug(name: str) -> str:
    return hashlib.sha1(name.encode("utf-8")).hexdigest()[:12]


def parse_industries(raw: str) -> list[str]:
    out = [x.strip() for x in raw.split(",") if x.strip()]
    dedup: list[str] = []
    seen: set[str] = set()
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        dedup.append(x)
    return dedup


def match_industry(payload: dict[str, Any], keyword: str) -> bool:
    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
    haystack = " ".join(
        [
            str(payload.get("company") or ""),
            str(payload.get("ticker") or ""),
            str(profile.get("industry") or ""),
            str(profile.get("sector") or ""),
        ]
    ).lower()
    terms = [keyword.lower(), *INDUSTRY_ALIASES.get(keyword, [])]
    return any(t and t in haystack for t in terms)


def build_tamsam_payload(
    industry: str,
    files: list[Path],
    tam_multiplier: float,
    sam_ratio: float,
    som_ratio: float,
    min_samples: int,
) -> dict[str, Any] | None:
    revenues: list[float] = []
    tickers: list[str] = []

    for p in files:
        payload = load_json(p)
        if not payload or not match_industry(payload, industry):
            continue
        profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
        rev = to_float(profile.get("revenue"))
        if rev and rev > 0:
            revenues.append(rev)
        ticker = str(payload.get("ticker") or "").strip()
        if ticker:
            tickers.append(ticker)

    if len(revenues) < min_samples:
        return None

    base_revenue = sum(revenues)
    tam = base_revenue * tam_multiplier
    sam = tam * sam_ratio
    som = sam * som_ratio

    collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    summary = (
        f"{industry} 산업의 표본 매출 합계는 약 {base_revenue:,.0f}이며, "
        f"가정치(tam_multiplier={tam_multiplier}, sam_ratio={sam_ratio}, som_ratio={som_ratio}) 기준 "
        f"TAM {tam:,.0f}, SAM {sam:,.0f}, SOM {som:,.0f}로 추정됩니다."
    )

    return {
        "company": f"{industry} 산업",
        "ticker": None,
        "market": "OTHER",
        "source": "industry_tamsam_estimator",
        "industry_name": industry,
        "profile": {
            "industry": industry,
            "sector": "Industry",
            "market_cap": None,
            "revenue": base_revenue,
            "operating_margins": None,
        },
        "title": f"{industry} 산업 TAM/SAM/SOM 추정",
        "summary": summary,
        "content": summary,
        "published_at": collected_at,
        "collected_at": collected_at,
        "tam_sam_som": {
            "sample_count": len(revenues),
            "base_revenue_sum": round(base_revenue, 2),
            "tam": round(tam, 2),
            "sam": round(sam, 2),
            "som": round(som, 2),
            "assumptions": {
                "tam_multiplier": tam_multiplier,
                "sam_ratio": sam_ratio,
                "som_ratio": som_ratio,
            },
            "sample_tickers": sorted(set(tickers))[:100],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build TAM/SAM/SOM estimates from yahoo raw data")
    parser.add_argument(
        "--industries",
        default="반도체,바이오,2차전지,자동차,방산,조선,클라우드,에너지",
        help="쉼표 구분 산업 키워드",
    )
    parser.add_argument("--tam-multiplier", type=float, default=2.0)
    parser.add_argument("--sam-ratio", type=float, default=0.35)
    parser.add_argument("--som-ratio", type=float, default=0.1)
    parser.add_argument("--min-samples", type=int, default=3, help="산업별 최소 표본 수")
    parser.add_argument("--resume", action="store_true", help="기존 파일이 있으면 skip")
    args = parser.parse_args()

    if args.tam_multiplier <= 0:
        raise SystemExit("tam-multiplier must be > 0")
    if not (0 < args.sam_ratio <= 1):
        raise SystemExit("sam-ratio must be in (0,1]")
    if not (0 < args.som_ratio <= 1):
        raise SystemExit("som-ratio must be in (0,1]")

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
        out = RAW_DIR / f"tam_{industry_slug(industry)}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"[{idx}/{len(industries)}] skip (exists): {out}")
            continue

        payload = build_tamsam_payload(
            industry=industry,
            files=yahoo_files,
            tam_multiplier=args.tam_multiplier,
            sam_ratio=args.sam_ratio,
            som_ratio=args.som_ratio,
            min_samples=max(1, args.min_samples),
        )
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
