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

# 월간 변동률(예시/대체 가능): +0.10 = +10%
DEFAULT_COMMODITY_CHANGES: dict[str, float] = {
    "원유": 0.07,
    "천연가스": -0.04,
    "구리": 0.05,
    "니켈": 0.03,
    "리튬": -0.08,
}

# 산업별 원가 민감도 가중치 합은 1.0 기준
INDUSTRY_SENSITIVITY_WEIGHTS: dict[str, dict[str, float]] = {
    "반도체": {"전력(천연가스)": 0.45, "구리": 0.35, "원유": 0.20},
    "2차전지": {"리튬": 0.50, "니켈": 0.30, "구리": 0.20},
    "자동차": {"원유": 0.35, "구리": 0.35, "니켈": 0.30},
    "조선": {"원유": 0.20, "구리": 0.30, "니켈": 0.50},
    "방산": {"원유": 0.30, "구리": 0.40, "니켈": 0.30},
    "에너지": {"원유": 0.50, "천연가스": 0.50},
    "클라우드": {"전력(천연가스)": 0.70, "구리": 0.30},
    "바이오": {"원유": 0.40, "천연가스": 0.60},
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
    out = [x.strip() for x in raw.split(",") if x.strip()]
    dedup: list[str] = []
    seen: set[str] = set()
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        dedup.append(x)
    return dedup


def to_float(v: Any) -> float | None:
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


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


def load_commodity_changes(path: str | None) -> dict[str, float]:
    if not path:
        return dict(DEFAULT_COMMODITY_CHANGES)
    data = load_json(Path(path))
    if not isinstance(data, dict):
        return dict(DEFAULT_COMMODITY_CHANGES)
    out: dict[str, float] = {}
    for k, v in data.items():
        fv = to_float(v)
        if fv is None:
            continue
        out[str(k)] = fv
    if not out:
        return dict(DEFAULT_COMMODITY_CHANGES)
    return out


def industry_margin_baseline(industry: str, yahoo_files: list[Path]) -> float | None:
    margins: list[float] = []
    for p in yahoo_files:
        payload = load_json(p)
        if not payload or not match_industry(payload, industry):
            continue
        profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
        m = to_float(profile.get("operating_margins"))
        if m is not None:
            margins.append(m)
    if not margins:
        return None
    return sum(margins) / len(margins)


def get_change(changes: dict[str, float], commodity: str) -> float:
    if commodity == "전력(천연가스)":
        return changes.get("천연가스", 0.0)
    return changes.get(commodity, 0.0)


def risk_band(impact_pp: float) -> str:
    x = abs(impact_pp)
    if x >= 1.2:
        return "HIGH"
    if x >= 0.5:
        return "MEDIUM"
    return "LOW"


def build_payload(industry: str, changes: dict[str, float], yahoo_files: list[Path]) -> dict[str, Any]:
    weights = INDUSTRY_SENSITIVITY_WEIGHTS.get(
        industry,
        {"원유": 0.34, "구리": 0.33, "천연가스": 0.33},
    )
    weighted_change = 0.0
    detail: list[dict[str, float | str]] = []
    for commodity, w in weights.items():
        chg = get_change(changes, commodity)
        weighted_change += w * chg
        detail.append(
            {
                "commodity": commodity,
                "weight": round(w, 4),
                "price_change": round(chg, 4),
                "weighted_contribution": round(w * chg, 4),
            }
        )

    # 보수적 가정: 원가 충격의 30%가 영업이익률에 전이
    impact_pp = weighted_change * 30.0
    baseline_margin = industry_margin_baseline(industry, yahoo_files)
    projected_margin = (baseline_margin - (impact_pp / 100.0)) if baseline_margin is not None else None
    band = risk_band(impact_pp)

    collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    summary = (
        f"{industry} 산업은 최근 원자재 변동을 반영한 가중 가격변동률이 {weighted_change * 100:.2f}%로 추정되며, "
        f"영업이익률 영향은 약 {impact_pp:+.2f}%p 수준으로 계산됩니다. "
        f"민감도 리스크 등급은 {band}입니다."
    )

    return {
        "company": f"{industry} 산업",
        "ticker": None,
        "market": "OTHER",
        "source": "industry_commodity_sensitivity",
        "industry_name": industry,
        "profile": {
            "industry": industry,
            "sector": "Industry",
            "market_cap": None,
            "revenue": None,
            "operating_margins": baseline_margin,
        },
        "title": f"{industry} 산업 원자재 민감도 분석",
        "summary": summary,
        "content": summary,
        "published_at": collected_at,
        "collected_at": collected_at,
        "commodity_sensitivity": {
            "weighted_change": round(weighted_change, 6),
            "margin_impact_pp": round(impact_pp, 4),
            "baseline_operating_margin": round(baseline_margin, 6) if baseline_margin is not None else None,
            "projected_operating_margin": round(projected_margin, 6) if projected_margin is not None else None,
            "risk_band": band,
            "details": detail,
            "commodity_changes": {k: round(v, 6) for k, v in changes.items()},
            "assumption": "margin_impact_pp = weighted_change * 30",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build industry commodity sensitivity snapshots")
    parser.add_argument(
        "--industries",
        default="반도체,바이오,2차전지,자동차,방산,조선,클라우드,에너지",
        help="쉼표 구분 산업 키워드",
    )
    parser.add_argument("--commodity-file", default="", help="원자재 변동률 JSON 파일(옵션)")
    parser.add_argument("--resume", action="store_true", help="기존 파일이 있으면 skip")
    args = parser.parse_args()

    industries = parse_industries(args.industries)
    if not industries:
        raise SystemExit("산업 목록이 비어 있습니다.")
    changes = load_commodity_changes(args.commodity_file or None)

    yahoo_files = sorted(RAW_DIR.glob("yahoo_*.json"))
    if not yahoo_files:
        raise SystemExit("yahoo raw 파일이 없습니다. 먼저 fetch_yahoo.py를 실행하세요.")

    ok = 0
    skip = 0
    for idx, industry in enumerate(industries, start=1):
        out = RAW_DIR / f"commodity_{industry_slug(industry)}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"[{idx}/{len(industries)}] skip (exists): {out}")
            continue

        payload = build_payload(industry, changes, yahoo_files)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        ok += 1
        print(f"[{idx}/{len(industries)}] saved: {out}")

    print(f"done. success={ok}, skip={skip}, total={len(industries)}")


if __name__ == "__main__":
    main()
