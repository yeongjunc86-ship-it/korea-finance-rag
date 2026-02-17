#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def to_float(v: Any) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x == x else None


def sector_hint(profile: dict[str, Any]) -> str:
    return str(profile.get("sector") or "일반").strip()


def industry_hint(profile: dict[str, Any]) -> str:
    return str(profile.get("industry") or "일반산업").strip()


def build_one(yahoo_payload: dict[str, Any], notes_payload: dict[str, Any] | None) -> dict[str, Any] | None:
    company = str(yahoo_payload.get("company") or "").strip()
    ticker = str(yahoo_payload.get("ticker") or "").strip()
    if not company or not ticker:
        return None

    profile = yahoo_payload.get("profile") if isinstance(yahoo_payload.get("profile"), dict) else {}
    revenue = to_float(profile.get("revenue"))
    market_cap = to_float(profile.get("market_cap"))
    op_margin = to_float(profile.get("operating_margins"))
    op_margin = op_margin if op_margin is not None else 0.12
    op_margin = max(0.03, min(0.5, op_margin))
    revenue = revenue if revenue is not None else (market_cap * 0.35 if market_cap else None)
    if revenue is None or revenue <= 0:
        return None

    sector = sector_hint(profile)
    industry = industry_hint(profile)

    note_blocks = {}
    if notes_payload and isinstance(notes_payload.get("dart_notes"), dict):
        note_blocks = notes_payload.get("dart_notes") or {}

    customer_notes = note_blocks.get("customer_dependency") if isinstance(note_blocks.get("customer_dependency"), list) else []
    segment_notes = note_blocks.get("business_segments") if isinstance(note_blocks.get("business_segments"), list) else []
    capex_notes = note_blocks.get("capex_investment") if isinstance(note_blocks.get("capex_investment"), list) else []
    debt_notes = note_blocks.get("debt_maturity") if isinstance(note_blocks.get("debt_maturity"), list) else []

    # Simple synergy model assumptions
    rev_synergy_pct = 0.02 if "technology" in sector.lower() else 0.015
    cost_synergy_pct = 0.03 if "industrial" in sector.lower() else 0.025
    procurement_save_pct = 0.018
    distribution_save_pct = 0.012
    cross_sell_uplift_pct = 0.01
    brand_risk_score = 42 if "consumer" in sector.lower() else 33

    it_integration_cost = revenue * 0.007
    pmi_months = 18 if len(segment_notes) >= 2 else 12
    overlap_ratio = 0.11 if len(customer_notes) >= 2 else 0.08
    legal_entity_reduction = 2 if debt_notes else 1

    annual_revenue_synergy = revenue * rev_synergy_pct
    annual_cost_synergy = revenue * cost_synergy_pct
    annual_distribution_saving = revenue * distribution_save_pct
    annual_procurement_saving = revenue * procurement_save_pct
    annual_cross_sell = revenue * cross_sell_uplift_pct

    summary = (
        f"{company} 시너지 케이스입니다. 매출 시너지 {rev_synergy_pct*100:.1f}%, "
        f"비용 시너지 {cost_synergy_pct*100:.1f}% 가정이며 PMI 예상 기간은 {pmi_months}개월입니다."
    )
    collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    return {
        "company": company,
        "ticker": ticker,
        "market": str(yahoo_payload.get("market") or "OTHER"),
        "source": "synergy_case_builder",
        "title": f"{company} 시너지 분석 케이스",
        "summary": summary,
        "content": summary,
        "published_at": collected_at,
        "collected_at": collected_at,
        "profile": {
            "industry": industry,
            "sector": sector,
            "market_cap": market_cap,
            "revenue": revenue,
            "operating_margins": op_margin,
        },
        "synergy_case": {
            "revenue_synergy_items": [
                "교차판매 확대",
                "신규 채널 진입",
                "제품 번들 업셀링",
            ],
            "revenue_synergy_pct": rev_synergy_pct,
            "annual_revenue_synergy": round(annual_revenue_synergy, 2),
            "cost_synergy_items": [
                "중복 조직 통합",
                "간접비 축소",
                "공통 플랫폼 사용",
            ],
            "cost_synergy_pct": cost_synergy_pct,
            "annual_cost_synergy": round(annual_cost_synergy, 2),
            "workforce_overlap_ratio": overlap_ratio,
            "distribution_integration_saving_pct": distribution_save_pct,
            "annual_distribution_saving": round(annual_distribution_saving, 2),
            "it_integration_cost": round(it_integration_cost, 2),
            "expected_pmi_months": pmi_months,
            "cross_selling_uplift_pct": cross_sell_uplift_pct,
            "annual_cross_selling_effect": round(annual_cross_sell, 2),
            "brand_integration_risk_score": brand_risk_score,
            "procurement_saving_pct": procurement_save_pct,
            "annual_procurement_saving": round(annual_procurement_saving, 2),
            "legal_entity_reduction_estimate": legal_entity_reduction,
            "supporting_notes": {
                "customer_dependency_count": len(customer_notes),
                "segment_note_count": len(segment_notes),
                "capex_note_count": len(capex_notes),
                "debt_note_count": len(debt_notes),
            },
            "assumptions_note": "자동 추정 기반 시너지 케이스이며 확정 PMI 계획 수치가 아님",
        },
    }


def build_notes_map() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for p in sorted(RAW_DIR.glob("dart_notes_*.json")):
        payload = load_json(p)
        if not payload:
            continue
        t = str(payload.get("ticker") or "").strip()
        c = str(payload.get("company") or "").strip().lower()
        if t:
            out[t] = payload
        if c:
            out[c] = payload
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build synergy_case raw files for 31~40 analysis")
    parser.add_argument("--limit", type=int, default=0, help="상위 N개만 생성")
    parser.add_argument("--resume", action="store_true", help="기존 파일이 있으면 건너뜀")
    args = parser.parse_args()

    yahoo_files = sorted(RAW_DIR.glob("yahoo_*.json"))
    if args.limit > 0:
        yahoo_files = yahoo_files[: args.limit]
    if not yahoo_files:
        raise SystemExit("yahoo raw 파일이 없습니다.")

    notes_map = build_notes_map()

    ok = 0
    skip = 0
    fail = 0
    for idx, p in enumerate(yahoo_files, start=1):
        payload = load_json(p)
        if not payload:
            fail += 1
            print(f"[{idx}/{len(yahoo_files)}] fail: invalid json ({p.name})")
            continue
        ticker = str(payload.get("ticker") or "").strip()
        company = str(payload.get("company") or "").strip().lower()
        if not ticker:
            fail += 1
            print(f"[{idx}/{len(yahoo_files)}] fail: missing ticker ({p.name})")
            continue
        out = RAW_DIR / f"synergy_case_{ticker.replace('.', '_')}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"[{idx}/{len(yahoo_files)}] skip (exists): {out}")
            continue
        notes_payload = notes_map.get(ticker) or notes_map.get(company)
        row = build_one(payload, notes_payload)
        if not row:
            skip += 1
            print(f"[{idx}/{len(yahoo_files)}] skip: insufficient data ({p.name})")
            continue
        out.write_text(json.dumps(row, ensure_ascii=False, indent=2), encoding="utf-8")
        ok += 1
        print(f"[{idx}/{len(yahoo_files)}] saved: {out}")

    print(f"done. success={ok}, skip={skip}, fail={fail}, total={len(yahoo_files)}")


if __name__ == "__main__":
    main()

