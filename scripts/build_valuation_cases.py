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


def clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))


def default_multiple_by_sector(sector: str) -> tuple[float, float]:
    s = (sector or "").lower()
    if "technology" in s or "it" in s:
        return 7.5, 11.0
    if "health" in s or "biotech" in s:
        return 8.0, 12.5
    if "energy" in s:
        return 5.0, 8.0
    if "financial" in s:
        return 5.5, 8.5
    return 6.0, 9.5


def build_one(payload: dict[str, Any], valuation_by_industry: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    ticker = str(payload.get("ticker") or "").strip()
    company = str(payload.get("company") or "").strip()
    if not ticker or not company:
        return None

    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
    industry = str(profile.get("industry") or "정보 부족").strip()
    sector = str(profile.get("sector") or "").strip()
    market = str(payload.get("market") or "OTHER").strip() or "OTHER"

    revenue = to_float(profile.get("revenue"))
    op_margin = to_float(profile.get("operating_margins"))
    if revenue is None or revenue <= 0:
        return None

    op_margin = op_margin if op_margin is not None else 0.12
    op_margin = clamp(op_margin, 0.03, 0.45)
    ebitda = revenue * op_margin

    iv = valuation_by_industry.get(industry.lower(), {})
    low_mult = to_float(iv.get("low_multiple")) or default_multiple_by_sector(sector)[0]
    high_mult = to_float(iv.get("high_multiple")) or default_multiple_by_sector(sector)[1]
    base_mult = (low_mult + high_mult) / 2.0

    ev_low = ebitda * low_mult
    ev_base = ebitda * base_mult
    ev_high = ebitda * high_mult

    # DCF/WACC input assumptions
    risk_free = 0.032
    erp = 0.055
    beta = 1.05 if "technology" in sector.lower() else 0.95
    cost_of_equity = risk_free + beta * erp
    cost_of_debt = 0.055
    tax_rate = 0.24
    debt_ratio = 0.35
    wacc = (1 - debt_ratio) * cost_of_equity + debt_ratio * cost_of_debt * (1 - tax_rate)

    # Scenario EV
    scenario = {
        "conservative": {
            "ebitda_growth": 0.01,
            "multiple": round(low_mult, 2),
            "enterprise_value": round(ev_low, 2),
        },
        "base": {
            "ebitda_growth": 0.03,
            "multiple": round(base_mult, 2),
            "enterprise_value": round(ev_base, 2),
        },
        "aggressive": {
            "ebitda_growth": 0.06,
            "multiple": round(high_mult, 2),
            "enterprise_value": round(ev_high, 2),
        },
    }

    # 20% premium IRR estimate (simple 5Y hold)
    entry_ev = ev_base * 1.2
    exit_ev = ev_base * ((1 + scenario["base"]["ebitda_growth"]) ** 5)
    irr = (exit_ev / entry_ev) ** (1 / 5) - 1 if entry_ev > 0 else None

    # Synergy impact
    revenue_synergy = 0.03
    cost_synergy = 0.02
    synergy_ev = ev_base * (1 + revenue_synergy + cost_synergy)

    # PER premium/discount factors (proxy)
    net_margin_assumed = max(0.03, op_margin * 0.62)
    est_net_income = revenue * net_margin_assumed
    market_cap = to_float(profile.get("market_cap")) or ev_base
    per_company = market_cap / est_net_income if est_net_income > 0 else None
    per_industry = max(5.0, base_mult * 1.4)
    per_gap_pct = ((per_company / per_industry) - 1) * 100 if per_company else None

    # FX sensitivity (KRW weak +10%)
    export_ratio = 0.45 if "technology" in sector.lower() else 0.25
    fx_impact_ev_pct = export_ratio * 0.10 * 0.6  # pass-through assumption

    # LBO leverage
    max_debt_multiple = 4.5 if "technology" in sector.lower() else 3.8
    debt_capacity = ebitda * max_debt_multiple

    # EBITDA adjustments
    ebitda_adjustments = [
        "일회성 비용/수익 제거",
        "리스(IFRS16) 영향 분리",
        "비경상 충당금 및 소송비 조정",
        "계열사/특수관계자 거래 정상화",
        "주식보상비용(SBC) 반영 정책 일관화",
    ]

    collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    summary = (
        f"{company} 밸류에이션 케이스입니다. EV/EBITDA 범위는 {low_mult:.1f}x~{high_mult:.1f}x, "
        f"기준 EV는 {ev_base:,.0f}, 프리미엄 20% 적용 5년 IRR 추정치는 "
        f"{(irr * 100):.1f}%입니다."
        if irr is not None
        else f"{company} 밸류에이션 케이스입니다. EV/EBITDA 범위는 {low_mult:.1f}x~{high_mult:.1f}x입니다."
    )

    return {
        "company": company,
        "ticker": ticker,
        "market": market,
        "source": "valuation_case_builder",
        "title": f"{company} 밸류에이션 케이스",
        "summary": summary,
        "content": summary,
        "published_at": collected_at,
        "collected_at": collected_at,
        "profile": {
            "industry": industry,
            "sector": sector or "정보 부족",
            "market_cap": market_cap,
            "revenue": revenue,
            "operating_margins": op_margin,
        },
        "valuation_case": {
            "ev_ebitda": {
                "low": round(low_mult, 2),
                "base": round(base_mult, 2),
                "high": round(high_mult, 2),
            },
            "enterprise_value": {
                "low": round(ev_low, 2),
                "base": round(ev_base, 2),
                "high": round(ev_high, 2),
            },
            "wacc_inputs": {
                "risk_free_rate": risk_free,
                "equity_risk_premium": erp,
                "beta": beta,
                "cost_of_equity": round(cost_of_equity, 6),
                "cost_of_debt": cost_of_debt,
                "tax_rate": tax_rate,
                "debt_ratio": debt_ratio,
                "wacc": round(wacc, 6),
            },
            "scenario_value": scenario,
            "premium_irr": {
                "premium_pct": 0.20,
                "holding_years": 5,
                "irr": round(irr, 6) if irr is not None else None,
            },
            "synergy_value": {
                "pre_synergy_ev": round(ev_base, 2),
                "post_synergy_ev": round(synergy_ev, 2),
                "revenue_synergy_pct": revenue_synergy,
                "cost_synergy_pct": cost_synergy,
            },
            "per_gap": {
                "company_per": round(per_company, 4) if per_company is not None else None,
                "industry_per": round(per_industry, 4),
                "gap_pct": round(per_gap_pct, 4) if per_gap_pct is not None else None,
            },
            "fx_sensitivity": {
                "base_currency": "KRW",
                "fx_shock": "KRW 약세 10%",
                "export_ratio_assumed": export_ratio,
                "ev_impact_pct": round(fx_impact_ev_pct * 100, 4),
            },
            "lbo": {
                "max_net_debt_to_ebitda": max_debt_multiple,
                "debt_capacity": round(debt_capacity, 2),
            },
            "ebitda_adjustments": ebitda_adjustments,
            "assumptions_note": "해당 값은 raw 데이터 기반의 자동 추정치이며 투자판단용 확정 수치가 아님",
        },
    }


def load_industry_valuation_map() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for p in sorted(RAW_DIR.glob("valuation_*.json")):
        payload = load_json(p)
        if not payload:
            continue
        industry = str(payload.get("industry_name") or "").strip().lower()
        val = payload.get("valuation") if isinstance(payload.get("valuation"), dict) else {}
        avg = to_float(val.get("average"))
        med = to_float(val.get("median"))
        if not industry:
            continue
        if avg is None and med is None:
            continue
        low = min(x for x in [avg, med] if x is not None) * 0.9
        high = max(x for x in [avg, med] if x is not None) * 1.1
        out[industry] = {"low_multiple": low, "high_multiple": high}
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build valuation cases for companies from yahoo raw")
    parser.add_argument("--limit", type=int, default=0, help="상위 N개만 생성")
    parser.add_argument("--resume", action="store_true", help="기존 valuation_case 파일 건너뜀")
    args = parser.parse_args()

    yahoo_files = sorted(RAW_DIR.glob("yahoo_*.json"))
    if args.limit > 0:
        yahoo_files = yahoo_files[: args.limit]
    if not yahoo_files:
        raise SystemExit("yahoo raw 파일이 없습니다.")

    industry_map = load_industry_valuation_map()

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
        if not ticker:
            fail += 1
            print(f"[{idx}/{len(yahoo_files)}] fail: missing ticker ({p.name})")
            continue
        out = RAW_DIR / f"valuation_case_{ticker.replace('.', '_')}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"[{idx}/{len(yahoo_files)}] skip (exists): {out}")
            continue
        row = build_one(payload, industry_map)
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

