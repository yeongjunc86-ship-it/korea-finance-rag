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


def to_float(v: Any, default: float | None = None) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    return x if x == x else default


def build_map(prefix: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for p in sorted(RAW_DIR.glob(f"{prefix}_*.json")):
        payload = load_json(p)
        if not payload:
            continue
        ticker = str(payload.get("ticker") or "").strip()
        company = str(payload.get("company") or "").strip().lower()
        if ticker:
            out[ticker] = payload
        if company:
            out[company] = payload
    return out


def pick_exit_strategy(growth: float, leverage: float, fit: int) -> str:
    if fit >= 75 and growth >= 0.08:
        return "3년 내 통합가치 제고 후 전략적 재매각"
    if leverage >= 3.5:
        return "현금흐름 안정화 후 단계적 지분매각"
    return "부분 엑시트(구주매각) + 잔여지분 보유"


def build_one(
    yahoo_payload: dict[str, Any],
    valuation_payload: dict[str, Any] | None,
    synergy_payload: dict[str, Any] | None,
    dd_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    company = str(yahoo_payload.get("company") or "").strip()
    ticker = str(yahoo_payload.get("ticker") or "").strip()
    if not company or not ticker:
        return None

    profile = yahoo_payload.get("profile") if isinstance(yahoo_payload.get("profile"), dict) else {}
    revenue = to_float(profile.get("revenue"))
    market_cap = to_float(profile.get("market_cap"))
    op_margin = to_float(profile.get("operating_margins"), default=0.12) or 0.12

    valuation_case = valuation_payload.get("valuation_case") if isinstance(valuation_payload, dict) else {}
    synergy_case = synergy_payload.get("synergy_case") if isinstance(synergy_payload, dict) else {}
    dd_case = dd_payload.get("due_diligence_case") if isinstance(dd_payload, dict) else {}

    ev_ebitda = to_float((valuation_case or {}).get("ev_ebitda"), default=8.0) or 8.0
    leverage = to_float((valuation_case or {}).get("target_debt_ebitda"), default=3.0) or 3.0
    synergy_pct = to_float((synergy_case or {}).get("revenue_synergy_pct"), default=0.06) or 0.06
    synergy_cost = to_float((synergy_case or {}).get("cost_synergy_pct"), default=0.04) or 0.04
    pmi_months = int(to_float((synergy_case or {}).get("integration_period_months"), default=18) or 18)
    pmi_fail_risk = to_float((dd_case or {}).get("pmi_failure_risk"), default=45) or 45
    coc_risk = to_float((dd_case or {}).get("change_of_control_clause_risk"), default=40) or 40

    growth = 0.06 if not revenue or not market_cap else max(0.02, min(0.15, (market_cap / max(revenue, 1.0)) * 0.04))
    fit_score = int(round(max(20.0, min(95.0, 45 + (synergy_pct * 220) + (synergy_cost * 160) - (pmi_fail_risk * 0.2)))))
    opp_cost = int(round(max(20.0, min(95.0, 35 + fit_score * 0.45 + growth * 120))))
    competitor_impact = int(round(max(20.0, min(95.0, 30 + opp_cost * 0.55))))
    earnout_score = int(round(max(20.0, min(90.0, 65 - (coc_risk * 0.2) + (pmi_months * -0.3) + (growth * 100)))))

    deal_type = "전략적 인수" if (fit_score >= 60 and synergy_pct >= 0.05) else "재무적 투자 성격"
    staged_feasible = coc_risk < 60 and leverage <= 4.0
    merger_vs_sub = "자회사 편입" if (pmi_fail_risk >= 55 or pmi_months >= 24) else "합병"
    cash_vs_stock = "주식 교환 비중 확대" if leverage >= 3.8 else "현금 인수 비중 확대"
    optimal = (
        "초기 60~80% 지분 인수 + Earn-out + 2~3년 내 잔여지분 콜옵션"
        if staged_feasible
        else "초기 과반 인수 + 성과연동 대금조정 + 통합 후 구조개편"
    )
    exit_strategy = pick_exit_strategy(growth=growth, leverage=leverage, fit=fit_score)

    summary = (
        f"{company} 전략 의사결정 케이스입니다. 포트폴리오 적합성, 인수 구조 옵션, Exit 경로를 "
        f"정량 점수와 가정 기반으로 구조화했습니다."
    )
    collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    return {
        "company": company,
        "ticker": ticker,
        "market": str(yahoo_payload.get("market") or "OTHER"),
        "source": "strategic_case_builder",
        "title": f"{company} 전략 의사결정 케이스",
        "summary": summary,
        "content": summary,
        "published_at": collected_at,
        "collected_at": collected_at,
        "profile": {
            "industry": str(profile.get("industry") or "정보 부족"),
            "sector": str(profile.get("sector") or "정보 부족"),
            "market_cap": market_cap,
            "revenue": revenue,
            "operating_margins": op_margin,
        },
        "strategic_case": {
            "deal_type_assessment": deal_type,
            "portfolio_fit_score": fit_score,
            "exit_strategy_3y": exit_strategy,
            "opportunity_cost_score": opp_cost,
            "competitor_acquisition_impact_score": competitor_impact,
            "staged_acquisition_feasible": staged_feasible,
            "earnout_feasibility_score": earnout_score,
            "merger_vs_subsidiary": merger_vs_sub,
            "cash_vs_stock": cash_vs_stock,
            "optimal_deal_structure": optimal,
            "supporting_metrics": {
                "ev_ebitda": ev_ebitda,
                "target_debt_ebitda": leverage,
                "revenue_synergy_pct": synergy_pct,
                "cost_synergy_pct": synergy_cost,
                "integration_period_months": pmi_months,
                "pmi_failure_risk": pmi_fail_risk,
                "change_of_control_clause_risk": coc_risk,
            },
            "assumptions_note": "자동 생성 케이스이며 실제 투자/법무/세무 자문을 대체하지 않음",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build strategic_case raw files for 51~60 analysis")
    parser.add_argument("--limit", type=int, default=0, help="상위 N개만 생성")
    parser.add_argument("--resume", action="store_true", help="기존 파일 건너뜀")
    args = parser.parse_args()

    yahoo_files = sorted(RAW_DIR.glob("yahoo_*.json"))
    if args.limit > 0:
        yahoo_files = yahoo_files[: args.limit]
    if not yahoo_files:
        raise SystemExit("yahoo raw 파일이 없습니다.")

    valuation_map = build_map("valuation_case")
    synergy_map = build_map("synergy_case")
    dd_map = build_map("due_diligence_case")

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
        out = RAW_DIR / f"strategic_case_{ticker.replace('.', '_')}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"[{idx}/{len(yahoo_files)}] skip (exists): {out}")
            continue

        valuation_payload = valuation_map.get(ticker) or valuation_map.get(company)
        synergy_payload = synergy_map.get(ticker) or synergy_map.get(company)
        dd_payload = dd_map.get(ticker) or dd_map.get(company)
        row = build_one(payload, valuation_payload, synergy_payload, dd_payload)
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
