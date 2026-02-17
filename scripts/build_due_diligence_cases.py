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
    op_margin = max(0.02, min(0.5, op_margin))
    if revenue is None:
        revenue = market_cap * 0.35 if market_cap else None
    if revenue is None or revenue <= 0:
        return None

    notes = {}
    if notes_payload and isinstance(notes_payload.get("dart_notes"), dict):
        notes = notes_payload.get("dart_notes") or {}
    n_customer = len(notes.get("customer_dependency") or [])
    n_segment = len(notes.get("business_segments") or [])
    n_capex = len(notes.get("capex_investment") or [])
    n_debt = len(notes.get("debt_maturity") or [])

    # Signal-style scoring (0~100)
    accounting_risk = min(100, 28 + (10 if op_margin < 0.08 else 0) + (8 if n_customer >= 3 else 0))
    contingent_liability_risk = min(100, 25 + (12 if n_debt >= 3 else 0))
    revenue_recognition_risk = min(100, 30 + (10 if n_customer >= 2 else 0))
    inventory_valuation_risk = min(100, 32 + (8 if n_segment >= 3 else 0))
    tax_risk = min(100, 27 + (10 if revenue > 1_000_000_000_000 else 0))
    key_person_risk = min(100, 35 + (7 if n_segment >= 2 else 0))
    coc_clause_risk = min(100, 30 + (12 if n_debt >= 2 else 0))
    privacy_security_risk = min(100, 33 + (12 if "platform" in str(profile.get("industry") or "").lower() else 0))
    supply_chain_risk = min(100, 30 + (10 if n_customer >= 4 else 0))
    pmi_failure_risk = min(100, 34 + (10 if n_segment >= 3 else 0) + (8 if n_debt >= 2 else 0))

    summary = (
        f"{company} 실사 케이스입니다. 재무/계약/보안/공급망/PMI 기준 리스크 신호를 구조화했으며 "
        f"점검 우선순위는 매출인식, 법무조항, PMI 실행력 순입니다."
    )
    collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    return {
        "company": company,
        "ticker": ticker,
        "market": str(yahoo_payload.get("market") or "OTHER"),
        "source": "due_diligence_case_builder",
        "title": f"{company} 실사 리스크 케이스",
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
        "due_diligence_case": {
            "financial_focus_items": [
                "매출 인식 정책",
                "운전자본 변동",
                "비경상 손익",
                "현금흐름 질",
            ],
            "contingent_liability_risk": contingent_liability_risk,
            "revenue_recognition_risk": revenue_recognition_risk,
            "inventory_valuation_risk": inventory_valuation_risk,
            "tax_risk": tax_risk,
            "key_person_risk": key_person_risk,
            "change_of_control_clause_risk": coc_clause_risk,
            "privacy_security_risk": privacy_security_risk,
            "supply_chain_dependency_risk": supply_chain_risk,
            "pmi_failure_risk": pmi_failure_risk,
            "accounting_risk": accounting_risk,
            "supporting_notes": {
                "customer_dependency_count": n_customer,
                "segment_note_count": n_segment,
                "capex_note_count": n_capex,
                "debt_note_count": n_debt,
            },
            "assumptions_note": "자동 추정 리스크 신호이며 법률/회계 자문 대체 불가",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build due_diligence_case raw files for 41~50 analysis")
    parser.add_argument("--limit", type=int, default=0, help="상위 N개만 생성")
    parser.add_argument("--resume", action="store_true", help="기존 파일 건너뜀")
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
        out = RAW_DIR / f"due_diligence_case_{ticker.replace('.', '_')}.json"
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

