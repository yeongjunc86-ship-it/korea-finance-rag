#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/raw")

CUSTOMER_PATTERNS = [
    re.compile(r"(주요|상위)\s*고객"),
    re.compile(r"고객\s*의존"),
    re.compile(r"매출처"),
    re.compile(r"거래처"),
]
SEGMENT_PATTERNS = [
    re.compile(r"사업부"),
    re.compile(r"사업\s*부문"),
    re.compile(r"세그먼트"),
    re.compile(r"매출\s*비중"),
    re.compile(r"수익성"),
]
CAPEX_PATTERNS = [
    re.compile(r"CAPEX", re.IGNORECASE),
    re.compile(r"시설투자"),
    re.compile(r"설비투자"),
    re.compile(r"투자\s*계획"),
]
DEBT_PATTERNS = [
    re.compile(r"부채\s*만기"),
    re.compile(r"차입금"),
    re.compile(r"리파이낸싱", re.IGNORECASE),
    re.compile(r"만기\s*구조"),
    re.compile(r"유동성"),
]


def norm_text(v: str) -> str:
    x = str(v or "").strip().lower()
    x = x.replace("(주)", "").replace("주식회사", "").replace("㈜", "")
    x = re.sub(r"[^a-z0-9가-힣]+", "", x)
    return x


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def text_lines_from_any(v: Any, out: list[str]) -> None:
    if isinstance(v, str):
        s = re.sub(r"\s+", " ", v).strip()
        if len(s) >= 8:
            out.append(s)
        return
    if isinstance(v, dict):
        for vv in v.values():
            text_lines_from_any(vv, out)
        return
    if isinstance(v, list):
        for vv in v:
            text_lines_from_any(vv, out)


def dedup_keep_order(items: list[str], limit: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
        if len(out) >= limit:
            break
    return out


def pick_snippets(lines: list[str], patterns: list[re.Pattern[str]], limit: int = 12) -> list[str]:
    picked: list[str] = []
    for ln in lines:
        if any(p.search(ln) for p in patterns):
            picked.append(ln[:280])
    return dedup_keep_order(picked, limit)


def parse_one(path: Path, payload: dict[str, Any]) -> dict[str, Any] | None:
    company = str(payload.get("company") or "").strip()
    if not company:
        dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
        company = str(dart.get("corp_name") or path.stem).strip()

    corp_code = str(payload.get("corp_code") or "").strip()
    ticker = str(payload.get("ticker") or "").strip() or None
    market = str(payload.get("market") or "OTHER").strip() or "OTHER"
    source_name = str(payload.get("source") or "")

    lines: list[str] = []
    text_lines_from_any(payload, lines)
    lines = dedup_keep_order(lines, 5000)
    if not lines:
        return None

    customers = pick_snippets(lines, CUSTOMER_PATTERNS, limit=10)
    segments = pick_snippets(lines, SEGMENT_PATTERNS, limit=10)
    capex = pick_snippets(lines, CAPEX_PATTERNS, limit=10)
    debt = pick_snippets(lines, DEBT_PATTERNS, limit=10)

    if not (customers or segments or capex or debt):
        return None

    collected_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    summary_parts = []
    if customers:
        summary_parts.append(f"고객의존도 관련 단서 {len(customers)}건")
    if segments:
        summary_parts.append(f"사업부/세그먼트 단서 {len(segments)}건")
    if capex:
        summary_parts.append(f"CAPEX/투자 단서 {len(capex)}건")
    if debt:
        summary_parts.append(f"부채만기/유동성 단서 {len(debt)}건")
    summary = f"{company} DART 주석 파싱 결과: " + ", ".join(summary_parts) + "."

    if corp_code:
        out_name = f"dart_notes_{corp_code}.json"
    else:
        h = hashlib.sha1(f"{company}:{path.name}".encode("utf-8")).hexdigest()[:12]
        out_name = f"dart_notes_{h}.json"

    parsed = {
        "company": company,
        "ticker": ticker,
        "market": market,
        "source": "dart_notes_parser",
        "title": f"{company} DART 주석 구조화",
        "summary": summary,
        "content": summary,
        "published_at": payload.get("published_at"),
        "collected_at": collected_at,
        "corp_code": corp_code or None,
        "origin_source_file": str(path),
        "origin_source_name": source_name,
        "dart_notes": {
            "customer_dependency": customers,
            "business_segments": segments,
            "capex_investment": capex,
            "debt_maturity": debt,
        },
    }
    out_path = OUT_DIR / out_name
    return {"path": str(out_path), "payload": parsed}


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse DART raw files into note-focused structured docs")
    parser.add_argument("--limit", type=int, default=0, help="상위 N개 파일만 처리 (0은 전체)")
    parser.add_argument("--resume", action="store_true", help="기존 dart_notes 파일 있으면 건너뜀")
    parser.add_argument("--companies", nargs="*", default=[], help="특정 회사명/티커/corp_code만 처리")
    args = parser.parse_args()

    files = sorted([*RAW_DIR.glob("dart_*.json"), *RAW_DIR.glob("dart_report_*.json")])
    if args.limit > 0:
        files = files[: args.limit]
    if not files:
        raise SystemExit("dart raw 파일이 없습니다. 먼저 fetch_dart_bulk.py 또는 dart report 수집을 실행하세요.")

    filters_raw = [str(x).strip().lower() for x in args.companies if str(x).strip()]
    filters_norm = [norm_text(x) for x in filters_raw]

    ok = 0
    skip = 0
    fail = 0
    for idx, p in enumerate(files, start=1):
        payload = load_json(p)
        if not payload:
            fail += 1
            print(f"[{idx}/{len(files)}] fail: invalid json ({p})")
            continue

        if filters_raw:
            company = str(payload.get("company") or "").strip()
            ticker = str(payload.get("ticker") or "").strip()
            corp_code = str(payload.get("corp_code") or "").strip()
            dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
            corp_name = str(dart.get("corp_name") or "").strip()
            stock_code = str(dart.get("stock_code") or "").strip()
            cand_raw = {
                company.lower(),
                ticker.lower(),
                corp_code.lower(),
                corp_name.lower(),
                stock_code.lower(),
            }
            cand_norm = {norm_text(x) for x in [company, ticker, corp_code, corp_name, stock_code] if x}
            matched = False
            for fr, fn in zip(filters_raw, filters_norm):
                if any(fr and (fr in c or c in fr) for c in cand_raw if c):
                    matched = True
                    break
                if any(fn and (fn in c or c in fn) for c in cand_norm if c):
                    matched = True
                    break
            if not matched:
                skip += 1
                continue

        parsed = parse_one(p, payload)
        if not parsed:
            skip += 1
            print(f"[{idx}/{len(files)}] skip: no note signal ({p.name})")
            continue

        out_path = Path(parsed["path"])
        if args.resume and out_path.exists():
            skip += 1
            print(f"[{idx}/{len(files)}] skip (exists): {out_path}")
            continue

        out_path.write_text(json.dumps(parsed["payload"], ensure_ascii=False, indent=2), encoding="utf-8")
        ok += 1
        print(f"[{idx}/{len(files)}] saved: {out_path}")

    print(f"done. success={ok}, skip={skip}, fail={fail}, total={len(files)}")


if __name__ == "__main__":
    main()
