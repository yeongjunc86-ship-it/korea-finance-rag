#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)

OUT_FACTS = PROC_DIR / "customer_dependency_facts.jsonl"

CUSTOMER_SIGNAL = re.compile(
    r"(주요\s*고객|상위\s*고객|고객\s*의존|매출처|거래처|customer\s+concentration|top\s+customer)",
    re.IGNORECASE,
)
PCT_PATTERN = re.compile(r"(\d{1,2}(?:\.\d+)?)\s*%")
NAMED_PCT_PATTERN = re.compile(
    r"([A-Za-z0-9가-힣\(\)\.\-·&/\s]{2,40})\s*[:\-]?\s*(\d{1,2}(?:\.\d+)?)\s*%",
    re.IGNORECASE,
)
GENERIC_NAME_TERMS = {
    "고객",
    "주요 고객",
    "상위 고객",
    "매출 비중",
    "비중",
    "매출",
    "의존도",
    "고객의존도",
    "customer",
    "top customer",
}


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def clean_line(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def norm_text(v: Any) -> str:
    x = clean_line(v).lower()
    x = x.replace("(주)", "").replace("주식회사", "").replace("㈜", "")
    x = re.sub(r"[^a-z0-9가-힣]+", "", x)
    return x


def to_float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace("%", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def is_generic_name(name: str) -> bool:
    n = clean_line(name).lower()
    if not n:
        return True
    if n in {x.lower() for x in GENERIC_NAME_TERMS}:
        return True
    if len(n) <= 1:
        return True
    return False


def normalize_customer_name(name: str, anon_idx: int) -> tuple[str, bool]:
    n = clean_line(name).strip(" -:")
    if not n or is_generic_name(n):
        return (f"익명고객#{anon_idx}", True)
    if re.fullmatch(r"[A-Z]사", n) or re.fullmatch(r"[A-Z]", n):
        return (n, True)
    return (n[:60], False)


def iter_text_lines(v: Any, out: list[str]) -> None:
    if isinstance(v, str):
        s = clean_line(v)
        if len(s) >= 8:
            out.append(s)
        return
    if isinstance(v, dict):
        for vv in v.values():
            iter_text_lines(vv, out)
        return
    if isinstance(v, list):
        for vv in v:
            iter_text_lines(vv, out)


def dedup_keep_order(items: list[str], limit: int = 200) -> list[str]:
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


def extract_from_line(line: str, source_file: str) -> list[dict[str, Any]]:
    if not CUSTOMER_SIGNAL.search(line):
        return []
    out: list[dict[str, Any]] = []
    anon_idx = 1

    for m in NAMED_PCT_PATTERN.finditer(line):
        raw_name = clean_line(m.group(1))
        pct = to_float(m.group(2))
        if pct is None:
            continue
        name, anonymized = normalize_customer_name(raw_name, anon_idx)
        if anonymized and name.startswith("익명고객#"):
            anon_idx += 1
        out.append(
            {
                "name": name,
                "revenue_share_pct": pct,
                "anonymized": anonymized,
                "source_type": "text_signal",
                "confidence": 0.75 if not anonymized else 0.55,
                "evidence": line[:240],
                "source_file": source_file,
            }
        )

    if not out:
        pcts = [to_float(x) for x in PCT_PATTERN.findall(line)]
        pcts = [x for x in pcts if x is not None]
        for pct in pcts[:10]:
            out.append(
                {
                    "name": f"익명고객#{anon_idx}",
                    "revenue_share_pct": pct,
                    "anonymized": True,
                    "source_type": "text_signal",
                    "confidence": 0.45,
                    "evidence": line[:240],
                    "source_file": source_file,
                }
            )
            anon_idx += 1
    if not out:
        out.append(
            {
                "name": "익명고객#1",
                "revenue_share_pct": None,
                "anonymized": True,
                "source_type": "text_signal",
                "confidence": 0.3,
                "evidence": line[:240],
                "source_file": source_file,
            }
        )
    return out


def gather_candidates(path: Path, payload: dict[str, Any]) -> tuple[str, str | None, str, list[dict[str, Any]]]:
    company = clean_line(payload.get("company") or payload.get("corp_name") or path.stem)
    ticker = clean_line(payload.get("ticker") or "") or None
    market = clean_line(payload.get("market") or "OTHER") or "OTHER"
    source_file = str(path)
    candidates: list[dict[str, Any]] = []

    ext = payload.get("customer_dependency")
    if isinstance(ext, dict):
        ext_list = ext.get("top_customers")
        if isinstance(ext_list, list):
            for row in ext_list:
                if not isinstance(row, dict):
                    continue
                pct = to_float(row.get("revenue_share_pct"))
                evidence_text = clean_line(
                    str(
                        row.get("note")
                        or row.get("evidence")
                        or row.get("source_url")
                        or row.get("source_type")
                        or "external_input"
                    )
                )[:240]
                if pct is None and not evidence_text:
                    continue
                name, anonymized = normalize_customer_name(str(row.get("name") or ""), 1)
                conf = to_float(row.get("confidence"))
                candidates.append(
                    {
                        "name": name,
                        "revenue_share_pct": pct,
                        "anonymized": anonymized,
                        "source_type": str(row.get("source_type") or "external"),
                        "confidence": conf if conf is not None else 0.9,
                        "evidence": evidence_text,
                        "source_file": source_file,
                    }
                )

    notes = payload.get("dart_notes")
    if isinstance(notes, dict):
        snippets = notes.get("customer_dependency")
        if isinstance(snippets, list):
            for s in snippets:
                line = clean_line(s)
                if not line:
                    continue
                candidates.extend(extract_from_line(line, source_file))

    if path.name.startswith("dart_") or path.name.startswith("news_"):
        lines: list[str] = []
        iter_text_lines(payload, lines)
        for line in dedup_keep_order(lines, limit=600):
            if not CUSTOMER_SIGNAL.search(line):
                continue
            candidates.extend(extract_from_line(line, source_file))

    return company, ticker, market, candidates


def company_key(company: str, ticker: str | None) -> str:
    if ticker:
        return ticker.replace(".", "_")
    return re.sub(r"[^a-zA-Z0-9가-힣]+", "_", company).strip("_")[:80] or "unknown"


def build_company_payload(
    company: str,
    ticker: str | None,
    market: str,
    rows: list[dict[str, Any]],
    source_files: set[str],
) -> dict[str, Any]:
    best_by_name: dict[str, dict[str, Any]] = {}
    for r in rows:
        name = str(r.get("name") or "").strip()
        pct = to_float(r.get("revenue_share_pct"))
        conf = to_float(r.get("confidence")) or 0.4
        if not name:
            continue
        cur = best_by_name.get(name)
        if cur is None or conf > (to_float(cur.get("confidence")) or 0):
            best_by_name[name] = {
                "name": name,
                "revenue_share_pct": pct,
                "anonymized": bool(r.get("anonymized")),
                "source_type": str(r.get("source_type") or "unknown"),
                "confidence": conf,
                "evidence": str(r.get("evidence") or "")[:240],
                "source_file": str(r.get("source_file") or ""),
            }

    customers = sorted(
        best_by_name.values(),
        key=lambda x: (
            -1 if x.get("revenue_share_pct") is None else float(x.get("revenue_share_pct") or 0),
            float(x.get("confidence") or 0),
        ),
        reverse=True,
    )[:10]

    top1 = next(
        (float(x["revenue_share_pct"]) for x in customers if isinstance(x.get("revenue_share_pct"), (int, float))),
        None,
    )
    top3_vals = [
        float(x["revenue_share_pct"])
        for x in customers[:3]
        if isinstance(x.get("revenue_share_pct"), (int, float))
    ]
    top3 = sum(top3_vals) if top3_vals else None
    avg_conf = (
        sum(float(x.get("confidence") or 0) for x in customers) / len(customers) if customers else 0.0
    )
    anonymized_ratio = (
        (sum(1 for x in customers if x.get("anonymized")) / len(customers)) if customers else 1.0
    )

    if not customers:
        coverage_status = "insufficient"
    elif avg_conf >= 0.75 and anonymized_ratio < 0.5:
        coverage_status = "named_high_confidence"
    elif avg_conf >= 0.55:
        coverage_status = "partial_mixed"
    else:
        coverage_status = "anonymized_or_low_confidence"

    concentration_risk = "정보 부족"
    if top1 is not None:
        if top1 >= 40:
            concentration_risk = "높음"
        elif top1 >= 20:
            concentration_risk = "중간"
        else:
            concentration_risk = "낮음"

    generated_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    summary = (
        f"{company} 주요 매출 고객/의존도 추출 결과입니다. "
        f"Top1 {top1:.1f}%."
        if top1 is not None
        else f"{company} 주요 매출 고객/의존도 추출 결과입니다. 명시적 수치가 제한적입니다."
    )

    return {
        "company": company,
        "ticker": ticker,
        "market": market,
        "source": "customer_dependency_builder",
        "title": f"{company} 주요 매출 고객/의존도 프로파일",
        "summary": summary,
        "content": summary,
        "published_at": None,
        "collected_at": generated_at,
        "customer_dependency": {
            "coverage_status": coverage_status,
            "top_customers": customers,
            "metrics": {
                "top1_share_pct": top1,
                "top3_share_pct": top3,
                "customer_count": len(customers),
                "avg_confidence": round(avg_conf, 3) if customers else None,
                "anonymized_ratio": round(anonymized_ratio, 3) if customers else None,
                "concentration_risk": concentration_risk,
            },
            "source_files": sorted(source_files),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build customer concentration profiles from raw sources")
    parser.add_argument("--out", default=str(OUT_FACTS))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--min-customers", type=int, default=1)
    parser.add_argument("--write-raw", action="store_true")
    parser.add_argument("--companies", nargs="*", default=[], help="특정 회사명/티커만 생성")
    args = parser.parse_args()

    files = sorted(
        [
            *RAW_DIR.glob("customer_dependency_external_*.json"),
            *RAW_DIR.glob("customer_dependency_llm_*.json"),
            *RAW_DIR.glob("dart_notes_*.json"),
            *RAW_DIR.glob("dart_*.json"),
            *RAW_DIR.glob("news_*.json"),
        ]
    )
    if not files:
        raise SystemExit("입력 raw 파일이 없습니다.")

    if args.limit > 0:
        files = files[: args.limit]

    filters_raw = [str(x).strip().lower() for x in args.companies if str(x).strip()]
    filters_norm = [norm_text(x) for x in filters_raw]

    by_company: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"company": "", "ticker": None, "market": "OTHER", "rows": [], "sources": set()}
    )

    for p in files:
        payload = load_json(p)
        if not payload:
            continue
        company, ticker, market, rows = gather_candidates(p, payload)
        if not company:
            continue
        if filters_raw:
            cands_raw = {str(x).lower() for x in [company, ticker or ""] if str(x).strip()}
            cands_norm = {norm_text(x) for x in [company, ticker or ""] if str(x).strip()}
            matched = False
            for fr, fn in zip(filters_raw, filters_norm):
                if any(fr and (fr in c or c in fr) for c in cands_raw):
                    matched = True
                    break
                if any(fn and (fn in c or c in fn) for c in cands_norm):
                    matched = True
                    break
            if not matched:
                continue
        key = company.lower().strip()
        ent = by_company[key]
        ent["company"] = ent["company"] or company
        ent["ticker"] = ent["ticker"] or ticker
        ent["market"] = ent["market"] if ent["market"] != "OTHER" else market
        ent["rows"].extend(rows)
        ent["sources"].add(str(p))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ok = 0
    skip = 0
    with out_path.open("w", encoding="utf-8") as out:
        for _, ent in sorted(by_company.items(), key=lambda kv: kv[1]["company"]):
            payload = build_company_payload(
                company=str(ent["company"]),
                ticker=ent["ticker"],
                market=str(ent["market"] or "OTHER"),
                rows=list(ent["rows"]),
                source_files=set(ent["sources"]),
            )
            top_customers = payload.get("customer_dependency", {}).get("top_customers", [])
            if len(top_customers) < max(0, args.min_customers):
                skip += 1
                continue

            fact_row = {
                "company": payload["company"],
                "ticker": payload.get("ticker"),
                "market": payload.get("market"),
                "coverage_status": payload["customer_dependency"]["coverage_status"],
                "top1_share_pct": payload["customer_dependency"]["metrics"]["top1_share_pct"],
                "top3_share_pct": payload["customer_dependency"]["metrics"]["top3_share_pct"],
                "customer_count": payload["customer_dependency"]["metrics"]["customer_count"],
                "source_files": payload["customer_dependency"]["source_files"],
            }
            out.write(json.dumps(fact_row, ensure_ascii=False) + "\n")
            ok += 1

            if args.write_raw:
                raw_name = f"customer_dependency_{company_key(payload['company'], payload.get('ticker'))}.json"
                raw_out = RAW_DIR / raw_name
                raw_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"saved: {out_path}")
    print(f"done. success={ok}, skip={skip}, total_companies={len(by_company)}")


if __name__ == "__main__":
    main()
