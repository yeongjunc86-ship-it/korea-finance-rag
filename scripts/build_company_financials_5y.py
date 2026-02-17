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

OUT_JSONL = PROC_DIR / "company_financials_5y.jsonl"


def parse_amount(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = s.replace(",", "").replace(" ", "")
    try:
        x = float(s)
    except ValueError:
        return None
    return -x if neg else x


def norm_account(v: str) -> str:
    x = str(v or "").lower()
    x = re.sub(r"[^a-z0-9가-힣]+", "", x)
    return x


def norm_text(v: str) -> str:
    x = str(v or "").strip().lower()
    x = x.replace("(주)", "").replace("주식회사", "").replace("㈜", "")
    x = re.sub(r"[^a-z0-9가-힣]+", "", x)
    return x


def pick_amount(rows: list[dict[str, Any]], keys: list[str]) -> float | None:
    norm_keys = [norm_account(k) for k in keys]
    for r in rows:
        nm = norm_account(str(r.get("account_nm") or ""))
        enm = norm_account(str(r.get("account_nm_en") or ""))
        aid = norm_account(str(r.get("account_id") or ""))
        bag = " ".join([nm, enm, aid])
        if any(k in bag for k in norm_keys):
            for col in ("thstrm_amount", "frmtrm_amount", "bfefrmtrm_amount"):
                val = parse_amount(r.get(col))
                if val is not None:
                    return val
    return None


def load_fin_rows(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
    rows = dart.get("list")
    if not isinstance(rows, list):
        rows = []
    out_rows = [r for r in rows if isinstance(r, dict)]
    return payload, out_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build 5Y company financial facts from DART financial raws")
    parser.add_argument("--out", default=str(OUT_JSONL))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--min-years", type=int, default=3, help="최소 연도 수")
    parser.add_argument("--write-raw", action="store_true", help="data/raw/financials_5y_*.json도 생성")
    parser.add_argument("--companies", nargs="*", default=[], help="특정 회사명/티커/corp_code만 생성")
    args = parser.parse_args()

    files = sorted(RAW_DIR.glob("dart_financials_*_CFS.json"))
    if not files:
        raise SystemExit("dart_financials raw 파일이 없습니다. fetch_dart_financials.py를 먼저 실행하세요.")

    grouped: dict[str, list[Path]] = defaultdict(list)
    for p in files:
        m = re.match(r"dart_financials_(\d{8})_(\d{4})_CFS\.json$", p.name)
        if not m:
            continue
        grouped[m.group(1)].append(p)

    corp_codes = sorted(grouped.keys())
    if args.limit > 0:
        corp_codes = corp_codes[: args.limit]

    filters_raw = [str(x).strip().lower() for x in args.companies if str(x).strip()]
    filters_norm = [norm_text(x) for x in filters_raw]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ok = 0
    skip = 0
    with out_path.open("w", encoding="utf-8") as out:
        for corp_code in corp_codes:
            by_year: dict[int, dict[str, Any]] = {}
            company = ""
            ticker = ""
            market = "OTHER"
            used_sources: list[str] = []
            target_match_checked = False
            target_matched = not filters_raw

            for p in sorted(grouped[corp_code]):
                m = re.match(r"dart_financials_(\d{8})_(\d{4})_CFS\.json$", p.name)
                if not m:
                    continue
                year = int(m.group(2))
                payload, rows = load_fin_rows(p)
                dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
                status = str(dart.get("status") or "")
                if status != "000" or not rows:
                    continue
                company = str(payload.get("company") or company or corp_code)
                ticker = str(payload.get("ticker") or ticker)
                market = str(payload.get("market") or market)
                used_sources.append(str(p))

                if filters_raw and not target_match_checked:
                    cands_raw = {str(x).lower() for x in [company, ticker, corp_code] if str(x).strip()}
                    cands_norm = {norm_text(x) for x in [company, ticker, corp_code] if str(x).strip()}
                    for fr, fn in zip(filters_raw, filters_norm):
                        if any(fr and (fr in c or c in fr) for c in cands_raw):
                            target_matched = True
                            break
                        if any(fn and (fn in c or c in fn) for c in cands_norm):
                            target_matched = True
                            break
                    target_match_checked = True

                revenue = pick_amount(rows, ["매출액", "영업수익", "수익(매출액)", "revenue", "sales"])
                op_income = pick_amount(rows, ["영업이익", "영업이익(손실)", "영업손익", "operatingincome"])
                net_income = pick_amount(rows, ["당기순이익", "당기순이익(손실)", "순이익", "netincome", "profitloss"])
                dep = pick_amount(rows, ["감가상각비", "depreciation"])
                amort = pick_amount(rows, ["무형자산상각비", "amortization"])
                ebitda = pick_amount(rows, ["ebitda"])
                if ebitda is None and op_income is not None:
                    ebitda = op_income + (dep or 0.0) + (amort or 0.0)

                rec = {
                    "year": year,
                    "revenue": revenue,
                    "operating_income": op_income,
                    "net_income": net_income,
                    "ebitda": ebitda,
                    "ebitda_margin_pct": (ebitda / revenue * 100.0) if (ebitda is not None and revenue and revenue != 0) else None,
                }
                by_year[year] = rec

            if not target_matched:
                skip += 1
                continue

            if len(by_year) < max(1, args.min_years):
                skip += 1
                continue

            years = sorted(by_year.keys(), reverse=True)[:5]
            years_sorted = sorted(years)
            rows_5y = [by_year[y] for y in years_sorted]
            for i in range(1, len(rows_5y)):
                prev = rows_5y[i - 1].get("revenue")
                cur = rows_5y[i].get("revenue")
                if prev and cur is not None:
                    rows_5y[i]["revenue_growth_pct"] = ((cur / prev) - 1.0) * 100.0
                else:
                    rows_5y[i]["revenue_growth_pct"] = None
            if rows_5y:
                rows_5y[0]["revenue_growth_pct"] = None

            latest = rows_5y[-1]
            profile = {
                "industry": "정보 부족",
                "sector": "정보 부족",
                "market_cap": None,
                "revenue": latest.get("revenue"),
                "operating_margins": (latest["operating_income"] / latest["revenue"]) if latest.get("operating_income") is not None and latest.get("revenue") else None,
            }

            payload = {
                "company": company or corp_code,
                "ticker": ticker,
                "market": market or "OTHER",
                "corp_code": corp_code,
                "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "source": "dart_financials_5y_builder",
                "years": rows_5y,
                "source_files": sorted(set(used_sources)),
            }
            out.write(json.dumps(payload, ensure_ascii=False) + "\n")
            ok += 1

            if args.write_raw:
                key = (ticker or corp_code).replace(".", "_")
                raw_out = RAW_DIR / f"financials_5y_{key}.json"
                summary = f"{payload['company']} 최근 {len(rows_5y)}개년 재무 요약입니다. 매출/영업이익/순이익/EBITDA를 포함합니다."
                raw_doc = {
                    "company": payload["company"],
                    "ticker": ticker,
                    "market": payload["market"],
                    "source": "financials_5y_compiler",
                    "title": f"{payload['company']} 5개년 재무 팩트",
                    "summary": summary,
                    "content": summary,
                    "published_at": payload["generated_at"],
                    "collected_at": payload["generated_at"],
                    "profile": profile,
                    "financials_5y": {
                        "corp_code": corp_code,
                        "years": rows_5y,
                        "source_files": payload["source_files"],
                    },
                }
                raw_out.write_text(json.dumps(raw_doc, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"saved: {out_path}")
    print(f"done. success={ok}, skip={skip}, total={len(corp_codes)}")


if __name__ == "__main__":
    main()
