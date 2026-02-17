#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
OUT_PATH = PROC_DIR / "company_master.json"


def norm_name(name: str) -> str:
    x = unicodedata.normalize("NFKC", str(name or "")).strip().lower()
    if not x:
        return ""
    x = x.replace("(주)", "").replace("주식회사", "").replace("㈜", "")
    x = re.sub(r"[^a-z0-9가-힣]+", "", x)
    return x


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def is_mfg_from_induty(ind: str) -> bool | None:
    ind = str(ind or "").strip()
    if not ind or not ind[:2].isdigit():
        return None
    sec = int(ind[:2])
    return 10 <= sec <= 34


def is_mfg_from_yahoo(industry: str, sector: str) -> bool | None:
    text = f"{industry} {sector}".strip().lower()
    if not text:
        return None
    if any(
        k in text
        for k in [
            "insurance",
            "bank",
            "financial",
            "asset management",
            "brokerage",
            "real estate",
            "construction",
            "engineering",
            "software",
            "internet",
            "telecom",
            "media",
            "retail",
            "services",
            "transportation",
            "utilities",
        ]
    ):
        return False
    if any(
        k in text
        for k in [
            "manufact",
            "industrial",
            "electronic",
            "semiconductor",
            "machinery",
            "automotive",
            "chemical",
            "food",
            "steel",
            "metals",
            "aerospace",
            "components",
        ]
    ):
        return True
    return None


def pick_text(values: list[str]) -> str | None:
    vals = [v for v in values if str(v).strip()]
    if not vals:
        return None
    cnt: dict[str, int] = defaultdict(int)
    for v in vals:
        cnt[v] += 1
    return sorted(cnt.items(), key=lambda x: x[1], reverse=True)[0][0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build canonical company master metadata")
    parser.add_argument("--out", default=str(OUT_PATH))
    args = parser.parse_args()

    raw_dir = RAW_DIR
    if not raw_dir.exists():
        raise SystemExit(f"raw dir not found: {raw_dir}")

    data: dict[str, dict[str, Any]] = {}
    by_ticker_key: dict[str, str] = {}
    by_corp_code: dict[str, str] = {}

    def ensure(key: str, company_name: str | None = None) -> dict[str, Any]:
        row = data.get(key)
        if row is None:
            row = {
                "canonical_name": company_name or "정보 부족",
                "aliases": set(),
                "tickers": set(),
                "corp_codes": set(),
                "markets": set(),
                "industry_codes": [],
                "industries": [],
                "sectors": [],
                "is_manufacturing_votes": [],
                "segment_note_count": 0,
                "customer_note_count": 0,
                "capex_note_count": 0,
                "debt_note_count": 0,
            }
            data[key] = row
        elif company_name and row["canonical_name"] == "정보 부족":
            row["canonical_name"] = company_name
        return row

    # 1) DART
    for p in sorted(raw_dir.glob("dart_*.json")):
        payload = load_json(p)
        if not payload:
            continue
        company = str(payload.get("company") or "").strip()
        dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
        corp_code = str(payload.get("corp_code") or dart.get("corp_code") or "").strip()
        ticker = str(payload.get("ticker") or "").strip()
        market = str(payload.get("market") or "OTHER").strip() or "OTHER"
        induty_code = str(dart.get("induty_code") or "").strip()

        key = norm_name(company) if company else ""
        if not key and corp_code and corp_code in by_corp_code:
            key = by_corp_code[corp_code]
        if not key and ticker and ticker in by_ticker_key:
            key = by_ticker_key[ticker]
        if not key:
            key = corp_code or ticker or p.stem

        row = ensure(key, company_name=company or None)
        if company:
            row["aliases"].add(company)
        if corp_code:
            row["corp_codes"].add(corp_code)
            by_corp_code[corp_code] = key
        if ticker:
            row["tickers"].add(ticker)
            by_ticker_key[ticker] = key
        row["markets"].add(market)
        if induty_code:
            row["industry_codes"].append(induty_code)
            mfg = is_mfg_from_induty(induty_code)
            if mfg is not None:
                row["is_manufacturing_votes"].append(mfg)

    # 2) Yahoo
    for p in sorted(raw_dir.glob("yahoo_*.json")):
        payload = load_json(p)
        if not payload:
            continue
        company = str(payload.get("company") or "").strip()
        ticker = str(payload.get("ticker") or "").strip()
        market = str(payload.get("market") or "OTHER").strip() or "OTHER"
        profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
        industry = str(profile.get("industry") or "").strip()
        sector = str(profile.get("sector") or "").strip()

        key = ""
        if ticker and ticker in by_ticker_key:
            key = by_ticker_key[ticker]
        if not key:
            key = norm_name(company) if company else ""
        if not key:
            key = ticker or p.stem
        row = ensure(key, company_name=company or None)
        if company:
            row["aliases"].add(company)
        if ticker:
            row["tickers"].add(ticker)
            by_ticker_key[ticker] = key
        row["markets"].add(market)
        if industry:
            row["industries"].append(industry)
        if sector:
            row["sectors"].append(sector)
        mfg = is_mfg_from_yahoo(industry, sector)
        if mfg is not None:
            row["is_manufacturing_votes"].append(mfg)

    # 3) DART notes -> section signals
    for p in sorted(raw_dir.glob("dart_notes_*.json")):
        payload = load_json(p)
        if not payload:
            continue
        company = str(payload.get("company") or "").strip()
        corp_code = str(payload.get("corp_code") or "").strip()
        ticker = str(payload.get("ticker") or "").strip()
        notes = payload.get("dart_notes") if isinstance(payload.get("dart_notes"), dict) else {}

        key = ""
        if corp_code and corp_code in by_corp_code:
            key = by_corp_code[corp_code]
        if not key and ticker and ticker in by_ticker_key:
            key = by_ticker_key[ticker]
        if not key:
            key = norm_name(company)
        if not key:
            continue

        row = ensure(key, company_name=company or None)
        if company:
            row["aliases"].add(company)
        if corp_code:
            row["corp_codes"].add(corp_code)
            by_corp_code[corp_code] = key
        if ticker:
            row["tickers"].add(ticker)
            by_ticker_key[ticker] = key
        row["segment_note_count"] += len(notes.get("business_segments") or [])
        row["customer_note_count"] += len(notes.get("customer_dependency") or [])
        row["capex_note_count"] += len(notes.get("capex_investment") or [])
        row["debt_note_count"] += len(notes.get("debt_maturity") or [])

    # 4) customer dependency profile docs
    for p in sorted(raw_dir.glob("customer_dependency_*.json")):
        payload = load_json(p)
        if not payload:
            continue
        company = str(payload.get("company") or "").strip()
        ticker = str(payload.get("ticker") or "").strip()
        market = str(payload.get("market") or "").strip()
        dep = payload.get("customer_dependency") if isinstance(payload.get("customer_dependency"), dict) else {}
        top_customers = dep.get("top_customers") if isinstance(dep.get("top_customers"), list) else []

        key = ""
        if ticker and ticker in by_ticker_key:
            key = by_ticker_key[ticker]
        if not key:
            key = norm_name(company)
        if not key:
            continue

        row = ensure(key, company_name=company or None)
        if company:
            row["aliases"].add(company)
        if ticker:
            row["tickers"].add(ticker)
            by_ticker_key[ticker] = key
        if market:
            row["markets"].add(market)
        row["customer_note_count"] += len(top_customers)

    items: list[dict[str, Any]] = []
    alias_index: dict[str, dict[str, Any]] = {}
    ticker_index: dict[str, dict[str, Any]] = {}
    for key, row in data.items():
        votes = row["is_manufacturing_votes"]
        is_mfg: bool | None
        if any(v is True for v in votes):
            is_mfg = True
        elif any(v is False for v in votes):
            is_mfg = False
        else:
            is_mfg = None

        canonical_name = str(row["canonical_name"]).strip() or "정보 부족"
        industry_code = pick_text([str(x) for x in row["industry_codes"] if str(x).strip()])
        industry = pick_text([str(x) for x in row["industries"] if str(x).strip()])
        sector = pick_text([str(x) for x in row["sectors"] if str(x).strip()])
        aliases = sorted([str(x).strip() for x in row["aliases"] if str(x).strip()])
        if canonical_name not in aliases and canonical_name != "정보 부족":
            aliases.insert(0, canonical_name)

        item = {
            "company_key": key,
            "canonical_name": canonical_name,
            "aliases": aliases,
            "tickers": sorted([str(x) for x in row["tickers"] if str(x).strip()]),
            "corp_codes": sorted([str(x) for x in row["corp_codes"] if str(x).strip()]),
            "markets": sorted([str(x) for x in row["markets"] if str(x).strip()]),
            "industry_code": industry_code,
            "industry": industry,
            "sector": sector,
            "is_manufacturing": is_mfg,
            "section_signals": {
                "business_segments": int(row["segment_note_count"]),
                "customer_dependency": int(row["customer_note_count"]),
                "capex_investment": int(row["capex_note_count"]),
                "debt_maturity": int(row["debt_note_count"]),
            },
        }
        items.append(item)

        for a in aliases:
            ak = norm_name(a)
            if ak:
                alias_index[ak] = item
        for t in item["tickers"]:
            ticker_index[str(t)] = item

    payload = {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "total_companies": len(items),
        "items": sorted(items, key=lambda x: (x["canonical_name"], x["company_key"])),
        "alias_index": alias_index,
        "ticker_index": ticker_index,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"saved: {out_path}")
    print(f"done. total_companies={len(items)}")


if __name__ == "__main__":
    main()
