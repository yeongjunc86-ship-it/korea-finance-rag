#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
OUT_JSONL = PROC_DIR / "normalized_manifest.jsonl"
OUT_REPORT = PROC_DIR / "normalized_manifest_report.json"


def parse_source(path: Path) -> tuple[str, str]:
    name = path.name
    if name.startswith("yahoo_"):
        return "yahoo", name.removeprefix("yahoo_").removesuffix(".json").replace("_", ".", 1)
    if name.startswith("dart_financials_"):
        return "dart_financials", name.removeprefix("dart_financials_").removesuffix(".json")
    if name.startswith("dart_"):
        return "dart", name.removeprefix("dart_").removesuffix(".json")
    if name.startswith("news_"):
        return "news", name.removeprefix("news_").removesuffix(".json")
    if name.startswith("financials_5y_"):
        return "financials_5y", name.removeprefix("financials_5y_").removesuffix(".json")
    if name.startswith("customer_dependency_external_"):
        return "customer_dependency_external", name.removeprefix("customer_dependency_external_").removesuffix(".json")
    if name.startswith("customer_dependency_llm_"):
        return "customer_dependency_llm", name.removeprefix("customer_dependency_llm_").removesuffix(".json")
    if name.startswith("customer_dependency_"):
        return "customer_dependency", name.removeprefix("customer_dependency_").removesuffix(".json")
    if name.startswith("valuation_"):
        return "valuation", name.removeprefix("valuation_").removesuffix(".json")
    if name.startswith("tam_"):
        return "tam_sam_som", name.removeprefix("tam_").removesuffix(".json")
    if name.startswith("commodity_"):
        return "commodity", name.removeprefix("commodity_").removesuffix(".json")
    if name.startswith("valuation_case_"):
        return "valuation_case", name.removeprefix("valuation_case_").removesuffix(".json")
    if name.startswith("synergy_case_"):
        return "synergy_case", name.removeprefix("synergy_case_").removesuffix(".json")
    if name.startswith("due_diligence_case_"):
        return "due_diligence_case", name.removeprefix("due_diligence_case_").removesuffix(".json")
    if name.startswith("strategic_case_"):
        return "strategic_case", name.removeprefix("strategic_case_").removesuffix(".json")
    if name.startswith("dart_notes_"):
        return "dart_notes", name.removeprefix("dart_notes_").removesuffix(".json")
    if name.startswith("mna_"):
        return "mna", name.removeprefix("mna_").removesuffix(".json")
    if name.startswith("market_share_"):
        return "market_share", name.removeprefix("market_share_").removesuffix(".json")
    if name.startswith("patent_"):
        return "patent", name.removeprefix("patent_").removesuffix(".json")
    if name.startswith("esg_"):
        return "esg", name.removeprefix("esg_").removesuffix(".json")
    return "unknown", path.stem


def file_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def infer_market(ticker: str | None, market_field: str | None) -> str:
    if market_field and market_field != "OTHER":
        return market_field
    if ticker:
        if ticker.endswith(".KS"):
            return "KOSPI"
        if ticker.endswith(".KQ"):
            return "KOSDAQ"
    return "OTHER"


def normalize_yahoo(path: Path, payload: dict[str, Any], collected_at: str) -> dict[str, Any]:
    ticker = str(payload.get("ticker") or "")
    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
    prices = payload.get("price_history_1m") if isinstance(payload.get("price_history_1m"), list) else []
    published_at = None
    if prices:
        last = prices[-1]
        if isinstance(last, dict):
            published_at = last.get("Date")

    return {
        "doc_id": path.stem,
        "source_type": "yahoo",
        "source_name": "yahoo_finance",
        "company_name": payload.get("company") or "정보 부족",
        "ticker": ticker or None,
        "corp_code": None,
        "market": infer_market(ticker, payload.get("market")),
        "language": "en",
        "published_at": published_at,
        "collected_at": collected_at,
        "raw_path": str(path),
        "raw_sha1": file_sha1(path),
        "profile": {
            "industry": profile.get("industry"),
            "sector": profile.get("sector"),
            "market_cap": profile.get("market_cap"),
            "revenue": profile.get("revenue"),
            "operating_margins": profile.get("operating_margins"),
        },
        "status": "ok",
        "issues": [],
    }


def normalize_dart(path: Path, payload: dict[str, Any], collected_at: str) -> dict[str, Any]:
    ticker = str(payload.get("ticker") or "")
    dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
    corp_code = payload.get("corp_code") or dart.get("corp_code")

    issues: list[str] = []
    if dart.get("status") and dart.get("status") != "000":
        issues.append(f"dart_status={dart.get('status')}")

    return {
        "doc_id": path.stem,
        "source_type": "dart",
        "source_name": "opendart_company",
        "company_name": payload.get("company") or dart.get("corp_name") or "정보 부족",
        "ticker": ticker or (f"{payload.get('stock_code')}.KS" if payload.get("stock_code") else None),
        "corp_code": str(corp_code) if corp_code else None,
        "market": infer_market(ticker or None, payload.get("market")),
        "language": "ko",
        "published_at": None,
        "collected_at": collected_at,
        "raw_path": str(path),
        "raw_sha1": file_sha1(path),
        "profile": {
            "industry": dart.get("induty_code"),
            "sector": None,
            "market_cap": None,
            "revenue": None,
            "operating_margins": None,
        },
        "status": "ok" if not issues else "warn",
        "issues": issues,
    }


def normalize_news(path: Path, payload: dict[str, Any], collected_at: str) -> dict[str, Any]:
    ticker = payload.get("ticker")
    return {
        "doc_id": path.stem,
        "source_type": "news",
        "source_name": payload.get("source") or "news_feed",
        "company_name": payload.get("company") or "정보 부족",
        "ticker": ticker,
        "corp_code": payload.get("corp_code"),
        "market": infer_market(ticker, payload.get("market")),
        "language": payload.get("language") or "ko",
        "published_at": payload.get("published_at"),
        "collected_at": payload.get("collected_at") or collected_at,
        "raw_path": str(path),
        "raw_sha1": file_sha1(path),
        "profile": {
            "industry": None,
            "sector": None,
            "market_cap": None,
            "revenue": None,
            "operating_margins": None,
        },
        "status": "ok",
        "issues": [],
    }


def normalize_industry_dataset(
    path: Path,
    payload: dict[str, Any],
    collected_at: str,
    source_type: str,
) -> dict[str, Any]:
    ticker = payload.get("ticker")
    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
    return {
        "doc_id": path.stem,
        "source_type": source_type,
        "source_name": payload.get("source") or source_type,
        "company_name": payload.get("company") or payload.get("industry_name") or "정보 부족",
        "ticker": ticker,
        "corp_code": payload.get("corp_code"),
        "market": infer_market(ticker, payload.get("market")),
        "language": payload.get("language") or "ko",
        "published_at": payload.get("published_at"),
        "collected_at": payload.get("collected_at") or collected_at,
        "raw_path": str(path),
        "raw_sha1": file_sha1(path),
        "profile": {
            "industry": profile.get("industry") or payload.get("industry_name"),
            "sector": profile.get("sector"),
            "market_cap": profile.get("market_cap"),
            "revenue": profile.get("revenue"),
            "operating_margins": profile.get("operating_margins"),
        },
        "status": "ok",
        "issues": [],
    }


def normalize_one(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    source_type, _ = parse_source(path)
    collected_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat().replace("+00:00", "Z")

    if source_type == "yahoo":
        return normalize_yahoo(path, payload, collected_at)
    if source_type == "dart":
        return normalize_dart(path, payload, collected_at)
    if source_type == "news":
        return normalize_news(path, payload, collected_at)
    if source_type in {
        "valuation",
        "tam_sam_som",
        "commodity",
        "valuation_case",
        "synergy_case",
        "due_diligence_case",
        "strategic_case",
        "dart_notes",
        "mna",
        "market_share",
        "patent",
        "esg",
        "dart_financials",
        "financials_5y",
        "customer_dependency",
        "customer_dependency_external",
        "customer_dependency_llm",
    }:
        return normalize_industry_dataset(path, payload, collected_at, source_type)

    return {
        "doc_id": path.stem,
        "source_type": "unknown",
        "source_name": "unknown",
        "company_name": payload.get("company") if isinstance(payload, dict) else "정보 부족",
        "ticker": None,
        "corp_code": None,
        "market": "OTHER",
        "language": "ko",
        "published_at": None,
        "collected_at": collected_at,
        "raw_path": str(path),
        "raw_sha1": file_sha1(path),
        "profile": {
            "industry": None,
            "sector": None,
            "market_cap": None,
            "revenue": None,
            "operating_margins": None,
        },
        "status": "warn",
        "issues": ["unknown_source_type"],
    }


def build_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_source: dict[str, int] = {}
    by_status: dict[str, int] = {}
    issue_count = 0

    for r in rows:
        s = str(r.get("source_type") or "unknown")
        by_source[s] = by_source.get(s, 0) + 1

        st = str(r.get("status") or "unknown")
        by_status[st] = by_status.get(st, 0) + 1

        issues = r.get("issues") if isinstance(r.get("issues"), list) else []
        issue_count += len(issues)

    return {
        "generated_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        "total_docs": len(rows),
        "by_source": by_source,
        "by_status": by_status,
        "total_issues": issue_count,
        "output": {
            "manifest_jsonl": str(OUT_JSONL),
            "report_json": str(OUT_REPORT),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize raw documents to a common manifest format")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Raw directory")
    parser.add_argument("--out", default=str(OUT_JSONL), help="Output manifest jsonl path")
    parser.add_argument("--report", default=str(OUT_REPORT), help="Output report json path")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_path = Path(args.out)
    report_path = Path(args.report)

    if not raw_dir.exists():
        raise SystemExit(f"raw dir not found: {raw_dir}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(raw_dir.glob("*.json"))
    if not files:
        raise SystemExit("no raw json files found")

    rows: list[dict[str, Any]] = []
    with out_path.open("w", encoding="utf-8") as out:
        for p in files:
            row = normalize_one(p)
            rows.append(row)
            out.write(json.dumps(row, ensure_ascii=False) + "\n")

    report = build_report(rows)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"done. docs={report['total_docs']}, out={out_path}")
    print(f"report: {report_path}")


if __name__ == "__main__":
    main()
