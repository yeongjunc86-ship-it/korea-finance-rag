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
RAW_DIR.mkdir(parents=True, exist_ok=True)

NAMED_PCT_PATTERN = re.compile(
    r"([A-Za-z0-9가-힣\(\)\.\-·&/\s]{2,40})\s*[:\-]?\s*(\d{1,2}(?:\.\d+)?)\s*%",
    re.IGNORECASE,
)
PCT_PATTERN = re.compile(r"(\d{1,2}(?:\.\d+)?)\s*%")
CUSTOMER_SIGNAL = re.compile(r"(주요\s*고객|고객\s*의존|매출처|customer\s+concentration|top\s+customer)", re.IGNORECASE)


def slug(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def to_float(v: Any) -> float | None:
    s = clean(v).replace("%", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_text(path: Path) -> tuple[str, dict[str, Any]]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return "", {}
        text = clean(payload.get("content") or payload.get("text") or payload.get("summary"))
        return text, payload
    text = clean(path.read_text(encoding="utf-8"))
    return text, {}


def infer_company(path: Path, meta: dict[str, Any]) -> str:
    company = clean(meta.get("company"))
    if company:
        return company
    stem = path.stem
    if "__" in stem:
        return clean(stem.split("__", 1)[0])
    return clean(stem)


def extract_top_customers(text: str) -> list[dict[str, Any]]:
    lines = [clean(x) for x in re.split(r"[\n\r]+", text) if clean(x)]
    out: list[dict[str, Any]] = []
    for ln in lines:
        if not CUSTOMER_SIGNAL.search(ln):
            continue
        for m in NAMED_PCT_PATTERN.finditer(ln):
            name = clean(m.group(1)).strip(" -:")
            pct = to_float(m.group(2))
            if not name or pct is None:
                continue
            out.append(
                {
                    "name": name[:60],
                    "revenue_share_pct": pct,
                    "source_type": "ir_report_text",
                    "confidence": 0.75,
                    "note": ln[:240],
                }
            )
        if not out:
            pcts = [to_float(x) for x in PCT_PATTERN.findall(ln)]
            pcts = [x for x in pcts if x is not None]
            for i, pct in enumerate(pcts[:10], start=1):
                out.append(
                    {
                        "name": f"익명고객#{i}",
                        "revenue_share_pct": pct,
                        "source_type": "ir_report_text",
                        "confidence": 0.5,
                        "note": ln[:240],
                    }
                )
    dedup: dict[str, dict[str, Any]] = {}
    for r in out:
        nm = str(r["name"])
        cur = dedup.get(nm)
        if cur is None or float(r.get("confidence") or 0) > float(cur.get("confidence") or 0):
            dedup[nm] = r
    rows = sorted(dedup.values(), key=lambda x: float(x.get("revenue_share_pct") or 0), reverse=True)
    return rows[:20]


def main() -> None:
    parser = argparse.ArgumentParser(description="Import customer concentration from local IR report texts")
    parser.add_argument("--input-dir", default="data/external/customer_reports")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    in_dir = Path(args.input_dir)
    if not in_dir.exists():
        raise SystemExit(f"input dir not found: {in_dir}")

    files = sorted([*in_dir.glob("*.txt"), *in_dir.glob("*.md"), *in_dir.glob("*.json")])
    if not files:
        raise SystemExit("report files not found (.txt/.md/.json)")

    ok = 0
    skip = 0
    for p in files:
        try:
            text, meta = load_text(p)
        except Exception as e:  # noqa: BLE001
            print(f"fail: {p} ({e})")
            continue
        if not text:
            continue
        company = infer_company(p, meta)
        top_customers = extract_top_customers(text)
        if not top_customers:
            print(f"skip (no customer signal): {p}")
            continue

        ticker = clean(meta.get("ticker")) or None
        market = clean(meta.get("market")) or "OTHER"
        key = (ticker or slug(company)).replace(".", "_")
        out = RAW_DIR / f"customer_dependency_external_{key}.json"
        if args.resume and out.exists():
            skip += 1
            print(f"skip (exists): {out}")
            continue

        top1 = float(top_customers[0]["revenue_share_pct"]) if top_customers else None
        top3 = sum(float(x["revenue_share_pct"]) for x in top_customers[:3]) if top_customers else None
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        summary = (
            f"{company} IR/보고서 기반 고객의존도 추출입니다. Top1 {top1:.1f}%."
            if top1 is not None
            else f"{company} IR/보고서 기반 고객의존도 추출입니다."
        )
        payload = {
            "company": company,
            "ticker": ticker,
            "market": market,
            "source": "external_customer_dependency_report",
            "title": f"{company} 고객의존도(IR/보고서)",
            "summary": summary,
            "content": summary,
            "published_at": clean(meta.get("published_at")) or None,
            "collected_at": now,
            "customer_dependency": {
                "coverage_status": "external_report_parse",
                "top_customers": top_customers,
                "metrics": {
                    "top1_share_pct": top1,
                    "top3_share_pct": top3,
                    "customer_count": len(top_customers),
                },
                "source_files": [str(p)],
            },
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        ok += 1
        print(f"saved: {out}")

    print(f"done. success={ok}, skip={skip}, total_files={len(files)}")


if __name__ == "__main__":
    main()

