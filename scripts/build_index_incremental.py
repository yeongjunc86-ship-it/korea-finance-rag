#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

# Allow running as: python scripts/build_index_incremental.py
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.config import settings
from app.services.ollama_client import OllamaClient

load_dotenv()

RAW_DIR = Path("data/raw")
OUT_PATH = Path("data/index/chunks.jsonl")
STATE_PATH = Path("data/index/index_state.json")


def chunk_text(text: str, chunk_size: int = 700, overlap: int = 120) -> list[str]:
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return []
    chunks: list[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(n, start + chunk_size)
        chunks.append(text[start:end])
        if end == n:
            break
        start = max(0, end - overlap)
    return chunks


def infer_source_meta(path: Path, payload: dict) -> tuple[str, str, bool]:
    explicit_layer = str(payload.get("source_layer") or "").strip().lower()
    if explicit_layer in {"authoritative", "secondary", "ai"}:
        layer = explicit_layer
    elif path.name.startswith("dart_") or path.name.startswith("financials_5y_"):
        layer = "authoritative"
    elif path.name.startswith("ai_company_search_"):
        layer = "ai"
    else:
        layer = "secondary"
    source_type = str(payload.get("source_type") or "").strip().lower() or (
        "ai_provider" if layer == "ai" else "raw_json"
    )
    approved = bool(payload.get("approved", True))
    return layer, source_type, approved


def normalize_record(path: Path, payload: dict) -> tuple[str, str, str]:
    company = payload.get("company") or payload.get("corp_name") or payload.get("stock_name") or path.stem
    market = payload.get("market") or payload.get("corp_cls") or "OTHER"

    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
    industry = profile.get("industry") or "정보 부족"
    sector = profile.get("sector") or "정보 부족"
    market_cap = profile.get("market_cap")
    revenue = profile.get("revenue")
    op_margin = profile.get("operating_margins")
    ticker = payload.get("ticker") or "정보 부족"

    lines: list[str] = [
        f"회사명: {company}",
        f"티커: {ticker}",
        f"시장: {market}",
        f"섹터: {sector}",
        f"산업: {industry}",
        f"시가총액: {market_cap if market_cap is not None else '정보 부족'}",
        f"매출: {revenue if revenue is not None else '정보 부족'}",
        f"영업이익률(추정): {op_margin if op_margin is not None else '정보 부족'}",
    ]

    financials = payload.get("financials_5y") if isinstance(payload.get("financials_5y"), dict) else {}
    years = financials.get("years") if isinstance(financials.get("years"), list) else []
    if years:
        lines.append("최근 5개년 재무 요약:")
        for y in years[:5]:
            if not isinstance(y, dict):
                continue
            year = y.get("year", "정보 부족")
            rev = y.get("revenue", "정보 부족")
            opi = y.get("operating_income", "정보 부족")
            ni = y.get("net_income", "정보 부족")
            ebt = y.get("ebitda", "정보 부족")
            mgn = y.get("ebitda_margin_pct")
            mgn_text = f"{mgn:.2f}%" if isinstance(mgn, (int, float)) else "정보 부족"
            lines.append(
                f"- {year}: 매출 {rev}, 영업이익 {opi}, 순이익 {ni}, EBITDA {ebt}, EBITDA마진 {mgn_text}"
            )

    customer_dep = payload.get("customer_dependency") if isinstance(payload.get("customer_dependency"), dict) else {}
    top_customers = customer_dep.get("top_customers") if isinstance(customer_dep.get("top_customers"), list) else []
    metrics = customer_dep.get("metrics") if isinstance(customer_dep.get("metrics"), dict) else {}
    coverage = customer_dep.get("coverage_status") if customer_dep else None
    if customer_dep:
        lines.append("주요 매출 고객/의존도 요약:")
        lines.append(f"- 커버리지: {coverage if coverage else '정보 부족'}")
        if isinstance(metrics.get("top1_share_pct"), (int, float)):
            lines.append(f"- Top1 의존도: {float(metrics['top1_share_pct']):.2f}%")
        else:
            lines.append("- Top1 의존도: 정보 부족")
        if isinstance(metrics.get("top3_share_pct"), (int, float)):
            lines.append(f"- Top3 의존도 합계: {float(metrics['top3_share_pct']):.2f}%")
        if top_customers:
            lines.append("- 상위 고객 목록:")
            for row in top_customers[:10]:
                if not isinstance(row, dict):
                    continue
                nm = row.get("name") or "익명고객"
                pct = row.get("revenue_share_pct")
                cf = row.get("confidence")
                pct_text = f"{float(pct):.2f}%" if isinstance(pct, (int, float)) else "정보 부족"
                cf_text = f"{float(cf):.2f}" if isinstance(cf, (int, float)) else "정보 부족"
                lines.append(f"  - {nm}: 매출비중 {pct_text}, 신뢰도 {cf_text}")

    price_rows = payload.get("price_history_1m")
    if isinstance(price_rows, list) and price_rows:
        latest = price_rows[-1]
        close_p = latest.get("Close")
        vol = latest.get("Volume")
        dt = latest.get("Date")
        lines.extend(
            [
                "최근 1개월 일봉 요약:",
                f"- 최근 거래일: {dt}",
                f"- 종가: {close_p if close_p is not None else '정보 부족'}",
                f"- 거래량: {vol if vol is not None else '정보 부족'}",
            ]
        )

    news_title = payload.get("title")
    news_summary = payload.get("summary") or payload.get("content")
    news_url = payload.get("url")
    news_published_at = payload.get("published_at")
    if news_title or news_summary or news_url:
        lines.extend(
            [
                "최신 뉴스 정보:",
                f"- 제목: {news_title if news_title else '정보 부족'}",
                f"- 요약: {news_summary if news_summary else '정보 부족'}",
                f"- 발행시각: {news_published_at if news_published_at else '정보 부족'}",
                f"- 기사 URL: {news_url if news_url else '정보 부족'}",
            ]
        )

    compact_payload = {
        "company": company,
        "ticker": ticker,
        "market": market,
        "profile": profile,
        "financials_5y": financials if financials else None,
        "customer_dependency": customer_dep if customer_dep else None,
        "title": news_title,
        "summary": news_summary,
        "url": news_url,
        "published_at": news_published_at,
        "source": payload.get("source", ""),
    }
    lines.append(f"원본 요약 JSON: {json.dumps(compact_payload, ensure_ascii=False)}")
    text = "\n".join(lines)
    return str(company), str(market), text


def file_fingerprint(path: Path) -> dict[str, int]:
    st = path.stat()
    return {"mtime_ns": st.st_mtime_ns, "size": st.st_size}


def load_state() -> dict[str, dict[str, int]]:
    if not STATE_PATH.exists():
        return {}
    data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    files = data.get("files")
    if not isinstance(files, dict):
        return {}
    out: dict[str, dict[str, int]] = {}
    for k, v in files.items():
        if isinstance(v, dict) and isinstance(v.get("mtime_ns"), int) and isinstance(v.get("size"), int):
            out[k] = {"mtime_ns": int(v["mtime_ns"]), "size": int(v["size"])}
    return out


def write_state(files: dict[str, dict[str, int]]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "files": files,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def detect_changes(raw_files: list[Path], prev: dict[str, dict[str, int]]) -> tuple[list[Path], set[str], dict[str, dict[str, int]]]:
    current: dict[str, dict[str, int]] = {}
    changed: list[Path] = []

    for p in raw_files:
        key = str(p)
        fp = file_fingerprint(p)
        current[key] = fp
        if key not in prev or prev[key] != fp:
            changed.append(p)

    removed = {k for k in prev.keys() if k not in current}
    return changed, removed, current


def load_existing_rows() -> list[dict]:
    if not OUT_PATH.exists():
        return []
    rows: list[dict] = []
    with OUT_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def build_rows_for_files(files: list[Path], client: OllamaClient) -> list[dict]:
    out: list[dict] = []
    for path in files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        company, market, full_text = normalize_record(path, payload)
        source_layer, source_type, approved = infer_source_meta(path, payload)
        for i, ch in enumerate(chunk_text(full_text)):
            emb = client.embed(settings.ollama_embed_model, ch)
            out.append(
                {
                    "id": f"{path.stem}:{i}",
                    "company": company,
                    "market": market,
                    "source": str(path),
                    "text": ch,
                    "embedding": emb,
                    "source_layer": source_layer,
                    "source_type": source_type,
                    "approved": approved,
                }
            )
    return out


def atomic_write_index(rows: list[dict]) -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_PATH.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp.replace(OUT_PATH)


def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental index update based on raw file state")
    parser.add_argument("--dry-run", action="store_true", help="변경 감지만 수행하고 인덱스는 수정하지 않음")
    parser.add_argument(
        "--allow-bootstrap",
        action="store_true",
        help="state/index가 없어도 전체 파일로 최초 인덱스 생성 허용",
    )
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    raw_files = sorted(RAW_DIR.glob("*.json"))
    if not raw_files:
        raise SystemExit("data/raw/*.json 파일이 없습니다. 먼저 fetch 스크립트를 실행하세요.")

    prev_state = load_state()
    changed, removed, current_state = detect_changes(raw_files, prev_state)

    print(
        f"change-detect. changed={len(changed)}, removed={len(removed)}, total_raw={len(raw_files)}, dry_run={args.dry_run}"
    )

    if args.dry_run:
        return

    if (not OUT_PATH.exists() or not prev_state) and not args.allow_bootstrap:
        raise SystemExit(
            "state/index 파일이 없습니다. 먼저 `./scripts/run_index_full.sh` 실행 후 증분 모드로 전환하세요."
        )

    if not changed and not removed:
        print("no changes. index untouched.")
        return

    existing_rows = load_existing_rows()
    target_sources = {str(p) for p in changed} | removed
    kept_rows = [r for r in existing_rows if str(r.get("source", "")) not in target_sources]

    client = OllamaClient(settings.ollama_base_url)
    new_rows = build_rows_for_files(changed, client) if changed else []

    merged = kept_rows + new_rows
    atomic_write_index(merged)
    write_state(current_state)

    print(
        "done. "
        f"old_rows={len(existing_rows)}, kept_rows={len(kept_rows)}, new_rows={len(new_rows)}, total_rows={len(merged)}"
    )


if __name__ == "__main__":
    main()
