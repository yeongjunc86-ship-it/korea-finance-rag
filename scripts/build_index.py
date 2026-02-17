#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

# Allow running as: python scripts/build_index.py
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


def normalize_record(path: Path, payload: dict) -> tuple[str, str, str]:
    company = payload.get("company") or payload.get("corp_name") or payload.get("stock_name") or path.stem
    market = payload.get("market") or payload.get("corp_cls") or "OTHER"
    # Build retrieval text from core fundamentals first, not raw candle JSON.
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

    # Keep lightweight raw tail for fallback, avoiding huge arrays dominating chunks.
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


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise SystemExit("data/raw/*.json 파일이 없습니다. 먼저 fetch 스크립트를 실행하세요.")

    client = OllamaClient(settings.ollama_base_url)
    count = 0

    file_state: dict[str, dict[str, int]] = {}
    with OUT_PATH.open("w", encoding="utf-8") as out:
        for path in files:
            payload = json.loads(path.read_text(encoding="utf-8"))
            company, market, full_text = normalize_record(path, payload)
            for i, ch in enumerate(chunk_text(full_text)):
                emb = client.embed(settings.ollama_embed_model, ch)
                row = {
                    "id": f"{path.stem}:{i}",
                    "company": company,
                    "market": market,
                    "source": str(path),
                    "text": ch,
                    "embedding": emb,
                }
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
                count += 1
            st = path.stat()
            file_state[str(path)] = {"mtime_ns": st.st_mtime_ns, "size": st.st_size}

    STATE_PATH.write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "files": file_state,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"done. chunks={count}, index={OUT_PATH}, state={STATE_PATH}")


if __name__ == "__main__":
    main()
