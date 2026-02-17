#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import requests

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"


def strip_html(text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", text or "")
    no_entities = html.unescape(no_tags)
    return re.sub(r"\s+", " ", no_entities).strip()


def to_iso_z(value: str | None) -> str | None:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_tickers_file(path: Path) -> list[str]:
    rows = path.read_text(encoding="utf-8").splitlines()
    out: list[str] = []
    for row in rows:
        t = row.strip()
        if not t or t.startswith("#"):
            continue
        out.append(t)
    return out


def load_universe(path: Path) -> dict[str, dict[str, str]]:
    rows = read_json(path)
    out: dict[str, dict[str, str]] = {}
    if not isinstance(rows, list):
        return out
    for r in rows:
        if not isinstance(r, dict):
            continue
        ticker = str(r.get("yahoo_ticker") or "").strip()
        if not ticker:
            continue
        out[ticker] = {
            "name": str(r.get("name") or ticker).strip(),
            "market": str(r.get("market") or "OTHER").strip() or "OTHER",
        }
    return out


def fetch_rss(query: str) -> list[dict[str, str]]:
    params = {
        "q": query,
        "hl": "ko",
        "gl": "KR",
        "ceid": "KR:ko",
    }
    resp = requests.get(GOOGLE_NEWS_RSS, params=params, timeout=20)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    channel = root.find("channel")
    if channel is None:
        return []

    items: list[dict[str, str]] = []
    for it in channel.findall("item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        desc = (it.findtext("description") or "").strip()
        pub = (it.findtext("pubDate") or "").strip()

        if not link:
            continue

        publisher = ""
        if " - " in title:
            parts = title.rsplit(" - ", 1)
            if len(parts) == 2:
                title, publisher = parts[0].strip(), parts[1].strip()

        items.append(
            {
                "title": title,
                "url": link,
                "description": strip_html(desc),
                "published_at": to_iso_z(pub) or "",
                "publisher": publisher,
            }
        )
    return items


def news_id(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:20]


def save_news(payload: dict[str, Any]) -> Path:
    nid = payload.get("news_id") or news_id(str(payload.get("url") or ""))
    out = RAW_DIR / f"news_{nid}.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def dedup(items: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for it in items:
        url = it.get("url", "")
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(it)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe-file", default="data/processed/korea_universe.json")
    parser.add_argument("--tickers-file", default="")
    parser.add_argument("--limit-company", type=int, default=100, help="상위 N개 기업만 뉴스 수집")
    parser.add_argument("--per-company", type=int, default=3, help="기업당 최대 뉴스 저장 개수")
    parser.add_argument("--sleep", type=float, default=0.2, help="요청 간 대기(초)")
    parser.add_argument("--resume", action="store_true", help="기존 news 파일 존재 시 건너뛰기")
    parser.add_argument("--query-suffix", default="주식 OR 실적 OR 공시")
    parser.add_argument("--companies", nargs="*", default=[], help="특정 회사명/티커만 수집")
    args = parser.parse_args()

    universe = load_universe(Path(args.universe_file))
    if not universe:
        raise SystemExit("유니버스 파일이 비어있거나 형식이 올바르지 않습니다.")

    tickers: list[str]
    if args.tickers_file:
        tickers = read_tickers_file(Path(args.tickers_file))
    else:
        tickers = sorted(universe.keys())

    if args.companies:
        qraw = [str(x).strip().lower() for x in args.companies if str(x).strip()]
        qnorm = [re.sub(r"[^a-z0-9가-힣]+", "", x) for x in qraw]
        filtered: list[str] = []
        for t in tickers:
            meta = universe.get(t) or {"name": t}
            name_raw = str(meta.get("name") or "").strip().lower()
            t_raw = str(t).strip().lower()
            name_norm = re.sub(r"[^a-z0-9가-힣]+", "", name_raw)
            t_norm = re.sub(r"[^a-z0-9가-힣]+", "", t_raw)
            matched = False
            for r, n in zip(qraw, qnorm):
                if (r and (r in name_raw or r in t_raw or name_raw in r or t_raw in r)) or (
                    n and (n in name_norm or n in t_norm or name_norm in n or t_norm in n)
                ):
                    matched = True
                    break
            if matched:
                filtered.append(t)
        tickers = filtered

    if args.limit_company > 0:
        tickers = tickers[: args.limit_company]

    total_company = len(tickers)
    if total_company == 0:
        raise SystemExit("뉴스 수집 대상 티커가 없습니다.")

    ok = 0
    skip = 0
    fail = 0

    for i, ticker in enumerate(tickers, start=1):
        meta = universe.get(ticker) or {"name": ticker, "market": "OTHER"}
        company = meta.get("name") or ticker
        market = meta.get("market") or "OTHER"
        query = f'"{company}" {args.query_suffix}'.strip()

        try:
            items = dedup(fetch_rss(query))
            if args.per_company > 0:
                items = items[: args.per_company]

            if not items:
                print(f"[{i}/{total_company}] no-news: {ticker} ({company})")
                time.sleep(max(0.0, args.sleep))
                continue

            saved_count = 0
            for it in items:
                nid = news_id(it["url"])
                out = RAW_DIR / f"news_{nid}.json"
                if args.resume and out.exists():
                    skip += 1
                    continue

                payload = {
                    "news_id": nid,
                    "company": company,
                    "ticker": ticker,
                    "market": market,
                    "source": "google_news_rss",
                    "language": "ko",
                    "title": it.get("title", ""),
                    "summary": it.get("description", ""),
                    "content": it.get("description", ""),
                    "url": it.get("url", ""),
                    "publisher": it.get("publisher", ""),
                    "published_at": it.get("published_at") or None,
                    "collected_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    "query": query,
                }
                save_news(payload)
                ok += 1
                saved_count += 1

            print(f"[{i}/{total_company}] saved={saved_count}: {ticker} ({company})")
        except Exception as e:  # noqa: BLE001
            fail += 1
            print(f"[{i}/{total_company}] fail: {ticker} ({company}) ({e})")

        time.sleep(max(0.0, args.sleep))

    print(f"done. success={ok}, skip={skip}, fail={fail}, companies={total_company}")


if __name__ == "__main__":
    main()
