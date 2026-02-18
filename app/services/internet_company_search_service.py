from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import requests

from app.config import settings

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
DUCKDUCKGO_HTML = "https://duckduckgo.com/html/"


def _strip_html(text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", text or "")
    no_entities = html.unescape(no_tags)
    return re.sub(r"\s+", " ", no_entities).strip()


def _to_iso_z(value: str | None) -> str | None:
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


class InternetCompanySearchService:
    def fetch_news(self, query: str, max_items: int = 10) -> list[dict[str, str]]:
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
        seen_url: set[str] = set()
        for it in channel.findall("item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            desc = (it.findtext("description") or "").strip()
            pub = (it.findtext("pubDate") or "").strip()
            if not link or link in seen_url:
                continue
            seen_url.add(link)

            publisher = ""
            if " - " in title:
                parts = title.rsplit(" - ", 1)
                if len(parts) == 2:
                    title, publisher = parts[0].strip(), parts[1].strip()
            items.append(
                {
                    "source_type": "news",
                    "title": title,
                    "summary": _strip_html(desc),
                    "url": link,
                    "publisher": publisher,
                    "published_at": _to_iso_z(pub) or "",
                }
            )
            if len(items) >= max_items:
                break
        return items

    def fetch_web(self, query: str, max_items: int = 10) -> list[dict[str, str]]:
        resp = requests.get(
            DUCKDUCKGO_HTML,
            params={"q": query, "kl": "kr-ko"},
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
                )
            },
            timeout=20,
        )
        resp.raise_for_status()
        text = resp.text
        blocks = re.findall(r'(<div class="result__body".*?</div>\s*</div>)', text, flags=re.S)
        if not blocks:
            blocks = re.findall(r'(<a[^>]*class="result__a"[^>]*>.*?</a>)', text, flags=re.S)

        items: list[dict[str, str]] = []
        seen_url: set[str] = set()
        for blk in blocks:
            m_href = re.search(r'class="result__a"[^>]*href="([^"]+)"', blk)
            m_title = re.search(r'class="result__a"[^>]*>(.*?)</a>', blk, flags=re.S)
            if not m_href:
                continue
            href = html.unescape(m_href.group(1))
            parsed = urlparse(href)
            q = parse_qs(parsed.query)
            final_url = ""
            if "uddg" in q and q["uddg"]:
                final_url = unquote(q["uddg"][0])
            elif href.startswith("http://") or href.startswith("https://"):
                final_url = href
            if not final_url or final_url in seen_url:
                continue
            seen_url.add(final_url)

            title = _strip_html(m_title.group(1) if m_title else "")
            m_snip = re.search(r'class="result__snippet"[^>]*>(.*?)</a?>', blk, flags=re.S)
            if not m_snip:
                m_snip = re.search(r'class="result__snippet"[^>]*>(.*?)</div>', blk, flags=re.S)
            snippet = _strip_html(m_snip.group(1) if m_snip else "")
            domain = urlparse(final_url).netloc
            items.append(
                {
                    "source_type": "web",
                    "title": title or final_url,
                    "summary": snippet,
                    "url": final_url,
                    "publisher": domain,
                    "published_at": "",
                }
            )
            if len(items) >= max_items:
                break
        return items

    @staticmethod
    def build_query(company_hint: str, prompt: str, txt_content: str) -> str:
        ch = str(company_hint or "").strip()
        if ch:
            return f"\"{ch}\" (주식 OR 실적 OR 공시 OR 뉴스)"
        seed = str(prompt or "").strip()
        if not seed:
            seed = re.sub(r"\s+", " ", str(txt_content or "")).strip()[:400]
        terms = re.findall(r"[a-zA-Z0-9가-힣]{2,}", seed)
        stop = {"그리고", "또는", "주식회사", "기업", "회사", "매출", "영업이익", "이상", "관련", "검색"}
        picked: list[str] = []
        for t in terms:
            if t in stop:
                continue
            if t in picked:
                continue
            picked.append(t)
            if len(picked) >= 5:
                break
        if not picked:
            picked = ["한국", "기업", "뉴스"]
        return " ".join(picked) + " 기업 뉴스"

    def summarize_as_overview(
        self,
        pipeline: Any,
        prompt: str,
        txt_content: str,
        company_hint: str,
        internet_items: list[dict[str, str]],
        top_k: int,
    ) -> dict[str, Any]:
        evidence_lines: list[str] = []
        for i, it in enumerate(internet_items[: max(1, min(20, top_k * 2))], start=1):
            evidence_lines.append(
                f"[{i}] 출처유형: {it.get('source_type', 'internet')}\n"
                f"제목: {it.get('title', '')}\n"
                f"매체: {it.get('publisher', '')}\n"
                f"일시: {it.get('published_at', '')}\n"
                f"요약: {it.get('summary', '')}\n"
                f"링크: {it.get('url', '')}"
            )
        evidence_text = "\n\n".join(evidence_lines) if evidence_lines else "인터넷 증거 없음"

        llm_prompt = f"""
너는 한국 M&A 분석가다. 아래 인터넷 증거(뉴스+일반 웹검색)를 활용해서 company_overview 템플릿 JSON을 작성하라.
출력은 JSON 객체 하나만 허용한다.

JSON 스키마:
{{
  "template_id": "company_overview",
  "template_name": "기업 개요 분석",
  "company_name": "string",
  "market": "KOSPI|KOSDAQ|OTHER|정보 부족",
  "summary": "string",
  "company_overview": "string",
  "business_structure": "string",
  "revenue_operating_income_5y_trend": "string",
  "ebitda": "string",
  "market_cap": "string",
  "competitors": ["string"],
  "key_risks": ["string"],
  "recent_disclosures": ["string"],
  "highlights": ["string"],
  "financial_snapshot": {{
    "market_cap": "string",
    "revenue": "string",
    "operating_income": "string",
    "net_income": "string"
  }},
  "risks": ["string"],
  "sources": ["string"],
  "similar_companies": ["string"],
  "inferred_candidates": [
    {{
      "company": "string",
      "market": "KOSPI|KOSDAQ|OTHER|정보 부족",
      "confidence": 0,
      "reason": "string"
    }}
  ],
  "selected_company": {{
    "company": "string",
    "market": "KOSPI|KOSDAQ|OTHER|정보 부족",
    "confidence": 0,
    "reason": "string"
  }}
}}

규칙:
- 확실하지 않으면 "정보 부족"으로 채운다.
- inferred_candidates는 3~5개를 우선 시도하고, 불가능하면 가능한 수만 채운다.
- company_name은 가능한 실제 업체명(고유명사)로 작성한다.
- 마스킹명(예: 00산업, OO전자)과 티커표기(예: 000000.KQ)를 company_name으로 쓰지 마라.

[업체 힌트]
{company_hint or "없음"}

[사용자 프롬프트]
{prompt or "없음"}

[첨부 TXT 요약]
{(txt_content or "없음")[:2500]}

[인터넷 증거(뉴스+웹검색)]
{evidence_text}
""".strip()

        raw = pipeline.client.generate_json(settings.ollama_chat_model, llm_prompt)
        answer = raw if isinstance(raw, dict) else {}
        if not isinstance(answer, dict):
            answer = {}
        answer["template_id"] = "company_overview"
        answer["template_name"] = "기업 개요 분석"

        canonical_hint = str(company_hint or "").strip()
        if canonical_hint:
            guessed = pipeline.infer_companies_from_text(canonical_hint, top_k=1)
            if guessed:
                canonical_hint = guessed[0]
            elif pipeline._extract_company_from_query(canonical_hint):
                canonical_hint = str(pipeline._extract_company_from_query(canonical_hint) or canonical_hint)

        answer_company = str(answer.get("company_name") or "").strip()
        if (
            not answer_company
            or pipeline._is_masked_company_name(answer_company)
            or pipeline._is_ticker_like_name(answer_company)
        ):
            answer["company_name"] = canonical_hint or "정보 부족"
        else:
            normalized = pipeline.infer_companies_from_text(answer_company, top_k=1)
            if normalized:
                answer["company_name"] = normalized[0]

        canonical_company = str(answer.get("company_name") or "").strip()
        market_hint = "정보 부족"
        if canonical_company and canonical_company != "정보 부족":
            master = pipeline._company_master_item(canonical_company)
            if isinstance(master, dict):
                markets = master.get("markets")
                if isinstance(markets, list):
                    for m in markets:
                        mm = str(m).upper().strip()
                        if mm in {"KOSPI", "KOSDAQ", "OTHER"}:
                            market_hint = mm
                            break
        if str(answer.get("market") or "").strip() in {"", "정보 부족"} and market_hint != "정보 부족":
            answer["market"] = market_hint

        urls = [str(x.get("url") or "").strip() for x in internet_items if str(x.get("url") or "").strip()]
        if not isinstance(answer.get("sources"), list) or not answer.get("sources"):
            answer["sources"] = urls[:10] or ["internet://google_news"]

        retrieved: list[dict[str, Any]] = []
        if canonical_company and canonical_company != "정보 부족":
            retrieve_query = (
                f"{canonical_company} 회사 개요, 사업부 구조, 매출/영업이익 5년 추이, "
                "EBITDA, 시가총액, 경쟁사, 핵심 리스크, 최근 주요 공시를 정리해줘."
            )
            retrieved = pipeline._retrieve_for_company_query(
                canonical_company,
                retrieve_query,
                top_k=max(8, top_k),
                allow_fallback=True,
            )
            answer = pipeline._backfill_answer_from_evidence(
                answer=answer,
                retrieved=retrieved,
                company_hint=canonical_company,
                question=retrieve_query,
            )
        answer = pipeline._sanitize_answer(answer, retrieved)
        answer_sources = answer.get("sources") if isinstance(answer.get("sources"), list) else []
        merged_sources = list(answer_sources) + urls[:10]
        dedup_sources: list[str] = []
        seen_sources: set[str] = set()
        for s in merged_sources:
            ss = str(s).strip()
            if not ss or ss in seen_sources:
                continue
            seen_sources.add(ss)
            dedup_sources.append(ss)
        answer["sources"] = dedup_sources[:15]

        inferred_rows = answer.get("inferred_candidates") if isinstance(answer.get("inferred_candidates"), list) else []
        similar_names = answer.get("similar_companies") if isinstance(answer.get("similar_companies"), list) else []
        similar: list[dict[str, Any]] = []
        for row in inferred_rows:
            if not isinstance(row, dict):
                continue
            nm = str(row.get("company") or "").strip()
            if not nm or pipeline._is_ticker_like_name(nm) or pipeline._is_masked_company_name(nm):
                continue
            confidence = int(row.get("confidence") or 0)
            confidence = max(0, min(100, confidence))
            similar.append(
                {
                    "company": nm,
                    "market": str(row.get("market") or "정보 부족"),
                    "score": round(confidence / 100.0, 4),
                    "strategic_fit_score": confidence,
                    "reason": str(row.get("reason") or "인터넷 뉴스 기반 후보"),
                    "source": urls[0] if urls else "internet://google_news",
                    "source_layer": "internet",
                    "approved": False,
                }
            )
            if len(similar) >= top_k:
                break

        if not similar:
            for name in similar_names:
                nm = str(name).strip()
                if not nm or pipeline._is_ticker_like_name(nm) or pipeline._is_masked_company_name(nm):
                    continue
                similar.append(
                    {
                        "company": nm,
                        "market": "정보 부족",
                        "score": 0.0,
                        "strategic_fit_score": 0,
                        "reason": "인터넷 뉴스 템플릿 유사기업 항목",
                        "source": urls[0] if urls else "internet://google_news",
                        "source_layer": "internet",
                        "approved": False,
                    }
                )
                if len(similar) >= top_k:
                    break

        if not similar:
            candidate = str(answer.get("company_name") or "").strip()
            if candidate and not pipeline._is_masked_company_name(candidate) and not pipeline._is_ticker_like_name(candidate):
                similar.append(
                    {
                        "company": candidate,
                        "market": str(answer.get("market") or "정보 부족"),
                        "score": 0.6,
                        "strategic_fit_score": 60,
                        "reason": str(answer.get("summary") or "인터넷 뉴스 회사 개요 템플릿 결과"),
                        "source": urls[0] if urls else "internet://google_news",
                        "source_layer": "internet",
                        "approved": False,
                    }
                )

        return {
            "answer": answer,
            "answer_text": pipeline.to_korean_readable(answer),
            "prompt_used": llm_prompt,
            "raw_response": raw if isinstance(raw, dict) else {"raw": str(raw)},
            "similar_results": similar,
            "internet_items": internet_items,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }
