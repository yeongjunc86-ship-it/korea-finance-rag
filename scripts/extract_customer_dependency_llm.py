#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
RAW_DIR.mkdir(parents=True, exist_ok=True)

OPENAI_URL = "https://api.openai.com/v1/chat/completions"
GEMINI_URL_TMPL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
GEMINI_LIST_URL = "https://generativelanguage.googleapis.com/v1beta/models"
PROMPT_VERSION = "customer_dependency_llm_v1"


def clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def norm_name(v: Any) -> str:
    s = clean(v).lower()
    s = s.replace("(주)", "").replace("주식회사", "").replace("㈜", "")
    s = re.sub(r"[^a-z0-9가-힣]+", "", s)
    return s


def to_float(v: Any) -> float | None:
    if v is None:
        return None
    s = clean(v).replace("%", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def slug(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def extract_json_block(text: str) -> dict[str, Any] | None:
    s = clean(text)
    if not s:
        return None
    for cand in [s]:
        try:
            obj = json.loads(cand)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass
    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def load_company_candidates(limit: int, company_filters: set[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    filter_raw = {clean(x).lower() for x in company_filters if clean(x)}
    filter_norm = {norm_name(x) for x in company_filters if norm_name(x)}

    def match_filter(company: str, ticker: str | None, aliases: list[str] | None = None) -> bool:
        if not filter_raw and not filter_norm:
            return True
        candidates_raw = {clean(company).lower(), clean(ticker or "").lower()}
        candidates_norm = {norm_name(company), norm_name(ticker or "")}
        if aliases:
            for a in aliases:
                candidates_raw.add(clean(a).lower())
                candidates_norm.add(norm_name(a))
        candidates_raw = {x for x in candidates_raw if x}
        candidates_norm = {x for x in candidates_norm if x}
        for f in filter_raw:
            if any(f in c or c in f for c in candidates_raw):
                return True
        for f in filter_norm:
            if any(f in c or c in f for c in candidates_norm):
                return True
        return False

    cm_path = PROC_DIR / "company_master.json"
    if cm_path.exists():
        try:
            payload = json.loads(cm_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        items = payload.get("items") if isinstance(payload, dict) else None
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                company = clean(it.get("canonical_name"))
                tickers = it.get("tickers") if isinstance(it.get("tickers"), list) else []
                ticker = clean(tickers[0]) if tickers else None
                aliases = it.get("aliases") if isinstance(it.get("aliases"), list) else []
                if not company:
                    continue
                if not match_filter(company=company, ticker=ticker, aliases=[str(x) for x in aliases]):
                    continue
                out.append(
                    {
                        "company": company,
                        "ticker": ticker,
                        "market": clean((it.get("markets") or ["OTHER"])[0] if isinstance(it.get("markets"), list) and it.get("markets") else "OTHER"),
                    }
                )
    if not out:
        for p in sorted(RAW_DIR.glob("yahoo_*.json")):
            try:
                payload = json.loads(p.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            company = clean(payload.get("company"))
            ticker = clean(payload.get("ticker")) or None
            if not company:
                continue
            if not match_filter(company=company, ticker=ticker):
                continue
            out.append({"company": company, "ticker": ticker, "market": clean(payload.get("market") or "OTHER")})
    if limit > 0:
        out = out[:limit]
    return out


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def gather_local_context(company: str, ticker: str | None, max_chars: int) -> tuple[str, list[str]]:
    refs: list[str] = []
    snippets: list[str] = []
    company_l = company.lower()
    ticker_l = clean(ticker or "").lower()
    candidates = sorted(
        [
            *RAW_DIR.glob("customer_dependency_external_*.json"),
            *RAW_DIR.glob("customer_dependency_*.json"),
            *RAW_DIR.glob("news_*.json"),
            *RAW_DIR.glob("dart_notes_*.json"),
            *RAW_DIR.glob("dart_*.json"),
        ]
    )
    for p in candidates:
        if p.name.startswith("customer_dependency_llm_"):
            # Prevent recursive self-training from prior LLM outputs.
            continue
        payload = read_json(p)
        if not payload:
            continue
        p_company = clean(payload.get("company")).lower()
        p_ticker = clean(payload.get("ticker")).lower()
        if company_l not in p_company and p_company not in company_l:
            if ticker_l and ticker_l != p_ticker:
                continue
            if not ticker_l:
                continue
        refs.append(str(p))
        text_parts: list[str] = []
        for k in ["title", "summary", "content"]:
            v = payload.get(k)
            if isinstance(v, str) and v.strip():
                text_parts.append(clean(v))
        dep = payload.get("customer_dependency")
        if isinstance(dep, dict):
            tc = dep.get("top_customers")
            if isinstance(tc, list):
                for row in tc[:10]:
                    if not isinstance(row, dict):
                        continue
                    text_parts.append(
                        f"고객={clean(row.get('name'))}, 비중={clean(row.get('revenue_share_pct'))}, 출처={clean(row.get('source_url') or row.get('source_type'))}, 근거={clean(row.get('note') or row.get('evidence'))}"
                    )
        note = payload.get("dart_notes")
        if isinstance(note, dict):
            cust = note.get("customer_dependency")
            if isinstance(cust, list):
                text_parts.extend([clean(x) for x in cust[:20]])
        joined = "\n".join([x for x in text_parts if x])
        if joined:
            snippets.append(f"[{p.name}]\n{joined}")
        if len("\n\n".join(snippets)) >= max_chars:
            break
    context = "\n\n".join(snippets)
    if len(context) > max_chars:
        context = context[:max_chars]
    return context, refs


def build_prompt(company: str, ticker: str | None, local_context: str) -> tuple[str, str]:
    system = (
        "너는 기업 고객집중도 추출기다. 반드시 JSON 객체만 출력한다. "
        "모르는 값은 null로 둔다. 출처 URL 또는 근거 문장을 최대한 포함한다."
    )
    user = f"""
회사명: {company}
티커: {ticker or "정보 없음"}

요청:
- 주요 매출 고객(top 10)과 매출 의존도(%)를 추출해라.
- 로컬 문맥이 부족하면 일반 지식/공개 정보 기반으로 추정 가능하나, 반드시 confidence를 낮게 주고 notes에 한계를 적어라.
- 고객명을 모르면 익명고객#번호로 기입 가능.

출력 스키마(JSON only):
{{
  "company": "{company}",
  "ticker": "{ticker or ""}",
  "as_of": "YYYY-MM-DD 또는 null",
  "top_customers": [
    {{
      "name": "고객명",
      "revenue_share_pct": 0~100 또는 null,
      "fiscal_year": "YYYY 또는 null",
      "source_url": "https://... 또는 null",
      "evidence": "근거 문장(짧게) 또는 null",
      "confidence": 0~1
    }}
  ],
  "notes": ["한계/가정/주의사항"]
}}

로컬 문맥:
{local_context if local_context else "없음"}
"""
    return system, user


def call_openai(model: str, system: str, user: str, timeout: int) -> str:
    api_key = clean(os.getenv("OPENAI_API_KEY"))
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }
    resp = requests.post(OPENAI_URL, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("invalid openai response: no choices")
    message = choices[0].get("message") if isinstance(choices[0], dict) else {}
    content = message.get("content") if isinstance(message, dict) else ""
    return clean(content)


def call_gemini(model: str, system: str, user: str, timeout: int) -> str:
    api_key = clean(os.getenv("GEMINI_API_KEY"))
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is required")
    model_name = model.removeprefix("models/")
    url = GEMINI_URL_TMPL.format(model=model_name)
    payload = {
        "systemInstruction": {"parts": [{"text": system}]},
        "contents": [{"role": "user", "parts": [{"text": user}]}],
        "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"},
    }
    resp = requests.post(url, params={"key": api_key}, json=payload, timeout=timeout)
    if resp.status_code == 404:
        suggestions = list_gemini_generate_models(api_key=api_key, timeout=timeout)[:8]
        sug_text = ", ".join(suggestions) if suggestions else "모델 목록 조회 실패"
        raise RuntimeError(
            f"gemini model not found or unsupported for generateContent: {model_name}. "
            f"available examples: {sug_text}"
        )
    resp.raise_for_status()
    data = resp.json()
    cands = data.get("candidates")
    if not isinstance(cands, list) or not cands:
        raise RuntimeError("invalid gemini response: no candidates")
    parts = cands[0].get("content", {}).get("parts", [])
    if not isinstance(parts, list) or not parts:
        raise RuntimeError("invalid gemini response: no parts")
    text = parts[0].get("text") if isinstance(parts[0], dict) else ""
    return clean(text)


def list_gemini_generate_models(api_key: str, timeout: int) -> list[str]:
    resp = requests.get(GEMINI_LIST_URL, params={"key": api_key, "pageSize": 1000}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    models = data.get("models")
    if not isinstance(models, list):
        return []
    out: list[str] = []
    for m in models:
        if not isinstance(m, dict):
            continue
        methods = m.get("supportedGenerationMethods")
        if not isinstance(methods, list) or "generateContent" not in methods:
            continue
        name = clean(m.get("name"))
        if not name.startswith("models/"):
            continue
        out.append(name.removeprefix("models/"))
    return sorted(set(out))


def validate_llm_output(company: str, ticker: str | None, obj: dict[str, Any]) -> dict[str, Any]:
    rows = obj.get("top_customers") if isinstance(obj.get("top_customers"), list) else []
    cleaned_rows: list[dict[str, Any]] = []
    for row in rows[:20]:
        if not isinstance(row, dict):
            continue
        name = clean(row.get("name")) or "익명고객"
        pct = to_float(row.get("revenue_share_pct"))
        if pct is not None and (pct < 0 or pct > 100):
            pct = None
        conf = to_float(row.get("confidence"))
        if conf is None:
            conf = 0.35
        conf = max(0.0, min(1.0, conf))
        url = clean(row.get("source_url")) or None
        evidence = clean(row.get("evidence")) or None
        if pct is None and not url and not evidence:
            continue
        cleaned_rows.append(
            {
                "name": name[:60],
                "revenue_share_pct": pct,
                "fiscal_year": clean(row.get("fiscal_year")) or None,
                "source_url": url,
                "evidence": evidence,
                "confidence": conf,
                "source_type": "llm_extraction",
            }
        )
    cleaned_rows.sort(
        key=lambda x: ((x.get("revenue_share_pct") is not None), float(x.get("revenue_share_pct") or 0), float(x.get("confidence") or 0)),
        reverse=True,
    )
    cleaned_rows = cleaned_rows[:10]

    top1 = next((float(x["revenue_share_pct"]) for x in cleaned_rows if isinstance(x.get("revenue_share_pct"), (int, float))), None)
    top3_vals = [float(x["revenue_share_pct"]) for x in cleaned_rows[:3] if isinstance(x.get("revenue_share_pct"), (int, float))]
    top3 = sum(top3_vals) if top3_vals else None
    url_count = sum(1 for x in cleaned_rows if x.get("source_url"))
    numeric_count = sum(1 for x in cleaned_rows if x.get("revenue_share_pct") is not None)
    verification_status = "verified_llm" if (url_count >= 2 and numeric_count >= 1) else "unverified_llm"

    notes = obj.get("notes") if isinstance(obj.get("notes"), list) else []
    notes = [clean(x) for x in notes if clean(x)]
    if verification_status != "verified_llm":
        notes.append("LLM 추출 결과는 추가 검증 전까지 참고용입니다.")

    return {
        "company": clean(obj.get("company")) or company,
        "ticker": clean(obj.get("ticker")) or ticker,
        "as_of": clean(obj.get("as_of")) or None,
        "top_customers": cleaned_rows,
        "metrics": {
            "top1_share_pct": top1,
            "top3_share_pct": top3,
            "customer_count": len(cleaned_rows),
            "url_count": url_count,
            "numeric_count": numeric_count,
        },
        "verification_status": verification_status,
        "notes": notes[:10],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract customer concentration via OpenAI/Gemini and save raw docs")
    parser.add_argument("--provider", choices=["openai", "gemini"], default="openai")
    parser.add_argument("--model", default="")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--company", action="append", default=[], help="특정 회사만 (복수 가능)")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--max-context-chars", type=int, default=10000)
    parser.add_argument("--min-confidence", type=float, default=0.3)
    parser.add_argument("--allow-empty-context", action="store_true")
    args = parser.parse_args()

    provider = args.provider
    model = args.model.strip() or ("gpt-4o-mini" if provider == "openai" else "gemini-2.5-flash")
    company_filters = {clean(x).lower() for x in args.company if clean(x)}
    companies = load_company_candidates(limit=args.limit, company_filters=company_filters)
    if not companies:
        raise SystemExit("대상 회사가 없습니다.")

    ok = 0
    skip = 0
    fail = 0
    for idx, row in enumerate(companies, start=1):
        company = clean(row.get("company"))
        ticker = clean(row.get("ticker")) or None
        market = clean(row.get("market") or "OTHER") or "OTHER"
        key = (ticker or slug(company)).replace(".", "_")
        out_path = RAW_DIR / f"customer_dependency_llm_{key}.json"
        if args.resume and out_path.exists():
            skip += 1
            continue

        local_context, refs = gather_local_context(company=company, ticker=ticker, max_chars=args.max_context_chars)
        if not local_context and not args.allow_empty_context:
            skip += 1
            continue
        system, user = build_prompt(company=company, ticker=ticker, local_context=local_context)

        try:
            if provider == "openai":
                content = call_openai(model=model, system=system, user=user, timeout=args.timeout)
            else:
                content = call_gemini(model=model, system=system, user=user, timeout=args.timeout)
            obj = extract_json_block(content)
            if not obj:
                raise RuntimeError("llm output has no valid json object")
            parsed = validate_llm_output(company=company, ticker=ticker, obj=obj)
            top_customers = [
                x for x in parsed["top_customers"] if (float(x.get("confidence") or 0) >= args.min_confidence)
            ]
            parsed["top_customers"] = top_customers[:10]
            parsed["metrics"]["customer_count"] = len(parsed["top_customers"])
            parsed["metrics"]["top1_share_pct"] = next(
                (float(x["revenue_share_pct"]) for x in parsed["top_customers"] if isinstance(x.get("revenue_share_pct"), (int, float))),
                None,
            )
            parsed["metrics"]["top3_share_pct"] = sum(
                float(x["revenue_share_pct"])
                for x in parsed["top_customers"][:3]
                if isinstance(x.get("revenue_share_pct"), (int, float))
            ) or None

            if not parsed["top_customers"]:
                skip += 1
                continue

            now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
            summary = (
                f"{company} 고객의존도 LLM 추출 결과입니다. "
                f"Top1 {parsed['metrics']['top1_share_pct']:.1f}%."
                if isinstance(parsed["metrics"]["top1_share_pct"], (int, float))
                else f"{company} 고객의존도 LLM 추출 결과입니다."
            )
            payload = {
                "company": company,
                "ticker": ticker,
                "market": market,
                "source": f"llm_customer_dependency_{provider}",
                "title": f"{company} 고객의존도(LLM 추출)",
                "summary": summary,
                "content": summary,
                "published_at": parsed.get("as_of"),
                "collected_at": now,
                "llm_meta": {
                    "provider": provider,
                    "model": model,
                    "prompt_version": PROMPT_VERSION,
                    "verification_status": parsed.get("verification_status"),
                    "used_local_sources": refs[:50],
                },
                "customer_dependency": {
                    "coverage_status": "llm_inferred",
                    "top_customers": parsed["top_customers"],
                    "metrics": parsed["metrics"],
                    "notes": parsed.get("notes") or [],
                    "source_files": refs[:50],
                },
            }
            out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            ok += 1
        except Exception as e:  # noqa: BLE001
            fail += 1
            print(f"[{idx}/{len(companies)}] fail company={company} ({e})")
            continue

    print(
        f"done. provider={provider} model={model} total={len(companies)} success={ok} skip={skip} fail={fail}"
    )


if __name__ == "__main__":
    main()
