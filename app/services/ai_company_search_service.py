from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


class AiCompanySearchService:
    def __init__(self) -> None:
        root = Path(__file__).resolve().parents[2]
        load_dotenv(root / ".env")
        self._openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
        self._gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
        configured_model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
        # 최신 안정 모델 우선, 실패 시 순차 폴백
        self._gemini_models = []
        for m in [configured_model, "gemini-2.5-flash", "gemini-2.0-flash", "gemini-flash-latest", "gemini-1.5-flash"]:
            if m and m not in self._gemini_models:
                self._gemini_models.append(m)

    def available_providers(self, settings: dict[str, bool]) -> list[str]:
        out: list[str] = []
        if settings.get("enable_openai") and self._openai_api_key:
            out.append("openai")
        if settings.get("enable_gemini") and self._gemini_api_key:
            out.append("gemini")
        return out

    def search(self, provider: str, query: str, top_k: int) -> list[dict[str, Any]]:
        if provider == "openai":
            return self._search_openai(query, top_k)
        if provider == "gemini":
            return self._search_gemini(query, top_k)
        return []

    def company_overview(self, provider: str, query: str) -> dict[str, Any]:
        if provider == "openai":
            return self._company_overview_openai(query)
        if provider == "gemini":
            return self._company_overview_gemini(query)
        return {}

    def _json_prompt(self, query: str, top_k: int) -> str:
        return f"""
너는 한국 M&A 후보 기업 발굴 분석가다.
질문에 맞는 기업 후보를 최대 {top_k}개 제시하라.
반드시 JSON만 출력하고, 아래 스키마를 지켜라.

{{
  "results": [
    {{
      "company": "string",
      "market": "KOSPI|KOSDAQ|OTHER|정보 부족",
      "strategic_fit_score": 0,
      "reason": "string"
    }}
  ]
}}

규칙:
- 근거 없는 과장 금지
- strategic_fit_score는 0~100 정수
- reason은 한국어 한 문장

질문: {query}
""".strip()

    @staticmethod
    def _safe_parse_json(text: str) -> dict[str, Any]:
        raw = (text or "").strip()
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            start = raw.find("{")
            end = raw.rfind("}")
            if start >= 0 and end > start:
                try:
                    return json.loads(raw[start : end + 1])
                except json.JSONDecodeError:
                    return {}
            return {}

    def _search_openai(self, query: str, top_k: int) -> list[dict[str, Any]]:
        if not self._openai_api_key:
            return []
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {self._openai_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "temperature": 0.2,
                "messages": [
                    {"role": "system", "content": "너는 기업 검색 분석 보조자다. 출력은 JSON만 허용된다."},
                    {"role": "user", "content": self._json_prompt(query, top_k)},
                ],
            },
            timeout=45,
        )
        resp.raise_for_status()
        data = resp.json()
        text = ""
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            msg = choices[0].get("message") if isinstance(choices[0], dict) else None
            text = str(msg.get("content") if isinstance(msg, dict) else "")
        payload = self._safe_parse_json(text)
        rows = payload.get("results") if isinstance(payload, dict) else []
        return rows if isinstance(rows, list) else []

    def _search_gemini(self, query: str, top_k: int) -> list[dict[str, Any]]:
        if not self._gemini_api_key:
            return []
        last_error: Exception | None = None
        for model in self._gemini_models:
            endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            try:
                resp = requests.post(
                    endpoint,
                    headers={
                        "Content-Type": "application/json",
                        "x-goog-api-key": self._gemini_api_key,
                    },
                    json={
                        "contents": [{"parts": [{"text": self._json_prompt(query, top_k)}]}],
                        "generationConfig": {
                            "temperature": 0.2,
                            "response_mime_type": "application/json",
                        },
                    },
                    timeout=45,
                )
                resp.raise_for_status()
                data = resp.json()
                text = ""
                cands = data.get("candidates")
                if isinstance(cands, list) and cands:
                    content = cands[0].get("content") if isinstance(cands[0], dict) else None
                    parts = content.get("parts") if isinstance(content, dict) else None
                    if isinstance(parts, list) and parts:
                        text = str(parts[0].get("text") if isinstance(parts[0], dict) else "")
                payload = self._safe_parse_json(text)
                rows = payload.get("results") if isinstance(payload, dict) else []
                return rows if isinstance(rows, list) else []
            except requests.HTTPError as e:
                last_error = e
                status = e.response.status_code if e.response is not None else None
                if status in {404, 400}:
                    continue
                raise
            except Exception as e:
                last_error = e
                raise
        if last_error:
            raise last_error
        return []

    def _company_overview_prompt(self, query: str) -> str:
        return f"""
너는 한국 상장사 분석 어시스턴트다.
회사 개요 템플릿으로 JSON만 출력하라.
아래 질의의 핵심 대상 회사를 먼저 식별하고, 후보군을 만든 뒤 확률이 가장 높은 1개를 선택하라.
질의가 짧아도(예: '삼성전자') 회사 유추 절차를 수행하라.
확실하지 않은 수치/사실은 '정보 부족'으로 표기하라.
질의에 명시 업체명이 없더라도, 산업/고객/공정/재무 단서를 근거로 실제 후보 회사를 추정하라.

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

엄격 규칙:
- JSON 외 텍스트 금지
- inferred_candidates는 3~5개 후보를 confidence(0~100 정수) 내림차순으로 작성
- selected_company는 inferred_candidates의 1순위와 동일해야 함
- company_name은 selected_company.company와 동일해야 함
- market은 KOSPI|KOSDAQ|OTHER|정보 부족 중 하나
- similar_companies는 가능하면 3~5개
- inferred_candidates.company는 반드시 실제 업체명(고유명사)으로 작성
- 마스킹/가명(예: 00산업, OO전자, A사, B사) 금지
- 텍스트에 명시 업체명이 없어도 후보군은 최대한 제시하고, 확신이 낮으면 confidence를 낮춰 표현
- 정말 후보를 제시할 수 없는 경우에만 company_name='정보 부족'

질문: {query}
""".strip()

    def _company_overview_openai(self, query: str) -> dict[str, Any]:
        if not self._openai_api_key:
            return {}
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {self._openai_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "temperature": 0.2,
                "messages": [
                    {"role": "system", "content": "출력은 JSON 객체 하나만 허용된다."},
                    {"role": "user", "content": self._company_overview_prompt(query)},
                ],
            },
            timeout=45,
        )
        resp.raise_for_status()
        data = resp.json()
        text = ""
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            msg = choices[0].get("message") if isinstance(choices[0], dict) else None
            text = str(msg.get("content") if isinstance(msg, dict) else "")
        return self._safe_parse_json(text)

    def _company_overview_gemini(self, query: str) -> dict[str, Any]:
        if not self._gemini_api_key:
            return {}
        last_error: Exception | None = None
        for model in self._gemini_models:
            endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            try:
                resp = requests.post(
                    endpoint,
                    headers={
                        "Content-Type": "application/json",
                        "x-goog-api-key": self._gemini_api_key,
                    },
                    json={
                        "contents": [{"parts": [{"text": self._company_overview_prompt(query)}]}],
                        "generationConfig": {
                            "temperature": 0.2,
                            "response_mime_type": "application/json",
                        },
                    },
                    timeout=45,
                )
                resp.raise_for_status()
                data = resp.json()
                text = ""
                cands = data.get("candidates")
                if isinstance(cands, list) and cands:
                    content = cands[0].get("content") if isinstance(cands[0], dict) else None
                    parts = content.get("parts") if isinstance(content, dict) else None
                    if isinstance(parts, list) and parts:
                        text = str(parts[0].get("text") if isinstance(parts[0], dict) else "")
                return self._safe_parse_json(text)
            except requests.HTTPError as e:
                last_error = e
                status = e.response.status_code if e.response is not None else None
                if status in {404, 400}:
                    continue
                raise
            except Exception as e:
                last_error = e
                raise
        if last_error:
            raise last_error
        return {}
