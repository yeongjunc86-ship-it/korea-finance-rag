from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.schemas import (
    CompanySearchRequest,
    CompanySearchResponse,
    CompanySearchSettingsRequest,
    CompanySearchSettingsResponse,
    CompanyOverviewSearchRequest,
    CompanyOverviewSearchResponse,
    DeleteAiSourceRequest,
    InternetCompanySearchRequest,
    DueDiligenceAnalysisRequest,
    DueDiligenceAnalysisResponse,
    IndustryAnalysisRequest,
    IndustryAnalysisResponse,
    QueryRequest,
    QueryResponse,
    SimilarCompaniesRequest,
    SimilarCompaniesResponse,
    TargetAnalysisRequest,
    TargetAnalysisResponse,
    StrategicAnalysisRequest,
    StrategicAnalysisResponse,
    SynergyAnalysisRequest,
    SynergyAnalysisResponse,
    ValuationAnalysisRequest,
    ValuationAnalysisResponse,
    RegisterAiResultsRequest,
    RegisterInternetResultsRequest,
)
from app.services.rag_pipeline import RagPipeline
from app.services.admin_service import DataAdminService
from app.services.auth_service import AuthService
from app.services.ai_company_search_service import AiCompanySearchService
from app.services.company_search_settings_service import CompanySearchSettingsService
from app.services.internet_company_search_service import InternetCompanySearchService

router = APIRouter(prefix="/api", tags=["api"])
pipeline = RagPipeline()
admin_service = DataAdminService()
auth_service = AuthService()
ai_company_search_service = AiCompanySearchService()
company_search_settings_service = CompanySearchSettingsService()
internet_company_search_service = InternetCompanySearchService()
ROOT_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT_DIR / "data" / "raw"
LOG_DIR = ROOT_DIR / "logs"
QUERY_LOG_PATH = LOG_DIR / "query_history.jsonl"


class AdminTaskRequest(BaseModel):
    task: str = Field(..., min_length=2)
    options: dict | None = None


class LoginRequest(BaseModel):
    email: str = Field(..., min_length=3)
    password: str = Field(..., min_length=1)


def _require_admin(request: Request) -> None:
    payload = _session_payload(request)
    role = payload.get("role") if isinstance(payload, dict) else None
    if role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="관리자 권한이 필요합니다.",
        )


def _session_payload(request: Request) -> dict | None:
    token = request.cookies.get(auth_service.session_cookie_name(), "")
    if not token:
        return None
    payload = auth_service.parse_session_token(token)
    return payload if isinstance(payload, dict) else None


def _append_query_log(
    req: QueryRequest,
    answer: dict[str, Any],
    answer_text: str,
    retrieved: list[dict[str, Any]],
) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "question": req.question,
        "top_k": req.top_k,
        "answer_text": answer_text,
        "answer": answer,
        "retrieved_count": len(retrieved),
    }
    with QUERY_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _build_template_forced_query(
    template_id: str,
    base_prompt: str,
    company_hint: str = "",
    txt: str = "",
) -> str:
    tid = str(template_id or "").strip().lower()
    prompt = str(base_prompt or "").strip() or "기업 분석"
    hint = str(company_hint or "").strip()
    txt_part = str(txt or "").strip()

    guides = {
        "target_company_overview": (
            "타겟 기업 개요 분석 템플릿으로 작성하세요. "
            "기업 정의/주요 제품서비스/사업단계, 사업부 구조/매출구성/주요고객, "
            "매출/영업이익/EBITDA/성장률, 핵심 리스크/최근 공시를 포함하세요."
        ),
        "industry_market_analysis": (
            "산업 및 시장 분석 템플릿으로 작성하세요. "
            "산업 정의, 주요 제품/서비스, 산업 단계, 국내/글로벌 시장 규모, 최근 성장률, "
            "주요 경쟁사/점유율/경쟁강도, 향후 성장 전망/기회/위협을 포함하세요. "
            "가능하면 숫자와 근거를 함께 제시하세요."
        ),
        "valuation_analysis": (
            "밸류에이션 분석 템플릿으로 작성하세요. "
            "재무 요약(매출/영업이익/EBITDA/성장률), 멀티플 비교, 적정 가치 범위를 포함하세요."
        ),
        "synergy_pair_analysis": (
            "두 회사간 시너지 분석 템플릿으로 작성하세요. "
            "전략적 적합성, 매출 시너지, 비용 시너지, 종합 실현 가능성/기간을 포함하세요."
        ),
        "due_diligence_risk_analysis": (
            "리스크 및 실사 템플릿으로 작성하세요. "
            "재무/법무·규제/운영/통합 리스크를 구분해서 제시하세요."
        ),
        "strategic_decision_analysis": (
            "전략적 의사결정 템플릿으로 작성하세요. "
            "인수 필요성, 재무 타당성, 핵심 리스크 TOP3, 최종 의견(Go/Conditional Go/No-Go)을 제시하세요."
        ),
    }
    guide = guides.get(tid, "선택한 템플릿 형식에 맞춰 구조화해서 작성하세요.")
    schema_blocks = {
        "target_company_overview": (
            "출력은 JSON 객체 하나만 허용합니다.\n"
            "필수 JSON 스키마:\n"
            "{\n"
            '  "template_id": "target_company_overview",\n'
            '  "template_name": "타겟 기업 개요 분석",\n'
            '  "summary": "string",\n'
            '  "target_overview": {\n'
            '    "company_definition": "string",\n'
            '    "major_products_services": "string",\n'
            '    "business_stage": "성장기|성숙기|도입기|쇠퇴기|정보 부족"\n'
            "  },\n"
            '  "business_structure": {\n'
            '    "major_business_units": "string",\n'
            '    "revenue_mix": "string",\n'
            '    "key_customers": "string"\n'
            "  },\n"
            '  "financial_overview": {\n'
            '    "revenue": "string",\n'
            '    "operating_income": "string",\n'
            '    "ebitda": "string",\n'
            '    "recent_growth_rate": "string"\n'
            "  },\n"
            '  "risk_disclosure": {\n'
            '    "key_risks": ["string"],\n'
            '    "recent_disclosures": ["string"],\n'
            '    "watchpoints": ["string"]\n'
            "  }\n"
            "}\n"
            "규칙: 각 항목을 하나씩 작성하고, 모르는 항목만 '정보 부족' 사용\n"
        ),
        "industry_market_analysis": (
            "출력은 JSON 객체 하나만 허용합니다.\n"
            "필수 JSON 스키마:\n"
            "{\n"
            '  "template_id": "industry_market_analysis",\n'
            '  "template_name": "산업 및 시장 분석",\n'
            '  "summary": "string",\n'
            '  "industry_overview": {\n'
            '    "industry_definition": "string",\n'
            '    "major_products_services": "string",\n'
            '    "industry_stage": "성장기|성숙기|도입기|쇠퇴기|정보 부족"\n'
            "  },\n"
            '  "market_size": {\n'
            '    "domestic_market_size": "string",\n'
            '    "global_market_size": "string",\n'
            '    "recent_growth_rate": "string"\n'
            "  },\n"
            '  "competitive_environment": {\n'
            '    "key_competitors": ["string"],\n'
            '    "market_share": "string",\n'
            '    "competition_intensity": "낮음|보통|높음|정보 부족"\n'
            "  },\n"
            '  "industry_outlook": {\n'
            '    "future_growth_outlook": "string",\n'
            '    "key_opportunities": ["string"],\n'
            '    "key_threats": ["string"]\n'
            "  }\n"
            "}\n"
            "규칙:\n"
            "1) 위 스키마의 각 항목을 하나씩 작성\n"
            "2) key_competitors, key_opportunities, key_threats는 가능한 한 비우지 말 것\n"
            "3) 숫자/비율은 근거가 있을 때만 작성\n"
            "4) 모르는 항목만 '정보 부족' 사용\n"
        ),
        "valuation_analysis": (
            "출력은 JSON 객체 하나만 허용합니다.\n"
            "필수 JSON 스키마:\n"
            "{\n"
            '  "template_id": "valuation_analysis",\n'
            '  "template_name": "밸류에이션 관련",\n'
            '  "summary": "string",\n'
            '  "financial_summary": {\n'
            '    "revenue": "string",\n'
            '    "operating_income": "string",\n'
            '    "ebitda": "string",\n'
            '    "recent_growth_rate": "string"\n'
            "  },\n"
            '  "multiple_comparison": {\n'
            '    "applied_multiple": "string",\n'
            '    "peer_average": "string",\n'
            '    "target_value_range": "string"\n'
            "  },\n"
            '  "fair_value_range": {\n'
            '    "conservative_scenario": "string",\n'
            '    "base_scenario": "string",\n'
            '    "aggressive_scenario": "string"\n'
            "  }\n"
            "}\n"
            "규칙: 수치/배수는 단위를 포함하고, 근거 없으면 '정보 부족'으로 표기\n"
        ),
        "synergy_pair_analysis": (
            "출력은 JSON 객체 하나만 허용합니다.\n"
            "필수 JSON 스키마:\n"
            "{\n"
            '  "template_id": "synergy_pair_analysis",\n'
            '  "template_name": "두 회사간 시너지 분석",\n'
            '  "summary": "string",\n'
            '  "strategic_fit": {\n'
            '    "business_complementarity": "string",\n'
            '    "customer_market_overlap": "string"\n'
            "  },\n"
            '  "revenue_synergy": {\n'
            '    "new_customer_potential": "string",\n'
            '    "product_service_combination_effect": "string"\n'
            "  },\n"
            '  "cost_synergy": {\n'
            '    "organization_integration_effect": "string",\n'
            '    "purchasing_production_efficiency": "string"\n'
            "  },\n"
            '  "overall_synergy_judgment": {\n'
            '    "feasibility": "높음|보통|낮음|정보 부족",\n'
            '    "expected_realization_period": "string"\n'
            "  }\n"
            "}\n"
            "규칙: 양사 비교 관점으로 각 항목을 채우고, 제한사항이 있으면 문장으로 명시\n"
        ),
        "due_diligence_risk_analysis": (
            "출력은 JSON 객체 하나만 허용합니다.\n"
            "필수 JSON 스키마:\n"
            "{\n"
            '  "template_id": "due_diligence_risk_analysis",\n'
            '  "template_name": "리스크 및 실사",\n'
            '  "summary": "string",\n'
            '  "financial_risk": {\n'
            '    "earnings_volatility": "string",\n'
            '    "debt_level": "string"\n'
            "  },\n"
            '  "legal_regulatory_risk": {\n'
            '    "litigation_status": "string",\n'
            '    "regulatory_impact": "string"\n'
            "  },\n"
            '  "operational_risk": {\n'
            '    "key_person_dependency": "string",\n'
            '    "major_customer_dependency": "string"\n'
            "  },\n"
            '  "integration_risk": {\n'
            '    "organizational_culture_gap": "string",\n'
            '    "system_integration_difficulty": "string"\n'
            "  }\n"
            "}\n"
            "규칙: 재무/법무·규제/운영/통합 4영역을 각각 작성\n"
        ),
        "strategic_decision_analysis": (
            "출력은 JSON 객체 하나만 허용합니다.\n"
            "필수 JSON 스키마:\n"
            "{\n"
            '  "template_id": "strategic_decision_analysis",\n'
            '  "template_name": "전략적 의사결정",\n'
            '  "summary": "string",\n'
            '  "acquisition_necessity": {\n'
            '    "strategic_rationale": "string",\n'
            '    "expected_effect": "string"\n'
            "  },\n"
            '  "financial_feasibility": {\n'
            '    "return_potential": "string",\n'
            '    "risk_level": "string"\n'
            "  },\n"
            '  "key_risks_summary": {\n'
            '    "top3_key_risks": ["string"]\n'
            "  },\n"
            '  "final_opinion": {\n'
            '    "decision": "Go|Conditional Go|No-Go|정보 부족",\n'
            '    "conditions": "string"\n'
            "  }\n"
            "}\n"
            "규칙: 최종 의견은 Go/Conditional Go/No-Go 중 하나로 제시\n"
        ),
    }
    schema_block = schema_blocks.get(tid, "")

    lines = [
        f"[template:{tid}]",
        guide,
    ]
    if schema_block:
        lines.extend(["", schema_block])
    lines.extend([
        "",
        f"[사용자 프롬프트]",
        prompt,
    ])
    if hint:
        lines.extend(["", "[대상 업체]", hint])
    if txt_part:
        lines.extend(["", "[원문 텍스트]", txt_part[:4000]])
    return "\n".join(lines).strip()


@router.get("/health")
def health() -> dict:
    meta = pipeline.health_meta()
    return {"ok": True, **meta}


@router.post("/reload-index")
def reload_index(request: Request) -> dict:
    _require_admin(request)
    count = pipeline.reload_index()
    return {"ok": True, "chunk_count": count}


@router.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    answer, retrieved = pipeline.answer(req.question, req.top_k)
    answer_text = pipeline.to_korean_readable(answer)
    try:
        _append_query_log(req, answer, answer_text, retrieved)
    except OSError:
        # Logging failures should not break user-facing query responses.
        pass
    return QueryResponse(answer=answer, answer_text=answer_text, retrieved_chunks=retrieved)


@router.post("/company-search", response_model=CompanySearchResponse)
def company_search(req: CompanySearchRequest):
    query = req.prompt.strip()
    txt = (req.txt_content or "").strip()
    if txt:
        query = f"{query}\n\n첨부 텍스트:\n{txt[:6000]}"

    local_results = pipeline.similar_companies(query, top_k=req.top_k)
    settings_payload = company_search_settings_service.load()
    ai_providers_enabled = ai_company_search_service.available_providers(settings_payload)
    ai_used: list[str] = []
    ai_results: list[dict[str, Any]] = []
    notes: list[str] = []
    for provider in ai_providers_enabled:
        try:
            rows = ai_company_search_service.search(provider, query=query, top_k=req.top_k)
        except Exception:
            rows = []
        if not rows:
            notes.append(f"{provider} 결과 없음")
            continue
        ai_used.append(provider)
        for row in rows:
            company = str(row.get("company") or "").strip()
            if not company:
                continue
            strategic = int(row.get("strategic_fit_score") or 0)
            score = max(0.0, min(1.0, strategic / 100.0))
            ai_results.append(
                {
                    "company": company,
                    "market": str(row.get("market") or "정보 부족"),
                    "score": round(score, 4),
                    "strategic_fit_score": max(0, min(100, strategic)),
                    "reason": str(row.get("reason") or "정보 부족"),
                    "source": f"ai://{provider}",
                    "source_layer": "ai",
                    "approved": False,
                }
            )
    ai_results.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)

    return CompanySearchResponse(
        query=req.prompt,
        local_results=local_results,
        ai_results=ai_results[: req.top_k],
        ai_providers_enabled=ai_providers_enabled,
        ai_providers_used=ai_used,
        notes=notes,
    )


@router.post("/company-overview-search", response_model=CompanyOverviewSearchResponse)
def company_overview_search(req: CompanyOverviewSearchRequest):
    prompt = str(req.prompt or "").strip()
    template_id = str(req.template_id or "company_overview").strip().lower()
    txt = str(req.txt_content or "").strip()
    if not prompt and not txt:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="prompt 또는 txt_content 중 하나는 필요합니다.")

    infer_query = prompt
    if txt:
        infer_query = f"{prompt}\n\n첨부 텍스트:\n{txt[:8000]}".strip()
    inferred_candidates: list[dict[str, Any]] = []
    inferred_company = ""
    explicit_company = ""

    # 프롬프트에 업체명이 명시된 경우는 유추를 생략하고 해당 업체를 우선 사용한다.
    if prompt:
        explicit_hits = pipeline.infer_companies_from_text(prompt, top_k=1)
        for nm in explicit_hits:
            if pipeline._is_ticker_like_name(nm) or pipeline._is_masked_company_name(nm):
                continue
            explicit_company = nm
            break
        if not explicit_company:
            ext = pipeline._extract_company_from_query(prompt)
            if ext and (not pipeline._is_ticker_like_name(ext)) and (not pipeline._is_masked_company_name(ext)):
                explicit_company = ext
        if explicit_company:
            inferred_company = explicit_company

    # 1) 텍스트 내 회사명 alias 매칭 우선
    if not inferred_company:
        alias_hits = pipeline.infer_companies_from_text(infer_query, top_k=req.top_k)
        for nm in alias_hits:
            if pipeline._is_ticker_like_name(nm):
                continue
            inferred_company = nm
            break

    # 2) 매칭 실패 시 벡터 후보 사용 (케이스 소스/저점수 제외)
    if not explicit_company:
        raw_candidates = pipeline.similar_companies(
            infer_query,
            top_k=max(req.top_k * 2, 10),
            allowed_layers={"authoritative", "secondary"},
        )
        for row in raw_candidates:
            source = str(row.get("source") or "").lower()
            if "_case_" in source:
                continue
            if int(row.get("strategic_fit_score") or 0) < 40:
                continue
            inferred_candidates.append(row)
            if len(inferred_candidates) >= req.top_k:
                break

    if not inferred_company:
        for row in inferred_candidates:
            cand = str(row.get("company") or "").strip()
            if not cand:
                continue
            if pipeline._is_ticker_like_name(cand):
                continue
            inferred_company = cand
            break

    effective_query = prompt
    if explicit_company:
        effective_query = (
            f"{explicit_company} 회사 개요, 사업부 구조, 매출/영업이익 5년 추이, "
            "EBITDA, 시가총액, 경쟁사, 핵심 리스크, 최근 주요 공시를 정리해줘."
        )
    if not effective_query and inferred_company:
        effective_query = (
            f"{inferred_company} 회사 개요, 사업부 구조, 매출/영업이익 5년 추이, "
            "EBITDA, 시가총액, 경쟁사, 핵심 리스크, 최근 주요 공시를 정리해줘."
        )
    if not effective_query:
        effective_query = infer_query

    settings_payload = company_search_settings_service.load()
    providers = ai_company_search_service.available_providers(settings_payload) if req.include_ai else []
    provider = ""
    requested_provider = str(req.ai_provider or "").strip().lower()
    if requested_provider and requested_provider in providers:
        provider = requested_provider
    elif providers:
        provider = providers[0]

    if template_id != "company_overview":
        forced_q = _build_template_forced_query(
            template_id=template_id,
            base_prompt=effective_query,
            company_hint=inferred_company,
            txt=txt,
        )
        forced_answer, forced_retrieved = pipeline.answer(forced_q, top_k=req.top_k)
        forced_text = pipeline.to_korean_readable(forced_answer)
        forced_similar = (
            forced_answer.get("similar_companies_detail")
            if isinstance(forced_answer.get("similar_companies_detail"), list)
            else []
        )
        return CompanyOverviewSearchResponse(
            query=forced_q,
            inferred_company=inferred_company or None,
            local_answer_text=forced_text,
            ai_answer_text=forced_text,
            provider=provider,
            local_similar_results=forced_similar,
            ai_similar_results=forced_similar,
            ai_prompt_used=forced_q,
            ai_raw_response=forced_answer if isinstance(forced_answer, dict) else {"retrieved_count": len(forced_retrieved)},
        )

    # 검색 페이지는 항상 company_overview 템플릿으로 통일한다.
    company_hint = inferred_company or pipeline._extract_company_from_query(effective_query) or ""
    if company_hint:
        local_retrieved = pipeline._retrieve_for_company_query(
            company_hint, effective_query, top_k=req.top_k, allow_fallback=True
        )
    else:
        local_retrieved = pipeline.retrieve(effective_query, top_k=req.top_k)

    local_market = "정보 부족"
    if company_hint:
        cm = pipeline._company_master_item(company_hint)
        if isinstance(cm, dict):
            markets = cm.get("markets")
            if isinstance(markets, list):
                mk = next((str(x).upper() for x in markets if str(x).upper() in {"KOSPI", "KOSDAQ", "OTHER"}), "")
                if mk:
                    local_market = mk

    local_answer: dict[str, Any] = {
        "template_id": "company_overview",
        "template_name": "기업 개요 분석",
        "company_name": company_hint or "정보 부족",
        "market": local_market,
        "summary": "정보 부족",
        "company_overview": "정보 부족",
        "business_structure": "정보 부족",
        "revenue_operating_income_5y_trend": "정보 부족",
        "ebitda": "정보 부족",
        "market_cap": "정보 부족",
        "competitors": [],
        "key_risks": [],
        "recent_disclosures": [],
        "highlights": [],
        "financial_snapshot": {
            "market_cap": "정보 부족",
            "revenue": "정보 부족",
            "operating_income": "정보 부족",
            "net_income": "정보 부족",
        },
        "risks": [],
        "sources": [],
        "similar_companies": [str(r.get("company") or "").strip() for r in inferred_candidates if str(r.get("company") or "").strip()],
        "similar_companies_detail": inferred_candidates,
    }
    if local_retrieved:
        local_answer = pipeline._backfill_answer_from_evidence(
            answer=local_answer,
            retrieved=local_retrieved,
            company_hint=company_hint or None,
            question=effective_query,
        )
    local_answer = pipeline._sanitize_answer(local_answer, local_retrieved)
    local_answer["template_id"] = "company_overview"
    local_answer["template_name"] = "기업 개요 분석"
    local_text = pipeline.to_korean_readable(local_answer)
    if not inferred_company and not prompt and not local_retrieved:
        local_text = (
            "업체명: 정보 부족\n"
            "[기본 템플릿: 기업 개요 분석 (company_overview)]\n"
            "회사 식별 불가: 업로드 텍스트에서 신뢰 가능한 업체명을 찾지 못했습니다."
        )
    local_similar = inferred_candidates

    ai_answer: dict[str, Any] = {}
    ai_answer = {
        "template_id": "company_overview",
        "template_name": "기업 개요 분석",
        "company_name": inferred_company or "정보 부족",
        "market": "정보 부족",
        "summary": "AI provider가 비활성화되었거나 API Key가 없어 결과를 생성하지 못했습니다.",
        "company_overview": "정보 부족",
        "business_structure": "정보 부족",
        "revenue_operating_income_5y_trend": "정보 부족",
        "ebitda": "정보 부족",
        "market_cap": "정보 부족",
        "competitors": [],
        "key_risks": [],
        "recent_disclosures": [],
        "highlights": [],
        "financial_snapshot": {
            "market_cap": "정보 부족",
            "revenue": "정보 부족",
            "operating_income": "정보 부족",
            "net_income": "정보 부족",
        },
        "risks": [],
        "sources": [],
        "similar_companies": [],
    }
    ai_text = pipeline.to_korean_readable(ai_answer)
    ai_error: str | None = None
    ai_prompt_used: str | None = None
    source_text = txt[:7000] if txt else infer_query[:7000]
    if explicit_company:
        ai_query = (
            f"사용자 프롬프트에 업체명이 명시되어 있으므로 유추를 수행하지 마세요. 대상 업체는 '{explicit_company}' 입니다.\n"
            "아래 원문을 참고해 해당 업체 기준으로 company_overview 템플릿의 각 항목을 하나씩 반드시 작성하세요.\n"
            "출력은 JSON 객체 하나만 허용합니다.\n\n"
            "필수 JSON 스키마:\n"
            "{\n"
            '  "template_id": "company_overview",\n'
            '  "template_name": "기업 개요 분석",\n'
            '  "company_name": "string",\n'
            '  "market": "KOSPI|KOSDAQ|OTHER|정보 부족",\n'
            '  "summary": "string",\n'
            '  "company_overview": "string",\n'
            '  "business_structure": "string",\n'
            '  "revenue_operating_income_5y_trend": "string",\n'
            '  "ebitda": "string",\n'
            '  "market_cap": "string",\n'
            '  "competitors": ["string"],\n'
            '  "key_risks": ["string"],\n'
            '  "recent_disclosures": ["string"],\n'
            '  "highlights": ["string"],\n'
            '  "financial_snapshot": {\n'
            '    "market_cap": "string",\n'
            '    "revenue": "string",\n'
            '    "operating_income": "string",\n'
            '    "net_income": "string"\n'
            "  },\n"
            '  "risks": ["string"],\n'
            '  "sources": ["string"],\n'
            '  "similar_companies": ["string"],\n'
            '  "inferred_candidates": [\n'
            "    {\n"
            '      "company": "string",\n'
            '      "market": "KOSPI|KOSDAQ|OTHER|정보 부족",\n'
            '      "confidence": 100,\n'
            '      "reason": "사용자 프롬프트에 업체명 명시"\n'
            "    }\n"
            "  ],\n"
            '  "selected_company": {\n'
            f'    "company": "{explicit_company}",\n'
            '    "market": "KOSPI|KOSDAQ|OTHER|정보 부족",\n'
            '    "confidence": 100,\n'
            '    "reason": "사용자 프롬프트에 업체명 명시"\n'
            "  }\n"
            "}\n\n"
            "규칙:\n"
            "1) 후보군(inferred_candidates)은 명시 업체 1개만 작성\n"
            "2) selected_company는 반드시 명시 업체 1개만 사용\n"
            "3) company_name은 반드시 명시 업체명과 동일\n"
            "4) 각 항목을 가능한 범위에서 채우고, 정말 모를 때만 '정보 부족' 사용\n"
            "5) business_structure/competitors/key_risks/recent_disclosures/highlights/sources도 비우지 말고 가능한 값 작성\n\n"
            f"[명시 업체명]\n{explicit_company}\n\n"
            f"[사용자 프롬프트]\n{prompt or '없음'}\n\n"
            f"[원문 텍스트]\n{source_text}"
        )
    else:
        ai_query = (
            "아래 원문을 바탕으로 회사를 유추하세요.\n"
            "1) 먼저 후보 업체명을 3~5개 리스트로 만들고 confidence(0~100)와 근거를 붙이세요.\n"
            "2) 그중 가장 가능성이 높은 1개를 selected_company로 선택하세요.\n"
            "3) selected_company 기준으로 company_overview 템플릿을 채우세요.\n"
            "4) 후보 업체명은 반드시 실제 한국 상장사/비상장사 고유명사로 적으세요. (가명/마스킹 금지)\n"
            "5) 텍스트에 업체명이 직접 없어도 산업 단서(클린룸/UT배관/반도체/2차전지/식품, 매출 규모, 인력 구조)를 근거로 유추하세요.\n"
            "6) 정말 유추 불가능할 때만 company_name='정보 부족'으로 두세요.\n\n"
            f"[사용자 프롬프트]\n{prompt or '없음'}\n\n"
            f"[원문 텍스트]\n{source_text}"
        )
    ai_prompt_used = ai_query

    if provider:
        try:
            ai_answer = ai_company_search_service.company_overview(provider, ai_query)
        except Exception as e:
            ai_answer = {}
            ai_error = f"{provider}: {e}"
        if isinstance(ai_answer, dict) and ai_answer:
            ai_answer["template_id"] = "company_overview"
            ai_answer["template_name"] = "기업 개요 분석"
            ai_answer.setdefault("sources", [f"ai://{provider}"])
            if explicit_company:
                ai_answer["company_name"] = explicit_company
                ai_answer["selected_company"] = {
                    "company": explicit_company,
                    "market": str(ai_answer.get("market") or "정보 부족"),
                    "confidence": 100,
                    "reason": "사용자 프롬프트에 업체명 명시",
                }
                ai_answer["inferred_candidates"] = [
                    {
                        "company": explicit_company,
                        "market": str(ai_answer.get("market") or "정보 부족"),
                        "confidence": 100,
                        "reason": "사용자 프롬프트에 업체명 명시",
                    }
                ]
                if not isinstance(ai_answer.get("similar_companies"), list) or not ai_answer.get("similar_companies"):
                    ai_answer["similar_companies"] = [explicit_company]
            selected = ai_answer.get("selected_company") if isinstance(ai_answer.get("selected_company"), dict) else {}
            sel_company = str(selected.get("company") or "").strip()
            sel_market = str(selected.get("market") or "").strip()
            if (
                sel_company
                and not pipeline._is_ticker_like_name(sel_company)
                and not pipeline._is_masked_company_name(sel_company)
            ):
                ai_answer["company_name"] = sel_company
                if sel_market:
                    ai_answer["market"] = sel_market
            elif not str(ai_answer.get("company_name") or "").strip():
                ai_answer["company_name"] = inferred_company or "정보 부족"
            if local_retrieved:
                ai_answer = pipeline._backfill_answer_from_evidence(
                    answer=ai_answer,
                    retrieved=local_retrieved,
                    company_hint=explicit_company or company_hint or None,
                    question=effective_query,
                )
            ai_answer = pipeline._sanitize_answer(ai_answer, [])
            ai_answer["template_id"] = "company_overview"
            ai_answer["template_name"] = "기업 개요 분석"
            ai_text = pipeline.to_korean_readable(ai_answer)
        else:
            ai_answer = pipeline._sanitize_answer(
                {
                    **ai_answer,
                    "template_id": "company_overview",
                    "template_name": "기업 개요 분석",
                    "company_name": inferred_company or "정보 부족",
                    "summary": (
                        f"AI provider({provider}) 호출은 되었지만 유효한 템플릿 응답이 비어 있습니다."
                        + (f" 오류: {ai_error}" if ai_error else "")
                    ),
                    "sources": [f"ai://{provider}"],
                },
                [],
            )
            ai_text = pipeline.to_korean_readable(ai_answer)

    inferred_rows = ai_answer.get("inferred_candidates") if isinstance(ai_answer.get("inferred_candidates"), list) else []
    ai_similar_names = ai_answer.get("similar_companies") if isinstance(ai_answer.get("similar_companies"), list) else []
    ai_similar: list[dict[str, Any]] = []
    for row in inferred_rows:
        if not isinstance(row, dict):
            continue
        nm = str(row.get("company") or "").strip()
        if not nm:
            continue
        if pipeline._is_ticker_like_name(nm) or pipeline._is_masked_company_name(nm):
            continue
        confidence = int(row.get("confidence") or 0)
        confidence = max(0, min(100, confidence))
        ai_similar.append(
            {
                "company": nm,
                "market": str(row.get("market") or "정보 부족"),
                "score": round(confidence / 100.0, 4),
                "strategic_fit_score": confidence,
                "reason": str(row.get("reason") or "AI 후보군 유추"),
                "source": f"ai://{provider or 'none'}",
                "source_layer": "ai",
                "approved": False,
            }
        )
    if ai_similar:
        ai_similar = ai_similar[: req.top_k]
    else:
        # fallback to previous similar_companies behavior
        pass
    for name in ai_similar_names:
        if ai_similar:
            break
        nm = str(name).strip()
        if not nm:
            continue
        if pipeline._is_ticker_like_name(nm) or pipeline._is_masked_company_name(nm):
            continue
        ai_similar.append(
            {
                "company": nm,
                "market": "정보 부족",
                "score": 0.0,
                "strategic_fit_score": 0,
                "reason": "AI 템플릿 유사기업 항목",
                "source": f"ai://{provider or 'none'}",
                "source_layer": "ai",
                "approved": False,
            }
        )
    if not ai_similar and isinstance(ai_answer, dict):
        candidate = str(ai_answer.get("company_name") or "").strip()
        if candidate:
            ai_similar.append(
                {
                    "company": candidate,
                    "market": str(ai_answer.get("market") or "정보 부족"),
                    "score": 0.6,
                    "strategic_fit_score": 60,
                    "reason": str(ai_answer.get("summary") or "AI 회사 개요 템플릿 결과"),
                    "source": f"ai://{provider or 'none'}",
                    "source_layer": "ai",
                    "approved": False,
                }
            )

    # 검색 페이지에서 생성된 AI 결과는 즉시 벡터 DB(인덱스)에 반영해 재사용 가능하게 한다.
    if provider and ai_similar:
        try:
            pipeline.register_ai_company_results(
                query=effective_query,
                provider=provider,
                items=ai_similar,
                approved_by="system:auto_company_overview_search",
            )
        except Exception:
            # 저장 실패가 검색 응답을 깨뜨리면 안 되므로 best-effort로 처리한다.
            pass

    return CompanyOverviewSearchResponse(
        query=effective_query,
        inferred_company=inferred_company or None,
        local_answer_text=local_text,
        ai_answer_text=ai_text,
        provider=provider,
        local_similar_results=local_similar,
        ai_similar_results=ai_similar,
        ai_prompt_used=ai_prompt_used,
        ai_raw_response=ai_answer if isinstance(ai_answer, dict) else None,
    )


@router.post("/similar", response_model=SimilarCompaniesResponse)
def similar(req: SimilarCompaniesRequest):
    requested_count = max(req.top_k, pipeline._extract_requested_top_k(req.company_or_query))
    results = pipeline.similar_companies(req.company_or_query, top_k=req.top_k)
    returned_count = len(results)
    notice: str | None = None
    if returned_count < requested_count:
        notice = f"근거 부족으로 {returned_count}개만 반환"
    return SimilarCompaniesResponse(
        query=req.company_or_query,
        results=results,
        requested_count=requested_count,
        returned_count=returned_count,
        notice=notice,
    )


@router.post("/target-analysis", response_model=TargetAnalysisResponse)
def target_analysis(req: TargetAnalysisRequest):
    payload = pipeline.target_analysis(
        company_name=req.company_name,
        top_k_per_question=req.top_k_per_question,
    )
    return TargetAnalysisResponse(**payload)


@router.post("/industry-analysis", response_model=IndustryAnalysisResponse)
def industry_analysis(req: IndustryAnalysisRequest):
    payload = pipeline.industry_analysis(
        industry_name=req.industry_name,
        top_k_per_question=req.top_k_per_question,
    )
    return IndustryAnalysisResponse(**payload)


@router.post("/valuation-analysis", response_model=ValuationAnalysisResponse)
def valuation_analysis(req: ValuationAnalysisRequest):
    payload = pipeline.valuation_analysis(
        company_name=req.company_name,
        top_k_per_question=req.top_k_per_question,
    )
    return ValuationAnalysisResponse(**payload)


@router.post("/synergy-analysis", response_model=SynergyAnalysisResponse)
def synergy_analysis(req: SynergyAnalysisRequest):
    payload = pipeline.synergy_analysis(
        company_name=req.company_name,
        top_k_per_question=req.top_k_per_question,
    )
    return SynergyAnalysisResponse(**payload)


@router.post("/due-diligence-analysis", response_model=DueDiligenceAnalysisResponse)
def due_diligence_analysis(req: DueDiligenceAnalysisRequest):
    payload = pipeline.due_diligence_analysis(
        company_name=req.company_name,
        top_k_per_question=req.top_k_per_question,
    )
    return DueDiligenceAnalysisResponse(**payload)


@router.post("/strategic-analysis", response_model=StrategicAnalysisResponse)
def strategic_analysis(req: StrategicAnalysisRequest):
    payload = pipeline.strategic_analysis(
        company_name=req.company_name,
        top_k_per_question=req.top_k_per_question,
    )
    return StrategicAnalysisResponse(**payload)


@router.get("/source")
def source(path: str):
    if not path or ".." in path:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid path")

    p = Path(path)
    if not p.is_absolute():
        p = ROOT_DIR / p
    p = p.resolve()

    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="source not found")

    if RAW_DIR.resolve() not in p.parents:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="forbidden path")

    try:
        text = p.read_text(encoding="utf-8")
    except OSError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e

    return {"ok": True, "path": str(p.relative_to(ROOT_DIR)), "content": text}


@router.get("/admin/status")
def admin_status(request: Request) -> dict:
    _require_admin(request)
    return {
        "ok": True,
        "data": admin_service.status(pipeline.health_meta()),
    }


@router.post("/admin/run-task")
def admin_run_task(req: AdminTaskRequest, request: Request) -> dict:
    _require_admin(request)
    if req.task == "reload_index":
        count = pipeline.reload_index()
        return {"ok": True, "task": req.task, "chunk_count": count}

    result = admin_service.run_task(req.task, req.options or {})
    if req.task in {"full_index", "incremental_index", "fetch_disclosure_bulk"} and result.get("ok"):
        count = pipeline.reload_index()
        result["chunk_count"] = count
        result["reloaded"] = True
    return result


@router.post("/admin/disclosure/start")
def admin_disclosure_start(request: Request) -> dict:
    _require_admin(request)
    return admin_service.start_disclosure_bulk({})


@router.get("/admin/disclosure/status")
def admin_disclosure_status(request: Request) -> dict:
    _require_admin(request)
    out = admin_service.disclosure_bulk_status()
    if out.get("ok") and (not out.get("running")) and int(out.get("return_code") or -1) == 0:
        # Ensure in-memory index reflects completed disclosure collection.
        out["chunk_count"] = pipeline.reload_index()
        out["reloaded"] = True
    return out


@router.post("/admin/disclosure/stop")
def admin_disclosure_stop(request: Request) -> dict:
    _require_admin(request)
    return admin_service.stop_disclosure_bulk()


@router.post("/admin/internet-company-search")
def admin_internet_company_search(req: InternetCompanySearchRequest, request: Request) -> dict:
    _require_admin(request)
    prompt = str(req.prompt or "").strip()
    company_name = str(req.company_name or "").strip()
    template_id = str(req.template_id or "company_overview").strip().lower()
    txt = str(req.txt_content or "").strip()
    if not prompt and not company_name and not txt:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="prompt, company_name, txt_content 중 하나는 필요합니다.",
        )

    top_k = max(1, min(30, int(req.top_k or 10)))
    include_web = bool(req.include_web)
    company_hint = company_name
    if company_hint:
        canonical = pipeline.infer_companies_from_text(company_hint, top_k=1)
        if canonical:
            company_hint = canonical[0]
        else:
            extracted = pipeline._extract_company_from_query(company_hint)
            if extracted:
                company_hint = extracted
    if not company_hint:
        aliases = pipeline.infer_companies_from_text(f"{prompt}\n\n{txt}"[:9000], top_k=top_k)
        for nm in aliases:
            if pipeline._is_ticker_like_name(nm) or pipeline._is_masked_company_name(nm):
                continue
            company_hint = nm
            break

    search_query = internet_company_search_service.build_query(
        company_hint=company_hint,
        prompt=prompt,
        txt_content=txt,
    )
    news_items = internet_company_search_service.fetch_news(
        query=search_query,
        max_items=max(3, min(10, top_k)),
    )
    web_items: list[dict[str, str]] = []
    if include_web:
        try:
            web_items = internet_company_search_service.fetch_web(
                query=search_query,
                max_items=max(3, min(10, top_k)),
            )
        except Exception:
            web_items = []
    internet_items = news_items + web_items

    if template_id != "company_overview":
        base_q = prompt or company_hint or "기업 분석"
        forced_q = _build_template_forced_query(
            template_id=template_id,
            base_prompt=base_q,
            company_hint=company_hint,
            txt=txt[:3000] if txt else "",
        )
        answer, retrieved = pipeline.answer(forced_q, top_k=top_k)
        return {
            "ok": True,
            "query": forced_q,
            "inferred_company": company_hint or None,
            "internet_answer_text": pipeline.to_korean_readable(answer),
            "internet_prompt_used": forced_q,
            "internet_raw_response": answer,
            "internet_similar_results": answer.get("similar_companies_detail") if isinstance(answer.get("similar_companies_detail"), list) else [],
            "news_items": news_items,
            "web_items": web_items,
            "internet_items": internet_items,
            "retrieved_count": len(retrieved),
            "source_provider": "pipeline_forced_template",
        }

    summary = internet_company_search_service.summarize_as_overview(
        pipeline=pipeline,
        prompt=prompt,
        txt_content=txt,
        company_hint=company_hint,
        internet_items=internet_items,
        top_k=top_k,
    )
    return {
        "ok": True,
        "query": search_query,
        "inferred_company": company_hint or None,
        "internet_answer_text": summary.get("answer_text") or "결과 없음",
        "internet_prompt_used": summary.get("prompt_used") or "",
        "internet_raw_response": summary.get("raw_response") if isinstance(summary.get("raw_response"), dict) else None,
        "internet_similar_results": summary.get("similar_results") or [],
        "news_items": news_items,
        "web_items": web_items,
        "internet_items": summary.get("internet_items") or [],
        "source_provider": "google_news+web+llama3",
    }


@router.get("/admin/company-search-settings", response_model=CompanySearchSettingsResponse)
def admin_company_search_settings(request: Request):
    _require_admin(request)
    data = company_search_settings_service.load()
    return CompanySearchSettingsResponse(**data)


@router.post("/admin/company-search-settings", response_model=CompanySearchSettingsResponse)
def admin_company_search_settings_save(req: CompanySearchSettingsRequest, request: Request):
    _require_admin(request)
    data = company_search_settings_service.save(req.model_dump())
    return CompanySearchSettingsResponse(**data)


@router.post("/company-search/register-ai-results")
def register_ai_results(req: RegisterAiResultsRequest, request: Request) -> dict:
    _require_admin(request)
    payload = _session_payload(request) or {}
    approved_by = str(payload.get("uid") or payload.get("email") or "admin")
    total_added = 0
    sources: list[str] = []
    ok = False
    messages: list[str] = []

    if req.items:
        item_result = pipeline.register_ai_company_results(
            query=req.query,
            provider=req.provider,
            items=[x.model_dump() for x in req.items],
            approved_by=approved_by,
        )
        ok = ok or bool(item_result.get("ok"))
        total_added += int(item_result.get("added_chunks") or 0)
        src = str(item_result.get("source") or "").strip()
        if src:
            sources.append(src)
        msg = str(item_result.get("message") or "").strip()
        if msg:
            messages.append(msg)

    if req.answer_text or req.answer_json:
        template_result = pipeline.register_ai_template_result(
            query=req.query,
            provider=req.provider,
            template_id=str(req.template_id or "company_overview"),
            template_name=str(req.template_name or ""),
            company_name=str(req.company_name or ""),
            answer_text=str(req.answer_text or ""),
            answer_json=req.answer_json if isinstance(req.answer_json, dict) else None,
            approved_by=approved_by,
        )
        ok = ok or bool(template_result.get("ok"))
        total_added += int(template_result.get("added_chunks") or 0)
        src = str(template_result.get("source") or "").strip()
        if src:
            sources.append(src)

    if not req.items and not req.answer_text and not req.answer_json:
        return {"ok": False, "added_chunks": 0, "message": "등록할 AI 결과(items 또는 answer_text/answer_json)가 없습니다."}

    return {
        "ok": ok,
        "added_chunks": total_added,
        "source": sources[0] if sources else "",
        "sources": sources,
        "message": "; ".join(messages) if messages else "",
    }


@router.post("/company-search/register-internet-results")
def register_internet_results(req: RegisterInternetResultsRequest, request: Request) -> dict:
    _require_admin(request)
    payload = _session_payload(request) or {}
    approved_by = str(payload.get("uid") or payload.get("email") or "admin")
    result = pipeline.register_internet_company_results(
        query=req.query,
        items=[x.model_dump() for x in req.items],
        approved_by=approved_by,
    )
    return {"ok": bool(result.get("ok")), **result}


@router.post("/company-search/delete-ai-source")
def delete_ai_source(req: DeleteAiSourceRequest, request: Request) -> dict:
    _require_admin(request)
    result = pipeline.delete_ai_company_source(req.source)
    return {"ok": bool(result.get("ok")), **result}


@router.post("/auth/login")
def auth_login(req: LoginRequest, request: Request) -> dict:
    user = auth_service.authenticate(req.email, req.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="이메일 또는 비밀번호가 올바르지 않습니다.",
        )

    token = auth_service.issue_session_token(user)
    resp = JSONResponse({"ok": True, "user": {"email": user.email, "role": user.role}})
    resp.set_cookie(
        key=auth_service.session_cookie_name(),
        value=token,
        max_age=60 * 60 * 12,
        httponly=True,
        samesite="lax",
    )
    return resp


@router.post("/auth/logout")
def auth_logout(request: Request) -> dict:
    _ = request
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(auth_service.session_cookie_name())
    return resp


@router.get("/auth/me")
def auth_me(request: Request) -> dict:
    payload = _session_payload(request)
    user_id = payload.get("uid") if isinstance(payload, dict) else None
    if not user_id:
        return {"ok": False, "authenticated": False}

    user = auth_service.get_user_by_id(str(user_id))
    if not user:
        return {"ok": False, "authenticated": False}

    return {
        "ok": True,
        "authenticated": True,
        "user": {"user_id": user.user_id, "email": user.email, "role": user.role},
    }
