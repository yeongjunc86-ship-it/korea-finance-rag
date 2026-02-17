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
)
from app.services.rag_pipeline import RagPipeline
from app.services.admin_service import DataAdminService
from app.services.auth_service import AuthService
from app.services.ai_company_search_service import AiCompanySearchService
from app.services.company_search_settings_service import CompanySearchSettingsService

router = APIRouter(prefix="/api", tags=["api"])
pipeline = RagPipeline()
admin_service = DataAdminService()
auth_service = AuthService()
ai_company_search_service = AiCompanySearchService()
company_search_settings_service = CompanySearchSettingsService()
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
    txt = str(req.txt_content or "").strip()
    if not prompt and not txt:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="prompt 또는 txt_content 중 하나는 필요합니다.")

    infer_query = prompt
    if txt:
        infer_query = f"{prompt}\n\n첨부 텍스트:\n{txt[:8000]}".strip()
    inferred_candidates: list[dict[str, Any]] = []
    inferred_company = ""

    # 1) 텍스트 내 회사명 alias 매칭 우선
    alias_hits = pipeline.infer_companies_from_text(infer_query, top_k=req.top_k)
    for nm in alias_hits:
        if pipeline._is_ticker_like_name(nm):
            continue
        inferred_company = nm
        break

    # 2) 매칭 실패 시 벡터 후보 사용 (케이스 소스/저점수 제외)
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
    if not effective_query and inferred_company:
        effective_query = (
            f"{inferred_company} 회사 개요, 사업부 구조, 매출/영업이익 5년 추이, "
            "EBITDA, 시가총액, 경쟁사, 핵심 리스크, 최근 주요 공시를 정리해줘."
        )
    if not effective_query:
        effective_query = infer_query

    local_answer, _ = pipeline.answer(effective_query, req.top_k)
    local_text = pipeline.to_korean_readable(local_answer)
    if not inferred_company and not prompt:
        local_text = (
            "업체명: 정보 부족\n"
            "[기본 템플릿: 기업 개요 분석 (company_overview)]\n"
            "회사 식별 불가: 업로드 텍스트에서 신뢰 가능한 업체명을 찾지 못했습니다."
        )
    local_similar = inferred_candidates

    settings_payload = company_search_settings_service.load()
    providers = ai_company_search_service.available_providers(settings_payload)
    provider = providers[0] if providers else ""
    ai_answer: dict[str, Any] = {}
    ai_text = "AI provider가 비활성화되었거나 API Key가 없어 결과를 생성하지 못했습니다."
    ai_error: str | None = None
    ai_prompt_used: str | None = None
    if provider:
        try:
            # Keep AI path independent from DB inferred company and enforce candidate-first reasoning.
            source_text = txt[:7000] if txt else infer_query[:7000]
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
            ai_answer = ai_company_search_service.company_overview(provider, ai_query)
        except Exception as e:
            ai_answer = {}
            ai_error = str(e)
        if isinstance(ai_answer, dict) and ai_answer:
            ai_answer.setdefault("template_id", "company_overview")
            ai_answer.setdefault("template_name", "기업 개요 분석")
            ai_answer.setdefault("sources", [f"ai://{provider}"])
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
                ai_answer["company_name"] = "정보 부족"
            ai_text = pipeline.to_korean_readable(ai_answer)
        else:
            ai_text = f"AI provider({provider}) 호출은 되었지만 유효한 템플릿 응답이 비어 있습니다."
            if ai_error:
                ai_text += f" 오류: {ai_error}"

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
    if req.task in {"full_index", "incremental_index"} and result.get("ok"):
        pipeline.reload_index()
    return result


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
    result = pipeline.register_ai_company_results(
        query=req.query,
        provider=req.provider,
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
