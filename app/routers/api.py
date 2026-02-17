from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.schemas import (
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
)
from app.services.rag_pipeline import RagPipeline
from app.services.admin_service import DataAdminService
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api", tags=["api"])
pipeline = RagPipeline()
admin_service = DataAdminService()
auth_service = AuthService()
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
