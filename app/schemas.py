from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=2)
    top_k: int | None = None


class SimilarCompaniesRequest(BaseModel):
    company_or_query: str = Field(..., min_length=2)
    top_k: int = 5


class QueryResponse(BaseModel):
    answer: dict[str, Any]
    answer_text: str
    retrieved_chunks: list[dict[str, Any]]


class SimilarCompanyItem(BaseModel):
    company: str
    market: str
    score: float
    strategic_fit_score: int
    reason: str
    source: str


class SimilarCompaniesResponse(BaseModel):
    query: str
    results: list[SimilarCompanyItem]
    requested_count: int
    returned_count: int
    notice: str | None = None


class TargetAnalysisRequest(BaseModel):
    company_name: str = Field(..., min_length=2)
    top_k_per_question: int = Field(default=6, ge=3, le=20)


class TargetAnalysisQuestionResult(BaseModel):
    question_id: int
    question: str
    readiness: str
    answer: str
    evidence_sources: list[str]


class TargetAnalysisResponse(BaseModel):
    company_name: str
    generated_at: str
    results: list[TargetAnalysisQuestionResult]


class IndustryAnalysisRequest(BaseModel):
    industry_name: str = Field(..., min_length=2)
    top_k_per_question: int = Field(default=6, ge=3, le=20)


class IndustryAnalysisQuestionResult(BaseModel):
    question_id: int
    question: str
    readiness: str
    answer: str
    evidence_sources: list[str]


class IndustryAnalysisResponse(BaseModel):
    industry_name: str
    generated_at: str
    results: list[IndustryAnalysisQuestionResult]


class ValuationAnalysisRequest(BaseModel):
    company_name: str = Field(..., min_length=2)
    top_k_per_question: int = Field(default=6, ge=3, le=20)


class ValuationAnalysisQuestionResult(BaseModel):
    question_id: int
    question: str
    readiness: str
    answer: str
    evidence_sources: list[str]


class ValuationAnalysisResponse(BaseModel):
    company_name: str
    generated_at: str
    results: list[ValuationAnalysisQuestionResult]


class SynergyAnalysisRequest(BaseModel):
    company_name: str = Field(..., min_length=2)
    top_k_per_question: int = Field(default=6, ge=3, le=20)


class SynergyAnalysisQuestionResult(BaseModel):
    question_id: int
    question: str
    readiness: str
    answer: str
    evidence_sources: list[str]


class SynergyAnalysisResponse(BaseModel):
    company_name: str
    generated_at: str
    results: list[SynergyAnalysisQuestionResult]


class DueDiligenceAnalysisRequest(BaseModel):
    company_name: str = Field(..., min_length=2)
    top_k_per_question: int = Field(default=6, ge=3, le=20)


class DueDiligenceAnalysisQuestionResult(BaseModel):
    question_id: int
    question: str
    readiness: str
    answer: str
    evidence_sources: list[str]


class DueDiligenceAnalysisResponse(BaseModel):
    company_name: str
    generated_at: str
    results: list[DueDiligenceAnalysisQuestionResult]


class StrategicAnalysisRequest(BaseModel):
    company_name: str = Field(..., min_length=2)
    top_k_per_question: int = Field(default=6, ge=3, le=20)


class StrategicAnalysisQuestionResult(BaseModel):
    question_id: int
    question: str
    readiness: str
    answer: str
    evidence_sources: list[str]


class StrategicAnalysisResponse(BaseModel):
    company_name: str
    generated_at: str
    results: list[StrategicAnalysisQuestionResult]
