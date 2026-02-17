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


class CompanySearchRequest(BaseModel):
    prompt: str = Field(..., min_length=2)
    top_k: int = Field(default=10, ge=1, le=30)
    txt_content: str | None = None


class CompanySearchItem(BaseModel):
    company: str
    market: str
    score: float
    strategic_fit_score: int
    reason: str
    source: str
    source_layer: str = "secondary"
    approved: bool = True


class CompanySearchResponse(BaseModel):
    query: str
    local_results: list[CompanySearchItem]
    ai_results: list[CompanySearchItem]
    ai_providers_enabled: list[str]
    ai_providers_used: list[str]
    notes: list[str] = []


class CompanySearchSettingsRequest(BaseModel):
    enable_openai: bool = True
    enable_gemini: bool = True


class CompanySearchSettingsResponse(BaseModel):
    enable_openai: bool
    enable_gemini: bool


class RegisterAiResultsRequest(BaseModel):
    query: str = Field(..., min_length=2)
    provider: str = Field(..., min_length=2)
    items: list[CompanySearchItem] = Field(default_factory=list)


class DeleteAiSourceRequest(BaseModel):
    source: str = Field(..., min_length=5)


class CompanyOverviewSearchRequest(BaseModel):
    prompt: str = Field(default="")
    top_k: int = Field(default=10, ge=1, le=30)
    txt_content: str | None = None


class CompanyOverviewSearchResponse(BaseModel):
    query: str
    inferred_company: str | None = None
    local_answer_text: str
    ai_answer_text: str
    provider: str
    local_similar_results: list[SimilarCompanyItem]
    ai_similar_results: list[SimilarCompanyItem]
    ai_prompt_used: str | None = None
    ai_raw_response: dict[str, Any] | None = None
