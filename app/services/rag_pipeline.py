from __future__ import annotations

import json
import math
import re
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.config import settings
from app.services.ollama_client import OllamaClient


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


class RagPipeline:
    ENTRY_TEMPLATE_COMPANY_OVERVIEW = "company_overview"
    ENTRY_TEMPLATE_NAME_COMPANY_OVERVIEW = "기업 개요 분석"
    ENTRY_TEMPLATE_TARGET_OVERVIEW = "target_company_overview"
    ENTRY_TEMPLATE_NAME_TARGET_OVERVIEW = "타겟 기업 개요 분석"
    ENTRY_TEMPLATE_INDUSTRY_MARKET = "industry_market_analysis"
    ENTRY_TEMPLATE_NAME_INDUSTRY_MARKET = "산업 및 시장 분석"
    ENTRY_TEMPLATE_VALUATION = "valuation_analysis"
    ENTRY_TEMPLATE_NAME_VALUATION = "밸류에이션 관련"
    ENTRY_TEMPLATE_SYNERGY_PAIR = "synergy_pair_analysis"
    ENTRY_TEMPLATE_NAME_SYNERGY_PAIR = "두 회사간 시너지 분석"
    ENTRY_TEMPLATE_DUE_DILIGENCE_RISK = "due_diligence_risk_analysis"
    ENTRY_TEMPLATE_NAME_DUE_DILIGENCE_RISK = "리스크 및 실사"
    ENTRY_TEMPLATE_STRATEGIC_DECISION = "strategic_decision_analysis"
    ENTRY_TEMPLATE_NAME_STRATEGIC_DECISION = "전략적 의사결정"
    ENTRY_TEMPLATE_ACQ_FEASIBILITY = "acquisition_feasibility"
    ENTRY_TEMPLATE_NAME_ACQ_FEASIBILITY = "인수 타당성 분석"
    ENTRY_TEMPLATE_PEER_LIST = "peer_list"
    ENTRY_TEMPLATE_NAME_PEER_LIST = "유사 업체 리스트"
    ENTRY_TEMPLATE_COMPARABLE_DEALS = "comparable_deals"
    ENTRY_TEMPLATE_NAME_COMPARABLE_DEALS = "유사 거래 분석"
    TARGET_ANALYSIS_QUESTIONS: list[tuple[int, str]] = [
        (1, "{company}의 최근 5년 매출 성장률과 EBITDA 마진 추이를 정리해줘."),
        (2, "{company}의 주요 매출 고객 상위 10개와 매출 의존도를 알려줘."),
        (3, "{company}의 사업부별 매출 비중과 수익성을 비교해줘."),
        (4, "{company}의 최근 3년 CAPEX 규모와 투자 방향성을 요약해줘."),
        (5, "{company}의 현금흐름 구조에서 가장 취약한 부분을 설명해줘."),
        (6, "{company}의 부채 만기 구조와 리파이낸싱 리스크를 정리해줘."),
        (7, "{company}의 주요 경쟁사와 시장 점유율 비교표를 작성해줘."),
        (8, "{company}의 핵심 기술 또는 특허를 요약해줘."),
        (9, "{company}의 최근 소송/분쟁 이력을 요약해줘."),
        (10, "{company}의 ESG 리스크 요인을 정리해줘."),
    ]
    INDUSTRY_ANALYSIS_QUESTIONS: list[tuple[int, str]] = [
        (11, "{industry} 산업의 최근 5년 CAGR과 향후 5년 전망을 정리해줘."),
        (12, "{industry} 산업 내 진입장벽을 정리해줘."),
        (13, "{industry} 산업의 현재 밸류에이션 멀티플 평균을 요약해줘."),
        (14, "{industry} 산업의 최근 M&A 사례를 정리해줘."),
        (15, "{industry} 산업에서 규제 변화가 미치는 영향을 요약해줘."),
        (16, "{industry} 시장의 TAM/SAM/SOM 추정치를 설명해줘."),
        (17, "{industry} 산업의 해외 주요 플레이어와 국내 플레이어를 비교해줘."),
        (18, "{industry} 산업의 기술 트렌드 변화를 요약해줘."),
        (19, "{industry} 산업에서 원자재 가격 변동이 수익성에 미치는 영향을 설명해줘."),
        (20, "{industry} 산업의 경기 침체 시 방어력을 평가해줘."),
    ]
    VALUATION_ANALYSIS_QUESTIONS: list[tuple[int, str]] = [
        (21, "{company}의 적정 EV/EBITDA 멀티플 범위를 제시해줘."),
        (22, "{company}의 최근 유사 거래 사례 기반 밸류에이션 비교표를 정리해줘."),
        (23, "{company} DCF 기준 WACC 산정에 필요한 요소를 정리해줘."),
        (24, "{company}의 보수적/중립/공격적 시나리오별 기업가치를 계산해줘."),
        (25, "{company} 인수 프리미엄 20% 적용 시 IRR을 추정해줘."),
        (26, "{company} 시너지 반영 전/후 기업가치를 비교해줘."),
        (27, "{company} 동종 업계 평균 PER 대비 할인/할증 요인을 분석해줘."),
        (28, "{company} 환율 변동이 기업가치에 미치는 영향을 설명해줘."),
        (29, "{company} LBO 구조 적용 시 예상 레버리지 한도를 추정해줘."),
        (30, "{company} EBITDA 조정 항목을 검토해줘."),
    ]
    SYNERGY_ANALYSIS_QUESTIONS: list[tuple[int, str]] = [
        (31, "{company} 매출 시너지 가능 항목을 정리해줘."),
        (32, "{company} 비용 시너지 가능 항목을 정리해줘."),
        (33, "{company} 인력 중복 구조를 분석해줘."),
        (34, "{company} 유통망 통합 시 비용 절감 효과를 정리해줘."),
        (35, "{company} IT 시스템 통합 비용을 추정해줘."),
        (36, "{company} 시너지 실현까지 걸리는 예상 기간을 제시해줘."),
        (37, "{company} Cross-selling 가능성을 분석해줘."),
        (38, "{company} 브랜드 통합 리스크를 정리해줘."),
        (39, "{company} 조달 단가 통합 시 효과를 정리해줘."),
        (40, "{company} 중복 법인/법무 구조를 정리해줘."),
    ]
    DUE_DILIGENCE_ANALYSIS_QUESTIONS: list[tuple[int, str]] = [
        (41, "{company} 재무 실사 시 집중 점검해야 할 항목을 정리해줘."),
        (42, "{company} 우발채무 가능성을 정리해줘."),
        (43, "{company} 매출 인식 방식의 위험 요소를 분석해줘."),
        (44, "{company} 재고 평가 방식의 문제 가능성을 분석해줘."),
        (45, "{company} 세무 리스크를 요약해줘."),
        (46, "{company} 핵심 인력 이탈 리스크를 분석해줘."),
        (47, "{company} 주요 계약의 Change of Control 조항을 정리해줘."),
        (48, "{company} 개인정보·보안 관련 리스크를 정리해줘."),
        (49, "{company} 공급망 의존 리스크를 정리해줘."),
        (50, "{company} PMI 실패 사례와 주요 원인을 정리해줘."),
    ]
    STRATEGIC_ANALYSIS_QUESTIONS: list[tuple[int, str]] = [
        (51, "{company} 인수가 전략적 인수인지 재무적 투자에 가까운지 판단해줘."),
        (52, "{company} 우리 회사 포트폴리오와의 전략적 적합성 점수를 제시해줘."),
        (53, "{company} 인수 후 3년 내 Exit 전략을 제시해줘."),
        (54, "{company} 인수하지 않을 경우 기회비용을 정리해줘."),
        (55, "{company} 경쟁사가 인수할 경우 우리에게 미치는 영향을 분석해줘."),
        (56, "{company} 단계적 지분 인수 구조 가능성을 검토해줘."),
        (57, "{company} Earn-out 구조 설계안을 제시해줘."),
        (58, "{company} 합병 vs 자회사 편입 중 무엇이 유리한지 비교해줘."),
        (59, "{company} 현금 인수 vs 주식 교환 인수를 비교해줘."),
        (60, "{company} 최적 딜 구조를 제안해줘."),
    ]

    def __init__(self) -> None:
        self.client = OllamaClient(settings.ollama_base_url)
        self._index_path = Path(settings.index_path)
        self._state_path = self._index_path.parent / "index_state.json"
        self._index_mtime: float | None = None
        self._chunks: list[dict[str, Any]] = []
        self._source_meta_cache: dict[str, dict[str, Any]] = {}
        self._company_master_index: dict[str, dict[str, Any]] | None = None
        self._company_manufacturing_cache: dict[str, bool | None] | None = None
        self.reload_index()

    @staticmethod
    def _load_index(path: str) -> list[dict[str, Any]]:
        p = Path(path)
        if not p.exists():
            return []
        rows: list[dict[str, Any]] = []
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows

    def reload_index(self) -> int:
        self._chunks = self._load_index(str(self._index_path))
        if self._index_path.exists():
            self._index_mtime = self._index_path.stat().st_mtime
        else:
            self._index_mtime = None
        return len(self._chunks)

    def _ensure_fresh_index(self) -> None:
        if not self._index_path.exists():
            if self._chunks:
                self._chunks = []
                self._index_mtime = None
            return
        current_mtime = self._index_path.stat().st_mtime
        if self._index_mtime is None or current_mtime > self._index_mtime:
            self.reload_index()

    def has_index(self) -> bool:
        self._ensure_fresh_index()
        return len(self._chunks) > 0

    def chunk_count(self) -> int:
        self._ensure_fresh_index()
        return len(self._chunks)

    @staticmethod
    def _to_iso_z(ts: float) -> str:
        return datetime.fromtimestamp(ts, tz=UTC).isoformat().replace("+00:00", "Z")

    def health_meta(self) -> dict[str, Any]:
        self._ensure_fresh_index()

        index_exists = self._index_path.exists()
        index_size = self._index_path.stat().st_size if index_exists else 0
        index_updated_at = self._to_iso_z(self._index_path.stat().st_mtime) if index_exists else None

        index_version: int | None = None
        indexed_doc_count = 0
        state_updated_at: str | None = None
        state_exists = self._state_path.exists()
        if state_exists:
            try:
                payload = json.loads(self._state_path.read_text(encoding="utf-8"))
                raw_version = payload.get("version")
                if isinstance(raw_version, int):
                    index_version = raw_version
                files = payload.get("files")
                if isinstance(files, dict):
                    indexed_doc_count = len(files)
                raw_state_updated = payload.get("updated_at")
                if isinstance(raw_state_updated, str) and raw_state_updated.strip():
                    state_updated_at = raw_state_updated
            except (json.JSONDecodeError, OSError):
                pass

        return {
            "index_loaded": len(self._chunks) > 0,
            "chunk_count": len(self._chunks),
            "index_version": index_version,
            "indexed_doc_count": indexed_doc_count,
            "index_path": str(self._index_path),
            "index_exists": index_exists,
            "index_size_bytes": index_size,
            "index_updated_at": index_updated_at,
            "state_path": str(self._state_path),
            "state_exists": state_exists,
            "state_updated_at": state_updated_at,
        }

    def retrieve(self, query: str, top_k: int | None = None) -> list[dict[str, Any]]:
        self._ensure_fresh_index()
        if not self._chunks:
            return []
        k = top_k or settings.top_k
        q_emb = self.client.embed(settings.ollama_embed_model, query)

        scored: list[tuple[float, dict[str, Any]]] = []
        for row in self._chunks:
            sim = cosine_similarity(q_emb, row["embedding"])
            scored.append((sim, row))

        scored.sort(key=lambda x: x[0], reverse=True)

        out: list[dict[str, Any]] = []
        for score, row in scored[:k]:
            out.append(
                {
                    "score": round(score, 4),
                    "company": row.get("company"),
                    "market": row.get("market"),
                    "source": row.get("source"),
                    "text": row.get("text", "")[:600],
                }
            )
        return out

    def answer(self, question: str, top_k: int | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        k = top_k or settings.top_k
        template_id, template_name = self._classify_entry_template(question)
        requested = max(5, self._extract_requested_top_k(question))
        similar_rows = self.similar_companies(question, top_k=requested)
        similar_names = [str(r.get("company") or "").strip() for r in similar_rows if str(r.get("company") or "").strip()]
        sources = self._dedup_sources_from_rows(similar_rows)
        global_company_hint = self._extract_company_from_query(question) or (similar_names[0] if similar_names else "")
        global_market_hint = "정보 부족"
        if global_company_hint:
            cm = self._company_master_item(global_company_hint)
            if isinstance(cm, dict):
                markets = cm.get("markets")
                if isinstance(markets, list):
                    mk = next((str(x).upper() for x in markets if str(x).upper() in {"KOSPI", "KOSDAQ", "OTHER"}), "")
                    if mk:
                        global_market_hint = mk

        def _base_answer(summary: str, company_name: str = "정보 부족", market: str = "정보 부족") -> dict[str, Any]:
            cname = company_name if company_name != "정보 부족" else (global_company_hint or "정보 부족")
            mkt = market if market != "정보 부족" else (global_market_hint if cname != "정보 부족" else "정보 부족")
            return {
                "template_id": template_id,
                "template_name": template_name,
                "company_name": cname,
                "market": mkt,
                "summary": summary,
                "company_overview": summary,
                "business_structure": "정보 부족",
                "revenue_operating_income_5y_trend": "정보 부족",
                "ebitda": "정보 부족",
                "market_cap": "정보 부족",
                "competitors": similar_names[:5],
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
                "sources": sources,
                "similar_companies": similar_names,
                "similar_companies_detail": similar_rows,
            }

        def _merge_rows(primary: list[dict[str, Any]], secondary: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
            merged: list[dict[str, Any]] = []
            seen: set[tuple[str, str, str]] = set()
            for row in list(primary) + list(secondary):
                if not isinstance(row, dict):
                    continue
                key = (
                    str(row.get("source") or ""),
                    str(row.get("company") or ""),
                    str(row.get("text") or ""),
                )
                if key in seen:
                    continue
                seen.add(key)
                merged.append(row)
                if len(merged) >= limit:
                    break
            return merged

        def _company_support_rows(company_name: str, limit: int = 6) -> list[dict[str, Any]]:
            if not company_name or company_name == "정보 부족":
                return []
            aliases = self._company_alias_candidates(company_name)
            support_q = (
                f"{company_name} 회사 개요, 사업부 구조, 매출/영업이익 5년 추이, "
                "EBITDA, 시가총액, 경쟁사, 핵심 리스크, 최근 주요 공시"
            )
            semantic_rows = self._retrieve_for_company_query(
                company_name,
                support_q,
                top_k=max(8, limit * 2),
                allow_fallback=True,
            )

            # 보완 검색: AI 등록 벡터가 회사명으로 저장된 경우 템플릿별 편차 없이 함께 반영한다.
            ai_rows: list[dict[str, Any]] = []
            if aliases:
                for row in self._chunks:
                    if not bool(row.get("approved", True)):
                        continue
                    if self._source_layer_of_row(row) != "ai":
                        continue
                    rc = self._normalize_company_name(str(row.get("company") or ""))
                    rt = self._normalize_company_name(str(row.get("text") or ""))
                    if not any((a in rc) or (rc and rc in a) or (a in rt) for a in aliases):
                        continue
                    ai_rows.append(
                        {
                            "score": 0.0,
                            "company": row.get("company"),
                            "market": row.get("market"),
                            "source": row.get("source"),
                            "text": str(row.get("text") or "")[:600],
                        }
                    )
                    if len(ai_rows) >= max(2, limit // 2):
                        break

            return _merge_rows(semantic_rows, ai_rows, limit=max(4, limit))

        if template_id == self.ENTRY_TEMPLATE_TARGET_OVERVIEW:
            company_hint = self._extract_company_from_query(question)
            if not company_hint and similar_rows:
                company_hint = str(similar_rows[0].get("company") or "").strip() or None
            if company_hint:
                retrieved = self._retrieve_for_company_query(company_hint, question, top_k=k, allow_fallback=False)
            else:
                retrieved = self.retrieve(question, top_k=k)
            retrieved = _merge_rows(retrieved, _company_support_rows(company_hint or "", limit=max(4, k // 2)), limit=max(k, 8))
            if not retrieved:
                msg = "질문에 필요한 근거를 찾지 못했습니다. 먼저 데이터를 수집하고 인덱스를 생성해 주세요."
                if company_hint:
                    msg = (
                        f"질문에서 감지한 대상 회사({company_hint})의 근거를 찾지 못했습니다. "
                        "회사명 표기(예: 삼성전자/삼성전자(주)/005930)를 확인하거나 인덱스를 갱신해 주세요."
                    )
                return _base_answer(msg, company_name=company_hint or "정보 부족"), []
            detail = self._target_overview_simple_template(company_hint or "정보 부족", question, retrieved)
            answer = _base_answer(str(detail.get("summary") or "정보 부족"), company_name=company_hint or "정보 부족")
            answer.update(detail)
            answer["sources"] = self._dedup_sources(retrieved)
            answer["similar_companies"] = similar_names
            answer["similar_companies_detail"] = similar_rows
            if company_hint:
                answer["company_name"] = company_hint
                cm = self._company_master_item(company_hint)
                if isinstance(cm, dict):
                    markets = cm.get("markets")
                    if isinstance(markets, list):
                        mk = next((str(x).upper() for x in markets if str(x).upper() in {"KOSPI", "KOSDAQ", "OTHER"}), "")
                        if mk:
                            answer["market"] = mk
            answer = self._backfill_answer_from_evidence(
                answer=answer,
                retrieved=retrieved,
                company_hint=company_hint,
                question=question,
            )
            return answer, retrieved

        if template_id == self.ENTRY_TEMPLATE_INDUSTRY_MARKET:
            industry_name = self._extract_industry_from_query(question) or "정보 부족"
            retrieved = self._retrieve_for_industry_query(industry_name if industry_name != "정보 부족" else question, question, top_k=k)
            company_hint = self._extract_company_from_query(question) or (similar_names[0] if similar_names else "")
            retrieved = _merge_rows(retrieved, _company_support_rows(company_hint, limit=max(4, k // 2)), limit=max(k, 8))
            if not retrieved:
                return _base_answer("산업/시장 분석 근거가 부족합니다. 인덱스를 갱신하거나 산업명을 더 구체화해 주세요."), []
            detail = self._industry_market_simple_template(industry_name, question, retrieved)
            company_hint = company_hint or "정보 부족"
            answer = _base_answer(str(detail.get("summary") or "정보 부족"), company_name=company_hint)
            if company_hint != "정보 부족":
                cm = self._company_master_item(company_hint)
                if isinstance(cm, dict):
                    markets = cm.get("markets")
                    if isinstance(markets, list):
                        mk = next((str(x).upper() for x in markets if str(x).upper() in {"KOSPI", "KOSDAQ", "OTHER"}), "")
                        if mk:
                            answer["market"] = mk
            answer["industry_name"] = industry_name
            answer["highlights"] = [f"분석 대상 산업: {industry_name}"]
            answer.update(detail)
            answer["sources"] = self._dedup_sources(retrieved)
            return answer, retrieved

        if template_id == self.ENTRY_TEMPLATE_VALUATION:
            company_name = self._extract_company_from_query(question) or (similar_names[0] if similar_names else "정보 부족")
            retrieved = (
                self._retrieve_for_company_query(company_name, question, top_k=k) if company_name != "정보 부족" else self.retrieve(question, top_k=k)
            )
            retrieved = _merge_rows(retrieved, _company_support_rows(company_name, limit=max(4, k // 2)), limit=max(k, 8))
            if not retrieved:
                return _base_answer("밸류에이션 분석 근거가 부족합니다. 대상 회사를 명시하거나 인덱스를 갱신해 주세요.", company_name=company_name), []
            detail = self._valuation_simple_template(company_name, question, retrieved)
            answer = _base_answer(str(detail.get("summary") or "정보 부족"), company_name=company_name)
            answer.update(detail)
            answer = self._backfill_answer_from_evidence(
                answer=answer,
                retrieved=retrieved,
                company_hint=company_name if company_name != "정보 부족" else None,
                question=question,
            )
            answer["sources"] = self._dedup_sources(retrieved)
            return answer, retrieved

        if template_id == self.ENTRY_TEMPLATE_SYNERGY_PAIR:
            companies = self.infer_companies_from_text(question, top_k=2)
            if len(companies) < 2:
                one = self._extract_company_from_query(question)
                if one and one not in companies:
                    companies.append(one)
            companies = companies[:2]
            company_label = " + ".join(companies) if companies else "정보 부족"
            if companies:
                merged: list[dict[str, Any]] = []
                seen: set[tuple[str, str, str]] = set()
                for nm in companies:
                    rows = self._retrieve_for_company_query(nm, question, top_k=max(3, k), allow_fallback=True)
                    for r in rows:
                        key = (str(r.get("source") or ""), str(r.get("company") or ""), str(r.get("text") or ""))
                        if key in seen:
                            continue
                        seen.add(key)
                        merged.append(r)
                        if len(merged) >= max(k, 6):
                            break
                    if len(merged) >= max(k, 6):
                        break
                retrieved = merged[: max(k, 6)]
            else:
                retrieved = self.retrieve(question, top_k=max(k, 6))
            if not retrieved:
                return _base_answer("두 회사 시너지 분석 근거가 부족합니다. 회사명을 명확히 입력해 주세요.", company_name=company_label), []
            detail = self._synergy_simple_template(company_label, question, retrieved)
            answer = _base_answer(str(detail.get("summary") or "정보 부족"), company_name=company_label)
            answer["highlights"] = [f"시너지 분석 대상: {company_label}"]
            answer.update(detail)
            return answer, retrieved

        if template_id == self.ENTRY_TEMPLATE_DUE_DILIGENCE_RISK:
            company_name = self._extract_company_from_query(question) or (similar_names[0] if similar_names else "정보 부족")
            retrieved = (
                self._retrieve_for_company_query(company_name, question, top_k=k) if company_name != "정보 부족" else self.retrieve(question, top_k=k)
            )
            retrieved = _merge_rows(retrieved, _company_support_rows(company_name, limit=max(4, k // 2)), limit=max(k, 8))
            if not retrieved:
                return _base_answer("리스크/실사 분석 근거가 부족합니다. 대상 회사를 명시하거나 인덱스를 갱신해 주세요.", company_name=company_name), []
            detail = self._due_diligence_simple_template(company_name, question, retrieved)
            answer = _base_answer(str(detail.get("summary") or "정보 부족"), company_name=company_name)
            answer.update(detail)
            answer = self._backfill_answer_from_evidence(
                answer=answer,
                retrieved=retrieved,
                company_hint=company_name if company_name != "정보 부족" else None,
                question=question,
            )
            answer["sources"] = self._dedup_sources(retrieved)
            return answer, retrieved

        if template_id == self.ENTRY_TEMPLATE_STRATEGIC_DECISION:
            company_name = self._extract_company_from_query(question) or (similar_names[0] if similar_names else "정보 부족")
            retrieved = (
                self._retrieve_for_company_query(company_name, question, top_k=k) if company_name != "정보 부족" else self.retrieve(question, top_k=k)
            )
            retrieved = _merge_rows(retrieved, _company_support_rows(company_name, limit=max(4, k // 2)), limit=max(k, 8))
            if not retrieved:
                return _base_answer("전략적 의사결정 분석 근거가 부족합니다. 대상 회사를 명시하거나 인덱스를 갱신해 주세요.", company_name=company_name), []
            detail = self._strategic_decision_simple_template(company_name, question, retrieved)
            answer = _base_answer(str(detail.get("summary") or "정보 부족"), company_name=company_name)
            answer.update(detail)
            answer = self._backfill_answer_from_evidence(
                answer=answer,
                retrieved=retrieved,
                company_hint=company_name if company_name != "정보 부족" else None,
                question=question,
            )
            answer["sources"] = self._dedup_sources(retrieved)
            return answer, retrieved

        return _base_answer("템플릿 분류 결과를 처리할 수 없습니다. 다시 질문해 주세요."), []

    def _classify_entry_template(self, question: str) -> tuple[str, str]:
        q = (question or "").strip()
        if not q:
            return self.ENTRY_TEMPLATE_TARGET_OVERVIEW, self.ENTRY_TEMPLATE_NAME_TARGET_OVERVIEW
        forced = re.match(r"^\s*\[template:([a-z_]+)\]\s*", q, flags=re.IGNORECASE)
        if forced:
            forced_tid = str(forced.group(1) or "").strip().lower()
            valid = {
                self.ENTRY_TEMPLATE_TARGET_OVERVIEW: self.ENTRY_TEMPLATE_NAME_TARGET_OVERVIEW,
                self.ENTRY_TEMPLATE_INDUSTRY_MARKET: self.ENTRY_TEMPLATE_NAME_INDUSTRY_MARKET,
                self.ENTRY_TEMPLATE_VALUATION: self.ENTRY_TEMPLATE_NAME_VALUATION,
                self.ENTRY_TEMPLATE_SYNERGY_PAIR: self.ENTRY_TEMPLATE_NAME_SYNERGY_PAIR,
                self.ENTRY_TEMPLATE_DUE_DILIGENCE_RISK: self.ENTRY_TEMPLATE_NAME_DUE_DILIGENCE_RISK,
                self.ENTRY_TEMPLATE_STRATEGIC_DECISION: self.ENTRY_TEMPLATE_NAME_STRATEGIC_DECISION,
            }
            if forced_tid in valid:
                return forced_tid, valid[forced_tid]
        prompt = f"""
너는 질의 라우터다. 아래 사용자 질의를 읽고 템플릿 1개만 선택하라.
출력은 JSON 하나만 반환한다.

허용 template_id:
- {self.ENTRY_TEMPLATE_TARGET_OVERVIEW}: 특정 기업 개요/사업구조/재무/공시 요약
- {self.ENTRY_TEMPLATE_INDUSTRY_MARKET}: 산업 구조/시장 규모/성장성/경쟁구도 분석
- {self.ENTRY_TEMPLATE_VALUATION}: 기업가치/멀티플/DCF/IRR 등 밸류에이션 분석
- {self.ENTRY_TEMPLATE_SYNERGY_PAIR}: 두 회사 간 시너지/통합효과 분석
- {self.ENTRY_TEMPLATE_DUE_DILIGENCE_RISK}: 리스크 식별/실사 체크포인트 분석
- {self.ENTRY_TEMPLATE_STRATEGIC_DECISION}: 인수 여부/구조/우선순위 등 전략적 의사결정

JSON 스키마:
{{
  "template_id": "target_company_overview|industry_market_analysis|valuation_analysis|synergy_pair_analysis|due_diligence_risk_analysis|strategic_decision_analysis",
  "confidence": 0.0,
  "reason": "string"
}}

분류 기준:
- target_company_overview: 특정 기업의 개요/사업구조/재무/공시 설명
- industry_market_analysis: 특정 산업/시장 전반 분석
- valuation_analysis: 기업가치/멀티플/DCF/IRR/프리미엄 등 가치평가 중심
- synergy_pair_analysis: 두 회사 간 합병/인수 시 시너지 분석
- due_diligence_risk_analysis: 실사 항목, 법무/재무/운영 리스크 점검
- strategic_decision_analysis: 인수전략, 구조 대안, 의사결정 권고

예시:
- "삼성전자 개요 알려줘" -> target_company_overview
- "2차전지 산업의 성장성과 경쟁구도 분석해줘" -> industry_market_analysis
- "LG전자 적정 EV/EBITDA 범위 알려줘" -> valuation_analysis
- "삼성전자랑 LG전자가 합병하면?" -> synergy_pair_analysis
- "한화오션 인수 전 실사 체크리스트 작성해줘" -> due_diligence_risk_analysis
- "현금/주식 혼합 인수 구조 중 무엇이 유리한지 결정해줘" -> strategic_decision_analysis

사용자 질의:
{q}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        valid = {
            self.ENTRY_TEMPLATE_TARGET_OVERVIEW: self.ENTRY_TEMPLATE_NAME_TARGET_OVERVIEW,
            self.ENTRY_TEMPLATE_INDUSTRY_MARKET: self.ENTRY_TEMPLATE_NAME_INDUSTRY_MARKET,
            self.ENTRY_TEMPLATE_VALUATION: self.ENTRY_TEMPLATE_NAME_VALUATION,
            self.ENTRY_TEMPLATE_SYNERGY_PAIR: self.ENTRY_TEMPLATE_NAME_SYNERGY_PAIR,
            self.ENTRY_TEMPLATE_DUE_DILIGENCE_RISK: self.ENTRY_TEMPLATE_NAME_DUE_DILIGENCE_RISK,
            self.ENTRY_TEMPLATE_STRATEGIC_DECISION: self.ENTRY_TEMPLATE_NAME_STRATEGIC_DECISION,
        }
        if isinstance(raw, dict):
            tid = str(raw.get("template_id") or "").strip()
            if tid in valid:
                return tid, valid[tid]
            # generate_json() parse 실패 시 raw 텍스트를 담아줄 수 있어 보조 파싱
            raw_text = str(raw.get("raw") or "").strip().lower()
            for tid, tname in valid.items():
                if tid in raw_text:
                    return tid, tname

        # 2차 LLM 분류(단일 토큰 응답). 여기도 LLM 판단으로 처리.
        retry_prompt = f"""
아래 질의를 템플릿 1개로 분류하라.
출력은 아래 6개 중 하나만 출력한다(설명 금지):
- {self.ENTRY_TEMPLATE_TARGET_OVERVIEW}
- {self.ENTRY_TEMPLATE_INDUSTRY_MARKET}
- {self.ENTRY_TEMPLATE_VALUATION}
- {self.ENTRY_TEMPLATE_SYNERGY_PAIR}
- {self.ENTRY_TEMPLATE_DUE_DILIGENCE_RISK}
- {self.ENTRY_TEMPLATE_STRATEGIC_DECISION}

규칙:
- 기업 개요/사업구조/재무/공시는 target_company_overview
- 산업/시장 전반 분석은 industry_market_analysis
- 가치평가/멀티플/DCF는 valuation_analysis
- 두 회사간 시너지/합병효과는 synergy_pair_analysis
- 실사/리스크 점검은 due_diligence_risk_analysis
- 인수 구조/우선순위/의사결정 권고는 strategic_decision_analysis

질의: {q}
""".strip()
        retry_raw = self.client.generate_json(settings.ollama_chat_model, retry_prompt)
        if isinstance(retry_raw, dict):
            cand = str(retry_raw.get("template_id") or retry_raw.get("raw") or "").strip().lower()
            for tid, tname in valid.items():
                if tid in cand:
                    return tid, tname

        # 최종 폴백
        return self.ENTRY_TEMPLATE_TARGET_OVERVIEW, self.ENTRY_TEMPLATE_NAME_TARGET_OVERVIEW

    @staticmethod
    def _dedup_sources_from_rows(rows: list[dict[str, Any]]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for r in rows:
            s = str(r.get("source") or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out

    def _peer_industry_definition(self, question: str, rows: list[dict[str, Any]]) -> str:
        terms = [t for t in self._query_terms(question) if t][:4]
        top_names = [str(r.get("company") or "").strip() for r in rows[:3] if str(r.get("company") or "").strip()]
        if terms:
            base = f"질의 핵심 키워드({', '.join(terms)})와 벡터 유사도를 기준으로 동종/인접 산업군을 정의했습니다."
        else:
            base = "질의 문맥과 벡터 유사도를 기준으로 동종/인접 산업군을 정의했습니다."
        if top_names:
            base += f" 대표 후보는 {', '.join(top_names)} 입니다."
        return base

    @staticmethod
    def _peer_screening_conditions(question: str, requested: int) -> list[str]:
        q = (question or "").strip()
        return [
            f"질의 원문 기반 유사도 검색 수행: {q if q else '정보 부족'}",
            f"상위 후보 수: {requested}개",
            "상장사 우선(KOSPI/KOSDAQ/OTHER), 데이터 부족 시 정보 부족 표기",
            "근거 출처가 있는 기업만 비교표에 반영",
        ]

    @staticmethod
    def _peer_listed_companies(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for r in rows:
            company = str(r.get("company") or "").strip()
            market = str(r.get("market") or "정보 부족").upper()
            if not company:
                continue
            if market not in {"KOSPI", "KOSDAQ", "OTHER"}:
                continue
            out.append(
                {
                    "company": company,
                    "market": market,
                    "strategic_fit_score": int(r.get("strategic_fit_score")) if isinstance(r.get("strategic_fit_score"), int) else None,
                    "reason": str(r.get("reason") or "정보 부족"),
                    "source": str(r.get("source") or "정보 부족"),
                }
            )
        return out

    @staticmethod
    def _peer_unlisted_companies(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for r in rows:
            company = str(r.get("company") or "").strip()
            market = str(r.get("market") or "정보 부족").upper()
            if not company:
                continue
            if market in {"KOSPI", "KOSDAQ", "OTHER"}:
                continue
            out.append(
                {
                    "company": company,
                    "market": market or "정보 부족",
                    "source": str(r.get("source") or "정보 부족"),
                }
            )
        return out

    def _peer_company_metrics(self, source: str) -> dict[str, str]:
        p = self._resolve_existing_json_path(source)
        if p is None:
            return {"revenue": "정보 부족", "ebitda": "정보 부족", "ev_ebitda": "정보 부족", "per": "정보 부족"}
        payload = self._safe_read_json(p)
        if not isinstance(payload, dict):
            return {"revenue": "정보 부족", "ebitda": "정보 부족", "ev_ebitda": "정보 부족", "per": "정보 부족"}

        latest_rev: str = "정보 부족"
        latest_ebitda: str = "정보 부족"
        f5 = payload.get("financials_5y") if isinstance(payload.get("financials_5y"), dict) else {}
        years = f5.get("years") if isinstance(f5.get("years"), list) else []
        latest_year = -1
        latest_row: dict[str, Any] | None = None
        for y in years:
            if not isinstance(y, dict):
                continue
            yr_raw = y.get("year")
            yr = yr_raw if isinstance(yr_raw, int) else int(yr_raw) if isinstance(yr_raw, str) and yr_raw.isdigit() else -1
            if yr > latest_year:
                latest_year = yr
                latest_row = y
        if isinstance(latest_row, dict):
            rv = self._to_float(latest_row.get("revenue"))
            if rv is not None:
                latest_rev = self._number_text(rv)
            mgn = self._to_float(latest_row.get("ebitda_margin_pct"))
            if mgn is not None:
                latest_ebitda = f"{mgn:.2f}%"

        profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
        ev_ebitda = self._to_float(profile.get("ev_ebitda") or profile.get("enterprise_to_ebitda") or payload.get("ev_ebitda"))
        per = self._to_float(profile.get("trailing_pe") or profile.get("per") or payload.get("per"))
        return {
            "revenue": latest_rev,
            "ebitda": latest_ebitda,
            "ev_ebitda": self._number_text(ev_ebitda) if ev_ebitda is not None else "정보 부족",
            "per": self._number_text(per) if per is not None else "정보 부족",
        }

    def _peer_revenue_ebitda_table(self, rows: list[dict[str, Any]]) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        for r in rows:
            company = str(r.get("company") or "").strip()
            source = str(r.get("source") or "").strip()
            if not company:
                continue
            m = self._peer_company_metrics(source)
            out.append(
                {
                    "company": company,
                    "revenue": m["revenue"],
                    "ebitda": m["ebitda"],
                }
            )
            if len(out) >= 10:
                break
        return out

    def _peer_multiple_table(self, rows: list[dict[str, Any]]) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        for r in rows:
            company = str(r.get("company") or "").strip()
            source = str(r.get("source") or "").strip()
            if not company:
                continue
            m = self._peer_company_metrics(source)
            out.append(
                {
                    "company": company,
                    "ev_ebitda": m["ev_ebitda"],
                    "per": m["per"],
                }
            )
            if len(out) >= 10:
                break
        return out

    def target_analysis(self, company_name: str, top_k_per_question: int = 6) -> dict[str, Any]:
        self._ensure_fresh_index()
        coverage = self._company_source_coverage(company_name)
        results: list[dict[str, Any]] = []

        for question_id, template in self.TARGET_ANALYSIS_QUESTIONS:
            question = template.format(company=company_name)
            readiness = self._target_question_readiness(question_id, coverage)
            retrieved = self._retrieve_for_company_query(company_name, question, top_k=top_k_per_question)
            sources = self._dedup_sources(retrieved)

            if readiness == "불가":
                answer_text = self._target_unavailable_message(question_id)
            else:
                answer_text = self._answer_target_question(
                    company_name=company_name,
                    question=question,
                    readiness=readiness,
                    retrieved=retrieved,
                )

            results.append(
                {
                    "question_id": question_id,
                    "question": question,
                    "readiness": readiness,
                    "answer": answer_text,
                    "evidence_sources": sources,
                }
            )

        return {
            "company_name": company_name,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "results": results,
        }

    def industry_analysis(self, industry_name: str, top_k_per_question: int = 6) -> dict[str, Any]:
        self._ensure_fresh_index()
        coverage = self._industry_source_coverage(industry_name)
        results: list[dict[str, Any]] = []

        for question_id, template in self.INDUSTRY_ANALYSIS_QUESTIONS:
            question = template.format(industry=industry_name)
            readiness = self._industry_question_readiness(question_id, coverage)
            retrieved = self._retrieve_for_industry_query(industry_name, question, top_k=top_k_per_question)
            sources = self._dedup_sources(retrieved)

            if readiness == "불가":
                answer_text = self._industry_unavailable_message(question_id)
            else:
                answer_text = self._answer_industry_question(
                    industry_name=industry_name,
                    question=question,
                    readiness=readiness,
                    retrieved=retrieved,
                )

            results.append(
                {
                    "question_id": question_id,
                    "question": question,
                    "readiness": readiness,
                    "answer": answer_text,
                    "evidence_sources": sources,
                }
            )

        return {
            "industry_name": industry_name,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "results": results,
        }

    def valuation_analysis(self, company_name: str, top_k_per_question: int = 6) -> dict[str, Any]:
        self._ensure_fresh_index()
        coverage = self._company_source_coverage(company_name)
        results: list[dict[str, Any]] = []

        for question_id, template in self.VALUATION_ANALYSIS_QUESTIONS:
            question = template.format(company=company_name)
            readiness = self._valuation_question_readiness(question_id, coverage)
            retrieved = self._retrieve_for_company_query(company_name, question, top_k=top_k_per_question)
            sources = self._dedup_sources(retrieved)

            if readiness == "불가":
                answer_text = self._valuation_unavailable_message(question_id)
            else:
                answer_text = self._answer_valuation_question(
                    company_name=company_name,
                    question=question,
                    readiness=readiness,
                    retrieved=retrieved,
                )

            results.append(
                {
                    "question_id": question_id,
                    "question": question,
                    "readiness": readiness,
                    "answer": answer_text,
                    "evidence_sources": sources,
                }
            )

        return {
            "company_name": company_name,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "results": results,
        }

    def synergy_analysis(self, company_name: str, top_k_per_question: int = 6) -> dict[str, Any]:
        self._ensure_fresh_index()
        coverage = self._company_source_coverage(company_name)
        results: list[dict[str, Any]] = []

        for question_id, template in self.SYNERGY_ANALYSIS_QUESTIONS:
            question = template.format(company=company_name)
            readiness = self._synergy_question_readiness(question_id, coverage)
            retrieved = self._retrieve_for_company_query(company_name, question, top_k=top_k_per_question)
            sources = self._dedup_sources(retrieved)

            if readiness == "불가":
                answer_text = self._synergy_unavailable_message(question_id)
            else:
                answer_text = self._answer_synergy_question(
                    company_name=company_name,
                    question=question,
                    readiness=readiness,
                    retrieved=retrieved,
                )

            results.append(
                {
                    "question_id": question_id,
                    "question": question,
                    "readiness": readiness,
                    "answer": answer_text,
                    "evidence_sources": sources,
                }
            )

        return {
            "company_name": company_name,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "results": results,
        }

    def due_diligence_analysis(self, company_name: str, top_k_per_question: int = 6) -> dict[str, Any]:
        self._ensure_fresh_index()
        coverage = self._company_source_coverage(company_name)
        results: list[dict[str, Any]] = []

        for question_id, template in self.DUE_DILIGENCE_ANALYSIS_QUESTIONS:
            question = template.format(company=company_name)
            readiness = self._due_diligence_question_readiness(question_id, coverage)
            retrieved = self._retrieve_for_company_query(company_name, question, top_k=top_k_per_question)
            sources = self._dedup_sources(retrieved)

            if readiness == "불가":
                answer_text = self._due_diligence_unavailable_message(question_id)
            else:
                answer_text = self._answer_due_diligence_question(
                    company_name=company_name,
                    question=question,
                    readiness=readiness,
                    retrieved=retrieved,
                )

            results.append(
                {
                    "question_id": question_id,
                    "question": question,
                    "readiness": readiness,
                    "answer": answer_text,
                    "evidence_sources": sources,
                }
            )

        return {
            "company_name": company_name,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "results": results,
        }

    def strategic_analysis(self, company_name: str, top_k_per_question: int = 6) -> dict[str, Any]:
        self._ensure_fresh_index()
        coverage = self._company_source_coverage(company_name)
        results: list[dict[str, Any]] = []

        for question_id, template in self.STRATEGIC_ANALYSIS_QUESTIONS:
            question = template.format(company=company_name)
            readiness = self._strategic_question_readiness(question_id, coverage)
            retrieved = self._retrieve_for_company_query(company_name, question, top_k=top_k_per_question)
            sources = self._dedup_sources(retrieved)

            if readiness == "불가":
                answer_text = self._strategic_unavailable_message(question_id)
            else:
                answer_text = self._answer_strategic_question(
                    company_name=company_name,
                    question=question,
                    readiness=readiness,
                    retrieved=retrieved,
                )

            results.append(
                {
                    "question_id": question_id,
                    "question": question,
                    "readiness": readiness,
                    "answer": answer_text,
                    "evidence_sources": sources,
                }
            )

        return {
            "company_name": company_name,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "results": results,
        }

    def similar_companies(
        self,
        query: str,
        top_k: int = 5,
        allowed_layers: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        self._ensure_fresh_index()
        if not self._chunks:
            return []

        top_k = max(top_k, self._extract_requested_top_k(query))
        q_emb = self.client.embed(settings.ollama_embed_model, query)
        query_terms = self._query_terms(query)
        core_terms, expanded_terms = self._expanded_terms(query_terms)
        has_topic_terms = len(core_terms) > 0
        intent = self._intent_flags(query, core_terms, expanded_terms)

        scored_rows: list[tuple[float, dict[str, Any], list[str]]] = []
        for row in self._chunks:
            if not bool(row.get("approved", True)):
                continue
            row_layer = self._source_layer_of_row(row)
            if allowed_layers and row_layer not in allowed_layers:
                continue
            text = str(row.get("text") or "")
            emb = row.get("embedding")
            if not isinstance(emb, list):
                continue
            if not self._passes_intent_filter(row, text, intent):
                continue
            semantic = cosine_similarity(q_emb, emb)
            lexical, matched = self._lexical_score(text, expanded_terms)
            core_lexical, core_matched = self._lexical_score(text, core_terms)

            # 의미 유사도 + 질의 핵심키워드 일치도를 함께 반영
            score = (0.65 * semantic) + (0.35 * lexical)
            # 키워드가 있는 질의인데 본문 매칭이 0이면 감점
            if has_topic_terms and core_lexical <= 0:
                score *= 0.35

            scored_rows.append((score, row, core_matched or matched))

        if has_topic_terms:
            # 핵심어와 한 개라도 맞는 후보가 있으면 그 후보군만 유지
            matched_rows = [x for x in scored_rows if x[2]]
            if matched_rows and not intent.get("manufacturing"):
                scored_rows = matched_rows

        scored_rows.sort(key=lambda x: x[0], reverse=True)

        # 기업 단위로 집계하여 가장 점수가 높은 근거를 대표로 사용
        best_by_company: dict[str, dict[str, Any]] = {}
        for score, row, matched in scored_rows:
            company = str(row.get("company") or "").strip()
            if not company:
                continue

            market = self._normalize_market(str(row.get("market") or ""))
            if market in {"", "UNKNOWN", "OTHER"}:
                cm = self._company_master_item(company)
                if isinstance(cm, dict):
                    markets = cm.get("markets")
                    if isinstance(markets, list):
                        pick = next((str(x) for x in markets if str(x).upper() in {"KOSPI", "KOSDAQ"}), "")
                        if pick:
                            market = pick.upper()
            source = str(row.get("source") or "")
            source_layer = self._source_layer_of_row(row)
            strategic_fit_score = int(round(max(0.0, min(1.0, score)) * 100))
            if strategic_fit_score < 1:
                continue

            reason = self._build_similarity_reason(core_terms, matched, strategic_fit_score)
            existing = best_by_company.get(company)
            cand = {
                "company": company,
                "market": market,
                "score": round(max(0.0, min(1.0, score)), 4),
                "strategic_fit_score": strategic_fit_score,
                "reason": reason,
                "source": source,
                "source_layer": source_layer,
                "approved": bool(row.get("approved", True)),
            }

            if existing is None:
                best_by_company[company] = cand
                continue

            cand_rank = float(cand["score"]) + (self._source_quality(source) * 0.01)
            existing_rank = float(existing.get("score", 0.0)) + (
                self._source_quality(str(existing.get("source") or "")) * 0.01
            )
            if cand_rank > existing_rank:
                best_by_company[company] = cand
            elif existing.get("market") in {"", "UNKNOWN"} and market not in {"", "UNKNOWN"}:
                existing["market"] = market

        ranked = sorted(best_by_company.values(), key=lambda x: x["score"], reverse=True)
        return ranked[:top_k]

    @staticmethod
    def _source_layer_of_row(row: dict[str, Any]) -> str:
        explicit = str(row.get("source_layer") or "").strip().lower()
        if explicit in {"authoritative", "secondary", "ai", "internet", "user_input"}:
            return explicit
        source = str(row.get("source") or "").lower()
        if "dart_" in source or "disclosure" in source or "financials_5y_" in source:
            return "authoritative"
        if "internet_company_search_" in source or "google_news" in source or "news_" in source:
            return "internet"
        if "ai_company_search_" in source or "chatgpt" in source or "gemini" in source:
            return "ai"
        return "secondary"

    @staticmethod
    def _chunk_text_for_index(text: str, chunk_size: int = 700, overlap: int = 120) -> list[str]:
        text = re.sub(r"\s+", " ", str(text or "")).strip()
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

    def register_ai_company_results(
        self,
        query: str,
        provider: str,
        items: list[dict[str, Any]],
        approved_by: str,
    ) -> dict[str, Any]:
        return self._register_company_search_results(
            query=query,
            provider=provider,
            items=items,
            approved_by=approved_by,
            source_layer="ai",
            source_type="ai_provider",
            file_prefix="ai_company_search",
            provider_label=f"AI Provider: {provider}",
        )

    def register_internet_company_results(
        self,
        query: str,
        items: list[dict[str, Any]],
        approved_by: str,
    ) -> dict[str, Any]:
        return self._register_company_search_results(
            query=query,
            provider="google_news",
            items=items,
            approved_by=approved_by,
            source_layer="internet",
            source_type="internet_search",
            file_prefix="internet_company_search",
            provider_label="Internet Source: google_news",
        )

    def _register_company_search_results(
        self,
        query: str,
        provider: str,
        items: list[dict[str, Any]],
        approved_by: str,
        source_layer: str,
        source_type: str,
        file_prefix: str,
        provider_label: str,
    ) -> dict[str, Any]:
        root = Path(__file__).resolve().parents[2]
        raw_dir = root / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        kept_items: list[dict[str, Any]] = []
        for it in items:
            if not isinstance(it, dict):
                continue
            company = str(it.get("company") or "").strip()
            if not company:
                continue
            kept_items.append(
                {
                    "company": company,
                    "market": str(it.get("market") or "정보 부족"),
                    "strategic_fit_score": int(it.get("strategic_fit_score") or 0),
                    "reason": str(it.get("reason") or "정보 부족"),
                    "source": str(it.get("source") or provider),
                }
            )

        if not kept_items:
            return {"ok": False, "added_chunks": 0, "message": "등록 가능한 검색 결과가 없습니다."}

        ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        file_path = raw_dir / f"{file_prefix}_{provider}_{ts}.json"
        payload = {
            "source_layer": source_layer,
            "source_type": source_type,
            "provider": provider,
            "query": query,
            "approved": True,
            "approved_by": approved_by,
            "collected_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "items": kept_items,
        }
        file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        index_path = Path(settings.index_path)
        index_path.parent.mkdir(parents=True, exist_ok=True)
        next_idx = 0
        if index_path.exists():
            try:
                with index_path.open("r", encoding="utf-8") as r:
                    for line in r:
                        line = line.strip()
                        if not line:
                            continue
                        row = json.loads(line)
                        rid = str(row.get("id") or "")
                        if rid.startswith(file_path.stem + ":"):
                            part = rid.split(":")[-1]
                            if part.isdigit():
                                next_idx = max(next_idx, int(part) + 1)
            except (OSError, json.JSONDecodeError):
                next_idx = 0

        add_rows: list[dict[str, Any]] = []
        for item in kept_items:
            text = (
                f"회사명: {item['company']}\n"
                f"시장: {item['market']}\n"
                f"전략 적합성 점수: {item['strategic_fit_score']}\n"
                f"추천 사유: {item['reason']}\n"
                f"질의: {query}\n"
                f"{provider_label}"
            )
            for ch in self._chunk_text_for_index(text):
                emb = self.client.embed(settings.ollama_embed_model, ch)
                add_rows.append(
                    {
                        "id": f"{file_path.stem}:{next_idx}",
                        "company": item["company"],
                        "market": item["market"],
                        "source": str(file_path),
                        "text": ch,
                        "embedding": emb,
                        "source_layer": source_layer,
                        "source_type": source_type,
                        "approved": True,
                        "approved_by": approved_by,
                    }
                )
                next_idx += 1

        with index_path.open("a", encoding="utf-8") as w:
            for row in add_rows:
                w.write(json.dumps(row, ensure_ascii=False) + "\n")

        self.reload_index()
        return {"ok": True, "added_chunks": len(add_rows), "source": str(file_path)}

    def delete_ai_company_source(self, source: str) -> dict[str, Any]:
        src = str(source or "").strip()
        if not src:
            return {"ok": False, "removed_chunks": 0, "message": "source가 비어 있습니다."}
        index_path = Path(settings.index_path)
        if not index_path.exists():
            return {"ok": False, "removed_chunks": 0, "message": "인덱스 파일이 없습니다."}

        kept: list[dict[str, Any]] = []
        removed = 0
        with index_path.open("r", encoding="utf-8") as r:
            for line in r:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if str(row.get("source") or "") == src:
                    removed += 1
                    continue
                kept.append(row)

        tmp = index_path.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as w:
            for row in kept:
                w.write(json.dumps(row, ensure_ascii=False) + "\n")
        tmp.replace(index_path)

        p = Path(src)
        if p.exists() and p.is_file():
            try:
                payload = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    payload["approved"] = False
                    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            except (OSError, json.JSONDecodeError):
                pass

        self.reload_index()
        return {"ok": True, "removed_chunks": removed, "source": src}

    @staticmethod
    def _normalize_market(market: str) -> str:
        m = (market or "").strip().upper()
        if not m:
            return "UNKNOWN"
        return m

    @staticmethod
    def _query_terms(query: str) -> list[str]:
        q = (query or "").lower()
        raw = re.findall(r"[a-z0-9가-힣]+", q)
        out: list[str] = []
        normalize_map = {
            "방산업": "방산",
            "방산업체": "방산",
            "방산기업": "방산",
            "방위산": "방산",
            "방위산업": "방산",
            "방위산업체": "방산",
            "방위산업기업": "방산",
            "방위사업": "방산",
            "방위사업체": "방산",
            "제조업": "제조",
            "자동차업": "자동차",
            "바이오업": "바이오",
        }
        for tok in raw:
            t0 = tok.strip().lower()
            if not t0 or t0.isdigit():
                continue

            pieces: list[str] = [t0]
            if t0 in normalize_map:
                pieces = [normalize_map[t0], t0]
            elif t0.endswith("산업") and len(t0) > 2:
                pieces = [t0[:-2], t0]
            if re.match(r"^\d+차전지$", t0):
                pieces = [t0]
            elif re.match(r"^ai[가-힣]+$", t0):
                pieces = ["ai", t0[2:]]
            elif re.match(r"^[a-z]+[가-힣]+$", t0):
                m = re.match(r"^([a-z]+)([가-힣]+)$", t0)
                if m:
                    pieces = [m.group(1), m.group(2)]

            for p in pieces:
                t = RagPipeline._trim_korean_suffix(p)
                if RagPipeline._is_noise_term(t):
                    continue
                if len(t) < 2:
                    continue
                out.append(t)
        dedup: list[str] = []
        seen: set[str] = set()
        for t in out:
            if t in seen:
                continue
            seen.add(t)
            dedup.append(t)
        return dedup

    @staticmethod
    def _expanded_terms(query_terms: list[str]) -> tuple[list[str], list[str]]:
        synonyms: dict[str, list[str]] = {
            "ai": ["ai", "인공지능", "생성형", "llm", "모델", "데이터센터", "반도체", "gpu"],
            "인공지능": ["ai", "인공지능", "생성형", "llm", "모델", "데이터센터", "반도체", "gpu"],
            "반도체": ["반도체", "파운드리", "메모리", "칩", "gpu", "npu"],
            "배터리": ["배터리", "2차전지", "양극재", "음극재", "전해질"],
            "2차전지": ["배터리", "2차전지", "양극재", "음극재", "전해질"],
            "바이오": ["바이오", "신약", "임상", "제약", "바이오시밀러"],
            "제약": ["제약", "신약", "임상", "바이오"],
            "방산": ["방산", "국방", "미사일", "항공우주", "무기체계"],
            "방위": ["방산", "방위", "국방", "미사일", "항공우주", "무기체계", "defense", "military", "aerospace"],
            "방위산업": ["방산", "방위", "국방", "미사일", "항공우주", "무기체계", "defense", "military", "aerospace"],
            "조선": ["조선", "해양", "선박", "lng선"],
            "자동차": ["자동차", "전기차", "자율주행", "모빌리티"],
            "로봇": ["로봇", "자동화", "센서", "모빌리티"],
            "클라우드": ["클라우드", "데이터센터", "saas", "인프라"],
            "에너지": ["에너지", "신재생", "태양광", "풍력", "전력", "원전"],
            "제조업": ["제조업", "제조", "생산", "공장", "부품", "소재", "장비", "가공"],
            "제조": ["제조업", "제조", "생산", "공장", "부품", "소재", "장비", "가공"],
        }
        core = list(query_terms)
        out: list[str] = list(core)
        for t in query_terms:
            out.extend(synonyms.get(t, []))
        # 순서 보존 중복 제거
        dedup: list[str] = []
        seen: set[str] = set()
        for t in out:
            tt = t.strip().lower()
            if not tt or tt in seen:
                continue
            seen.add(tt)
            dedup.append(tt)
        return core, dedup

    @staticmethod
    def _trim_korean_suffix(term: str) -> str:
        if not term:
            return term
        suffixes = [
            "으로", "에서", "에게", "까지", "부터", "처럼", "보다", "만의",
            "관련", "업체", "기업", "회사", "종목", "분야", "산업",
            "입니다", "해줘", "해주세요", "알려줘", "찾아줘", "추천해줘",
            "들을", "들을", "들을", "들을",
            "들은", "들이", "들을", "으로", "에서", "에게", "과", "와",
            "을", "를", "은", "는", "이", "가", "도", "만",
        ]
        out = term
        changed = True
        while changed:
            changed = False
            for s in suffixes:
                if len(out) > len(s) and out.endswith(s):
                    out = out[: -len(s)]
                    changed = True
                    break
        return out.strip()

    @staticmethod
    def _is_noise_term(term: str) -> bool:
        if not term:
            return True
        if re.match(r"^\d+(곳|개|건)$", term):
            return True
        stopwords = {
            "관련", "업체", "기업", "회사", "조사", "추천", "국내", "해외", "리스트",
            "가능", "가능성", "검토", "분석", "보고서", "요약", "비교", "후보",
            "어떤", "어느", "무엇", "찾", "알려", "알려줘", "찾아줘", "해줘", "정리", "정도", "가장",
            "곳", "개", "건", "질문", "최근", "향후", "부탁", "추천해줘", "유사", "업체들", "기업들",
        }
        if term in stopwords:
            return True
        if len(term) <= 1:
            return True
        return False

    @staticmethod
    def _lexical_score(text: str, terms: list[str]) -> tuple[float, list[str]]:
        if not terms:
            return 0.0, []
        lt = (text or "").lower()
        tokens = re.findall(r"[a-zA-Z0-9가-힣]+", lt)
        token_set = set(tokens)

        matched: list[str] = []
        for t in terms:
            tt = t.lower().strip()
            if not tt:
                continue
            # 짧은 토큰(예: ai)은 정확히 단어 단위로만 매칭
            if len(tt) <= 2:
                if tt in token_set:
                    matched.append(tt)
                continue
            # 긴 토큰은 단어 일치 우선, 합성어 대응 위해 부분 일치 보조 허용
            if tt in token_set or tt in lt:
                matched.append(tt)

        score = len(set(matched)) / max(1, len(set(terms)))
        return min(1.0, score), list(dict.fromkeys(matched))

    @staticmethod
    def _build_similarity_reason(query_terms: list[str], matched_terms: list[str], score_100: int) -> str:
        low_conf_note = " 다만 현재 확보된 관련 데이터가 제한적이어서 추가 검토가 필요합니다." if score_100 < 35 else ""
        if matched_terms:
            top = ", ".join(matched_terms[:3])
            return (
                f"질문 핵심 주제와 맞는 키워드({top})가 기사/문서에서 확인되어 "
                f"전략 적합성 점수를 {score_100}점으로 평가했습니다.{low_conf_note}"
            )
        if query_terms:
            top = ", ".join(query_terms[:2])
            return (
                f"질문 주제({top})와 문맥 유사도가 비교적 높아 "
                f"전략 검토 후보로 {score_100}점으로 제시했습니다.{low_conf_note}"
            )
        return f"질문 문맥과의 전반적 유사도를 기반으로 {score_100}점으로 평가했습니다.{low_conf_note}"

    @staticmethod
    def _intent_flags(query: str, core_terms: list[str], expanded_terms: list[str]) -> dict[str, bool]:
        q = (query or "").lower()
        terms = set([t.lower() for t in core_terms] + [t.lower() for t in expanded_terms])
        manufacturing_triggers = {"제조", "제조업", "생산", "공장", "부품", "소재", "장비", "가공"}
        manufacturing = any(t in terms for t in manufacturing_triggers) or ("제조업" in q)
        required_keywords = RagPipeline._required_keywords_from_terms(terms)
        return {"manufacturing": manufacturing, "required_keywords": bool(required_keywords), "rk": required_keywords}

    def _passes_intent_filter(self, row: dict[str, Any], text: str, intent: dict[str, bool]) -> bool:
        if intent.get("manufacturing"):
            if not self._is_manufacturing_candidate(row, text):
                return False
        rk = intent.get("rk")
        if isinstance(rk, set) and rk:
            if not self._row_matches_keywords(row, text, rk):
                return False
        return True

    @staticmethod
    def _required_keywords_from_terms(terms: set[str]) -> set[str]:
        theme_map: dict[str, set[str]] = {
            "방산": {
                "방산",
                "방위",
                "방위산업",
                "방위사업",
                "국방",
                "미사일",
                "무기",
                "항공우주",
                "defense",
                "defence",
                "military",
                "aerospace",
            },
            "반도체": {"반도체", "파운드리", "메모리", "칩", "semiconductor", "foundry", "memory"},
            "바이오": {"바이오", "제약", "신약", "임상", "biotech", "pharma", "drug"},
            "2차전지": {"2차전지", "배터리", "양극재", "음극재", "battery", "cathode", "anode"},
            "자동차": {"자동차", "완성차", "모빌리티", "전기차", "automotive", "ev"},
        }
        out: set[str] = set()
        for k, kws in theme_map.items():
            if k in terms or any(kw in terms for kw in kws):
                out.update(kws)
        return out

    def _row_matches_keywords(self, row: dict[str, Any], text: str, keywords: set[str]) -> bool:
        lt = (text or "").lower()
        if any(k in lt for k in keywords):
            return True

        company = str(row.get("company") or "").strip()
        cm = self._company_master_item(company)
        if isinstance(cm, dict):
            bag = " ".join(
                [
                    str(cm.get("industry") or ""),
                    str(cm.get("sector") or ""),
                    str(cm.get("industry_code") or ""),
                    " ".join(str(x) for x in (cm.get("aliases") or []) if str(x).strip()),
                ]
            ).lower()
            if any(k in bag for k in keywords):
                return True
        return False

    def _is_manufacturing_candidate(self, row: dict[str, Any], text: str) -> bool:
        company = str(row.get("company") or "").strip()
        company_flag = self._company_manufacturing(company)
        if company_flag is True:
            return True
        if company_flag is False:
            return False

        source = str(row.get("source") or "")
        meta = self._source_meta(source)
        if meta.get("is_manufacturing") is True:
            return True
        if meta.get("is_manufacturing") is False:
            return False

        # 뉴스 단편 키워드(공장/생산 등)만으로는 제조업으로 보지 않는다.
        if source.lower().endswith(".json") and "news_" in source.lower():
            return False

        # 메타 근거가 전혀 없으면 제조업 후보에서 제외해 정밀도 우선
        _ = text
        return False

    @staticmethod
    def _extract_requested_top_k(query: str) -> int:
        q = (query or "").lower()
        m = re.search(r"(\d{1,2})\s*(개|곳|종목|기업|업체)", q)
        if not m:
            return 5
        try:
            n = int(m.group(1))
        except ValueError:
            return 5
        return max(1, min(20, n))

    def _source_meta(self, source_path: str) -> dict[str, Any]:
        p = str(source_path or "")
        if not p:
            return {}
        cached = self._source_meta_cache.get(p)
        if cached is not None:
            return cached

        out: dict[str, Any] = {"is_manufacturing": None}
        path = Path(p)
        if not path.is_absolute():
            root = Path(__file__).resolve().parents[2]
            path = (root / path).resolve()
        if not path.exists() or not path.is_file():
            self._source_meta_cache[p] = out
            return out

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            self._source_meta_cache[p] = out
            return out
        if not isinstance(payload, dict):
            self._source_meta_cache[p] = out
            return out

        if path.name.startswith("dart_"):
            dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
            ind = str(dart.get("induty_code") or "").strip()
            if ind and ind[:2].isdigit():
                sec = int(ind[:2])
                out["is_manufacturing"] = 10 <= sec <= 34
        elif path.name.startswith("yahoo_"):
            profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
            industry = str(profile.get("industry") or "").lower()
            sector = str(profile.get("sector") or "").lower()
            text = f"{industry} {sector}"
            if text.strip():
                if any(
                    k in text
                    for k in [
                        "insurance",
                        "bank",
                        "financial",
                        "asset management",
                        "brokerage",
                        "construction",
                        "engineering",
                        "software",
                        "internet",
                        "telecom",
                        "media",
                        "retail",
                        "services",
                        "transportation",
                        "utilities",
                    ]
                ):
                    out["is_manufacturing"] = False
                elif any(k in text for k in ["manufact", "industrial", "electronic", "semiconductor", "machinery", "automotive", "chemical", "food"]):
                    out["is_manufacturing"] = True

        self._source_meta_cache[p] = out
        return out

    @staticmethod
    def _source_quality(source_path: str) -> int:
        s = (source_path or "").lower()
        if "yahoo_" in s:
            return 4
        if "dart_" in s and "dart_notes_" not in s:
            return 4
        if "dart_notes_" in s:
            return 3
        if "market_share_" in s or "valuation_case_" in s or "synergy_case_" in s or "due_diligence_case_" in s:
            return 3
        if "news_" in s:
            return 1
        return 2

    @staticmethod
    def _normalize_company_name(name: str) -> str:
        x = unicodedata.normalize("NFKC", str(name or "")).strip().lower()
        if not x:
            return ""
        x = x.replace("(주)", "").replace("주식회사", "").replace("㈜", "")
        x = re.sub(r"[^a-z0-9가-힣]+", "", x)
        return x

    def _company_manufacturing(self, company_name: str) -> bool | None:
        master = self._company_master_item(company_name)
        if isinstance(master, dict):
            v = master.get("is_manufacturing")
            if isinstance(v, bool):
                return v

        key = self._normalize_company_name(company_name)
        if not key:
            return None
        if self._company_manufacturing_cache is None:
            self._company_manufacturing_cache = self._build_company_manufacturing_cache()
        return self._company_manufacturing_cache.get(key)

    def _company_master_item(self, company_name: str) -> dict[str, Any] | None:
        idx = self._load_company_master_index()
        if not idx:
            return None
        key = self._normalize_company_name(company_name)
        if not key:
            return None
        item = idx.get(key)
        return item if isinstance(item, dict) else None

    def _load_company_master_index(self) -> dict[str, dict[str, Any]]:
        if self._company_master_index is not None:
            return self._company_master_index
        self._company_master_index = {}
        root = Path(__file__).resolve().parents[2]
        path = root / "data" / "processed" / "company_master.json"
        if not path.exists():
            return self._company_master_index

        payload = self._safe_read_json(path)
        if not isinstance(payload, dict):
            return self._company_master_index

        alias_idx = payload.get("alias_index")
        if isinstance(alias_idx, dict):
            for k, v in alias_idx.items():
                kk = self._normalize_company_name(str(k))
                if kk and isinstance(v, dict):
                    self._company_master_index[kk] = v
        return self._company_master_index

    def _build_company_manufacturing_cache(self) -> dict[str, bool | None]:
        root = Path(__file__).resolve().parents[2]
        raw_dir = root / "data" / "raw"
        out: dict[str, bool | None] = {}
        if not raw_dir.exists():
            return out

        def merge(name: str, val: bool | None) -> None:
            k = self._normalize_company_name(name)
            if not k or val is None:
                return
            prev = out.get(k)
            if prev is True:
                return
            if val is True:
                out[k] = True
                return
            if prev is None:
                out[k] = False

        for p in raw_dir.glob("dart_*.json"):
            payload = self._safe_read_json(p)
            if not isinstance(payload, dict):
                continue
            company = str(payload.get("company") or "")
            dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
            ind = str(dart.get("induty_code") or "").strip()
            val: bool | None = None
            if ind and ind[:2].isdigit():
                sec = int(ind[:2])
                val = 10 <= sec <= 34
            merge(company, val)

        for p in raw_dir.glob("yahoo_*.json"):
            payload = self._safe_read_json(p)
            if not isinstance(payload, dict):
                continue
            company = str(payload.get("company") or "")
            profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
            industry = str(profile.get("industry") or "").lower()
            sector = str(profile.get("sector") or "").lower()
            text = f"{industry} {sector}"
            val: bool | None = None
            if text.strip():
                if any(
                    k in text
                    for k in [
                        "insurance",
                        "bank",
                        "financial",
                        "asset management",
                        "brokerage",
                        "construction",
                        "engineering",
                        "software",
                        "internet",
                        "telecom",
                        "media",
                        "retail",
                        "services",
                        "transportation",
                        "utilities",
                    ]
                ):
                    val = False
                elif any(k in text for k in ["manufact", "industrial", "electronic", "semiconductor", "machinery", "automotive", "chemical", "food"]):
                    val = True
            merge(company, val)

        return out

    @staticmethod
    def _safe_read_json(path: Path) -> dict[str, Any] | None:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _sanitize_answer(answer: dict[str, Any], retrieved: list[dict[str, Any]]) -> dict[str, Any]:
        if not isinstance(answer, dict):
            return {
                "template_id": RagPipeline.ENTRY_TEMPLATE_COMPANY_OVERVIEW,
                "template_name": RagPipeline.ENTRY_TEMPLATE_NAME_COMPANY_OVERVIEW,
                "company_name": retrieved[0].get("company", "정보 부족") if retrieved else "정보 부족",
                "market": retrieved[0].get("market", "정보 부족") if retrieved else "정보 부족",
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
                "sources": [r.get("source", "") for r in retrieved if r.get("source")],
                "similar_companies": [],
            }

        def nz(v: Any) -> Any:
            if v is None:
                return "정보 부족"
            if isinstance(v, str):
                return v.strip() or "정보 부족"
            return v

        out = dict(answer)
        out["template_id"] = str(out.get("template_id") or RagPipeline.ENTRY_TEMPLATE_COMPANY_OVERVIEW).strip() or RagPipeline.ENTRY_TEMPLATE_COMPANY_OVERVIEW
        out["template_name"] = str(out.get("template_name") or RagPipeline.ENTRY_TEMPLATE_NAME_COMPANY_OVERVIEW).strip() or RagPipeline.ENTRY_TEMPLATE_NAME_COMPANY_OVERVIEW
        out["company_name"] = nz(out.get("company_name")) if out.get("company_name") is not None else (
            retrieved[0].get("company", "정보 부족") if retrieved else "정보 부족"
        )
        out["market"] = nz(out.get("market")) if out.get("market") is not None else (
            retrieved[0].get("market", "정보 부족") if retrieved else "정보 부족"
        )
        out["summary"] = nz(out.get("summary")) if out.get("summary") is not None else "정보 부족"
        out["company_overview"] = nz(out.get("company_overview")) if out.get("company_overview") is not None else out["summary"]
        out["business_structure"] = nz(out.get("business_structure")) if out.get("business_structure") is not None else "정보 부족"
        out["revenue_operating_income_5y_trend"] = (
            nz(out.get("revenue_operating_income_5y_trend"))
            if out.get("revenue_operating_income_5y_trend") is not None
            else "정보 부족"
        )
        out["ebitda"] = nz(out.get("ebitda")) if out.get("ebitda") is not None else "정보 부족"
        out["market_cap"] = nz(out.get("market_cap")) if out.get("market_cap") is not None else "정보 부족"

        highlights = out.get("highlights")
        if isinstance(highlights, list):
            out["highlights"] = [nz(x) for x in highlights if str(x).strip()]
        else:
            out["highlights"] = []

        risks = out.get("risks")
        if isinstance(risks, list):
            out["risks"] = [nz(x) for x in risks if str(x).strip()]
        else:
            out["risks"] = []
        key_risks = out.get("key_risks")
        if isinstance(key_risks, list):
            out["key_risks"] = [nz(x) for x in key_risks if str(x).strip()]
        else:
            out["key_risks"] = list(out["risks"])
        if not out["risks"] and out["key_risks"]:
            out["risks"] = list(out["key_risks"])

        comps = out.get("competitors")
        if isinstance(comps, list):
            out["competitors"] = [str(x).strip() for x in comps if str(x).strip()]
        else:
            out["competitors"] = []
        recents = out.get("recent_disclosures")
        if isinstance(recents, list):
            out["recent_disclosures"] = [str(x).strip() for x in recents if str(x).strip()]
        else:
            out["recent_disclosures"] = []

        srcs = out.get("sources")
        if isinstance(srcs, list):
            merged = [str(x).strip() for x in srcs if str(x).strip()]
        else:
            merged = []
        merged.extend([r.get("source", "").strip() for r in retrieved if r.get("source")])
        dedup: list[str] = []
        seen: set[str] = set()
        for s in merged:
            if s and s not in seen:
                seen.add(s)
                dedup.append(s)
        out["sources"] = dedup

        snap = out.get("financial_snapshot")
        if not isinstance(snap, dict):
            snap = {}
        out["financial_snapshot"] = {
            "market_cap": nz(snap.get("market_cap", "정보 부족")),
            "revenue": nz(snap.get("revenue", "정보 부족")),
            "operating_income": nz(snap.get("operating_income", "정보 부족")),
            "net_income": nz(snap.get("net_income", "정보 부족")),
        }
        if out["market_cap"] == "정보 부족":
            out["market_cap"] = out["financial_snapshot"]["market_cap"]

        sims = out.get("similar_companies")
        if isinstance(sims, list):
            out["similar_companies"] = [str(x).strip() for x in sims if str(x).strip()]
        else:
            out["similar_companies"] = []
        if not out["competitors"] and out["similar_companies"]:
            out["competitors"] = list(out["similar_companies"][:5])

        return out

    @staticmethod
    def _is_missing(v: Any) -> bool:
        if v is None:
            return True
        if isinstance(v, str):
            s = v.strip()
            return (not s) or (s == "정보 부족")
        return False

    @staticmethod
    def _number_text(v: Any) -> str:
        if v is None:
            return "정보 부족"
        if isinstance(v, (int, float)):
            return f"{int(v):,}" if float(v).is_integer() else f"{float(v):,.2f}"
        s = str(v).strip()
        return s or "정보 부족"

    @staticmethod
    def _to_float(v: Any) -> float | None:
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            s = v.strip().replace(",", "")
            if not s:
                return None
            try:
                return float(s)
            except ValueError:
                return None
        return None

    def _resolve_existing_json_path(self, source: str) -> Path | None:
        s = str(source or "").strip()
        if not s:
            return None
        p = Path(s)
        if not p.is_absolute():
            root = Path(__file__).resolve().parents[2]
            p = (root / p).resolve()
        if not p.exists() or not p.is_file():
            return None
        return p

    def _backfill_answer_from_evidence(
        self,
        answer: dict[str, Any],
        retrieved: list[dict[str, Any]],
        company_hint: str | None,
        question: str,
    ) -> dict[str, Any]:
        out = dict(answer) if isinstance(answer, dict) else {}
        snap = out.get("financial_snapshot")
        if not isinstance(snap, dict):
            snap = {}
        out["financial_snapshot"] = snap

        source_paths: list[Path] = []
        seen: set[str] = set()
        for row in retrieved:
            p = self._resolve_existing_json_path(str(row.get("source") or ""))
            if p is None:
                continue
            sp = str(p)
            if sp in seen:
                continue
            seen.add(sp)
            source_paths.append(p)
        for s in out.get("sources") or []:
            p = self._resolve_existing_json_path(str(s))
            if p is None:
                continue
            sp = str(p)
            if sp in seen:
                continue
            seen.add(sp)
            source_paths.append(p)

        # 회사 기준으로 5Y/Yahoo 원본을 추가 탐색해 누락을 줄인다.
        aliases = self._company_alias_candidates(company_hint or out.get("company_name") or "")
        if aliases:
            raw_dir = Path(__file__).resolve().parents[2] / "data" / "raw"
            for p in sorted(raw_dir.glob("financials_5y_*.json")) + sorted(raw_dir.glob("yahoo_*.json")):
                payload = self._safe_read_json(p)
                if not isinstance(payload, dict):
                    continue
                cn = self._normalize_company_name(str(payload.get("company") or ""))
                tk = self._normalize_company_name(str(payload.get("ticker") or ""))
                if not any((a in cn) or (cn and cn in a) or (a in tk) for a in aliases):
                    continue
                sp = str(p.resolve())
                if sp in seen:
                    continue
                seen.add(sp)
                source_paths.append(p.resolve())

        market_cap: float | None = None
        latest_year: int | None = None
        latest_revenue: float | None = None
        latest_op_income: float | None = None
        latest_net_income: float | None = None
        rev_points: list[tuple[int, float]] = []
        op_points: list[tuple[int, float]] = []
        latest_ebitda_margin: float | None = None
        profile_revenue: float | None = None
        profile_op_margin: float | None = None

        for p in source_paths:
            payload = self._safe_read_json(p)
            if not isinstance(payload, dict):
                continue
            profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
            mc = self._to_float(profile.get("market_cap"))
            if mc is not None and market_cap is None:
                market_cap = mc
            if profile_revenue is None:
                profile_revenue = self._to_float(profile.get("revenue"))
            if profile_op_margin is None:
                profile_op_margin = self._to_float(profile.get("operating_margins"))

            f5 = payload.get("financials_5y") if isinstance(payload.get("financials_5y"), dict) else {}
            years = f5.get("years") if isinstance(f5.get("years"), list) else []
            for y in years:
                if not isinstance(y, dict):
                    continue
                yv_raw = y.get("year")
                yv = int(yv_raw) if isinstance(yv_raw, int) else None
                if yv is None and isinstance(yv_raw, str) and yv_raw.isdigit():
                    yv = int(yv_raw)
                if yv is None:
                    continue
                rev = self._to_float(y.get("revenue"))
                opi = self._to_float(y.get("operating_income"))
                ni = self._to_float(y.get("net_income"))
                mgn = self._to_float(y.get("ebitda_margin_pct"))
                if rev is not None:
                    rev_points.append((yv, rev))
                if opi is not None:
                    op_points.append((yv, opi))
                if latest_year is None or yv > latest_year:
                    latest_year = yv
                    latest_revenue = rev
                    latest_op_income = opi
                    latest_net_income = ni
                    latest_ebitda_margin = mgn

        if latest_revenue is None and profile_revenue is not None:
            latest_revenue = profile_revenue
        if latest_op_income is None and profile_revenue is not None and profile_op_margin is not None:
            latest_op_income = profile_revenue * profile_op_margin
        if latest_ebitda_margin is None and profile_op_margin is not None:
            latest_ebitda_margin = profile_op_margin * 100.0

        if self._is_missing(snap.get("market_cap")) and market_cap is not None:
            snap["market_cap"] = self._number_text(market_cap)
        if self._is_missing(out.get("market_cap")) and market_cap is not None:
            out["market_cap"] = self._number_text(market_cap)
        if self._is_missing(snap.get("revenue")) and latest_revenue is not None:
            snap["revenue"] = self._number_text(latest_revenue)
        if self._is_missing(snap.get("operating_income")) and latest_op_income is not None:
            snap["operating_income"] = self._number_text(latest_op_income)
        if self._is_missing(snap.get("net_income")) and latest_net_income is not None:
            snap["net_income"] = self._number_text(latest_net_income)

        need_summary = self._is_missing(out.get("summary"))
        if need_summary and latest_year is not None:
            name = str(out.get("company_name") or company_hint or "해당 기업")
            cagr_text = "정보 부족"
            dedup_rev: dict[int, float] = {}
            for yv, rv in rev_points:
                dedup_rev[yv] = rv
            ordered = sorted(dedup_rev.items())
            if len(ordered) >= 2:
                y0, r0 = ordered[0]
                y1, r1 = ordered[-1]
                if r0 > 0 and y1 > y0:
                    years_diff = y1 - y0
                    cagr = ((r1 / r0) ** (1.0 / years_diff) - 1.0) * 100.0
                    cagr_text = f"{cagr:.2f}%"
            out["summary"] = (
                f"{name}의 최신 연도({latest_year}) 기준 매출/영업이익/순이익 데이터를 확인했습니다. "
                f"매출 CAGR(가용연도 기준)은 {cagr_text}이며 EBITDA 마진은 "
                f"{f'{latest_ebitda_margin:.2f}%' if latest_ebitda_margin is not None else '정보 부족'}입니다."
            )
        elif need_summary and (latest_revenue is not None or market_cap is not None):
            name = str(out.get("company_name") or company_hint or "해당 기업")
            out["summary"] = (
                f"{name}의 최신 재무 스냅샷 기준으로 매출/수익성 및 시가총액 정보를 일부 확인했습니다. "
                f"매출 {self._number_text(latest_revenue)}, 시가총액 {self._number_text(market_cap)}, "
                f"EBITDA 마진 {f'{latest_ebitda_margin:.2f}%' if latest_ebitda_margin is not None else '정보 부족'}입니다."
            )
        if self._is_missing(out.get("company_overview")):
            out["company_overview"] = out.get("summary") or "정보 부족"

        highs = out.get("highlights")
        if (not isinstance(highs, list) or not highs) and latest_year is not None:
            points: list[str] = []
            if latest_revenue is not None:
                points.append(f"{latest_year} 매출 {self._number_text(latest_revenue)}")
            if latest_op_income is not None:
                points.append(f"{latest_year} 영업이익 {self._number_text(latest_op_income)}")
            if latest_net_income is not None:
                points.append(f"{latest_year} 순이익 {self._number_text(latest_net_income)}")
            if latest_ebitda_margin is not None:
                points.append(f"EBITDA 마진 {latest_ebitda_margin:.2f}%")
            out["highlights"] = points[:4]
        if self._is_missing(out.get("ebitda")):
            if latest_ebitda_margin is not None and latest_year is not None:
                out["ebitda"] = f"{latest_year} EBITDA 마진 {latest_ebitda_margin:.2f}%"
            elif latest_ebitda_margin is not None:
                out["ebitda"] = f"EBITDA 마진 {latest_ebitda_margin:.2f}%"
            else:
                out["ebitda"] = "정보 부족"

        if self._is_missing(out.get("revenue_operating_income_5y_trend")):
            rev_map: dict[int, float] = {}
            for yv, rv in rev_points:
                rev_map[yv] = rv
            op_map: dict[int, float] = {}
            for yv, ov in op_points:
                op_map[yv] = ov
            rev_ordered = sorted(rev_map.items())
            op_ordered = sorted(op_map.items())
            if len(rev_ordered) >= 2:
                y0, r0 = rev_ordered[0]
                y1, r1 = rev_ordered[-1]
                rev_part = f"매출 {self._number_text(r0)} -> {self._number_text(r1)}"
                if r0 > 0 and y1 > y0:
                    cagr = ((r1 / r0) ** (1.0 / (y1 - y0)) - 1.0) * 100.0
                    rev_part += f" (CAGR {cagr:.2f}%)"
                op_part = "영업이익 정보 부족"
                if len(op_ordered) >= 2:
                    oy0, o0 = op_ordered[0]
                    oy1, o1 = op_ordered[-1]
                    op_part = f"영업이익 {self._number_text(o0)} -> {self._number_text(o1)} ({oy0}~{oy1})"
                out["revenue_operating_income_5y_trend"] = f"{y0}~{y1} {rev_part}, {op_part}"
            else:
                out["revenue_operating_income_5y_trend"] = "정보 부족"

        if self._is_missing(out.get("business_structure")):
            out["business_structure"] = "사업부 구조 세부 비중은 현재 컨텍스트에서 정보 부족"

        if not isinstance(out.get("competitors"), list) or not out.get("competitors"):
            sims = out.get("similar_companies") if isinstance(out.get("similar_companies"), list) else []
            out["competitors"] = [str(x).strip() for x in sims if str(x).strip()][:5]

        if not isinstance(out.get("key_risks"), list) or not out.get("key_risks"):
            rs = out.get("risks") if isinstance(out.get("risks"), list) else []
            out["key_risks"] = [str(x).strip() for x in rs if str(x).strip()][:5]

        if not isinstance(out.get("recent_disclosures"), list) or not out.get("recent_disclosures"):
            recents: list[str] = []
            for p in source_paths:
                pl = self._safe_read_json(p)
                if not isinstance(pl, dict):
                    continue
                title = str(pl.get("title") or "").strip()
                published_at = str(pl.get("published_at") or "").strip()
                src = str(pl.get("source") or "").strip()
                if title:
                    row = f"{published_at} {title}".strip()
                elif src:
                    row = src
                else:
                    row = p.name
                recents.append(row)
                if len(recents) >= 5:
                    break
            out["recent_disclosures"] = recents

        # target_company_overview 출력은 중첩 필드(target_overview/financial_overview/...)를 사용하므로
        # 상위 필드에서 보강된 값을 중첩 구조에도 동기화해 "정보 부족" 과다 표출을 줄인다.
        tv = out.get("target_overview") if isinstance(out.get("target_overview"), dict) else {}
        bs = out.get("business_structure") if isinstance(out.get("business_structure"), dict) else {}
        fo = out.get("financial_overview") if isinstance(out.get("financial_overview"), dict) else {}
        rd = out.get("risk_disclosure") if isinstance(out.get("risk_disclosure"), dict) else {}

        industry_text = "정보 부족"
        major_products = "정보 부족"
        key_customers_text = "정보 부족"
        growth_text = "정보 부족"
        key_risks_list: list[str] = []

        for p in source_paths:
            payload = self._safe_read_json(p)
            if not isinstance(payload, dict):
                continue

            profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
            if industry_text == "정보 부족":
                ind = str(profile.get("industry") or "").strip()
                if ind:
                    industry_text = ind
            if growth_text == "정보 부족":
                opm = self._to_float(profile.get("operating_margins"))
                if opm is not None:
                    growth_text = f"영업이익률 {opm * 100:.2f}%"

            dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
            if industry_text == "정보 부족":
                ind_code = str(dart.get("induty_code") or "").strip()
                if ind_code:
                    industry_text = f"한국표준산업분류 {ind_code}"

            dep = payload.get("customer_dependency") if isinstance(payload.get("customer_dependency"), dict) else {}
            tops = dep.get("top_customers") if isinstance(dep.get("top_customers"), list) else []
            if key_customers_text == "정보 부족" and tops:
                names = [str(x.get("name") or "").strip() for x in tops if isinstance(x, dict)]
                names = [x for x in names if x]
                if names:
                    key_customers_text = ", ".join(names[:5])

            esg = payload.get("esg") if isinstance(payload.get("esg"), dict) else {}
            if isinstance(esg.get("risk_flags"), list):
                for r in esg.get("risk_flags"):
                    rr = str(r).strip()
                    if rr:
                        key_risks_list.append(rr)

            if major_products == "정보 부족" and str(payload.get("source") or "").strip() == "external_market_share":
                ms = payload.get("market_share") if isinstance(payload.get("market_share"), dict) else {}
                ind = str(ms.get("industry") or "").strip()
                if ind:
                    major_products = f"{ind} 관련 제품/솔루션 중심"

        if self._is_missing(tv.get("company_definition")):
            tv["company_definition"] = industry_text
        if self._is_missing(tv.get("major_products_services")):
            tv["major_products_services"] = major_products
        if self._is_missing(tv.get("business_stage")):
            tv["business_stage"] = "성숙기" if industry_text != "정보 부족" else "정보 부족"

        if self._is_missing(bs.get("major_business_units")):
            bs["major_business_units"] = "사업부 세부 구조는 정보 부족"
        if self._is_missing(bs.get("revenue_mix")):
            bs["revenue_mix"] = "사업부별 매출 비중 정보 부족"
        if self._is_missing(bs.get("key_customers")):
            bs["key_customers"] = key_customers_text

        if self._is_missing(fo.get("revenue")):
            fo["revenue"] = snap.get("revenue", "정보 부족")
        if self._is_missing(fo.get("operating_income")):
            fo["operating_income"] = snap.get("operating_income", "정보 부족")
        if self._is_missing(fo.get("ebitda")):
            fo["ebitda"] = out.get("ebitda", "정보 부족")
        if self._is_missing(fo.get("recent_growth_rate")):
            fo["recent_growth_rate"] = growth_text

        if not isinstance(rd.get("key_risks"), list) or not rd.get("key_risks"):
            if key_risks_list:
                rd["key_risks"] = list(dict.fromkeys(key_risks_list))[:5]
            elif isinstance(out.get("key_risks"), list) and out.get("key_risks"):
                rd["key_risks"] = [str(x).strip() for x in out.get("key_risks") if str(x).strip()][:5]
        if not isinstance(rd.get("recent_disclosures"), list) or not rd.get("recent_disclosures"):
            rd["recent_disclosures"] = list(out.get("recent_disclosures") or [])[:5]
        if not isinstance(rd.get("watchpoints"), list) or not rd.get("watchpoints"):
            watch = []
            if isinstance(rd.get("key_risks"), list):
                watch = [f"{x} 모니터링" for x in rd.get("key_risks") if str(x).strip()]
            rd["watchpoints"] = watch[:5]

        out["target_overview"] = tv
        out["business_structure"] = bs
        out["financial_overview"] = fo
        out["risk_disclosure"] = rd

        # 5개년 질문인데 핵심 숫자가 비어있으면 최소 안내문으로 대체한다.
        q = (question or "").strip().lower()
        if ("5개년" in q or "최근 5년" in q) and all(self._is_missing(snap.get(k)) for k in ["revenue", "operating_income", "net_income"]):
            out["summary"] = (
                "해당 기업의 5개년 재무 원천은 존재하지만 현재 컨텍스트에서 핵심 계정 추출이 누락되었습니다. "
                "관리자 페이지에서 '5개년 재무 팩트 생성 -> 증분 인덱스 실행 -> 메모리 인덱스 새로고침'을 다시 실행해 주세요."
            )
        return out

    @staticmethod
    def _source_family(path: str) -> str:
        p = (path or "").lower()
        if "valuation_case_" in p:
            return "valuation_case"
        if "synergy_case_" in p:
            return "synergy_case"
        if "due_diligence_case_" in p:
            return "due_diligence_case"
        if "strategic_case_" in p:
            return "strategic_case"
        if "yahoo_" in p:
            return "yahoo"
        if "financials_5y_" in p:
            return "financials_5y"
        if "dart_financials_" in p:
            return "dart_financials"
        if "customer_dependency_external_" in p:
            return "customer_dependency_external"
        if "customer_dependency_llm_" in p:
            return "customer_dependency_llm"
        if "customer_dependency_" in p:
            return "customer_dependency"
        if "dart_notes_" in p:
            return "dart_notes"
        if "dart_" in p:
            return "dart"
        if "news_" in p:
            return "news"
        if "patent_" in p:
            return "patent"
        if "market_share_" in p:
            return "market_share"
        if "esg_" in p:
            return "esg"
        if "valuation_" in p or "multiple_" in p:
            return "valuation"
        if "mna_" in p:
            return "mna"
        if "regulation_" in p or "law_" in p:
            return "regulation"
        if "tam_" in p or "sam_" in p or "som_" in p:
            return "tam_sam_som"
        if "global_" in p or "overseas_" in p:
            return "global_players"
        if "techtrend_" in p:
            return "techtrend"
        if "commodity_" in p:
            return "commodity"
        if "macro_" in p:
            return "macro"
        if "fx_" in p:
            return "fx"
        if "privacy_" in p or "security_" in p:
            return "security"
        if "tax_" in p:
            return "tax"
        if "supply_chain_" in p:
            return "supply_chain"
        if "pmi_fail_" in p:
            return "pmi_fail"
        return "other"

    def _company_source_coverage(self, company_name: str) -> set[str]:
        q = (company_name or "").strip().lower()
        coverage: set[str] = set()
        if not q:
            return coverage
        for row in self._chunks:
            row_company = str(row.get("company") or "").strip().lower()
            text = str(row.get("text") or "").strip().lower()
            if not row_company:
                if q not in text:
                    continue
            if q not in row_company and row_company not in q and q not in text:
                continue
            fam = self._source_family(str(row.get("source") or ""))
            coverage.add(fam)
        return coverage

    def _industry_source_coverage(self, industry_name: str) -> set[str]:
        q = (industry_name or "").strip().lower()
        coverage: set[str] = set()
        if not q:
            return coverage
        for row in self._chunks:
            text = str(row.get("text") or "").lower()
            company = str(row.get("company") or "").lower()
            if q not in text and q not in company:
                continue
            coverage.add(self._source_family(str(row.get("source") or "")))
        return coverage

    @staticmethod
    def _target_question_readiness(question_id: int, coverage: set[str]) -> str:
        has_yahoo = "yahoo" in coverage
        has_dart = "dart" in coverage
        has_fin5y = "financials_5y" in coverage or "dart_financials" in coverage
        has_customer_dep = (
            "customer_dependency" in coverage
            or "customer_dependency_external" in coverage
            or "customer_dependency_llm" in coverage
        )
        has_news = "news" in coverage
        has_patent = "patent" in coverage
        has_mshare = "market_share" in coverage
        has_esg = "esg" in coverage

        if question_id == 1:
            return "가능" if (has_fin5y or has_yahoo) else ("부분" if has_dart else "불가")
        if question_id == 2:
            return "가능" if has_customer_dep else ("부분" if (has_dart or has_news) else "불가")
        if question_id == 3:
            return "부분" if (has_dart or has_yahoo) else "불가"
        if question_id == 4:
            return "부분" if has_dart else "불가"
        if question_id == 5:
            return "부분" if (has_yahoo or has_dart) else "불가"
        if question_id == 6:
            return "부분" if has_dart else "불가"
        if question_id == 7:
            return "가능" if has_mshare else "불가"
        if question_id == 8:
            return "가능" if has_patent else "불가"
        if question_id == 9:
            return "부분" if (has_dart or has_news) else "불가"
        if question_id == 10:
            return "가능" if has_esg else ("부분" if (has_dart or has_news) else "불가")
        return "불가"

    @staticmethod
    def _industry_question_readiness(question_id: int, coverage: set[str]) -> str:
        has_yahoo = "yahoo" in coverage
        has_dart = "dart" in coverage
        has_news = "news" in coverage
        has_valuation = "valuation" in coverage
        has_mna = "mna" in coverage
        has_regulation = "regulation" in coverage
        has_tam = "tam_sam_som" in coverage
        has_global = "global_players" in coverage
        has_techtrend = "techtrend" in coverage
        has_commodity = "commodity" in coverage
        has_macro = "macro" in coverage

        if question_id == 11:
            return "부분" if (has_news or has_yahoo or has_dart) else "불가"
        if question_id == 12:
            return "부분" if (has_news or has_dart) else "불가"
        if question_id == 13:
            return "가능" if has_valuation else "불가"
        if question_id == 14:
            return "가능" if has_mna else ("부분" if has_news else "불가")
        if question_id == 15:
            return "가능" if has_regulation else ("부분" if (has_news or has_dart) else "불가")
        if question_id == 16:
            return "가능" if has_tam else "불가"
        if question_id == 17:
            return "가능" if has_global else ("부분" if has_news else "불가")
        if question_id == 18:
            return "가능" if has_techtrend else ("부분" if has_news else "불가")
        if question_id == 19:
            return "가능" if has_commodity else ("부분" if (has_yahoo or has_dart) else "불가")
        if question_id == 20:
            return "가능" if has_macro else ("부분" if (has_yahoo or has_dart) else "불가")
        return "불가"

    @staticmethod
    def _valuation_question_readiness(question_id: int, coverage: set[str]) -> str:
        has_yahoo = "yahoo" in coverage
        has_dart = "dart" in coverage
        has_dart_notes = "dart_notes" in coverage
        has_valuation_case = "valuation_case" in coverage
        has_valuation = "valuation" in coverage
        has_mna = "mna" in coverage
        has_market_share = "market_share" in coverage
        has_patent = "patent" in coverage
        has_esg = "esg" in coverage
        has_fx = "fx" in coverage
        has_macro = "macro" in coverage

        if question_id == 21:
            return "가능" if (has_valuation_case or has_valuation) else ("부분" if has_yahoo else "불가")
        if question_id == 22:
            return "가능" if has_mna else "불가"
        if question_id == 23:
            return "가능" if has_valuation_case else ("부분" if (has_yahoo or has_dart) else "불가")
        if question_id == 24:
            return "가능" if has_valuation_case else ("부분" if has_yahoo else "불가")
        if question_id == 25:
            return "가능" if has_valuation_case else "불가"
        if question_id == 26:
            return "가능" if has_valuation_case else ("부분" if (has_market_share or has_patent or has_esg) else "불가")
        if question_id == 27:
            return "가능" if has_valuation_case else ("부분" if (has_valuation or has_yahoo) else "불가")
        if question_id == 28:
            return "가능" if (has_fx or has_macro or has_valuation_case) else ("부분" if has_yahoo else "불가")
        if question_id == 29:
            return "가능" if has_valuation_case else ("부분" if (has_yahoo or has_dart_notes) else "불가")
        if question_id == 30:
            return "가능" if (has_dart_notes or has_valuation_case) else ("부분" if has_dart else "불가")
        return "불가"

    @staticmethod
    def _synergy_question_readiness(question_id: int, coverage: set[str]) -> str:
        has_synergy_case = "synergy_case" in coverage
        has_dart_notes = "dart_notes" in coverage
        has_news = "news" in coverage
        has_market_share = "market_share" in coverage
        has_patent = "patent" in coverage
        has_esg = "esg" in coverage
        has_mna = "mna" in coverage

        if question_id in {31, 37}:
            if has_synergy_case or has_market_share or has_patent:
                return "가능"
            return "부분" if (has_dart_notes or has_news) else "불가"
        if question_id in {32, 34, 35, 36, 39}:
            return "가능" if has_synergy_case else ("부분" if (has_dart_notes or has_news) else "불가")
        if question_id == 33:
            return "가능" if has_synergy_case else ("부분" if has_dart_notes else "불가")
        if question_id == 38:
            return "가능" if (has_synergy_case or has_esg) else ("부분" if has_news else "불가")
        if question_id == 40:
            return "가능" if has_synergy_case else ("부분" if (has_dart_notes or has_mna) else "불가")
        return "불가"

    @staticmethod
    def _due_diligence_question_readiness(question_id: int, coverage: set[str]) -> str:
        has_dd = "due_diligence_case" in coverage
        has_dart = "dart" in coverage
        has_dart_notes = "dart_notes" in coverage
        has_news = "news" in coverage
        has_tax = "tax" in coverage
        has_security = "security" in coverage
        has_supply = "supply_chain" in coverage
        has_pmi_fail = "pmi_fail" in coverage
        has_mna = "mna" in coverage
        has_esg = "esg" in coverage
        has_synergy_case = "synergy_case" in coverage

        if question_id in {41, 42, 43, 44, 46, 47}:
            return "가능" if has_dd else ("부분" if (has_dart_notes or has_dart) else "불가")
        if question_id == 45:
            return "가능" if (has_dd or has_tax) else ("부분" if has_dart else "불가")
        if question_id == 48:
            return "가능" if (has_dd or has_security) else ("부분" if (has_news or has_esg) else "불가")
        if question_id == 49:
            return "가능" if (has_dd or has_supply) else ("부분" if (has_news or has_synergy_case) else "불가")
        if question_id == 50:
            return "가능" if (has_dd or has_pmi_fail) else ("부분" if (has_mna or has_news) else "불가")
        return "불가"

    @staticmethod
    def _strategic_question_readiness(question_id: int, coverage: set[str]) -> str:
        has_strategic = "strategic_case" in coverage
        has_valuation_case = "valuation_case" in coverage
        has_synergy_case = "synergy_case" in coverage
        has_dd = "due_diligence_case" in coverage
        has_mna = "mna" in coverage
        has_news = "news" in coverage
        has_dart = "dart" in coverage
        has_dart_notes = "dart_notes" in coverage

        if question_id in {51, 52, 54, 55, 60}:
            if has_strategic:
                return "가능"
            if has_valuation_case or has_synergy_case or has_dd:
                return "부분"
            return "불가"
        if question_id == 53:
            return "가능" if (has_strategic or has_mna) else ("부분" if (has_news or has_valuation_case) else "불가")
        if question_id in {56, 57, 58, 59}:
            if has_strategic:
                return "가능"
            if has_mna or has_dart_notes or has_dart:
                return "부분"
            return "불가"
        return "불가"

    @staticmethod
    def _target_unavailable_message(question_id: int) -> str:
        missing_reason: dict[int, str] = {
            2: "고객사별 매출 의존도는 공시 본문 테이블 정밀 파싱이 필요합니다.",
            7: "시장 점유율 비교는 외부 시장점유율 데이터셋이 필요합니다.",
            8: "특허/기술 요약은 특허 DB(예: KIPRIS) 연동 데이터가 필요합니다.",
            10: "ESG 전용 데이터 소스가 아직 인덱스에 없습니다.",
        }
        return "현재 데이터로 신뢰 가능한 답변을 만들기 어렵습니다. " + missing_reason.get(
            question_id, "관련 원천 데이터가 부족합니다."
        )

    @staticmethod
    def _industry_unavailable_message(question_id: int) -> str:
        missing_reason: dict[int, str] = {
            13: "산업 멀티플 원천 데이터(예: P/E, EV/EBITDA 집계)가 필요합니다.",
            16: "TAM/SAM/SOM 산정용 시장규모 데이터와 계산 모델이 필요합니다.",
            19: "원자재 가격 시계열과 원가구조 매핑 데이터가 필요합니다.",
        }
        return "현재 데이터로 신뢰 가능한 답변을 만들기 어렵습니다. " + missing_reason.get(
            question_id, "해당 질문에 필요한 산업 전용 데이터셋이 부족합니다."
        )

    @staticmethod
    def _valuation_unavailable_message(question_id: int) -> str:
        missing_reason: dict[int, str] = {
            22: "유사 거래(M&A comps) 데이터셋이 필요합니다.",
            25: "인수 구조/현금흐름 가정이 포함된 밸류에이션 케이스 데이터가 필요합니다.",
            29: "LBO 레버리지 가정(금리/상환/DSCR) 데이터가 필요합니다.",
        }
        return "현재 데이터로 신뢰 가능한 답변을 만들기 어렵습니다. " + missing_reason.get(
            question_id, "해당 질문에 필요한 밸류에이션 데이터셋이 부족합니다."
        )

    @staticmethod
    def _synergy_unavailable_message(question_id: int) -> str:
        missing_reason: dict[int, str] = {
            35: "IT 통합비 산정용 시스템 자산/전환비 데이터가 필요합니다.",
            36: "PMI 일정/통합 난이도 기준 데이터가 필요합니다.",
            40: "법인/법무 구조(계약/소송/법인목록) 정형 데이터가 필요합니다.",
        }
        return "현재 데이터로 신뢰 가능한 답변을 만들기 어렵습니다. " + missing_reason.get(
            question_id, "해당 질문에 필요한 시너지 데이터셋이 부족합니다."
        )

    @staticmethod
    def _due_diligence_unavailable_message(question_id: int) -> str:
        missing_reason: dict[int, str] = {
            47: "주요 계약 원문과 Change of Control 조항 데이터가 필요합니다.",
            48: "개인정보/보안 감사 및 사고 이력 데이터가 필요합니다.",
            50: "PMI 실패 사례 데이터셋(사례/원인/교훈)이 필요합니다.",
        }
        return "현재 데이터로 신뢰 가능한 답변을 만들기 어렵습니다. " + missing_reason.get(
            question_id, "해당 질문에 필요한 실사 데이터셋이 부족합니다."
        )

    @staticmethod
    def _strategic_unavailable_message(question_id: int) -> str:
        missing_reason: dict[int, str] = {
            52: "포트폴리오 시너지 매핑(제품/고객/채널) 데이터가 필요합니다.",
            57: "거래 구조(성과지표/정산식) 기반의 Earn-out 사례 데이터가 필요합니다.",
            60: "자본구조/세무/지배구조를 포함한 딜 구조 케이스 데이터가 필요합니다.",
        }
        return "현재 데이터로 신뢰 가능한 답변을 만들기 어렵습니다. " + missing_reason.get(
            question_id, "해당 질문에 필요한 전략 의사결정 데이터셋이 부족합니다."
        )

    @staticmethod
    def _dedup_sources(retrieved: list[dict[str, Any]]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for row in retrieved:
            src = str(row.get("source") or "").strip()
            if not src or src in seen:
                continue
            seen.add(src)
            out.append(src)
        return out[:8]

    def _retrieve_for_company_query(
        self,
        company_name: str,
        question: str,
        top_k: int,
        allow_fallback: bool = True,
    ) -> list[dict[str, Any]]:
        candidates = self.retrieve(question, top_k=max(top_k * 3, 12))
        c_raw = (company_name or "").strip().lower()
        c_norm = self._normalize_company_name(company_name)
        if not c_raw and not c_norm:
            return candidates[:top_k]

        matched = [
            row
            for row in candidates
            if (
                (c_raw and (c_raw in str(row.get("company") or "").strip().lower()))
                or (c_raw and (str(row.get("company") or "").strip().lower() in c_raw))
                or (c_raw and (c_raw in str(row.get("text") or "").strip().lower()))
                or (
                    c_norm
                    and (
                        c_norm in self._normalize_company_name(str(row.get("company") or ""))
                        or self._normalize_company_name(str(row.get("company") or "")) in c_norm
                        or c_norm in self._normalize_company_name(str(row.get("text") or ""))
                    )
                )
            )
        ]
        if len(matched) >= min(top_k, 4):
            return matched[:top_k]

        if not allow_fallback:
            direct = self._retrieve_company_direct(company_name, top_k=top_k)
            if direct:
                return direct
            return matched[:top_k]

        # 회사명이 잘 매칭되지 않으면, 매칭 결과를 우선하고 부족분은 일반 검색 결과로 보완한다.
        merged: list[dict[str, Any]] = list(matched)
        existing = {id(x) for x in merged}
        for row in candidates:
            if id(row) in existing:
                continue
            merged.append(row)
            if len(merged) >= top_k:
                break
        return merged[:top_k]

    def _retrieve_company_direct(self, company_name: str, top_k: int) -> list[dict[str, Any]]:
        cands = self._company_alias_candidates(company_name)
        if not cands:
            return []

        rows: list[dict[str, Any]] = []
        for row in self._chunks:
            rc = self._normalize_company_name(str(row.get("company") or ""))
            rt = self._normalize_company_name(str(row.get("text") or ""))
            if not any((c in rc) or (rc and rc in c) or (c in rt) for c in cands):
                continue
            src = str(row.get("source") or "")
            rows.append(
                {
                    "score": 0.0,
                    "company": row.get("company"),
                    "market": row.get("market"),
                    "source": src,
                    "text": str(row.get("text") or "")[:600],
                }
            )

        if not rows:
            return []
        rows.sort(key=lambda x: self._source_quality(str(x.get("source") or "")), reverse=True)
        return rows[:top_k]

    def _extract_company_from_query(self, question: str) -> str | None:
        idx = self._load_company_master_index()
        if not idx:
            return None
        nq = self._normalize_company_name(question)
        if not nq:
            return None

        best_key = ""
        best_item: dict[str, Any] | None = None
        for key, item in idx.items():
            if len(key) < 3:
                continue
            if key in nq and len(key) > len(best_key):
                best_key = key
                if isinstance(item, dict):
                    best_item = item
        if not best_item:
            return None
        name = str(best_item.get("canonical_name") or "").strip()
        return name or None

    @staticmethod
    def _extract_industry_from_query(question: str) -> str | None:
        q = str(question or "").strip()
        if not q:
            return None
        patterns = [
            r"([가-힣A-Za-z0-9\-\s]{2,20})\s*산업",
            r"([가-힣A-Za-z0-9\-\s]{2,20})\s*시장",
            r"([가-힣A-Za-z0-9\-\s]{2,20})\s*업종",
        ]
        for p in patterns:
            m = re.search(p, q)
            if not m:
                continue
            name = str(m.group(1) or "").strip()
            if name:
                return name
        return None

    def infer_companies_from_text(self, text: str, top_k: int = 5) -> list[str]:
        idx = self._load_company_master_index()
        if not idx:
            return []
        nq = self._normalize_company_name(text)
        if not nq:
            return []

        best_len_by_name: dict[str, int] = {}
        for key, item in idx.items():
            if len(key) < 3:
                continue
            if key not in nq:
                continue
            if not isinstance(item, dict):
                continue
            name = str(item.get("canonical_name") or "").strip()
            if not name:
                continue
            cur = best_len_by_name.get(name, 0)
            if len(key) > cur:
                best_len_by_name[name] = len(key)

        ranked = sorted(best_len_by_name.items(), key=lambda x: x[1], reverse=True)
        return [name for name, _ in ranked[: max(1, top_k)]]

    def _company_alias_candidates(self, company_name: str) -> set[str]:
        out: set[str] = set()
        n = self._normalize_company_name(company_name)
        if n:
            out.add(n)
        item = self._company_master_item(company_name)
        if isinstance(item, dict):
            for a in item.get("aliases") or []:
                na = self._normalize_company_name(str(a))
                if na:
                    out.add(na)
            for t in item.get("tickers") or []:
                nt = self._normalize_company_name(str(t))
                if nt:
                    out.add(nt)
        return out

    def _retrieve_for_industry_query(self, industry_name: str, question: str, top_k: int) -> list[dict[str, Any]]:
        candidates = self.retrieve(question, top_k=max(top_k * 3, 15))
        q = (industry_name or "").strip().lower()
        if not q:
            return candidates[:top_k]

        matched: list[dict[str, Any]] = []
        for row in candidates:
            text = str(row.get("text") or "").lower()
            company = str(row.get("company") or "").lower()
            if q in text or q in company:
                matched.append(row)

        if len(matched) >= min(top_k, 4):
            return matched[:top_k]

        merged: list[dict[str, Any]] = list(matched)
        existing = {id(x) for x in merged}
        for row in candidates:
            if id(row) in existing:
                continue
            merged.append(row)
            if len(merged) >= top_k:
                break
        return merged[:top_k]

    def _answer_target_question(
        self,
        company_name: str,
        question: str,
        readiness: str,
        retrieved: list[dict[str, Any]],
    ) -> str:
        if not retrieved:
            return "근거 문서가 부족해 답변을 만들 수 없습니다."

        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        limitation = (
            "현재 데이터가 부분 수준이므로 확정적 표현을 피하고, 부족한 항목을 함께 명시해라."
            if readiness == "부분"
            else "근거가 있는 범위에서만 답하고 추정은 하지 마라."
        )
        prompt = f"""
너는 한국 상장사 타겟 실사 보조 분석가다.
아래 컨텍스트만 사용해서 답해라. 모르면 '정보 부족'이라고 써라.
{limitation}
출력은 반드시 JSON 객체 하나만 출력한다.

JSON 스키마:
{{
  "answer": "한국어 3~6문장. 숫자는 가능한 경우만 포함. 마지막 문장에 데이터 한계를 한 문장으로 명시."
}}

기업명: {company_name}
질문: {question}

컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        if isinstance(raw, dict):
            text = str(raw.get("answer") or raw.get("raw") or "").strip()
            if text:
                return text
        return "정보 부족: 현재 인덱스 근거만으로는 질문에 대한 신뢰 가능한 답변을 만들기 어렵습니다."

    def _answer_industry_question(
        self,
        industry_name: str,
        question: str,
        readiness: str,
        retrieved: list[dict[str, Any]],
    ) -> str:
        if not retrieved:
            return "근거 문서가 부족해 답변을 만들 수 없습니다."

        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        limitation = (
            "현재 데이터가 부분 수준이므로 확정 수치 단정은 피하고, 추정치는 보수적으로 표현해라."
            if readiness == "부분"
            else "근거가 있는 범위에서만 답하고 추정은 하지 마라."
        )
        prompt = f"""
너는 한국 산업 분석 어시스턴트다.
아래 컨텍스트만 사용해서 답해라. 모르면 '정보 부족'이라고 써라.
{limitation}
출력은 반드시 JSON 객체 하나만 출력한다.

JSON 스키마:
{{
  "answer": "한국어 3~6문장. 가능한 경우만 숫자를 제시. 마지막 문장에 데이터 한계를 1문장으로 명시."
}}

산업명: {industry_name}
질문: {question}

컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        if isinstance(raw, dict):
            text = str(raw.get("answer") or raw.get("raw") or "").strip()
            if text:
                return text
        return "정보 부족: 현재 인덱스 근거만으로는 질문에 대한 신뢰 가능한 답변을 만들기 어렵습니다."

    def _answer_valuation_question(
        self,
        company_name: str,
        question: str,
        readiness: str,
        retrieved: list[dict[str, Any]],
    ) -> str:
        if not retrieved:
            return "근거 문서가 부족해 답변을 만들 수 없습니다."

        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        limitation = (
            "현재 데이터가 부분 수준이므로 숫자는 범위로 제시하고 가정을 명시해라."
            if readiness == "부분"
            else "근거가 있는 범위에서만 답하고 추정은 하지 마라."
        )
        prompt = f"""
너는 기업 밸류에이션 분석 어시스턴트다.
아래 컨텍스트만 사용해서 답해라. 모르면 '정보 부족'이라고 써라.
{limitation}
출력은 반드시 JSON 객체 하나만 출력한다.

JSON 스키마:
{{
  "answer": "한국어 4~8문장. 숫자는 가능한 경우에만 제시하고 단위(배, %, 억/조 등)를 포함. 마지막 문장에 데이터 한계를 1문장으로 명시."
}}

기업명: {company_name}
질문: {question}

컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        if isinstance(raw, dict):
            text = str(raw.get("answer") or raw.get("raw") or "").strip()
            if text:
                return text
        return "정보 부족: 현재 인덱스 근거만으로는 질문에 대한 신뢰 가능한 답변을 만들기 어렵습니다."

    def _answer_synergy_question(
        self,
        company_name: str,
        question: str,
        readiness: str,
        retrieved: list[dict[str, Any]],
    ) -> str:
        if not retrieved:
            return "근거 문서가 부족해 답변을 만들 수 없습니다."

        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        limitation = (
            "현재 데이터가 부분 수준이므로 수치는 범위로 제시하고 가정/한계를 명시해라."
            if readiness == "부분"
            else "근거가 있는 범위에서만 답하고 추정은 하지 마라."
        )
        prompt = f"""
너는 M&A 시너지 분석 어시스턴트다.
아래 컨텍스트만 사용해서 답해라. 모르면 '정보 부족'이라고 써라.
{limitation}
출력은 반드시 JSON 객체 하나만 출력한다.

JSON 스키마:
{{
  "answer": "한국어 4~8문장. 가능하면 정량/정성 효과를 함께 제시. 마지막 문장에 데이터 한계를 1문장으로 명시."
}}

기업명: {company_name}
질문: {question}

컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        if isinstance(raw, dict):
            text = str(raw.get("answer") or raw.get("raw") or "").strip()
            if text:
                return text
        return "정보 부족: 현재 인덱스 근거만으로는 질문에 대한 신뢰 가능한 답변을 만들기 어렵습니다."

    def _answer_due_diligence_question(
        self,
        company_name: str,
        question: str,
        readiness: str,
        retrieved: list[dict[str, Any]],
    ) -> str:
        if not retrieved:
            return "근거 문서가 부족해 답변을 만들 수 없습니다."

        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        limitation = (
            "현재 데이터가 부분 수준이므로 확정 단정보다 리스크 신호 중심으로 작성해라."
            if readiness == "부분"
            else "근거가 있는 범위에서만 답하고 추정은 하지 마라."
        )
        prompt = f"""
너는 M&A 실사(Due Diligence) 분석 어시스턴트다.
아래 컨텍스트만 사용해서 답해라. 모르면 '정보 부족'이라고 써라.
{limitation}
출력은 반드시 JSON 객체 하나만 출력한다.

JSON 스키마:
{{
  "answer": "한국어 4~8문장. 핵심 리스크, 영향도, 점검 우선순위를 포함하고 마지막 문장에 데이터 한계를 1문장으로 명시."
}}

기업명: {company_name}
질문: {question}

컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        if isinstance(raw, dict):
            text = str(raw.get("answer") or raw.get("raw") or "").strip()
            if text:
                return text
        return "정보 부족: 현재 인덱스 근거만으로는 질문에 대한 신뢰 가능한 답변을 만들기 어렵습니다."

    def _answer_strategic_question(
        self,
        company_name: str,
        question: str,
        readiness: str,
        retrieved: list[dict[str, Any]],
    ) -> str:
        if not retrieved:
            return "근거 문서가 부족해 답변을 만들 수 없습니다."

        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        limitation = (
            "현재 데이터가 부분 수준이므로 우선순위와 가정 중심으로 제시해라."
            if readiness == "부분"
            else "근거가 있는 범위에서만 답하고 추정은 하지 마라."
        )
        prompt = f"""
너는 M&A 전략 의사결정 분석 어시스턴트다.
아래 컨텍스트만 사용해서 답해라. 모르면 '정보 부족'이라고 써라.
{limitation}
출력은 반드시 JSON 객체 하나만 출력한다.

JSON 스키마:
{{
  "answer": "한국어 4~8문장. 전략 적합성, 실행 난이도, 구조 대안을 함께 제시하고 마지막 문장에 데이터 한계를 1문장으로 명시."
}}

기업명: {company_name}
질문: {question}

컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        if isinstance(raw, dict):
            text = str(raw.get("answer") or raw.get("raw") or "").strip()
            if text:
                return text
        return "정보 부족: 현재 인덱스 근거만으로는 질문에 대한 신뢰 가능한 답변을 만들기 어렵습니다."

    def _industry_market_simple_template(
        self, industry_name: str, question: str, retrieved: list[dict[str, Any]]
    ) -> dict[str, Any]:
        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        context_text = " ".join(str(c.get("text") or "") for c in retrieved)
        prompt = f"""
너는 산업 및 시장 분석가다. 아래 컨텍스트만 사용해서 Simple Version 템플릿으로 작성하라.
모르면 "정보 부족"으로 채워라. 단, 컨텍스트에 단서가 있으면 반드시 채워라.
출력은 JSON 객체 하나만.

JSON 스키마:
{{
  "summary": "string",
  "industry_overview": {{
    "industry_definition": "string",
    "major_products_services": "string",
    "industry_stage": "성장기|성숙기|도입기|쇠퇴기|정보 부족"
  }},
  "market_size": {{
    "domestic_market_size": "string",
    "global_market_size": "string",
    "recent_growth_rate": "string"
  }},
  "competitive_environment": {{
    "key_competitors": ["string"],
    "market_share": "string",
    "competition_intensity": "낮음|보통|높음|정보 부족"
  }},
  "industry_outlook": {{
    "future_growth_outlook": "string",
    "key_opportunities": ["string"],
    "key_threats": ["string"]
  }}
}}

작성 규칙:
- "산업: 정보 부족" 금지. 최소한 질문의 산업명 또는 질문 키워드로 산업 정의를 작성.
- key_competitors는 컨텍스트의 회사명에서 최소 3개를 추출 시도.
- recent_growth_rate, market_share는 숫자/퍼센트 단서가 있으면 반드시 반영.
- 숫자가 애매하면 범위/추정 표현으로 작성 가능.

질문: {question}
산업: {industry_name}
컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        out = raw if isinstance(raw, dict) else {}
        if "raw" in out and "summary" not in out:
            out = {}

        def _n(v: Any) -> str:
            s = str(v or "").strip()
            return s if s else "정보 부족"

        ov = out.get("industry_overview") if isinstance(out.get("industry_overview"), dict) else {}
        ms = out.get("market_size") if isinstance(out.get("market_size"), dict) else {}
        ce = out.get("competitive_environment") if isinstance(out.get("competitive_environment"), dict) else {}
        io = out.get("industry_outlook") if isinstance(out.get("industry_outlook"), dict) else {}

        # fallback: 산업명 보정
        fallback_industry = str(industry_name or "").strip()
        if fallback_industry in {"", "정보 부족"}:
            terms = [t for t in self._query_terms(question) if t]
            fallback_industry = terms[0] if terms else "관련 산업"

        # fallback: 회사 후보 추출
        competitors: list[str] = []
        seen_comp: set[str] = set()
        for r in retrieved:
            nm = str(r.get("company") or "").strip()
            if not nm or nm in {"정보 부족", "UNKNOWN"}:
                continue
            if self._is_ticker_like_name(nm) or self._is_masked_company_name(nm):
                continue
            if nm in seen_comp:
                continue
            seen_comp.add(nm)
            competitors.append(nm)
            if len(competitors) >= 6:
                break

        # fallback: 단순 숫자 추출
        money_matches = re.findall(r"(\d[\d,\.]*)\s*(조|억)\s*원", context_text)
        pct_matches = re.findall(r"(\d{1,2}(?:\.\d+)?)\s*%", context_text)
        domestic_market_size = "정보 부족"
        global_market_size = "정보 부족"
        if money_matches:
            v0, u0 = money_matches[0]
            domestic_market_size = f"약 {v0}{u0}원"
        if len(money_matches) >= 2:
            v1, u1 = money_matches[1]
            global_market_size = f"약 {v1}{u1}원"
        recent_growth_rate = "정보 부족"
        if pct_matches:
            recent_growth_rate = f"약 {pct_matches[0]}%"

        market_share = "정보 부족"
        share_match = re.search(r"(점유율[^\\n]{0,40}?\d{1,2}(?:\.\d+)?\s*%)", context_text)
        if share_match:
            market_share = share_match.group(1).strip()

        # fallback: 산업 단계
        text_l = context_text.lower()
        if any(k in text_l for k in ["역성장", "수요 둔화", "감소", "축소"]):
            stage = "쇠퇴기"
        elif any(k in text_l for k in ["도입", "초기", "신규"]):
            stage = "도입기"
        elif any(k in text_l for k in ["성숙", "안정", "고도화"]):
            stage = "성숙기"
        elif any(k in text_l for k in ["성장", "확대", "증가"]):
            stage = "성장기"
        else:
            stage = "정보 부족"

        # fallback: 제품/서비스
        product_hint = "정보 부족"
        keyword_map = [
            ("반도체", "반도체 소자/장비/공정 서비스"),
            ("2차전지", "2차전지 소재/셀/장비"),
            ("배관", "산업 배관 시공/설비"),
            ("클린룸", "클린룸/유틸리티 설비"),
            ("공조", "냉난방/공조 설비"),
            ("소프트웨어", "소프트웨어/플랫폼"),
            ("바이오", "바이오/제약"),
            ("자동차", "자동차/전장 부품"),
        ]
        q_and_c = f"{question} {context_text}"
        for key, desc in keyword_map:
            if key in q_and_c:
                product_hint = desc
                break

        # fallback: 기회/위협
        opp: list[str] = []
        th: list[str] = []
        if any(k in q_and_c for k in ["ai", "인공지능", "반도체", "데이터센터"]):
            opp.append("AI/디지털 전환 수요 확대")
        if any(k in q_and_c for k in ["친환경", "규제", "정책"]):
            opp.append("정책/규제 변화에 따른 신규 투자 수요")
        if any(k in q_and_c for k in ["해외", "수출", "글로벌"]):
            opp.append("해외 시장 확장 기회")
        if any(k in q_and_c for k in ["원가", "금리", "환율", "원자재"]):
            th.append("원가/금리/환율 변동 리스크")
        if any(k in q_and_c for k in ["경쟁", "포화", "진입"]):
            th.append("경쟁 심화 및 가격 압박")
        if any(k in q_and_c for k in ["규제", "인허가", "컴플라이언스"]):
            th.append("규제/인허가 불확실성")

        # LLM 값 우선 + fallback 보강
        industry_definition = _n(ov.get("industry_definition"))
        if self._is_missing(industry_definition):
            industry_definition = f"{fallback_industry}의 밸류체인(원재료-제조-유통/서비스) 기반 시장"
        major_products = _n(ov.get("major_products_services"))
        if self._is_missing(major_products):
            major_products = product_hint
        industry_stage = _n(ov.get("industry_stage"))
        if industry_stage == "정보 부족":
            industry_stage = stage

        domestic_size = _n(ms.get("domestic_market_size"))
        if self._is_missing(domestic_size):
            domestic_size = domestic_market_size
        global_size = _n(ms.get("global_market_size"))
        if self._is_missing(global_size):
            global_size = global_market_size
        growth_rate = _n(ms.get("recent_growth_rate"))
        if self._is_missing(growth_rate):
            growth_rate = recent_growth_rate

        key_comp = ce.get("key_competitors") if isinstance(ce.get("key_competitors"), list) else []
        key_comp = [str(x).strip() for x in key_comp if str(x).strip()]
        if len(key_comp) < 3:
            for nm in competitors:
                if nm not in key_comp:
                    key_comp.append(nm)
                if len(key_comp) >= 5:
                    break
        comp_intensity = _n(ce.get("competition_intensity"))
        if comp_intensity == "정보 부족":
            comp_intensity = "높음" if len(key_comp) >= 4 else ("보통" if len(key_comp) >= 2 else "낮음")
        ms_share = _n(ce.get("market_share"))
        if self._is_missing(ms_share):
            ms_share = market_share

        outlook = _n(io.get("future_growth_outlook"))
        if self._is_missing(outlook):
            outlook = (
                f"{fallback_industry}은(는) 수요/투자 사이클 영향이 커 변동성이 있으나 중장기 성장 기회가 존재"
            )
        key_opp = io.get("key_opportunities") if isinstance(io.get("key_opportunities"), list) else []
        key_opp = [str(x).strip() for x in key_opp if str(x).strip()]
        if not key_opp:
            key_opp = opp[:3] if opp else ["신규 수요처 확대 가능성"]
        key_threat = io.get("key_threats") if isinstance(io.get("key_threats"), list) else []
        key_threat = [str(x).strip() for x in key_threat if str(x).strip()]
        if not key_threat:
            key_threat = th[:3] if th else ["경쟁 심화에 따른 수익성 압박 가능성"]

        readiness = "부분" if len(retrieved) < 6 else "가능"
        summary = _n(out.get("summary"))
        if self._is_missing(summary):
            summary = self._answer_industry_question(
                industry_name=fallback_industry,
                question=question,
                readiness=readiness,
                retrieved=retrieved,
            )

        return {
            "summary": summary,
            "industry_overview": {
                "industry_definition": industry_definition,
                "major_products_services": major_products,
                "industry_stage": industry_stage,
            },
            "market_size": {
                "domestic_market_size": domestic_size,
                "global_market_size": global_size,
                "recent_growth_rate": growth_rate,
            },
            "competitive_environment": {
                "key_competitors": key_comp[:5],
                "market_share": ms_share,
                "competition_intensity": comp_intensity,
            },
            "industry_outlook": {
                "future_growth_outlook": outlook,
                "key_opportunities": key_opp[:5],
                "key_threats": key_threat[:5],
            },
        }

    def _target_overview_simple_template(
        self, company_name: str, question: str, retrieved: list[dict[str, Any]]
    ) -> dict[str, Any]:
        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        prompt = f"""
너는 타겟 기업 개요 분석가다. 아래 컨텍스트만 사용해서 Simple Version 템플릿으로 작성하라.
모르면 "정보 부족"으로 채워라. 출력은 JSON 객체 하나만.

JSON 스키마:
{{
  "summary": "string",
  "target_overview": {{
    "company_definition": "string",
    "major_products_services": "string",
    "business_stage": "성장기|성숙기|도입기|쇠퇴기|정보 부족"
  }},
  "business_structure": {{
    "major_business_units": "string",
    "revenue_mix": "string",
    "key_customers": "string"
  }},
  "financial_overview": {{
    "revenue": "string",
    "operating_income": "string",
    "ebitda": "string",
    "recent_growth_rate": "string"
  }},
  "risk_disclosure": {{
    "key_risks": ["string"],
    "recent_disclosures": ["string"],
    "watchpoints": ["string"]
  }}
}}

질문: {question}
기업: {company_name}
컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        return raw if isinstance(raw, dict) else {"summary": "정보 부족"}

    def _valuation_simple_template(self, company_name: str, question: str, retrieved: list[dict[str, Any]]) -> dict[str, Any]:
        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        prompt = f"""
너는 밸류에이션 분석가다. 아래 컨텍스트만 사용해서 Simple Version 템플릿으로 작성하라.
모르면 "정보 부족"으로 채워라. 출력은 JSON 객체 하나만.

JSON 스키마:
{{
  "summary": "string",
  "financial_summary": {{
    "revenue": "string",
    "operating_income": "string",
    "ebitda": "string",
    "recent_growth_rate": "string"
  }},
  "multiple_comparison": {{
    "applied_multiple": "string",
    "peer_average": "string",
    "target_value_range": "string"
  }},
  "fair_value_range": {{
    "conservative_scenario": "string",
    "base_scenario": "string",
    "aggressive_scenario": "string"
  }}
}}

질문: {question}
기업: {company_name}
컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        out = raw if isinstance(raw, dict) else {}
        if "raw" in out and "summary" not in out:
            out = {}

        def _n(v: Any) -> str:
            s = str(v or "").strip()
            return s if s else "정보 부족"

        def _extract_first(pattern: str) -> str:
            m = re.search(pattern, context, flags=re.IGNORECASE)
            return m.group(1).strip() if m else ""

        financial = out.get("financial_summary") if isinstance(out.get("financial_summary"), dict) else {}
        multiple = out.get("multiple_comparison") if isinstance(out.get("multiple_comparison"), dict) else {}
        fair = out.get("fair_value_range") if isinstance(out.get("fair_value_range"), dict) else {}

        # Reuse evidence backfill to populate revenue/op income/market cap when LLM output is sparse.
        seed_answer = {
            "template_id": self.ENTRY_TEMPLATE_VALUATION,
            "template_name": self.ENTRY_TEMPLATE_NAME_VALUATION,
            "company_name": company_name,
            "summary": _n(out.get("summary")),
            "market_cap": "정보 부족",
            "revenue": "정보 부족",
            "operating_income": "정보 부족",
            "net_income": "정보 부족",
            "ebitda": "정보 부족",
            "financial_snapshot": {
                "market_cap": "정보 부족",
                "revenue": "정보 부족",
                "operating_income": "정보 부족",
                "net_income": "정보 부족",
            },
            "sources": [],
        }
        filled = self._backfill_answer_from_evidence(
            answer=seed_answer,
            retrieved=retrieved,
            company_hint=company_name if company_name != "정보 부족" else None,
            question=question,
        )
        snap = filled.get("financial_snapshot") if isinstance(filled.get("financial_snapshot"), dict) else {}

        rev = _n(financial.get("revenue"))
        if self._is_missing(rev):
            rev = _n(snap.get("revenue"))
        opi = _n(financial.get("operating_income"))
        if self._is_missing(opi):
            opi = _n(snap.get("operating_income"))
        ebitda = _n(financial.get("ebitda"))
        if self._is_missing(ebitda):
            ebitda = _n(filled.get("ebitda"))
        growth = _n(financial.get("recent_growth_rate"))
        if self._is_missing(growth):
            growth = _extract_first(r"(CAGR[^,\n]{0,40})") or _extract_first(r"(\d{1,2}(?:\.\d+)?\s*%)") or "정보 부족"

        applied_multiple = _n(multiple.get("applied_multiple"))
        if self._is_missing(applied_multiple):
            applied_multiple = _extract_first(r"(EV/?EBITDA\s*\d+(?:\.\d+)?배)") or _extract_first(r"(PER\s*\d+(?:\.\d+)?배)") or "정보 부족"
        peer_average = _n(multiple.get("peer_average"))
        if self._is_missing(peer_average):
            peer_average = _extract_first(r"(peer[^,\n]{0,40}\d+(?:\.\d+)?배)") or "정보 부족"
        target_value_range = _n(multiple.get("target_value_range"))
        if self._is_missing(target_value_range):
            mc = _n(snap.get("market_cap"))
            target_value_range = f"시가총액 기준 참고: {mc}" if mc != "정보 부족" else "정보 부족"

        conservative = _n(fair.get("conservative_scenario"))
        base = _n(fair.get("base_scenario"))
        aggressive = _n(fair.get("aggressive_scenario"))
        if self._is_missing(base):
            mc = _n(snap.get("market_cap"))
            if mc != "정보 부족":
                base = f"시가총액 기준 참고: {mc}"
        if self._is_missing(conservative) and base != "정보 부족":
            conservative = f"{base} 대비 보수 할인 시나리오"
        if self._is_missing(aggressive) and base != "정보 부족":
            aggressive = f"{base} 대비 공격 프리미엄 시나리오"

        summary = _n(out.get("summary"))
        if self._is_missing(summary):
            readiness = "부분" if len(retrieved) < 6 else "가능"
            summary = self._answer_valuation_question(company_name, question, readiness, retrieved)

        return {
            "summary": summary,
            "financial_summary": {
                "revenue": rev,
                "operating_income": opi,
                "ebitda": ebitda,
                "recent_growth_rate": growth,
            },
            "multiple_comparison": {
                "applied_multiple": applied_multiple,
                "peer_average": peer_average,
                "target_value_range": target_value_range,
            },
            "fair_value_range": {
                "conservative_scenario": conservative,
                "base_scenario": base,
                "aggressive_scenario": aggressive,
            },
        }

    def _synergy_simple_template(self, company_name: str, question: str, retrieved: list[dict[str, Any]]) -> dict[str, Any]:
        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        prompt = f"""
너는 두 회사 간 시너지 분석가다. 아래 컨텍스트만 사용해서 Simple Version 템플릿으로 작성하라.
모르면 "정보 부족"으로 채워라. 출력은 JSON 객체 하나만.
단, 근거가 조금이라도 있으면 "정보 부족"만 반복하지 말고 제한사항을 명시한 보수적 문장으로 채워라.

JSON 스키마:
{{
  "summary": "string",
  "strategic_fit": {{
    "business_complementarity": "string",
    "customer_market_overlap": "string"
  }},
  "revenue_synergy": {{
    "new_customer_potential": "string",
    "product_service_combination_effect": "string"
  }},
  "cost_synergy": {{
    "organization_integration_effect": "string",
    "purchasing_production_efficiency": "string"
  }},
  "overall_synergy_judgment": {{
    "feasibility": "높음|보통|낮음|정보 부족",
    "expected_realization_period": "string"
  }}
}}

질문: {question}
대상: {company_name}
컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        out = raw if isinstance(raw, dict) else {}
        if "raw" in out and "summary" not in out:
            out = {}

        def _n(v: Any) -> str:
            s = str(v or "").strip()
            return s if s else "정보 부족"

        norms = [self._normalize_company_name(x) for x in re.split(r"\s*\+\s*", company_name) if str(x).strip()]
        seen_companies = [self._normalize_company_name(str(r.get("company") or "")) for r in retrieved]
        matched = 0
        for n in norms:
            if n and any((n in c) or (c and c in n) for c in seen_companies):
                matched += 1
        coverage = "양사 근거 일부 확인" if matched >= 2 else ("단일사 중심 근거" if matched == 1 else "기업 매칭 근거 부족")
        readiness = "부분" if len(retrieved) < 6 else "가능"
        fallback_summary = self._answer_synergy_question(company_name, question, readiness, retrieved)
        feasibility = "보통" if matched >= 2 else ("낮음" if matched == 1 else "정보 부족")
        period = "12~24개월(근거 제한)" if matched >= 1 else "정보 부족"

        strategic_fit = out.get("strategic_fit") if isinstance(out.get("strategic_fit"), dict) else {}
        revenue_synergy = out.get("revenue_synergy") if isinstance(out.get("revenue_synergy"), dict) else {}
        cost_synergy = out.get("cost_synergy") if isinstance(out.get("cost_synergy"), dict) else {}
        overall = out.get("overall_synergy_judgment") if isinstance(out.get("overall_synergy_judgment"), dict) else {}

        return {
            "summary": _n(out.get("summary")) if _n(out.get("summary")) != "정보 부족" else fallback_summary,
            "strategic_fit": {
                "business_complementarity": _n(strategic_fit.get("business_complementarity"))
                if _n(strategic_fit.get("business_complementarity")) != "정보 부족"
                else f"{coverage}: 사업 포트폴리오 보완성은 추가 검증 필요",
                "customer_market_overlap": _n(strategic_fit.get("customer_market_overlap"))
                if _n(strategic_fit.get("customer_market_overlap")) != "정보 부족"
                else f"{coverage}: 고객/시장 중첩도는 정량 데이터 보강 필요",
            },
            "revenue_synergy": {
                "new_customer_potential": _n(revenue_synergy.get("new_customer_potential"))
                if _n(revenue_synergy.get("new_customer_potential")) != "정보 부족"
                else "교차판매 가능성은 있으나 고객 세그먼트 데이터 추가 필요",
                "product_service_combination_effect": _n(revenue_synergy.get("product_service_combination_effect"))
                if _n(revenue_synergy.get("product_service_combination_effect")) != "정보 부족"
                else "제품/서비스 결합 효과는 상용화 시나리오 검증 필요",
            },
            "cost_synergy": {
                "organization_integration_effect": _n(cost_synergy.get("organization_integration_effect"))
                if _n(cost_synergy.get("organization_integration_effect")) != "정보 부족"
                else "조직 통합에 따른 비용 절감 여지는 있으나 PMI 계획 필요",
                "purchasing_production_efficiency": _n(cost_synergy.get("purchasing_production_efficiency"))
                if _n(cost_synergy.get("purchasing_production_efficiency")) != "정보 부족"
                else "구매/생산 효율화는 공급망 데이터 확보 후 추정 가능",
            },
            "overall_synergy_judgment": {
                "feasibility": _n(overall.get("feasibility")) if _n(overall.get("feasibility")) != "정보 부족" else feasibility,
                "expected_realization_period": _n(overall.get("expected_realization_period"))
                if _n(overall.get("expected_realization_period")) != "정보 부족"
                else period,
            },
        }

    def _due_diligence_simple_template(
        self, company_name: str, question: str, retrieved: list[dict[str, Any]]
    ) -> dict[str, Any]:
        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        context_text = " ".join(str(c.get("text") or "") for c in retrieved).lower()
        prompt = f"""
너는 M&A 리스크 및 실사 분석가다. 아래 컨텍스트만 사용해서 Simple Version 템플릿으로 작성하라.
모르면 "정보 부족"으로 채워라. 출력은 JSON 객체 하나만.

JSON 스키마:
{{
  "summary": "string",
  "financial_risk": {{
    "earnings_volatility": "string",
    "debt_level": "string"
  }},
  "legal_regulatory_risk": {{
    "litigation_status": "string",
    "regulatory_impact": "string"
  }},
  "operational_risk": {{
    "key_person_dependency": "string",
    "major_customer_dependency": "string"
  }},
  "integration_risk": {{
    "organizational_culture_gap": "string",
    "system_integration_difficulty": "string"
  }}
}}

질문: {question}
기업: {company_name}
컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        out = raw if isinstance(raw, dict) else {}
        if "raw" in out and "summary" not in out:
            out = {}

        def _n(v: Any) -> str:
            s = str(v or "").strip()
            return s if s else "정보 부족"

        fr = out.get("financial_risk") if isinstance(out.get("financial_risk"), dict) else {}
        lr = out.get("legal_regulatory_risk") if isinstance(out.get("legal_regulatory_risk"), dict) else {}
        orisk = out.get("operational_risk") if isinstance(out.get("operational_risk"), dict) else {}
        ir = out.get("integration_risk") if isinstance(out.get("integration_risk"), dict) else {}

        seed_answer = {
            "template_id": self.ENTRY_TEMPLATE_DUE_DILIGENCE_RISK,
            "template_name": self.ENTRY_TEMPLATE_NAME_DUE_DILIGENCE_RISK,
            "company_name": company_name,
            "summary": _n(out.get("summary")),
            "market_cap": "정보 부족",
            "revenue": "정보 부족",
            "operating_income": "정보 부족",
            "net_income": "정보 부족",
            "ebitda": "정보 부족",
            "financial_snapshot": {
                "market_cap": "정보 부족",
                "revenue": "정보 부족",
                "operating_income": "정보 부족",
                "net_income": "정보 부족",
            },
            "sources": [],
        }
        filled = self._backfill_answer_from_evidence(
            answer=seed_answer,
            retrieved=retrieved,
            company_hint=company_name if company_name != "정보 부족" else None,
            question=question,
        )
        snap = filled.get("financial_snapshot") if isinstance(filled.get("financial_snapshot"), dict) else {}

        earnings_volatility = _n(fr.get("earnings_volatility"))
        if self._is_missing(earnings_volatility):
            growth = str(filled.get("revenue_operating_income_5y_trend") or "").strip()
            earnings_volatility = growth if growth else "실적 변동성은 추가 재무 데이터 검증 필요"

        debt_level = _n(fr.get("debt_level"))
        if self._is_missing(debt_level):
            if any(k in context_text for k in ["부채 없음", "무차입", "대출금 없음"]):
                debt_level = "부채 부담 낮음(무차입/대출금 없음 단서)"
            elif any(k in context_text for k in ["차입", "부채", "대출"]):
                debt_level = "차입/부채 관련 단서 존재(상세 수치 추가 확인 필요)"
            else:
                debt_level = "정보 부족"

        litigation_status = _n(lr.get("litigation_status"))
        if self._is_missing(litigation_status):
            if any(k in context_text for k in ["소송", "분쟁", "법적", "법무"]):
                litigation_status = "법률 분쟁/소송 관련 단서 존재 여부 추가 확인 필요"
            else:
                litigation_status = "중대한 소송 단서는 현재 근거에서 확인되지 않음"

        regulatory_impact = _n(lr.get("regulatory_impact"))
        if self._is_missing(regulatory_impact):
            if any(k in context_text for k in ["규제", "인허가", "컴플라이언스", "면허"]):
                regulatory_impact = "규제/인허가 준수 영향 존재 가능"
            else:
                regulatory_impact = "규제 영향 단서는 제한적"

        key_person_dependency = _n(orisk.get("key_person_dependency"))
        if self._is_missing(key_person_dependency):
            if any(k in context_text for k in ["대표", "창업자", "오너", "핵심 인력"]):
                key_person_dependency = "대표/핵심 인력 의존 가능성 존재"
            else:
                key_person_dependency = "핵심 인력 의존도 정보 부족"

        major_customer_dependency = _n(orisk.get("major_customer_dependency"))
        if self._is_missing(major_customer_dependency):
            if any(k in context_text for k in ["주요거래처", "대기업", "상위 고객", "고객사"]):
                major_customer_dependency = "주요 고객 집중도 존재 가능(세부 비중 확인 필요)"
            else:
                major_customer_dependency = "고객 집중도 정보 부족"

        org_culture_gap = _n(ir.get("organizational_culture_gap"))
        if self._is_missing(org_culture_gap):
            org_culture_gap = "통합 시 조직문화/의사결정 방식 차이 점검 필요"

        system_integration_difficulty = _n(ir.get("system_integration_difficulty"))
        if self._is_missing(system_integration_difficulty):
            system_integration_difficulty = "IT/업무 프로세스 통합 난이도는 실사 단계에서 확인 필요"

        summary = _n(out.get("summary"))
        if self._is_missing(summary):
            readiness = "부분" if len(retrieved) < 6 else "가능"
            summary = self._answer_due_diligence_question(company_name, question, readiness, retrieved)

        # Use financial snapshot clue when still sparse.
        if debt_level == "정보 부족":
            mc = _n(snap.get("market_cap"))
            if mc != "정보 부족":
                debt_level = f"재무 스냅샷 존재(시가총액 {mc}), 부채 상세는 추가 확인 필요"

        return {
            "summary": summary,
            "financial_risk": {
                "earnings_volatility": earnings_volatility,
                "debt_level": debt_level,
            },
            "legal_regulatory_risk": {
                "litigation_status": litigation_status,
                "regulatory_impact": regulatory_impact,
            },
            "operational_risk": {
                "key_person_dependency": key_person_dependency,
                "major_customer_dependency": major_customer_dependency,
            },
            "integration_risk": {
                "organizational_culture_gap": org_culture_gap,
                "system_integration_difficulty": system_integration_difficulty,
            },
        }

    def _strategic_decision_simple_template(
        self, company_name: str, question: str, retrieved: list[dict[str, Any]]
    ) -> dict[str, Any]:
        context = "\n\n".join(
            f"[source={c['source']}, company={c['company']}, market={c['market']}, score={c['score']}]\n{c['text']}"
            for c in retrieved
        )
        context_text = " ".join(str(c.get("text") or "") for c in retrieved).lower()
        prompt = f"""
너는 M&A 전략 의사결정 어드바이저다. 아래 컨텍스트만 사용해서 Simple Version 템플릿으로 작성하라.
모르면 "정보 부족"으로 채워라. 출력은 JSON 객체 하나만.

JSON 스키마:
{{
  "summary": "string",
  "acquisition_necessity": {{
    "strategic_rationale": "string",
    "expected_effect": "string"
  }},
  "financial_feasibility": {{
    "return_potential": "string",
    "risk_level": "string"
  }},
  "key_risks_summary": {{
    "top3_key_risks": ["string"]
  }},
  "final_opinion": {{
    "decision": "Go|Conditional Go|No-Go|정보 부족",
    "conditions": "string"
  }}
}}

질문: {question}
기업: {company_name}
컨텍스트:
{context}
""".strip()
        raw = self.client.generate_json(settings.ollama_chat_model, prompt)
        out = raw if isinstance(raw, dict) else {}
        if "raw" in out and "summary" not in out:
            out = {}

        def _n(v: Any) -> str:
            s = str(v or "").strip()
            return s if s else "정보 부족"

        an = out.get("acquisition_necessity") if isinstance(out.get("acquisition_necessity"), dict) else {}
        ff = out.get("financial_feasibility") if isinstance(out.get("financial_feasibility"), dict) else {}
        kr = out.get("key_risks_summary") if isinstance(out.get("key_risks_summary"), dict) else {}
        fo = out.get("final_opinion") if isinstance(out.get("final_opinion"), dict) else {}

        seed_answer = {
            "template_id": self.ENTRY_TEMPLATE_STRATEGIC_DECISION,
            "template_name": self.ENTRY_TEMPLATE_NAME_STRATEGIC_DECISION,
            "company_name": company_name,
            "summary": _n(out.get("summary")),
            "market_cap": "정보 부족",
            "revenue": "정보 부족",
            "operating_income": "정보 부족",
            "net_income": "정보 부족",
            "ebitda": "정보 부족",
            "financial_snapshot": {
                "market_cap": "정보 부족",
                "revenue": "정보 부족",
                "operating_income": "정보 부족",
                "net_income": "정보 부족",
            },
            "key_risks": [],
            "sources": [],
        }
        filled = self._backfill_answer_from_evidence(
            answer=seed_answer,
            retrieved=retrieved,
            company_hint=company_name if company_name != "정보 부족" else None,
            question=question,
        )
        snap = filled.get("financial_snapshot") if isinstance(filled.get("financial_snapshot"), dict) else {}

        strategic_rationale = _n(an.get("strategic_rationale"))
        if self._is_missing(strategic_rationale):
            if any(k in context_text for k in ["성장", "확대", "진출", "신사업", "시너지"]):
                strategic_rationale = "신규 성장 동력 확보 및 사업 포트폴리오 확장 목적"
            else:
                strategic_rationale = "전략적 목적의 타당성은 추가 근거 확인 필요"

        expected_effect = _n(an.get("expected_effect"))
        if self._is_missing(expected_effect):
            expected_effect = "매출 기반 확대 및 중장기 수익성 개선 가능성(근거 제한)"

        return_potential = _n(ff.get("return_potential"))
        if self._is_missing(return_potential):
            rev = _n(snap.get("revenue"))
            opi = _n(snap.get("operating_income"))
            if rev != "정보 부족" or opi != "정보 부족":
                return_potential = f"재무 스냅샷 기반 수익성 검토 가능(매출 {rev}, 영업이익 {opi})"
            else:
                return_potential = "수익성 추정 근거 제한"

        risk_level = _n(ff.get("risk_level"))
        if self._is_missing(risk_level):
            if any(k in context_text for k in ["소송", "규제", "부채", "차입", "의존"]):
                risk_level = "보통~높음"
            else:
                risk_level = "보통"

        top3 = kr.get("top3_key_risks") if isinstance(kr.get("top3_key_risks"), list) else []
        top3 = [str(x).strip() for x in top3 if str(x).strip()]
        if len(top3) < 3:
            candidates = []
            if any(k in context_text for k in ["규제", "인허가", "컴플라이언스"]):
                candidates.append("규제/인허가 리스크")
            if any(k in context_text for k in ["고객", "거래처", "집중"]):
                candidates.append("고객 집중도 리스크")
            if any(k in context_text for k in ["원가", "환율", "금리", "원자재"]):
                candidates.append("원가/거시 변수 리스크")
            if any(k in context_text for k in ["통합", "조직", "시스템"]):
                candidates.append("PMI 통합 실행 리스크")
            if any(k in context_text for k in ["인력", "대표", "창업자"]):
                candidates.append("핵심 인력 의존 리스크")
            for c in candidates:
                if c not in top3:
                    top3.append(c)
                if len(top3) >= 3:
                    break
        if not top3:
            top3 = ["규제/컴플라이언스 리스크", "수익성 변동 리스크", "통합 실행 리스크"]

        decision = _n(fo.get("decision"))
        if decision not in {"Go", "Conditional Go", "No-Go"}:
            decision = "Conditional Go" if len(retrieved) >= 4 else "정보 부족"

        conditions = _n(fo.get("conditions"))
        if self._is_missing(conditions):
            conditions = "실사 완료 후 가격/구조 조건 확정, 핵심 리스크 완화 계획 수립 필요"

        summary = _n(out.get("summary"))
        if self._is_missing(summary):
            readiness = "부분" if len(retrieved) < 6 else "가능"
            summary = self._answer_strategic_question(company_name, question, readiness, retrieved)

        return {
            "summary": summary,
            "acquisition_necessity": {
                "strategic_rationale": strategic_rationale,
                "expected_effect": expected_effect,
            },
            "financial_feasibility": {
                "return_potential": return_potential,
                "risk_level": risk_level,
            },
            "key_risks_summary": {
                "top3_key_risks": top3[:3],
            },
            "final_opinion": {
                "decision": decision,
                "conditions": conditions,
            },
        }

    @staticmethod
    def _is_ticker_like_name(name: str) -> bool:
        n = str(name or "").strip().upper()
        if not n:
            return True
        if n in {"정보 부족", "UNKNOWN", "N/A"}:
            return True
        if re.fullmatch(r"[0-9A-Z]{4,}\.(KQ|KS|KR|US|JP|HK)", n):
            return True
        if re.fullmatch(r"[0-9]{6}", n):
            return True
        return False

    @staticmethod
    def _is_masked_company_name(name: str) -> bool:
        n = str(name or "").strip()
        if not n:
            return True
        # e.g. "주식회사 00산업", "OO테크", "**산업", "XX전자"
        compact = re.sub(r"\s+", "", n)
        if re.search(r"(주식회사)?[0O〇○]{2,}", compact, re.IGNORECASE):
            return True
        if re.search(r"(주식회사)?[xX\*]{2,}", compact):
            return True
        if re.search(r"(주식회사)?[oO]{2,}", compact):
            return True
        if "익명" in compact or "비공개" in compact:
            return True
        return False

    @staticmethod
    def to_korean_readable(answer: dict[str, Any]) -> str:
        template_id = str(answer.get("template_id") or RagPipeline.ENTRY_TEMPLATE_COMPANY_OVERVIEW)
        template_name = str(answer.get("template_name") or RagPipeline.ENTRY_TEMPLATE_NAME_COMPANY_OVERVIEW)
        if template_id == RagPipeline.ENTRY_TEMPLATE_TARGET_OVERVIEW:
            tv = answer.get("target_overview") if isinstance(answer.get("target_overview"), dict) else {}
            bs = answer.get("business_structure") if isinstance(answer.get("business_structure"), dict) else {}
            fo = answer.get("financial_overview") if isinstance(answer.get("financial_overview"), dict) else {}
            rd = answer.get("risk_disclosure") if isinstance(answer.get("risk_disclosure"), dict) else {}
            company_name = str(answer.get("company_name") or "정보 부족")
            sources = answer.get("sources") if isinstance(answer.get("sources"), list) else []
            summary = str(answer.get("summary") or "정보 부족")
            lines = [
                f"업체명: {company_name}",
                f"[기본 템플릿: {template_name} ({template_id})]",
                "1.1 기업 개요",
                f"- 산업 정의: {tv.get('company_definition', '정보 부족')}",
                f"- 주요 제품/서비스: {tv.get('major_products_services', '정보 부족')}",
                f"- 산업 단계: {tv.get('business_stage', '정보 부족')}",
                "1.2 사업 구조",
                f"- 주요 사업부: {bs.get('major_business_units', '정보 부족')}",
                f"- 매출 구성: {bs.get('revenue_mix', '정보 부족')}",
                f"- 주요 고객: {bs.get('key_customers', '정보 부족')}",
                "1.3 재무 요약",
                f"- 매출: {fo.get('revenue', '정보 부족')}",
                f"- 영업이익: {fo.get('operating_income', '정보 부족')}",
                f"- EBITDA: {fo.get('ebitda', '정보 부족')}",
                f"- 최근 성장률: {fo.get('recent_growth_rate', '정보 부족')}",
                "1.4 리스크 및 공시",
                "- 핵심 리스크: " + (", ".join(str(x) for x in (rd.get("key_risks") or [])) if rd.get("key_risks") else "정보 부족"),
                "- 최근 주요 공시: " + (", ".join(str(x) for x in (rd.get("recent_disclosures") or [])) if rd.get("recent_disclosures") else "정보 부족"),
                "- 체크포인트: " + (", ".join(str(x) for x in (rd.get("watchpoints") or [])) if rd.get("watchpoints") else "정보 부족"),
                f"대상: {company_name}",
                f"분석 요약: {summary}",
                "출처: " + (", ".join(str(x) for x in sources) if sources else "정보 부족"),
            ]
            return "\n".join(lines)
        six_templates = {
            RagPipeline.ENTRY_TEMPLATE_TARGET_OVERVIEW,
            RagPipeline.ENTRY_TEMPLATE_INDUSTRY_MARKET,
            RagPipeline.ENTRY_TEMPLATE_VALUATION,
            RagPipeline.ENTRY_TEMPLATE_SYNERGY_PAIR,
            RagPipeline.ENTRY_TEMPLATE_DUE_DILIGENCE_RISK,
            RagPipeline.ENTRY_TEMPLATE_STRATEGIC_DECISION,
        }
        if template_id in six_templates and template_id != RagPipeline.ENTRY_TEMPLATE_TARGET_OVERVIEW:
            summary = str(answer.get("summary") or "정보 부족")
            company_name = str(answer.get("company_name") or "정보 부족")
            industry_name = str(answer.get("industry_name") or "정보 부족")
            sources = answer.get("sources") if isinstance(answer.get("sources"), list) else []
            lines = [f"업체명: {company_name}", f"[기본 템플릿: {template_name} ({template_id})]"]
            if template_id == RagPipeline.ENTRY_TEMPLATE_INDUSTRY_MARKET:
                ov = answer.get("industry_overview") if isinstance(answer.get("industry_overview"), dict) else {}
                ms = answer.get("market_size") if isinstance(answer.get("market_size"), dict) else {}
                ce = answer.get("competitive_environment") if isinstance(answer.get("competitive_environment"), dict) else {}
                out = answer.get("industry_outlook") if isinstance(answer.get("industry_outlook"), dict) else {}
                lines.extend(
                    [
                        "2.1 산업 개요",
                        f"- 산업 정의: {ov.get('industry_definition', '정보 부족')}",
                        f"- 주요 제품/서비스: {ov.get('major_products_services', '정보 부족')}",
                        f"- 산업 단계: {ov.get('industry_stage', '정보 부족')}",
                        "2.2 시장 규모",
                        f"- 국내 시장 규모: {ms.get('domestic_market_size', '정보 부족')}",
                        f"- 글로벌 시장 규모: {ms.get('global_market_size', '정보 부족')}",
                        f"- 최근 성장률: {ms.get('recent_growth_rate', '정보 부족')}",
                        "2.3 경쟁 환경",
                        "- 주요 경쟁사: " + (", ".join(str(x) for x in (ce.get("key_competitors") or [])) if ce.get("key_competitors") else "정보 부족"),
                        f"- 시장 점유율: {ce.get('market_share', '정보 부족')}",
                        f"- 경쟁 강도: {ce.get('competition_intensity', '정보 부족')}",
                        "2.4 산업 전망",
                        f"- 향후 성장 전망: {out.get('future_growth_outlook', '정보 부족')}",
                        "- 주요 기회 요인: " + (", ".join(str(x) for x in (out.get("key_opportunities") or [])) if out.get("key_opportunities") else "정보 부족"),
                        "- 주요 위협 요인: " + (", ".join(str(x) for x in (out.get("key_threats") or [])) if out.get("key_threats") else "정보 부족"),
                    ]
                )
            elif template_id == RagPipeline.ENTRY_TEMPLATE_VALUATION:
                fs = answer.get("financial_summary") if isinstance(answer.get("financial_summary"), dict) else {}
                mc = answer.get("multiple_comparison") if isinstance(answer.get("multiple_comparison"), dict) else {}
                fr = answer.get("fair_value_range") if isinstance(answer.get("fair_value_range"), dict) else {}
                lines.extend(
                    [
                        "3.1 재무 요약",
                        f"- 매출: {fs.get('revenue', '정보 부족')}",
                        f"- 영업이익: {fs.get('operating_income', '정보 부족')}",
                        f"- EBITDA: {fs.get('ebitda', '정보 부족')}",
                        f"- 최근 성장률: {fs.get('recent_growth_rate', '정보 부족')}",
                        "3.2 멀티플 비교",
                        f"- 적용 멀티플: {mc.get('applied_multiple', '정보 부족')}",
                        f"- 비교 기업 평균: {mc.get('peer_average', '정보 부족')}",
                        f"- 타겟 적용 가치 범위: {mc.get('target_value_range', '정보 부족')}",
                        "3.3 적정 가치 범위",
                        f"- 보수적 시나리오: {fr.get('conservative_scenario', '정보 부족')}",
                        f"- 기준 시나리오: {fr.get('base_scenario', '정보 부족')}",
                        f"- 공격적 시나리오: {fr.get('aggressive_scenario', '정보 부족')}",
                    ]
                )
            elif template_id == RagPipeline.ENTRY_TEMPLATE_SYNERGY_PAIR:
                sf = answer.get("strategic_fit") if isinstance(answer.get("strategic_fit"), dict) else {}
                rs = answer.get("revenue_synergy") if isinstance(answer.get("revenue_synergy"), dict) else {}
                cs = answer.get("cost_synergy") if isinstance(answer.get("cost_synergy"), dict) else {}
                oj = answer.get("overall_synergy_judgment") if isinstance(answer.get("overall_synergy_judgment"), dict) else {}
                lines.extend(
                    [
                        "4.1 전략적 적합성",
                        f"- 사업 보완성: {sf.get('business_complementarity', '정보 부족')}",
                        f"- 고객/시장 중첩 여부: {sf.get('customer_market_overlap', '정보 부족')}",
                        "4.2 매출 시너지",
                        f"- 신규 고객 확보 가능성: {rs.get('new_customer_potential', '정보 부족')}",
                        f"- 제품/서비스 결합 효과: {rs.get('product_service_combination_effect', '정보 부족')}",
                        "4.3 비용 시너지",
                        f"- 인력/조직 통합 효과: {cs.get('organization_integration_effect', '정보 부족')}",
                        f"- 구매/생산 효율화: {cs.get('purchasing_production_efficiency', '정보 부족')}",
                        "4.4 종합 시너지 판단",
                        f"- 실현 가능성: {oj.get('feasibility', '정보 부족')}",
                        f"- 예상 실현 기간: {oj.get('expected_realization_period', '정보 부족')}",
                    ]
                )
            elif template_id == RagPipeline.ENTRY_TEMPLATE_DUE_DILIGENCE_RISK:
                fr = answer.get("financial_risk") if isinstance(answer.get("financial_risk"), dict) else {}
                lr = answer.get("legal_regulatory_risk") if isinstance(answer.get("legal_regulatory_risk"), dict) else {}
                orisk = answer.get("operational_risk") if isinstance(answer.get("operational_risk"), dict) else {}
                ir = answer.get("integration_risk") if isinstance(answer.get("integration_risk"), dict) else {}
                lines.extend(
                    [
                        "5.1 재무 리스크",
                        f"- 수익 변동성: {fr.get('earnings_volatility', '정보 부족')}",
                        f"- 부채 수준: {fr.get('debt_level', '정보 부족')}",
                        "5.2 법률/규제 리스크",
                        f"- 소송 여부: {lr.get('litigation_status', '정보 부족')}",
                        f"- 규제 영향: {lr.get('regulatory_impact', '정보 부족')}",
                        "5.3 운영 리스크",
                        f"- 핵심 인력 의존도: {orisk.get('key_person_dependency', '정보 부족')}",
                        f"- 주요 고객 의존도: {orisk.get('major_customer_dependency', '정보 부족')}",
                        "5.4 통합 리스크",
                        f"- 조직 문화 차이: {ir.get('organizational_culture_gap', '정보 부족')}",
                        f"- 시스템 통합 난이도: {ir.get('system_integration_difficulty', '정보 부족')}",
                    ]
                )
            elif template_id == RagPipeline.ENTRY_TEMPLATE_STRATEGIC_DECISION:
                an = answer.get("acquisition_necessity") if isinstance(answer.get("acquisition_necessity"), dict) else {}
                ff = answer.get("financial_feasibility") if isinstance(answer.get("financial_feasibility"), dict) else {}
                kr = answer.get("key_risks_summary") if isinstance(answer.get("key_risks_summary"), dict) else {}
                fo = answer.get("final_opinion") if isinstance(answer.get("final_opinion"), dict) else {}
                lines.extend(
                    [
                        "6.1 인수 필요성",
                        f"- 전략적 이유: {an.get('strategic_rationale', '정보 부족')}",
                        f"- 기대 효과: {an.get('expected_effect', '정보 부족')}",
                        "6.2 재무적 타당성",
                        f"- 투자 대비 수익 가능성: {ff.get('return_potential', '정보 부족')}",
                        f"- 리스크 수준: {ff.get('risk_level', '정보 부족')}",
                        "6.3 주요 리스크 요약",
                        "- 핵심 리스크 3가지: " + (", ".join(str(x) for x in (kr.get("top3_key_risks") or [])) if kr.get("top3_key_risks") else "정보 부족"),
                        "6.4 최종 의견",
                        f"- Go / Conditional Go / No-Go: {fo.get('decision', '정보 부족')}",
                        f"- 조건 제시: {fo.get('conditions', '정보 부족')}",
                    ]
                )
            if industry_name != "정보 부족":
                lines.append(f"산업: {industry_name}")
            if company_name != "정보 부족":
                lines.append(f"대상: {company_name}")
            lines.append(f"분석 요약: {summary}")
            lines.append("출처: " + (", ".join(str(x) for x in sources) if sources else "정보 부족"))
            return "\n".join(lines)
        if template_id == RagPipeline.ENTRY_TEMPLATE_PEER_LIST:
            listed = answer.get("listed_companies") if isinstance(answer.get("listed_companies"), list) else []
            unlisted = answer.get("unlisted_companies") if isinstance(answer.get("unlisted_companies"), list) else []
            rev_ebitda = answer.get("revenue_ebitda_comparison") if isinstance(answer.get("revenue_ebitda_comparison"), list) else []
            multiples = answer.get("multiple_comparison") if isinstance(answer.get("multiple_comparison"), list) else []
            industry_def = str(answer.get("industry_definition") or "정보 부족")
            screening = answer.get("screening_conditions") if isinstance(answer.get("screening_conditions"), list) else []
            lines = [f"[기본 템플릿: {template_name} ({template_id})]"]
            lines.append(str(answer.get("summary") or "정보 부족"))
            lines.append("")
            lines.append(f"산업 정의: {industry_def}")
            lines.append("스크리닝 조건: " + (", ".join(str(x) for x in screening) if screening else "정보 부족"))
            lines.append("상장사 리스트:")
            if listed:
                for idx, r in enumerate(listed, start=1):
                    company = str(r.get("company") or "정보 부족")
                    market = str(r.get("market") or "정보 부족")
                    fit = r.get("strategic_fit_score")
                    fit_txt = f"{fit}점" if isinstance(fit, int) else "N/A"
                    reason = str(r.get("reason") or "정보 부족")
                    lines.append(f"{idx}. {company} ({market}) | 적합성 {fit_txt} | {reason}")
            else:
                lines.append("- 정보 부족")

            lines.append("비상장사 리스트:")
            if unlisted:
                for idx, r in enumerate(unlisted, start=1):
                    company = str(r.get("company") or "정보 부족")
                    market = str(r.get("market") or "정보 부족")
                    lines.append(f"{idx}. {company} ({market})")
            else:
                lines.append("- 정보 부족")

            lines.append("매출/EBITDA 비교표:")
            if rev_ebitda:
                lines.append("회사 | 매출 | EBITDA")
                for r in rev_ebitda:
                    lines.append(
                        f"{str(r.get('company') or '정보 부족')} | "
                        f"{str(r.get('revenue') or '정보 부족')} | "
                        f"{str(r.get('ebitda') or '정보 부족')}"
                    )
            else:
                lines.append("- 정보 부족")

            lines.append("멀티플 비교:")
            if multiples:
                lines.append("회사 | EV/EBITDA | PER")
                for r in multiples:
                    lines.append(
                        f"{str(r.get('company') or '정보 부족')} | "
                        f"{str(r.get('ev_ebitda') or '정보 부족')} | "
                        f"{str(r.get('per') or '정보 부족')}"
                    )
            else:
                lines.append("- 정보 부족")
            return "\n".join(lines)

        name = str(answer.get("company_name") or "해당 기업")
        if RagPipeline._is_ticker_like_name(name) or RagPipeline._is_masked_company_name(name):
            lines = [
                "업체명: 정보 부족",
                f"[기본 템플릿: {template_name} ({template_id})]",
                "1.1 회사 식별",
                "- 상태: 업체명이 확인되지 않아 내부 정보를 사용할 수 없습니다.",
                "1.2 기업 개요",
                "- 회사 개요: 정보 부족",
                "- 사업부 구조: 정보 부족",
                "1.3 재무 요약",
                "- 매출/영업이익 5년 추이: 정보 부족",
                "- EBITDA: 정보 부족",
                "- 시가총액: 정보 부족",
                "- 재무 스냅샷: 시가총액 정보 부족, 매출 정보 부족, 영업이익 정보 부족, 순이익 정보 부족",
                "1.4 경쟁/리스크",
                "- 경쟁사: 정보 부족",
                "- 핵심 리스크: 정보 부족",
                "- 최근 주요 공시: 정보 부족",
                "- 유사 기업: 정보 부족",
                "1.5 요약",
                "- 요약: 업체명 확인 후 다시 질의해 주세요.",
                "- 핵심 포인트: 정보 부족",
                "출처: 정보 부족",
            ]
            return "\n".join(lines)
        market = str(answer.get("market") or "정보 부족")
        summary = str(answer.get("summary") or "정보 부족")
        company_overview = str(answer.get("company_overview") or summary or "정보 부족")
        business_structure = str(answer.get("business_structure") or "정보 부족")
        rev_op_trend = str(answer.get("revenue_operating_income_5y_trend") or "정보 부족")
        ebitda = str(answer.get("ebitda") or "정보 부족")
        market_cap_top = str(answer.get("market_cap") or "정보 부족")

        fs = answer.get("financial_snapshot") if isinstance(answer.get("financial_snapshot"), dict) else {}
        market_cap = fs.get("market_cap", "정보 부족")
        revenue = fs.get("revenue", "정보 부족")
        op_income = fs.get("operating_income", "정보 부족")
        net_income = fs.get("net_income", "정보 부족")

        highlights = answer.get("highlights") if isinstance(answer.get("highlights"), list) else []
        risks = answer.get("risks") if isinstance(answer.get("risks"), list) else []
        similar = answer.get("similar_companies") if isinstance(answer.get("similar_companies"), list) else []
        competitors = answer.get("competitors") if isinstance(answer.get("competitors"), list) else similar
        key_risks = answer.get("key_risks") if isinstance(answer.get("key_risks"), list) else risks
        recent_disclosures = answer.get("recent_disclosures") if isinstance(answer.get("recent_disclosures"), list) else []
        sources = answer.get("sources") if isinstance(answer.get("sources"), list) else []

        lines = [
            f"업체명: {name}",
            f"[기본 템플릿: {template_name} ({template_id})]",
            "1.1 기업 개요",
            f"- 시장: {market}",
            f"- 회사 개요: {company_overview}",
            f"- 사업부 구조: {business_structure}",
            "1.2 재무 요약",
            f"- 매출/영업이익 5년 추이: {rev_op_trend}",
            f"- EBITDA: {ebitda}",
            f"- 시가총액: {market_cap_top if market_cap_top != '정보 부족' else market_cap}",
            "- 재무 스냅샷: "
            f"시가총액 {market_cap}, 매출 {revenue}, 영업이익 {op_income}, 순이익 {net_income}",
            "1.3 경쟁/리스크",
            "- 경쟁사: " + (", ".join(str(x) for x in competitors) if competitors else "정보 부족"),
            "- 핵심 리스크: " + (", ".join(str(x) for x in key_risks) if key_risks else "정보 부족"),
            "- 최근 주요 공시: " + (", ".join(str(x) for x in recent_disclosures) if recent_disclosures else "정보 부족"),
            "- 유사 기업: " + (", ".join(str(x) for x in similar) if similar else "정보 부족"),
            "1.4 요약",
            f"- 분석 요약: {summary}",
            "- 핵심 포인트: " + (", ".join(str(x) for x in highlights) if highlights else "정보 부족"),
            "출처: " + (", ".join(str(x) for x in sources) if sources else "정보 부족"),
        ]
        return "\n".join(lines)
