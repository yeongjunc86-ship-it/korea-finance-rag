# DUE_DILIGENCE_41_50_PLAN

## 목표
- 리스크 및 실사 10문항(41~50)에 대해 질문별 준비도(`가능/부분/불가`)와 근거 기반 답변을 일관된 형식으로 제공한다.

## 범위
- API: `POST /api/due-diligence-analysis`
- 질문 세트: 41~50
- 데이터 소스 우선순위:
  - 1순위: `due_diligence_case_*`
  - 2순위: `dart_notes_*`, `dart_*`
  - 3순위: `tax_*`, `security_*`, `supply_chain_*`, `pmi_fail_*`, `news_*`, `mna_*`, `esg_*`

## 구현 항목
1. 스키마/라우터
- `app/schemas.py`에 요청/응답 모델 추가
- `app/routers/api.py`에 `/api/due-diligence-analysis` 엔드포인트 추가

2. 파이프라인
- `app/services/rag_pipeline.py`
  - 41~50 질문 템플릿 추가
  - 질문별 준비도 로직 추가
  - 질문별 fallback 안내문 추가
  - 실사 전용 응답 프롬프트 추가

3. 데이터 생성
- `scripts/build_due_diligence_cases.py`
  - Yahoo + DART 주석 기반 리스크 신호를 `due_diligence_case_*` raw JSON으로 생성

4. 평가
- 평가셋: `eval/due_diligence_analysis_41_50_v1.jsonl` (30건)
- 평가 스크립트: `scripts/eval_due_diligence_analysis.py`
  - 실무형 가중치 적용(47/48/50 가중치 상향)
  - 케이스 점수 임계치 + 전체 가중 PASS 기준
  - 핵심 질문(47/48/50) 게이트 적용

5. 운영/자동화
- `scripts/run_full_collection.sh`에 실사 케이스 생성 단계 연결
- 관리자 콘솔 작업 연결:
  - `build_due_diligence_cases`
  - `eval_due_diligence_analysis`

6. 문서
- `README.md` API/생성/평가/운영 옵션 반영
- `API_CONTRACT_SPEC.md` 엔드포인트 계약 반영
- `PROJECT_REPORT_2026-02-14.txt` 작업 요약 반영

## 완료 기준
- API 호출 시 41~50 전 문항 결과 반환
- 질문별 `readiness`, `answer`, `evidence_sources` 필드가 누락 없이 채워짐
- 평가 스크립트 실행 및 결과 JSON 생성
- 수집 파이프라인/관리자 UI에서 실사 작업 실행 가능
