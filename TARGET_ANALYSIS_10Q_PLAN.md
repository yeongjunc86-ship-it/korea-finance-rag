# Target Analysis 10Q Plan

Version: v1.0  
Last Updated: 2026-02-16

## 1) 목표
타겟 기업 분석 10문항을 `가능/부분/불가`로 명확히 구분하고, 부족 데이터는 수집 파이프라인에 단계적으로 편입한다.

## 2) 질문별 데이터 매핑

| ID | 질문 | 현재 상태 | 핵심 데이터 소스 | 구현 우선순위 |
|---|---|---|---|---|
| 1 | 최근 5년 매출 성장률/EBITDA 마진 | 부분~가능 | Yahoo, DART 재무 | P1 |
| 2 | 매출 고객 Top10/의존도 | 불가~부분 | DART 주석 테이블(고객), IR 자료 | P1 |
| 3 | 사업부별 매출 비중/수익성 | 부분 | DART 세그먼트 주석 | P1 |
| 4 | 최근 3년 CAPEX/투자 방향 | 부분 | DART 현금흐름/주석 | P1 |
| 5 | 현금흐름 취약점 | 부분 | DART+Yahoo 재무지표 | P1 |
| 6 | 부채 만기/리파이낸싱 리스크 | 부분 | DART 만기 스케줄 | P1 |
| 7 | 경쟁사/점유율 비교 | 불가 | 시장점유율 외부 DB | P2 |
| 8 | 핵심 기술/특허 | 불가 | 특허 DB(KIPRIS 등) | P2 |
| 9 | 소송/분쟁 이력 | 부분 | DART 공시, 뉴스 | P1 |
| 10 | ESG 리스크 | 부분 | DART/뉴스 + ESG 전용 DB | P2 |

## 3) 이번 반영(완료)
- `/api/target-analysis` 구현
- 10문항 고정 템플릿 + 질문별 `readiness` 반환
- 질문별 근거 source 파일 목록 반환
- `README.md`, `API_CONTRACT_SPEC.md`에 계약 반영
- `scripts/parse_dart_notes.py` 추가 (`dart_notes_*`)
- `eval/target_analysis_questions_v1.jsonl` 추가
- `scripts/eval_target_analysis.py` 추가
- 외부 데이터 파이프라인 추가 (`market_share_*`, `patent_*`, `esg_*`)

## 4) 다음 구현 단계
- P1-1: DART 본문/주석 테이블 파서 강화 (`고객 의존도`, `사업부`, `CAPEX`, `만기구조`)
- P1-2: 질문별 계산 필드 정식화 (`revenue_cagr_5y`, `ebitda_margin_series`, `debt_maturity_buckets`)
- P1-3: 검증 셋 자동 실행을 관리자 작업으로 연결
- P2-1: 외부 시장점유율 데이터셋 적재 파이프라인(연결 완료, 정밀 소스 확장 필요)
- P2-2: 특허/ESG 데이터셋 적재 파이프라인(연결 완료, 정밀 소스 확장 필요)
