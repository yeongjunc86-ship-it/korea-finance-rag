# Industry Analysis 11-20 Plan

Version: v1.0  
Last Updated: 2026-02-16

## 1) 목표
산업 및 시장 분석 10문항(11~20)을 `가능/부분/불가`로 일관되게 응답하고, 정량 질문은 데이터셋 확장으로 단계적 고도화한다.

## 2) 질문별 데이터 매핑

| ID | 질문 | 현재 상태 | 핵심 데이터 소스 | 구현 우선순위 |
|---|---|---|---|---|
| 11 | 최근 5년 CAGR/향후 5년 전망 | 부분 | 뉴스, 공시, 재무 시계열 | P1 |
| 12 | 산업 진입장벽 | 부분 | 뉴스, 공시 | P1 |
| 13 | 밸류에이션 멀티플 평균 | 부분~가능 | `valuation_*` 데이터셋 | P1 |
| 14 | 최근 M&A 사례 | 부분 | 뉴스(M&A 태깅), 공시 | P1 |
| 15 | 규제 변화 영향 | 부분 | 규제/정책 데이터, 뉴스 | P1 |
| 16 | TAM/SAM/SOM | 부분~가능 | `tam_*` 데이터셋 + 가정모델 | P1 |
| 17 | 해외 vs 국내 플레이어 | 부분 | 글로벌 기업 데이터, 뉴스 | P1 |
| 18 | 기술 트렌드 변화 | 부분 | 뉴스, 리서치 본문 | P1 |
| 19 | 원자재 가격 영향 | 부분~가능 | `commodity_*` 민감도 데이터 | P1 |
| 20 | 경기침체 방어력 | 부분 | 재무 시계열, 매크로 지표 | P1 |

## 3) 이번 반영(완료)
- `/api/industry-analysis` 구현
- 질문별 `readiness` 반환
- 질문별 근거 source 파일 목록 반환
- `README.md`, `API_CONTRACT_SPEC.md` 반영
- `scripts/fetch_industry_valuation.py` 추가 (`valuation_*`)
- `scripts/fetch_industry_tamsam.py` 추가 (`tam_*`)
- `scripts/fetch_industry_commodity_sensitivity.py` 추가 (`commodity_*`)
- `scripts/run_industry_special_pipeline.sh` 추가 (3개 파이프라인 묶음 실행)
- `run_full_collection.sh`에 산업 전용 단계 연결
- `scripts/run_external_enrichment.sh` 추가 (시장점유율/특허/ESG 외부 데이터 병합)

## 4) 다음 구현 단계
- P1-1: 뉴스 문서 태깅(규제/M&A/기술트렌드)
- P1-2: 산업별 플레이어 사전(국내/해외) 구축
- P1-3: 경기민감도 계산 지표(매출 변동성, OCF 방어력) 정의
- P1-4: 산업별 가정치 검증(도메인 전문가 피드백 반영)
- P2-1: 외부 상용 시장리포트 연동으로 13/16 정밀도 향상
- P2-2: 원자재 시세 실시간 연동 + 업종별 COGS 매핑 고도화
