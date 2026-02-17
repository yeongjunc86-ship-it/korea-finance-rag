# Synergy Analysis 31-40 Plan

Version: v1.0  
Last Updated: 2026-02-16

## 1) 목표
시너지 10문항(31~40)에 대해 `가능/부분/불가` 기준으로 일관된 응답을 제공하고, PMI/통합비용/법무구조 관련 데이터 공백을 단계적으로 축소한다.

## 2) 질문별 데이터 매핑

| ID | 질문 | 현재 상태 | 핵심 데이터 소스 | 구현 우선순위 |
|---|---|---|---|---|
| 31 | 매출 시너지 항목 | 부분~가능 | `synergy_case_*`, `market_share_*`, `patent_*` | P1 |
| 32 | 비용 시너지 항목 | 부분~가능 | `synergy_case_*`, `dart_notes_*` | P1 |
| 33 | 인력 중복 구조 | 부분~가능 | `synergy_case_*`, `dart_notes_*` | P1 |
| 34 | 유통망 통합 절감 | 부분~가능 | `synergy_case_*` | P1 |
| 35 | IT 통합 비용 | 부분~가능 | `synergy_case_*` | P1 |
| 36 | 시너지 실현 기간 | 부분~가능 | `synergy_case_*` | P1 |
| 37 | Cross-selling 가능성 | 부분~가능 | `synergy_case_*`, `market_share_*`, `patent_*` | P1 |
| 38 | 브랜드 통합 리스크 | 부분~가능 | `synergy_case_*`, `esg_*`, `news_*` | P1 |
| 39 | 조달 단가 통합 효과 | 부분~가능 | `synergy_case_*` | P1 |
| 40 | 중복 법인/법무 구조 | 부분~가능 | `synergy_case_*`, `dart_notes_*`, `mna_*` | P1 |

## 3) 이번 반영(완료)
- `/api/synergy-analysis` 구현
- `scripts/build_synergy_cases.py` 추가 (`synergy_case_*`)
- `eval/synergy_analysis_31_40_v1.jsonl` 추가 (30건)
- `scripts/eval_synergy_analysis.py` 추가
- 관리자 콘솔에 생성/평가 작업 연결

## 4) 다음 구현 단계
- P1-1: 실제 조직도/인력 데이터 연결(중복 인력 정확도 향상)
- P1-2: IT 자산 인벤토리/라이선스 데이터 연결(통합비 정밀화)
- P1-3: 법인/계약/소송 구조 데이터 연결(40번 신뢰도 강화)
- P2-1: PMI 타임라인 실적 기반 학습 모델 도입

