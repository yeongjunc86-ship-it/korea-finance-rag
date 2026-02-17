# Valuation Analysis 21-30 Plan

Version: v1.0  
Last Updated: 2026-02-16

## 1) 목표
밸류에이션 10문항(21~30)에 대해 `가능/부분/불가` 기준으로 일관된 응답을 제공하고, 데이터 부족 구간은 전용 파이프라인으로 보강한다.

## 2) 질문별 데이터 매핑

| ID | 질문 | 현재 상태 | 핵심 데이터 소스 | 구현 우선순위 |
|---|---|---|---|---|
| 21 | 적정 EV/EBITDA 범위 | 부분~가능 | `valuation_case_*`, `valuation_*` | P1 |
| 22 | 유사 거래 comps 비교 | 부분~가능 | `mna_*` | P1 |
| 23 | WACC 요소 정리 | 부분~가능 | `valuation_case_*`, Yahoo/DART | P1 |
| 24 | 보수/중립/공격 가치 | 부분~가능 | `valuation_case_*` | P1 |
| 25 | 프리미엄 20% IRR | 부분~가능 | `valuation_case_*` | P1 |
| 26 | 시너지 전/후 가치 | 부분~가능 | `valuation_case_*`, 외부 시너지 데이터 | P1 |
| 27 | 업계 PER 할인/할증 요인 | 부분~가능 | `valuation_case_*`, `valuation_*` | P1 |
| 28 | 환율 영향 | 부분~가능 | `valuation_case_*`, `fx_*`, `macro_*` | P2 |
| 29 | LBO 레버리지 한도 | 부분~가능 | `valuation_case_*` | P2 |
| 30 | EBITDA 조정 항목 검토 | 부분~가능 | `dart_notes_*`, `valuation_case_*` | P1 |

## 3) 이번 반영(완료)
- `/api/valuation-analysis` 구현
- `scripts/build_valuation_cases.py` 추가 (`valuation_case_*`)
- `scripts/import_mna_comps_external.py` 추가 (`mna_*`)
- `run_external_enrichment.sh`에 mna import 단계 연결
- `run_full_collection.sh`에 valuation_case 생성 단계 연결

## 4) 다음 구현 단계
- P1-1: `valuation_case` 계산식 고도화(FCF, 순부채, 희석주식수 반영)
- P1-2: comps 표준 스키마 확장(거래구조, Earn-out, Minority discount)
- P2-1: 실시간 금리/환율 연동(`macro_*`, `fx_*`)
- P2-2: LBO 구조 시뮬레이터(상환 스케줄/이자커버리지/Exit multiple)

