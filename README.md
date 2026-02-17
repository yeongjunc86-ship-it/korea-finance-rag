# Korea Finance RAG (Skeleton)

KOSPI/KOSDAQ/기타 상장사 데이터를 수집하고, Ollama(`llama3`) 기반 RAG로 정형 응답을 제공하는 웹 서비스 기본 뼈대입니다.

## 1. 설치
```bash
cd /home/aidome/workspace/korea-finance-rag
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## 2. 데이터 수집
```bash
# Yahoo Finance 기본정보/시세 수집
python scripts/fetch_yahoo.py --tickers 005930.KS 000660.KS 035420.KS

# DART 기업개요/공시 메타 수집 (API KEY 필요)
python scripts/fetch_dart.py --corp-codes 00126380 00164779

# 최신 뉴스 수집 (기업명 기반 RSS)
python scripts/fetch_news.py --limit-company 100 --per-company 2 --resume
```

## 2-1. 대량 수집(권장)
```bash
# 1) 한국 상장사 유니버스 생성 (KOSPI + KOSDAQ)
python scripts/build_universe.py

# 2) Yahoo 대량 수집 (이어받기 모드)
python scripts/fetch_yahoo.py \
  --tickers-file data/processed/korea_tickers_all.txt \
  --resume \
  --sleep 0.15

# 3) DART 대량 수집 (선택, API KEY 필요)
python scripts/fetch_dart_bulk.py --resume --sleep 0.25

# 4) 최신 뉴스 대량 수집 (권장)
python scripts/fetch_news.py \
  --universe-file data/processed/korea_universe.json \
  --limit-company 200 \
  --per-company 2 \
  --resume \
  --sleep 0.2
```

팁:
- 먼저 `--limit 100`으로 소규모 검증 후 전체 수집을 권장합니다.
- 중단 후 재실행 시 `--resume`으로 기존 파일을 건너뜁니다.

## 2-2. Yahoo 완료 후 DART 자동 실행 + tmux
```bash
cd /home/aidome/workspace/korea-finance-rag
source .venv/bin/activate

# .env에 DART_API_KEY가 있으면 자동 로드됨
tmux new -s finance_collect -d \
  "bash -lc 'set -a; [ -f .env ] && source .env; set +a; ./scripts/run_full_collection.sh'"

# 로그 확인
tmux attach -t finance_collect
# 분리: Ctrl+b, d
```

추가 모니터링:
```bash
tail -f logs/full_collection_*.log
```

기본 동작:
- `run_full_collection.sh`는 Yahoo -> DART -> DART 주석 파싱 -> DART 재무(5Y) 수집 -> 5개년 재무 팩트 생성 -> News -> 고객의존도 프로파일 생성 -> 산업(13/16/19) -> 밸류 케이스(21~30) -> 시너지 케이스(31~40) -> 실사 케이스(41~50) -> 전략 케이스(51~60) -> (옵션)외부데이터 병합 -> 회사 마스터 생성 -> 증분 인덱싱(`build_index_incremental.py`)까지 자동 실행합니다.

뉴스 수집 비활성화 옵션:
```bash
NEWS_ENABLED=0 ./scripts/run_full_collection.sh
```

증분 인덱싱 비활성화 옵션:
```bash
INDEX_ENABLED=0 ./scripts/run_full_collection.sh
```

산업(13/16/19) 전용 파이프라인 비활성화 옵션:
```bash
INDUSTRY_SPECIAL_ENABLED=0 ./scripts/run_full_collection.sh
```

DART 주석 파싱 비활성화 옵션:
```bash
DART_NOTES_ENABLED=0 ./scripts/run_full_collection.sh
```

DART 재무(5Y) 수집 비활성화 옵션:
```bash
DART_FINANCIALS_ENABLED=0 ./scripts/run_full_collection.sh
```

5개년 재무 팩트 생성 비활성화 옵션:
```bash
FINANCIALS_5Y_ENABLED=0 ./scripts/run_full_collection.sh
```

DART 재무 수집 연도 수 변경:
```bash
DART_FINANCIALS_YEARS=5 ./scripts/run_full_collection.sh
```

고객의존도 프로파일 생성 비활성화 옵션:
```bash
CUSTOMER_DEPENDENCY_ENABLED=0 ./scripts/run_full_collection.sh
```

고객의존도 LLM 추출 활성화 옵션(OpenAI/Gemini):
```bash
CUSTOMER_DEPENDENCY_LLM_ENABLED=1 \
CUSTOMER_DEPENDENCY_LLM_PROVIDER=openai \
CUSTOMER_DEPENDENCY_LLM_MODEL=gpt-4o-mini \
CUSTOMER_DEPENDENCY_LLM_ALLOW_EMPTY_CONTEXT=1 \
./scripts/run_full_collection.sh
```

외부 데이터 병합 활성화 옵션:
```bash
EXTERNAL_ENRICH_ENABLED=1 EXTERNAL_DIR=data/external ./scripts/run_full_collection.sh
```

밸류에이션 케이스 생성 비활성화 옵션:
```bash
VALUATION_CASE_ENABLED=0 ./scripts/run_full_collection.sh
```

시너지 케이스 생성 비활성화 옵션:
```bash
SYNERGY_CASE_ENABLED=0 ./scripts/run_full_collection.sh
```

실사 케이스 생성 비활성화 옵션:
```bash
DUE_DILIGENCE_CASE_ENABLED=0 ./scripts/run_full_collection.sh
```

전략 케이스 생성 비활성화 옵션:
```bash
STRATEGIC_CASE_ENABLED=0 ./scripts/run_full_collection.sh
```

회사 마스터(업종 정규화) 생성 비활성화 옵션:
```bash
COMPANY_MASTER_ENABLED=0 ./scripts/run_full_collection.sh
```

## 2-3. 산업 전용 데이터(13/16/19) 생성
```bash
# 13) 멀티플(valuation_*.json)
python scripts/fetch_industry_valuation.py \
  --industries "반도체,바이오,2차전지,자동차" \
  --min-samples 3 \
  --resume

# 16) TAM/SAM/SOM(tam_*.json)
python scripts/fetch_industry_tamsam.py \
  --industries "반도체,바이오,2차전지,자동차" \
  --tam-multiplier 2.0 \
  --sam-ratio 0.35 \
  --som-ratio 0.10 \
  --min-samples 3 \
  --resume

# 19) 원자재 민감도(commodity_*.json)
python scripts/fetch_industry_commodity_sensitivity.py \
  --industries "반도체,바이오,2차전지,자동차" \
  --resume
```

한 번에 실행:
```bash
./scripts/run_industry_special_pipeline.sh
```

## 2-4. DART 주석 테이블 파싱(고객의존도/사업부/CAPEX/부채만기)
```bash
python scripts/parse_dart_notes.py --resume
```

출력:
- `data/raw/dart_notes_*.json`

## 2-5. DART 재무제표 API 수집(최근 5년)
```bash
python scripts/fetch_dart_financials.py --years 5 --resume --sleep 0.25
```

출력:
- `data/raw/dart_financials_{corp_code}_{year}_CFS.json`

## 2-6. 회사별 5개년 재무 팩트 테이블 생성
```bash
python scripts/build_company_financials_5y.py --write-raw
```

출력:
- `data/processed/company_financials_5y.jsonl`
- `data/raw/financials_5y_*.json` (인덱스 반영용)

## 2-7. 고객의존도 프로파일 생성(상위 고객/매출의존도)
```bash
python scripts/build_customer_dependency.py --write-raw
```

출력:
- `data/processed/customer_dependency_facts.jsonl`
- `data/raw/customer_dependency_*.json` (인덱스 반영용)

## 2-7-1. 고객의존도 LLM 추출(OpenAI/Gemini)
```bash
# OpenAI
OPENAI_API_KEY=... \
python scripts/extract_customer_dependency_llm.py \
  --provider openai \
  --model gpt-4o-mini \
  --limit 50 \
  --resume

# Gemini
GEMINI_API_KEY=... \
python scripts/extract_customer_dependency_llm.py \
  --provider gemini \
  --model gemini-1.5-flash \
  --limit 50 \
  --resume
```

출력:
- `data/raw/customer_dependency_llm_*.json`

주의:
- LLM 결과는 `verification_status`가 `unverified_llm`일 수 있으므로, 투자/실사 의사결정에는 원문 근거 재검증이 필요합니다.

## 2-8. 외부 데이터 병합(시장점유율/특허/ESG/고객의존도)
입력 템플릿:
- `data/external/README.md`
- `data/external/customer_dependency.csv.example`
- `data/external/customer_reports/README.md`

병합 실행:
```bash
./scripts/run_external_enrichment.sh
```

고객의존도 단독 적재:
```bash
python scripts/import_customer_dependency_external.py --input-csv data/external/customer_dependency.csv --resume
python scripts/import_customer_dependency_reports.py --input-dir data/external/customer_reports --resume
```

출력:
- `data/raw/market_share_*.json`
- `data/raw/mna_*.json`
- `data/raw/patent_*.json`
- `data/raw/esg_*.json`
- `data/raw/customer_dependency_external_*.json`

## 2-9. 밸류에이션 케이스(21~30) 생성
```bash
python scripts/build_valuation_cases.py --resume
```

출력:
- `data/raw/valuation_case_*.json`

## 2-10. 시너지 케이스(31~40) 생성
```bash
python scripts/build_synergy_cases.py --resume
```

출력:
- `data/raw/synergy_case_*.json`

## 2-11. 실사 케이스(41~50) 생성
```bash
python scripts/build_due_diligence_cases.py --resume
```

출력:
- `data/raw/due_diligence_case_*.json`

## 2-12. 전략 의사결정 케이스(51~60) 생성
```bash
python scripts/build_strategic_cases.py --resume
```

출력:
- `data/raw/strategic_case_*.json`

## 2-13. 회사 마스터(업종/섹션 메타) 생성
```bash
python scripts/build_company_master.py
```

출력:
- `data/processed/company_master.json`

## 3. 인덱스 생성
```bash
# (권장) raw 데이터를 공통 매니페스트로 정규화
python scripts/normalize_manifest.py

# 전체 재생성(권장: 최초 1회, 모델 변경 시 필수)
./scripts/run_index_full.sh

# 증분 갱신(권장: 주기 실행)
./scripts/run_index_incremental.sh

# (마이그레이션) 기존 인덱스를 유지한 채 상태파일만 생성
python scripts/sync_index_state.py
```

상세 정책: `INDEXING_STRATEGY.md`

## 3-1. 검색 품질 베이스라인 평가
```bash
# 전체 케이스 실행
python scripts/eval_search_baseline.py

# 빠른 점검(상위 10개)
python scripts/eval_search_baseline.py --limit 10
```

평가 기준 문서: `QUALITY_BASELINE_SPEC.md`
케이스 파일: `eval/baseline_questions_v1.jsonl`

## 3-3. 타겟 10문항 평가셋
```bash
python scripts/eval_target_analysis.py \
  --cases eval/target_analysis_questions_v1.jsonl \
  --out logs/eval_target_analysis_latest.json
```

## 3-4. 밸류 10문항(21~30) 평가셋
```bash
python scripts/eval_valuation_analysis.py \
  --cases eval/valuation_analysis_21_30_v1.jsonl \
  --out logs/eval_valuation_analysis_latest.json \
  --case-score-threshold 0.7 \
  --overall-pass-threshold 80
```

## 3-5. 시너지 10문항(31~40) 평가셋
```bash
python scripts/eval_synergy_analysis.py \
  --cases eval/synergy_analysis_31_40_v1.jsonl \
  --out logs/eval_synergy_analysis_latest.json \
  --case-score-threshold 0.7 \
  --overall-pass-threshold 80
```

## 3-6. 실사 10문항(41~50) 평가셋
```bash
python scripts/eval_due_diligence_analysis.py \
  --cases eval/due_diligence_analysis_41_50_v1.jsonl \
  --out logs/eval_due_diligence_analysis_latest.json \
  --case-score-threshold 0.7 \
  --overall-pass-threshold 80
```

## 3-7. 전략 의사결정 10문항(51~60) 평가셋
```bash
python scripts/eval_strategic_analysis.py \
  --cases eval/strategic_analysis_51_60_v1.jsonl \
  --out logs/eval_strategic_analysis_latest.json \
  --case-score-threshold 0.7 \
  --overall-pass-threshold 80
```

## 3-2. 운영 최소 요건
```bash
# 헬스 체크(인덱스 버전/문서수 포함)
curl -s http://127.0.0.1:8000/api/health
```

운영 표준 문서: `OPS_MINIMUM_SPEC.md`

## 4. 서버 실행
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

브라우저에서 `http://localhost:8000` 접속.
- 로그인 화면: `http://localhost:8000/login`
- 데이터 관리 콘솔: `http://localhost:8000/admin` (관리자 로그인 필요)
- 독립 업체검색 화면: `http://localhost:8000/company-search`
- 관리자 콘솔에서 지원:
  - 뉴스 수집 옵션 입력(`limit_company`, `per_company`, `sleep`, `resume`) 후 실행
  - 기준 평가 옵션 입력(`limit`) 후 실행
  - 업체검색 AI 설정(OpenAI/Gemini 사용 여부) 저장

### 4-1. 관리자 계정 생성/갱신
```bash
source .venv/bin/activate
python scripts/create_admin_user.py --email admin@local --name 관리자
```

기본 시드 계정(최초 실행 시 자동 생성):
- 이메일: `admin@local`
- 비밀번호: `admin1234!`

### 4-2. 독립 업체검색 API
- `POST /api/company-search`
  - 입력: `prompt`, `top_k`, `txt_content(optional)`
  - 출력: `local_results`(보유 벡터DB), `ai_results`(OpenAI/Gemini), `ai_providers_enabled`
- `POST /api/company-search/register-ai-results` (관리자 권한 필요)
  - AI 결과를 `data/raw/ai_company_search_*.json`으로 저장하고 즉시 임베딩/인덱스 반영

## 5. 정형 응답 포맷
LLM 응답은 아래 JSON 스키마를 목표로 생성됩니다.
- `company_name`
- `market`
- `summary`
- `highlights` (list)
- `financial_snapshot`
- `risks`
- `sources`
- `similar_companies`

## 5-1. 타겟 기업 10문항 분석 API
M&A 실사형 질문(성장률, 고객의존도, CAPEX, 부채만기, ESG 등)용 전용 API입니다.

```bash
curl -s -X POST http://127.0.0.1:8000/api/target-analysis \
  -H "Content-Type: application/json" \
  -d '{"company_name":"삼성전자","top_k_per_question":6}'
```

응답 필드:
- `results[].readiness`: `가능|부분|불가`
- `results[].answer`: 질문별 한국어 설명(근거 부족 시 한계 명시)
- `results[].evidence_sources`: 사용된 raw 파일 경로 목록

## 5-2. 산업/시장 10문항 분석 API (11~20)
산업 분석 질문(CAGR, 진입장벽, 멀티플, M&A, TAM/SAM/SOM 등) 전용 API입니다.

```bash
curl -s -X POST http://127.0.0.1:8000/api/industry-analysis \
  -H "Content-Type: application/json" \
  -d '{"industry_name":"반도체","top_k_per_question":6}'
```

응답 필드:
- `results[].readiness`: `가능|부분|불가`
- `results[].answer`: 질문별 한국어 설명(근거 부족 시 한계 명시)
- `results[].evidence_sources`: 사용된 raw 파일 경로 목록

## 5-3. 밸류에이션 10문항 분석 API (21~30)
밸류에이션 질문(EV/EBITDA, comps, WACC, 시나리오 가치, IRR, LBO 등) 전용 API입니다.

```bash
curl -s -X POST http://127.0.0.1:8000/api/valuation-analysis \
  -H "Content-Type: application/json" \
  -d '{"company_name":"삼성전자","top_k_per_question":6}'
```

## 5-4. 시너지 10문항 분석 API (31~40)
시너지 질문(매출/비용 시너지, PMI 기간, IT 통합비, 브랜드/법무 리스크 등) 전용 API입니다.

```bash
curl -s -X POST http://127.0.0.1:8000/api/synergy-analysis \
  -H "Content-Type: application/json" \
  -d '{"company_name":"삼성전자","top_k_per_question":6}'
```

## 5-5. 리스크/실사 10문항 분석 API (41~50)
리스크·실사 질문(우발채무, 매출인식, 세무, CoC 조항, 보안, 공급망, PMI 실패요인 등) 전용 API입니다.

```bash
curl -s -X POST http://127.0.0.1:8000/api/due-diligence-analysis \
  -H "Content-Type: application/json" \
  -d '{"company_name":"삼성전자","top_k_per_question":6}'
```

## 5-6. 전략 의사결정 10문항 분석 API (51~60)
전략 질문(전략적/재무적 인수 성격, 포트폴리오 적합성, Exit, 기회비용, Earn-out, 딜 구조 등) 전용 API입니다.

```bash
curl -s -X POST http://127.0.0.1:8000/api/strategic-analysis \
  -H "Content-Type: application/json" \
  -d '{"company_name":"삼성전자","top_k_per_question":6}'
```

## 6. 다음 확장 포인트
- 기업 메타데이터(산업군/시총/매출 등) 정규화
- 임베딩 캐시 및 증분 인덱싱
- 재랭킹 추가(BM25 + 벡터 하이브리드)
- 답변 감사 로그/프롬프트 버전관리
