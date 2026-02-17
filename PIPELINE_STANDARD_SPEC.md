# Pipeline Standard Spec (Phase 1)

Version: v1.0
Last Updated: 2026-02-15
Scope: data ingestion and indexing standardization

## 1. Stage Overview
- `raw`: 외부 원천(JSON)을 원본 보존
- `processed`: 공통 스키마/유니버스/매니페스트 생성
- `index`: RAG 검색용 청크+임베딩 생성

Flow:
1. `build_universe.py` -> 분석 대상 티커 목록 생성
2. `fetch_yahoo.py`, `fetch_dart_bulk.py` -> `data/raw/*.json` 적재
3. `fetch_news.py` -> `data/raw/news_*.json` 최신 뉴스 적재
4. `normalize_manifest.py` -> `data/processed/normalized_manifest.jsonl`
5. `build_index.py` -> `data/index/chunks.jsonl`

## 2. Directory Contract
- `data/raw/`
  - naming:
    - Yahoo: `yahoo_{ticker_with_underscore}.json` (예: `yahoo_005930_KS.json`)
    - DART: `dart_{corp_code}.json` (예: `dart_00126380.json`)
    - News: `news_{id}.json` (예: `news_ab12cd34ef56.json`)
- `data/processed/`
  - `korea_universe.json`
  - `korea_tickers_all.txt`
  - `dart_corp_codes_listed.json`
  - `normalized_manifest.jsonl` (공통 문서 메타)
  - `normalized_manifest_report.json` (검증/집계)
- `data/index/`
  - `chunks.jsonl` (벡터 검색 인덱스)

## 3. Common Record Contract (`normalized_manifest.jsonl`)
Each line is one JSON record with required keys:
- `doc_id` (string)
- `source_type` (`yahoo|dart|news|unknown`)
- `source_name` (string)
- `company_name` (string)
- `ticker` (string|null)
- `corp_code` (string|null)
- `market` (`KOSPI|KOSDAQ|OTHER`)
- `language` (`ko|en`)
- `published_at` (string|null)
- `collected_at` (string)
- `raw_path` (string)
- `raw_sha1` (string)
- `profile` (object)
- `status` (`ok|warn`)
- `issues` (string[])

## 4. Quality Rules
- Raw 파일은 삭제/수정하지 않고 append-only 운영
- 모든 processed/index 산출물은 재생성 가능해야 함 (idempotent)
- `status=warn` 레코드는 보고서로 집계하고 후속 정제 대상 처리
- `issues`가 있는 문서는 인덱싱 전 필터링/보강 가능

## 5. Runbook
```bash
cd /home/aidome/workspace/korea-finance-rag
source .venv/bin/activate

# 1) 수집 대상 확정
python scripts/build_universe.py

# 2) 원천 수집
python scripts/fetch_yahoo.py --tickers-file data/processed/korea_tickers_all.txt --resume --sleep 0.15
python scripts/fetch_dart_bulk.py --resume --sleep 0.25
python scripts/fetch_news.py --universe-file data/processed/korea_universe.json --limit-company 200 --per-company 2 --resume --sleep 0.2

# 3) 공통 매니페스트 정규화
python scripts/normalize_manifest.py

# 4) 인덱스 생성
python scripts/build_index.py
```

## 6. Acceptance Criteria (Phase 1 Step 3)
- raw/processed/index 단계별 산출물 규칙 문서화 완료
- 공통 매니페스트(`normalized_manifest.jsonl`) 생성 가능
- 집계 리포트(`normalized_manifest_report.json`)로 데이터 품질 확인 가능
