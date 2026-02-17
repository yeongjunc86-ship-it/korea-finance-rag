# API Contract Spec (Phase 1)

Version: v1.0
Last Updated: 2026-02-15
Base URL: `http://<host>:8000`
Content-Type: `application/json; charset=utf-8`

## 0. Rules
- 모든 시간 필드는 ISO 8601 UTC 문자열 사용
- 모든 에러 응답은 공통 에러 포맷 사용
- 인증: 세션 로그인 기반 (`/api/auth/login`)
- 관리자 엔드포인트(`/api/admin/*`, `/api/reload-index`)는 `admin` 권한 필요

### Common Error Format
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "요청 값이 올바르지 않습니다.",
    "details": []
  }
}
```

Error code set:
- `VALIDATION_ERROR` (400)
- `NOT_FOUND` (404)
- `INDEX_NOT_READY` (409)
- `INTERNAL_ERROR` (500)

## 1. GET /api/health
Status: Implemented
Purpose: 서비스/인덱스 상태 확인

### Response 200
```json
{
  "ok": true,
  "index_loaded": true,
  "chunk_count": 11838,
  "index_version": 1,
  "indexed_doc_count": 11838,
  "index_path": "data/index/chunks.jsonl",
  "index_exists": true,
  "index_size_bytes": 128983411,
  "index_updated_at": "2026-02-15T06:55:00Z",
  "state_path": "data/index/index_state.json",
  "state_exists": true,
  "state_updated_at": "2026-02-15T07:47:00Z"
}
```

## 2. POST /api/reload-index
Status: Implemented
Purpose: 디스크 인덱스 강제 재로딩

### Request
- Body 없음

### Response 200
```json
{
  "ok": true,
  "chunk_count": 11838
}
```

## 3. POST /api/query
Status: Implemented
Purpose: RAG 질의응답

### Request
```json
{
  "question": "삼성전자의 최근 사업 리스크 요약해줘",
  "top_k": 5
}
```

Field rules:
- `question` (required, min length 2)
- `top_k` (optional, int, null 허용)

### Response 200
```json
{
  "answer": {
    "company_name": "삼성전자",
    "market": "KOSPI",
    "summary": "요약",
    "highlights": ["핵심 포인트"],
    "financial_snapshot": {
      "market_cap": "...",
      "revenue": "...",
      "operating_income": "...",
      "net_income": "..."
    },
    "risks": ["리스크"],
    "sources": ["data/raw/yahoo_005930_KS.json"],
    "similar_companies": ["SK하이닉스"]
  },
  "answer_text": "사용자 표시용 한글 보고서 텍스트",
  "retrieved_chunks": [
    {
      "score": 0.8123,
      "company": "삼성전자",
      "market": "KOSPI",
      "source": "data/raw/yahoo_005930_KS.json",
      "text": "..."
    }
  ]
}
```

### Error
- 400: 요청 형식 오류
- 409: 인덱스 비어 있음 (`INDEX_NOT_READY`)
- 500: 모델/시스템 오류

## 4. POST /api/similar
Status: Implemented
Purpose: 유사 기업 검색

### Request
```json
{
  "company_or_query": "반도체 설계 기업",
  "top_k": 5
}
```

Field rules:
- `company_or_query` (required, min length 2)
- `top_k` (optional, default 5)

### Response 200
```json
{
  "query": "반도체 설계 기업",
  "results": [
    {
      "company": "삼성전자",
      "market": "KOSPI",
      "score": 0.79,
      "strategic_fit_score": 79,
      "reason": "질의-기업 임베딩 코사인 유사도 기반 점수(79/100)",
      "source": "data/raw/yahoo_005930_KS.json"
    }
  ]
}
```

Scoring rule:
- `score`: 코사인 유사도 기반 0~1 실수(클램프)
- `strategic_fit_score`: `round(score * 100)` 정수

### Error
- 400, 500

## 4-1. GET /api/source
Status: Implemented
Purpose: 검색 결과의 raw 근거 JSON 확인

### Query Params
- `path` (required): 예) `data/raw/news_xxx.json`

### Response 200
```json
{
  "ok": true,
  "path": "data/raw/news_xxx.json",
  "content": "{ ...raw json text... }"
}
```

### Error
- 400: invalid path
- 403: forbidden path
- 404: source not found
- 500: read error

## 4-2. POST /api/target-analysis
Status: Implemented
Purpose: 타겟 기업 10문항 실사형 분석(가용 데이터 기준)

### Request
```json
{
  "company_name": "삼성전자",
  "top_k_per_question": 6
}
```

Field rules:
- `company_name` (required, min length 2)
- `top_k_per_question` (optional, default 6, range 3~20)

### Response 200
```json
{
  "company_name": "삼성전자",
  "generated_at": "2026-02-16T10:00:00Z",
  "results": [
    {
      "question_id": 1,
      "question": "삼성전자의 최근 5년 매출 성장률과 EBITDA 마진 추이를 정리해줘.",
      "readiness": "가능",
      "answer": "한국어 분석 문장 ...",
      "evidence_sources": [
        "data/raw/yahoo_005930_KS.json",
        "data/raw/dart_00126380.json"
      ]
    }
  ]
}
```

Readiness rule:
- `가능`: 현재 인덱스로 신뢰 가능한 분석 가능
- `부분`: 일부 근거만 가능, 한계 명시 필요
- `불가`: 핵심 데이터 소스 부재(예: 특허 DB, 시장점유율 DB, ESG 전용 데이터)

## 4-3. POST /api/industry-analysis
Status: Implemented
Purpose: 산업/시장 10문항 분석(11~20, 가용 데이터 기준)

### Request
```json
{
  "industry_name": "반도체",
  "top_k_per_question": 6
}
```

Field rules:
- `industry_name` (required, min length 2)
- `top_k_per_question` (optional, default 6, range 3~20)

### Response 200
```json
{
  "industry_name": "반도체",
  "generated_at": "2026-02-16T10:10:00Z",
  "results": [
    {
      "question_id": 11,
      "question": "반도체 산업의 최근 5년 CAGR과 향후 5년 전망을 정리해줘.",
      "readiness": "부분",
      "answer": "한국어 분석 문장 ...",
      "evidence_sources": [
        "data/raw/news_xxx.json",
        "data/raw/yahoo_xxx.json"
      ]
    }
  ]
}
```

Readiness rule:
- `가능`: 해당 질문에 필요한 산업 전용 데이터(예: valuation/TAM) 확보
- `부분`: 뉴스/공시/재무 기반의 제한적 분석 가능
- `불가`: 핵심 정량 데이터셋 부재

## 4-4. POST /api/valuation-analysis
Status: Implemented
Purpose: 밸류에이션 10문항 분석(21~30, 가용 데이터 기준)

### Request
```json
{
  "company_name": "삼성전자",
  "top_k_per_question": 6
}
```

Field rules:
- `company_name` (required, min length 2)
- `top_k_per_question` (optional, default 6, range 3~20)

### Response 200
```json
{
  "company_name": "삼성전자",
  "generated_at": "2026-02-16T11:20:00Z",
  "results": [
    {
      "question_id": 21,
      "question": "삼성전자의 적정 EV/EBITDA 멀티플 범위를 제시해줘.",
      "readiness": "부분",
      "answer": "한국어 분석 문장 ...",
      "evidence_sources": [
        "data/raw/valuation_case_005930_KS.json",
        "data/raw/valuation_xxx.json"
      ]
    }
  ]
}
```

Readiness rule:
- `가능`: valuation_case/mna/fx 등 질문별 핵심 데이터 확보
- `부분`: yahoo/dart/news 기반의 제한적 추정 가능
- `불가`: comps/IRR/LBO 등에 필요한 핵심 데이터셋 부재

## 4-5. POST /api/synergy-analysis
Status: Implemented
Purpose: 시너지 10문항 분석(31~40, 가용 데이터 기준)

### Request
```json
{
  "company_name": "삼성전자",
  "top_k_per_question": 6
}
```

Field rules:
- `company_name` (required, min length 2)
- `top_k_per_question` (optional, default 6, range 3~20)

### Response 200
```json
{
  "company_name": "삼성전자",
  "generated_at": "2026-02-16T11:40:00Z",
  "results": [
    {
      "question_id": 31,
      "question": "삼성전자 매출 시너지 가능 항목을 정리해줘.",
      "readiness": "부분",
      "answer": "한국어 분석 문장 ...",
      "evidence_sources": [
        "data/raw/synergy_case_005930_KS.json",
        "data/raw/dart_notes_00126380.json"
      ]
    }
  ]
}
```

Readiness rule:
- `가능`: synergy_case 및 보강 데이터(시장점유율/특허/ESG 등) 확보
- `부분`: 뉴스/공시/주석 기반의 제한적 분석 가능
- `불가`: PMI/통합 비용/법무 구조 등 핵심 데이터셋 부재

## 4-6. POST /api/due-diligence-analysis
Status: Implemented
Purpose: 리스크/실사 10문항 분석(41~50, 가용 데이터 기준)

### Request
```json
{
  "company_name": "삼성전자",
  "top_k_per_question": 6
}
```

Field rules:
- `company_name` (required, min length 2)
- `top_k_per_question` (optional, default 6, range 3~20)

### Response 200
```json
{
  "company_name": "삼성전자",
  "generated_at": "2026-02-16T12:00:00Z",
  "results": [
    {
      "question_id": 41,
      "question": "삼성전자 재무 실사 시 집중 점검해야 할 항목을 정리해줘.",
      "readiness": "부분",
      "answer": "한국어 분석 문장 ...",
      "evidence_sources": [
        "data/raw/due_diligence_case_005930_KS.json",
        "data/raw/dart_notes_00126380.json"
      ]
    }
  ]
}
```

Readiness rule:
- `가능`: due_diligence_case 또는 질문별 핵심 소스(tax/security/supply_chain/pmi_fail) 확보
- `부분`: dart/dart_notes/news 기반의 제한적 분석 가능
- `불가`: 계약/보안/PMI 사례 등 핵심 데이터셋 부재

## 4-7. POST /api/strategic-analysis
Status: Implemented
Purpose: 전략 의사결정 10문항 분석(51~60, 가용 데이터 기준)

### Request
```json
{
  "company_name": "삼성전자",
  "top_k_per_question": 6
}
```

Field rules:
- `company_name` (required, min length 2)
- `top_k_per_question` (optional, default 6, range 3~20)

### Response 200
```json
{
  "company_name": "삼성전자",
  "generated_at": "2026-02-16T12:20:00Z",
  "results": [
    {
      "question_id": 51,
      "question": "삼성전자 인수가 전략적 인수인지 재무적 투자에 가까운지 판단해줘.",
      "readiness": "부분",
      "answer": "한국어 분석 문장 ...",
      "evidence_sources": [
        "data/raw/strategic_case_005930_KS.json",
        "data/raw/valuation_case_005930_KS.json"
      ]
    }
  ]
}
```

Readiness rule:
- `가능`: strategic_case 확보(또는 질문별 핵심 구조 데이터 확보)
- `부분`: valuation/synergy/due_diligence/mna 기반의 제한적 분석 가능
- `불가`: 포트폴리오 적합성/딜 구조 설계용 핵심 데이터셋 부재

## 5. GET /api/company/search
Status: Planned (Phase 2)
Purpose: 기업명/티커/산업 검색

### Query Params
- `q` (required): 검색어
- `market` (optional): `KOSPI|KOSDAQ|KONEX|OTHER`
- `limit` (optional, default 20, max 100)
- `offset` (optional, default 0)

### Response 200 (planned)
```json
{
  "items": [
    {
      "company_id": "KRX:005930.KS",
      "ticker": "005930.KS",
      "company_name_ko": "삼성전자",
      "market": "KOSPI",
      "sector": "IT",
      "industry": "Semiconductors"
    }
  ],
  "total": 1,
  "limit": 20,
  "offset": 0
}
```

## 6. GET /api/company/{ticker}
Status: Planned (Phase 2)
Purpose: 기업 상세 조회

### Path Param
- `ticker`: 예) `005930.KS`

### Response 200 (planned)
```json
{
  "company_id": "KRX:005930.KS",
  "ticker": "005930.KS",
  "corp_code": "00126380",
  "company_name_ko": "삼성전자",
  "company_name_en": "Samsung Electronics",
  "market": "KOSPI",
  "sector": "IT",
  "industry": "Semiconductors",
  "market_cap": 0,
  "revenue": 0,
  "operating_margin": 0,
  "description": "...",
  "source_updated_at": "2026-02-15T00:00:00Z",
  "updated_at": "2026-02-15T00:00:00Z"
}
```

### Error
- 404: ticker 없음

## 7. Acceptance Criteria (Phase 1 Done)
- 구현 API와 계획 API를 구분해 문서화
- 각 엔드포인트 요청/응답 JSON 샘플 명시
- 공통 에러 포맷 및 상태코드 명시

## 8. GET /api/admin/status
Status: Implemented
Purpose: 웹 데이터 관리용 상태 조회

### Response 200 (example)
```json
{
  "ok": true,
  "data": {
    "generated_at": "2026-02-15T00:00:00Z",
    "health": { "ok": true },
    "raw": { "total_files": 11838, "yahoo_files": 7892, "dart_files": 3946, "news_files": 0 },
    "processed": { "files": [] },
    "index": { "files": [] },
    "logs": { "recent": [] }
  }
}
```

## 10. POST /api/auth/login
Status: Implemented
Purpose: 세션 로그인

### Request
```json
{ "email": "admin@local", "password": "admin1234!" }
```

### Response 200
```json
{
  "ok": true,
  "user": { "email": "admin@local", "role": "admin" }
}
```

## 11. GET /api/auth/me
Status: Implemented
Purpose: 현재 로그인 사용자 확인

### Response 200 (authenticated)
```json
{
  "ok": true,
  "authenticated": true,
  "user": { "user_id": "u_admin", "email": "admin@local", "role": "admin" }
}
```

### Response 200 (anonymous)
```json
{
  "ok": false,
  "authenticated": false
}
```

## 12. POST /api/auth/logout
Status: Implemented
Purpose: 세션 로그아웃

### Response 200
```json
{ "ok": true }
```

## 9. POST /api/admin/run-task
Status: Implemented
Purpose: 웹에서 데이터 관리 작업 실행

### Request
```json
{ "task": "incremental_index" }
```

또는 옵션 포함:
```json
{
  "task": "fetch_news",
  "options": {
    "limit_company": 300,
    "per_company": 3,
    "sleep": 0.2,
    "resume": true
  }
}
```

```json
{
  "task": "eval_baseline",
  "options": {
    "limit": 20
  }
}
```

Supported `task` values:
- `normalize_manifest`
- `fetch_dart_bulk`
- `fetch_news`
- `sync_index_state`
- `incremental_index`
- `full_index`
- `reload_index`
- `eval_baseline`

Supported `options` by task:
- `fetch_news`:
  - `limit_company` (int, 1~5000)
  - `per_company` (int, 1~20)
  - `sleep` (float, 0~5)
  - `resume` (bool)
- `eval_baseline`:
  - `limit` (int, 1~500)

### Response 200 (example)
```json
{
  "ok": true,
  "task": "incremental_index",
  "command": "./scripts/run_index_incremental.sh",
  "exit_code": 0,
  "stdout_tail": "...",
  "stderr_tail": "",
  "finished_at": "2026-02-15T00:00:00Z"
}
```
