# Data Schema Spec (Phase 1)

Version: v1.0
Last Updated: 2026-02-15
Scope: korea-finance-rag

## 0. Common Rules
- Time format: ISO 8601 (UTC), example `2026-02-15T01:43:27Z`
- Encoding: UTF-8
- Primary keys: string unless noted
- Soft delete: use `is_active` where needed
- Source-of-truth priority: `DART > Yahoo > Derived`

## 1. Entity: company
Purpose: 기업 검색/상세/관련기업 계산을 위한 기준 메타

### Fields
| Field | Type | Required | Description |
|---|---|---|---|
| `company_id` | string | Y | 내부 고유 ID (`KRX:{ticker}` 권장) |
| `ticker` | string | Y | 종목코드+시장 접미사 (`005930.KS`) |
| `corp_code` | string | N | DART 고유 기업코드 |
| `company_name_ko` | string | Y | 한글 기업명 |
| `company_name_en` | string | N | 영문 기업명 |
| `market` | enum | Y | `KOSPI|KOSDAQ|KONEX|OTHER` |
| `sector` | string | N | 섹터 |
| `industry` | string | N | 산업 |
| `market_cap` | number | N | 시가총액 |
| `revenue` | number | N | 최근 매출 |
| `operating_margin` | number | N | 최근 영업이익률 |
| `description` | string | N | 기업 요약 설명 |
| `source` | string | Y | `dart|yahoo|merged` |
| `source_updated_at` | datetime | Y | 원천 데이터 최신 시각 |
| `updated_at` | datetime | Y | 내부 레코드 갱신 시각 |
| `is_active` | boolean | Y | 상장 유지 상태 |

### Key/Index
- PK: `company_id`
- Unique: `ticker`, `corp_code` (nullable unique)
- Search index: `company_name_ko`, `ticker`, `industry`

### Refresh Policy
- Daily batch update (T+1)
- 상장폐지/변경 감지 시 `is_active=false`

## 2. Entity: news
Purpose: 최신 뉴스 기반 RAG 컨텍스트

### Fields
| Field | Type | Required | Description |
|---|---|---|---|
| `news_id` | string | Y | 고유 ID (`sha1(url)` 권장) |
| `company_id` | string | N | 연관 기업 ID (없으면 null) |
| `ticker` | string | N | 매칭된 티커 |
| `title` | string | Y | 기사 제목 |
| `summary` | string | N | 기사 요약 |
| `content` | string | N | 본문(허용 범위 내) |
| `url` | string | Y | 원문 링크 |
| `publisher` | string | N | 언론사 |
| `published_at` | datetime | Y | 기사 발행 시각 |
| `collected_at` | datetime | Y | 수집 시각 |
| `language` | string | Y | `ko|en` |
| `topic_tags` | string[] | N | 토픽 태그 |
| `sentiment` | enum | N | `positive|neutral|negative` |
| `is_active` | boolean | Y | 노출 여부 |

### Key/Index
- PK: `news_id`
- Unique: `url`
- Composite index: `(ticker, published_at desc)`

### Refresh Policy
- 10~30분 주기 증분 수집
- 30일 이후 cold 보관(검색은 가능, 우선순위 낮춤)

## 3. Entity: chunk
Purpose: 벡터 검색용 최소 의미 단위

### Fields
| Field | Type | Required | Description |
|---|---|---|---|
| `chunk_id` | string | Y | 고유 ID (`{doc_id}:{seq}`) |
| `doc_id` | string | Y | 원문 문서 ID |
| `doc_type` | enum | Y | `company_profile|dart_report|news` |
| `company_id` | string | N | 연관 기업 ID |
| `ticker` | string | N | 연관 티커 |
| `text` | string | Y | 청크 원문 텍스트 |
| `embedding` | float[] | Y | 임베딩 벡터 |
| `embedding_model` | string | Y | 예: `nomic-embed-text` |
| `token_count` | int | N | 토큰 수 |
| `seq` | int | Y | 문서 내 순서 |
| `source_path` | string | N | 원본 경로/URI |
| `published_at` | datetime | N | 원문 시각(뉴스/공시) |
| `updated_at` | datetime | Y | 생성/갱신 시각 |

### Key/Index
- PK: `chunk_id`
- Unique: `(doc_id, seq)`
- Vector index: `embedding`
- Filter index: `ticker`, `doc_type`, `published_at`

### Refresh Policy
- Daily full rebuild + hourly incremental append
- 모델 변경 시 전량 재임베딩

## 4. Entity: answer_log
Purpose: 응답 품질/감사/리포트 출력 근거 보존

### Fields
| Field | Type | Required | Description |
|---|---|---|---|
| `answer_id` | string | Y | 응답 고유 ID (UUID) |
| `user_id` | string | Y | 요청 사용자 |
| `question` | string | Y | 원 질문 |
| `normalized_question` | string | N | 정규화 질문 |
| `answer_json` | object | Y | 정형 답변(JSON 스키마) |
| `answer_text` | string | Y | 사용자 표시 텍스트 |
| `sources` | object[] | Y | 출처 목록(문서/URL/chunk_id) |
| `retrieved_chunk_ids` | string[] | Y | 검색된 청크 목록 |
| `top_k` | int | Y | 검색 개수 |
| `latency_ms` | int | Y | 응답 시간 |
| `model_name` | string | Y | 생성 모델명 |
| `created_at` | datetime | Y | 생성 시각 |

### Key/Index
- PK: `answer_id`
- Index: `user_id`, `created_at desc`

### Refresh Policy
- Immutable append-only
- 1년 보관 후 아카이브

## 5. Entity: user
Purpose: 회원 전용 로그인/권한관리

### Fields
| Field | Type | Required | Description |
|---|---|---|---|
| `user_id` | string | Y | 사용자 ID (UUID) |
| `email` | string | Y | 로그인 ID |
| `password_hash` | string | Y | 해시된 비밀번호 |
| `display_name` | string | Y | 표시 이름 |
| `role` | enum | Y | `admin|analyst|viewer` |
| `status` | enum | Y | `active|pending|suspended` |
| `last_login_at` | datetime | N | 마지막 로그인 |
| `created_at` | datetime | Y | 가입 시각 |
| `updated_at` | datetime | Y | 갱신 시각 |

### Key/Index
- PK: `user_id`
- Unique: `email`

### Refresh Policy
- 실시간 갱신
- 비활성 계정 정기 점검(월 1회)

## 6. Output Schema (for `/api/query`)
Target format for UI/PDF/report export:

```json
{
  "company_name": "삼성전자",
  "market": "KOSPI",
  "summary": "요약",
  "highlights": ["핵심 포인트1", "핵심 포인트2"],
  "financial_snapshot": {
    "market_cap": "...",
    "revenue": "...",
    "operating_margin": "..."
  },
  "risks": ["리스크1", "리스크2"],
  "sources": [
    {"type": "dart_report", "ref": "doc_id_or_url"},
    {"type": "news", "ref": "https://..."}
  ],
  "similar_companies": [
    {"company_name": "...", "ticker": "...", "reason": "..."}
  ]
}
```

## 7. ID/Dedup Rules
- Company dedup: `ticker` 기준 병합, 없으면 `corp_code`
- News dedup: `url` 우선, 보조로 `title+published_at` 해시
- Chunk dedup: `(doc_id, seq)`
- Answer dedup: 중복 제거하지 않음(감사용 원본 보존)

## 8. Acceptance Criteria (Phase 1 Done)
- `company/news/chunk/answer_log/user` 스키마가 문서화됨
- 각 엔터티에 대해 Required/Key/Refresh 정책 명시
- `/api/query` 출력 스키마와 출처 규칙 명시
