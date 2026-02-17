# External Data Templates

이 폴더는 외부 데이터(시장점유율/특허/ESG/고객의존도) CSV를 넣는 위치입니다.

## 1) market_share.csv
필수 컬럼:
- `industry`
- `company`

권장 컬럼:
- `market`
- `country`
- `share_pct`
- `as_of`
- `source_url`

## 2) patents.csv
필수 컬럼:
- `company`

권장 컬럼:
- `ticker`
- `market`
- `patent_id`
- `title`
- `tech_domain`
- `filed_date`
- `country`
- `status`
- `source_url`

## 3) mna_comps.csv
필수 컬럼:
- `industry`
- `target_company`

권장 컬럼:
- `acquirer`
- `announce_date`
- `deal_value`
- `currency`
- `ev_ebitda`
- `ev_sales`
- `country`
- `source_url`

## 4) esg_scores.csv
필수 컬럼:
- `company`

권장 컬럼:
- `ticker`
- `market`
- `esg_score`
- `e_score`
- `s_score`
- `g_score`
- `risk_flags` (세미콜론 구분)
- `as_of`
- `provider`
- `source_url`

## 5) customer_dependency.csv
필수 컬럼:
- `company`
- `customer_name`

권장 컬럼:
- `ticker`
- `market`
- `revenue_share_pct`
- `fiscal_year`
- `source_type` (예: `IR`, `earnings_call`, `annual_report`)
- `source_url`
- `confidence` (0~1)
- `note`

## 6) customer_reports/ (IR/실적발표 텍스트 폴더)
- 지원 포맷: `.txt`, `.md`, `.json`
- 파일명 권장: `회사명__YYYYMMDD.txt`
- `.json`일 경우 권장 필드:
  - `company`
  - `ticker`
  - `market`
  - `published_at`
  - `content` (본문 텍스트)

적재 실행:
```bash
./scripts/run_external_enrichment.sh
```
