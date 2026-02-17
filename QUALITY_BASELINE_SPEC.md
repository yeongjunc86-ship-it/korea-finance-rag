# Quality Baseline Spec (Phase 1 Step 5)

Version: v1.0
Last Updated: 2026-02-15

## 1. Goal
검색 성능을 주관적 체감이 아니라 수치로 관리하기 위한 기준선을 만든다.

## 2. Assets
- Case set: `eval/baseline_questions_v1.jsonl`
- Evaluator: `scripts/eval_search_baseline.py`
- Report output: `logs/eval_baseline_latest.json`

## 3. Case Types
- `query`: RAG 검색 결과가 기대 티커를 포함하는지 검증
- `similar`: 관련 기업 검색 결과 개수/점수를 검증

## 4. Metrics
- Overall pass rate
- Query pass rate
- Similar pass rate
- Query avg top score
- Similar avg score
- Freshness check (optional per case): 최신 소스 파일 나이(day)

## 5. Pass Rules
### query case
- 검색 결과 1개 이상
- 기대 티커가 `source` 또는 `text`에 포함
- 소스 1개 이상
- `freshness_days`가 지정된 경우, 최신 소스 나이가 해당 일수 이내

### similar case
- 결과 개수가 `min_results` 이상

## 6. Run Command
```bash
cd /home/aidome/workspace/korea-finance-rag
source .venv/bin/activate
python scripts/eval_search_baseline.py
```

Partial run:
```bash
python scripts/eval_search_baseline.py --limit 10
```

## 7. Suggested Threshold (initial)
- Overall pass rate >= 70%
- Query pass rate >= 75%
- Similar pass rate >= 60%

## 8. Next Improvement Loop
1. 실패 케이스 확인 (`logs/eval_baseline_latest.json`)
2. 청크/프롬프트/정규화 규칙 수정
3. 동일 케이스 재측정
4. 기준치 달성 전까지 반복
