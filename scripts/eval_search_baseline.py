#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Allow running as: python scripts/eval_search_baseline.py
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.rag_pipeline import RagPipeline

load_dotenv()


@dataclass
class EvalResult:
    case_id: str
    task: str
    passed: bool
    score: float
    detail: dict[str, Any]


def load_cases(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def normalize_ticker_tokens(ticker: str) -> set[str]:
    t = ticker.strip().upper()
    return {t, t.replace(".", "_"), t.replace(".", "")}


def hit_contains_expected_ticker(hits: list[dict[str, Any]], expected_tickers: list[str]) -> bool:
    if not expected_tickers:
        return True

    expected_tokens: set[str] = set()
    for t in expected_tickers:
        expected_tokens |= normalize_ticker_tokens(t)

    for h in hits:
        source = str(h.get("source") or "").upper()
        text = str(h.get("text") or "").upper()
        for token in expected_tokens:
            if token and (token in source or token in text):
                return True
    return False


def newest_source_age_days(hits: list[dict[str, Any]]) -> float | None:
    mtimes: list[float] = []
    for h in hits:
        src = str(h.get("source") or "")
        if not src:
            continue
        p = Path(src)
        if p.exists():
            mtimes.append(p.stat().st_mtime)
    if not mtimes:
        return None
    newest = max(mtimes)
    age_sec = datetime.now(UTC).timestamp() - newest
    return max(0.0, age_sec / 86400.0)


def eval_query_case(pipeline: RagPipeline, case: dict[str, Any]) -> EvalResult:
    top_k = int(case.get("top_k") or 5)
    question = str(case.get("question") or "").strip()
    expected_tickers = case.get("expected_tickers") if isinstance(case.get("expected_tickers"), list) else []
    freshness_days = case.get("freshness_days")

    hits = pipeline.retrieve(question, top_k=top_k)
    top_score = float(hits[0]["score"]) if hits else 0.0
    hit_ok = hit_contains_expected_ticker(hits, [str(x) for x in expected_tickers])

    freshness_ok = True
    age = newest_source_age_days(hits)
    if isinstance(freshness_days, int) or isinstance(freshness_days, float):
        freshness_ok = age is not None and age <= float(freshness_days)

    source_count = len({str(h.get("source") or "") for h in hits if h.get("source")})
    passed = bool(hits) and hit_ok and freshness_ok and source_count >= 1

    return EvalResult(
        case_id=str(case.get("id") or ""),
        task="query",
        passed=passed,
        score=top_score,
        detail={
            "question": question,
            "top_k": top_k,
            "retrieved": len(hits),
            "source_count": source_count,
            "hit_expected_ticker": hit_ok,
            "freshness_ok": freshness_ok,
            "newest_source_age_days": age,
            "top_score": top_score,
        },
    )


def eval_similar_case(pipeline: RagPipeline, case: dict[str, Any]) -> EvalResult:
    top_k = int(case.get("top_k") or 5)
    q = str(case.get("company_or_query") or "").strip()
    min_results = int(case.get("min_results") or 1)

    results = pipeline.similar_companies(q, top_k=top_k)
    passed = len(results) >= min_results
    avg_reason_score = 0.0
    if results:
        values: list[float] = []
        for r in results:
            reason = str(r.get("reason") or "")
            token = reason.split()[-1]
            try:
                values.append(float(token))
            except ValueError:
                continue
        if values:
            avg_reason_score = sum(values) / len(values)

    return EvalResult(
        case_id=str(case.get("id") or ""),
        task="similar",
        passed=passed,
        score=avg_reason_score,
        detail={
            "query": q,
            "top_k": top_k,
            "result_count": len(results),
            "min_results": min_results,
            "avg_reason_score": round(avg_reason_score, 4),
        },
    )


def summarize(results: list[EvalResult]) -> dict[str, Any]:
    total = len(results)
    passed = sum(1 for r in results if r.passed)

    query = [r for r in results if r.task == "query"]
    similar = [r for r in results if r.task == "similar"]

    def pass_rate(items: list[EvalResult]) -> float:
        return round((sum(1 for i in items if i.passed) / len(items)) * 100, 2) if items else 0.0

    return {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "total_cases": total,
        "passed_cases": passed,
        "pass_rate": pass_rate(results),
        "query": {
            "count": len(query),
            "pass_rate": pass_rate(query),
            "avg_top_score": round(sum(r.score for r in query) / len(query), 4) if query else 0.0,
        },
        "similar": {
            "count": len(similar),
            "pass_rate": pass_rate(similar),
            "avg_score": round(sum(r.score for r in similar) / len(similar), 4) if similar else 0.0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run retrieval/similar quality baseline")
    parser.add_argument("--cases", default="eval/baseline_questions_v1.jsonl", help="jsonl case file")
    parser.add_argument("--out", default="logs/eval_baseline_latest.json", help="output json file")
    parser.add_argument("--limit", type=int, default=0, help="run only first N cases")
    args = parser.parse_args()

    case_path = Path(args.cases)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cases = load_cases(case_path)
    if args.limit > 0:
        cases = cases[: args.limit]

    pipeline = RagPipeline()

    results: list[EvalResult] = []
    for case in cases:
        task = str(case.get("task") or "query")
        if task == "similar":
            results.append(eval_similar_case(pipeline, case))
        else:
            results.append(eval_query_case(pipeline, case))

    summary = summarize(results)
    payload = {
        "summary": summary,
        "results": [
            {
                "id": r.case_id,
                "task": r.task,
                "passed": r.passed,
                "score": r.score,
                "detail": r.detail,
            }
            for r in results
        ],
    }

    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"done. cases={len(results)}, pass_rate={summary['pass_rate']}%, out={out_path}")


if __name__ == "__main__":
    main()
