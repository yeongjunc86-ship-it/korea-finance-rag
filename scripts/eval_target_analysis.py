#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.rag_pipeline import RagPipeline

load_dotenv()


READINESS_RANK = {"불가": 0, "부분": 1, "가능": 2}


@dataclass
class CaseResult:
    case_id: str
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


def source_prefix_hit(sources: list[str], prefixes: list[str]) -> bool:
    if not prefixes:
        return True
    for s in sources:
        for p in prefixes:
            if s.startswith(p):
                return True
    return False


def evaluate_case(case: dict[str, Any], cache: dict[str, dict[str, Any]], pipeline: RagPipeline) -> CaseResult:
    case_id = str(case.get("id") or "")
    company_name = str(case.get("company_name") or "").strip()
    question_id = int(case.get("question_id") or 0)
    min_readiness = str(case.get("min_readiness") or "불가").strip()
    top_k_per_question = int(case.get("top_k_per_question") or 6)
    expected_prefixes = case.get("expected_source_prefixes")
    prefixes = [str(x) for x in expected_prefixes] if isinstance(expected_prefixes, list) else []

    if not company_name or question_id < 1:
        return CaseResult(
            case_id=case_id,
            passed=False,
            score=0.0,
            detail={"error": "invalid case format"},
        )

    cache_key = f"{company_name}:{top_k_per_question}"
    if cache_key not in cache:
        cache[cache_key] = pipeline.target_analysis(company_name, top_k_per_question=top_k_per_question)
    result = cache[cache_key]

    row = None
    for r in result.get("results", []):
        if int(r.get("question_id") or 0) == question_id:
            row = r
            break

    if not isinstance(row, dict):
        return CaseResult(
            case_id=case_id,
            passed=False,
            score=0.0,
            detail={"error": "question result not found", "question_id": question_id},
        )

    readiness = str(row.get("readiness") or "불가")
    sources = [str(x) for x in (row.get("evidence_sources") or []) if str(x).strip()]
    readiness_ok = READINESS_RANK.get(readiness, 0) >= READINESS_RANK.get(min_readiness, 0)
    source_ok = source_prefix_hit(sources, prefixes)
    answer = str(row.get("answer") or "").strip()
    answer_ok = len(answer) >= 20

    checks = [readiness_ok, source_ok, answer_ok]
    score = sum(1 for c in checks if c) / len(checks)
    passed = all(checks)

    return CaseResult(
        case_id=case_id,
        passed=passed,
        score=score,
        detail={
            "company_name": company_name,
            "question_id": question_id,
            "readiness": readiness,
            "min_readiness": min_readiness,
            "readiness_ok": readiness_ok,
            "source_ok": source_ok,
            "answer_ok": answer_ok,
            "source_count": len(sources),
        },
    )


def summarize(results: list[CaseResult]) -> dict[str, Any]:
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    avg_score = (sum(r.score for r in results) / total) if total else 0.0
    return {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "total_cases": total,
        "passed_cases": passed,
        "pass_rate": round((passed / total) * 100, 2) if total else 0.0,
        "avg_score": round(avg_score, 4),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate target-analysis (10Q) quality")
    parser.add_argument("--cases", default="eval/target_analysis_questions_v1.jsonl")
    parser.add_argument("--out", default="logs/eval_target_analysis_latest.json")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    case_path = Path(args.cases)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cases = load_cases(case_path)
    if args.limit > 0:
        cases = cases[: args.limit]

    pipeline = RagPipeline()
    cache: dict[str, dict[str, Any]] = {}
    results = [evaluate_case(c, cache, pipeline) for c in cases]

    payload = {
        "summary": summarize(results),
        "results": [
            {
                "id": r.case_id,
                "passed": r.passed,
                "score": r.score,
                "detail": r.detail,
            }
            for r in results
        ],
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"done. cases={payload['summary']['total_cases']}, "
        f"pass_rate={payload['summary']['pass_rate']}%, out={out_path}"
    )


if __name__ == "__main__":
    main()

