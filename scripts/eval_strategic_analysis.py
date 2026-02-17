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
QUESTION_WEIGHTS_DEFAULT: dict[int, float] = {
    51: 1.0,
    52: 2.0,
    53: 1.0,
    54: 1.0,
    55: 1.0,
    56: 1.0,
    57: 2.0,
    58: 1.0,
    59: 1.0,
    60: 2.0,
}
CRITICAL_QUESTIONS = {52, 57, 60}


@dataclass
class CaseResult:
    case_id: str
    question_id: int
    weight: float
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


def evaluate_case(
    case: dict[str, Any],
    cache: dict[str, dict[str, Any]],
    pipeline: RagPipeline,
    case_score_threshold: float,
    question_weights: dict[int, float],
) -> CaseResult:
    case_id = str(case.get("id") or "")
    company_name = str(case.get("company_name") or "").strip()
    question_id = int(case.get("question_id") or 0)
    min_readiness = str(case.get("min_readiness") or "불가").strip()
    top_k_per_question = int(case.get("top_k_per_question") or 6)
    expected_prefixes = case.get("expected_source_prefixes")
    prefixes = [str(x) for x in expected_prefixes] if isinstance(expected_prefixes, list) else []
    weight = float(question_weights.get(question_id, 1.0))

    if not company_name or question_id < 51 or question_id > 60:
        return CaseResult(
            case_id=case_id,
            question_id=question_id,
            weight=weight,
            passed=False,
            score=0.0,
            detail={"error": "invalid case format"},
        )

    cache_key = f"{company_name}:{top_k_per_question}"
    if cache_key not in cache:
        cache[cache_key] = pipeline.strategic_analysis(company_name, top_k_per_question=top_k_per_question)
    result = cache[cache_key]

    row = None
    for r in result.get("results", []):
        if int(r.get("question_id") or 0) == question_id:
            row = r
            break
    if not isinstance(row, dict):
        return CaseResult(
            case_id=case_id,
            question_id=question_id,
            weight=weight,
            passed=False,
            score=0.0,
            detail={"error": "question result not found"},
        )

    readiness = str(row.get("readiness") or "불가")
    sources = [str(x) for x in (row.get("evidence_sources") or []) if str(x).strip()]
    readiness_ok = READINESS_RANK.get(readiness, 0) >= READINESS_RANK.get(min_readiness, 0)
    source_ok = source_prefix_hit(sources, prefixes)
    answer = str(row.get("answer") or "").strip()
    answer_ok = len(answer) >= 20

    score = (0.50 if readiness_ok else 0.0) + (0.35 if source_ok else 0.0) + (0.15 if answer_ok else 0.0)
    critical_gate_ok = True
    if question_id in CRITICAL_QUESTIONS:
        critical_gate_ok = readiness_ok and source_ok
    passed = (score >= case_score_threshold) and critical_gate_ok

    return CaseResult(
        case_id=case_id,
        question_id=question_id,
        weight=weight,
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
            "critical_question": question_id in CRITICAL_QUESTIONS,
            "critical_gate_ok": critical_gate_ok,
            "case_score_threshold": case_score_threshold,
        },
    )


def summarize(results: list[CaseResult], overall_pass_threshold: float) -> dict[str, Any]:
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    avg_score = (sum(r.score for r in results) / total) if total else 0.0
    total_weight = sum(r.weight for r in results)
    passed_weight = sum(r.weight for r in results if r.passed)
    weighted_avg_score = (sum(r.score * r.weight for r in results) / total_weight) if total_weight > 0 else 0.0
    weighted_pass_rate = (passed_weight / total_weight) * 100 if total_weight > 0 else 0.0
    critical = [r for r in results if r.question_id in CRITICAL_QUESTIONS]
    critical_weight = sum(r.weight for r in critical)
    critical_pass_weight = sum(r.weight for r in critical if r.passed)
    critical_pass_rate = (critical_pass_weight / critical_weight) * 100 if critical_weight > 0 else 0.0

    return {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "total_cases": total,
        "passed_cases": passed,
        "pass_rate": round((passed / total) * 100, 2) if total else 0.0,
        "avg_score": round(avg_score, 4),
        "weighted_pass_rate": round(weighted_pass_rate, 2),
        "weighted_avg_score": round(weighted_avg_score, 4),
        "critical_pass_rate": round(critical_pass_rate, 2),
        "overall_pass_threshold": overall_pass_threshold,
        "overall_passed": weighted_pass_rate >= overall_pass_threshold,
        "question_weights": {str(k): v for k, v in QUESTION_WEIGHTS_DEFAULT.items()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate strategic-analysis (51~60) quality")
    parser.add_argument("--cases", default="eval/strategic_analysis_51_60_v1.jsonl")
    parser.add_argument("--out", default="logs/eval_strategic_analysis_latest.json")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--case-score-threshold", type=float, default=0.7)
    parser.add_argument("--overall-pass-threshold", type=float, default=80.0)
    args = parser.parse_args()

    if args.case_score_threshold < 0 or args.case_score_threshold > 1:
        raise SystemExit("--case-score-threshold must be between 0 and 1")
    if args.overall_pass_threshold < 0 or args.overall_pass_threshold > 100:
        raise SystemExit("--overall-pass-threshold must be between 0 and 100")

    cases = load_cases(Path(args.cases))
    if args.limit > 0:
        cases = cases[: args.limit]

    pipeline = RagPipeline()
    cache: dict[str, dict[str, Any]] = {}
    results = [
        evaluate_case(
            c,
            cache,
            pipeline,
            case_score_threshold=float(args.case_score_threshold),
            question_weights=QUESTION_WEIGHTS_DEFAULT,
        )
        for c in cases
    ]
    payload = {
        "summary": summarize(results, overall_pass_threshold=float(args.overall_pass_threshold)),
        "results": [
            {
                "id": r.case_id,
                "question_id": r.question_id,
                "weight": r.weight,
                "passed": r.passed,
                "score": r.score,
                "detail": r.detail,
            }
            for r in results
        ],
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"done. cases={payload['summary']['total_cases']}, "
        f"weighted_pass_rate={payload['summary']['weighted_pass_rate']}%, out={out_path}"
    )


if __name__ == "__main__":
    main()
