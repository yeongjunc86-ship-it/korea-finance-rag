from __future__ import annotations

import shlex
import subprocess
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROC_DIR = DATA_DIR / "processed"
INDEX_DIR = DATA_DIR / "index"
LOG_DIR = ROOT_DIR / "logs"
COMPANY_MASTER_PATH = PROC_DIR / "company_master.json"


def _norm_text(v: str) -> str:
    x = str(v or "").strip().lower()
    x = x.replace("(주)", "").replace("주식회사", "").replace("㈜", "")
    x = re.sub(r"[^a-z0-9가-힣]+", "", x)
    return x


def _split_companies(companies_raw: Any) -> list[str]:
    if isinstance(companies_raw, str):
        return [x.strip() for x in companies_raw.split(",") if x.strip()]
    if isinstance(companies_raw, list):
        return [str(x).strip() for x in companies_raw if str(x).strip()]
    return []


def _resolve_company_filters(companies_raw: Any) -> dict[str, list[str]]:
    companies = _split_companies(companies_raw)
    if not companies:
        return {"companies": [], "tickers": [], "corp_codes": []}

    q_raw = [x.lower() for x in companies]
    q_norm = [_norm_text(x) for x in companies if _norm_text(x)]
    resolved_companies: list[str] = []
    tickers: set[str] = set()
    corp_codes: set[str] = set()

    if COMPANY_MASTER_PATH.exists():
        try:
            payload = json.loads(COMPANY_MASTER_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        items = payload.get("items") if isinstance(payload, dict) else None
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                canonical = str(it.get("canonical_name") or "").strip()
                aliases = it.get("aliases") if isinstance(it.get("aliases"), list) else []
                item_tickers = [str(x).strip() for x in (it.get("tickers") or []) if str(x).strip()]
                item_corps = [str(x).strip() for x in (it.get("corp_codes") or []) if str(x).strip()]
                cands_raw = {canonical.lower(), *[str(x).lower() for x in aliases], *[x.lower() for x in item_tickers], *[x.lower() for x in item_corps]}
                cands_raw = {x for x in cands_raw if x}
                cands_norm = {_norm_text(x) for x in [canonical, *aliases, *item_tickers, *item_corps] if str(x).strip()}
                matched = False
                for fr in q_raw:
                    if any(fr and (fr in c or c in fr) for c in cands_raw):
                        matched = True
                        break
                if not matched:
                    for fn in q_norm:
                        if any(fn and (fn in c or c in fn) for c in cands_norm):
                            matched = True
                            break
                if not matched:
                    continue
                if canonical:
                    resolved_companies.append(canonical)
                tickers.update(item_tickers)
                corp_codes.update(item_corps)

    # fallback: preserve user input as company filter even if master miss
    if not resolved_companies:
        resolved_companies = companies

    return {
        "companies": sorted(set(resolved_companies)),
        "tickers": sorted(tickers),
        "corp_codes": sorted(corp_codes),
    }


def _as_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off", ""}:
        return False
    return default


def _iso_from_mtime(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat().replace("+00:00", "Z")


def _file_meta(path: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(ROOT_DIR)),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() else 0,
        "updated_at": _iso_from_mtime(path),
    }


def _task_raw_glob_patterns(task: str) -> list[str]:
    mapping: dict[str, list[str]] = {
        "fetch_dart_bulk": ["dart_*.json"],
        "fetch_dart_financials": ["dart_financials_*.json"],
        "fetch_news": ["news_*.json"],
        "build_company_financials_5y": ["financials_5y_*.json"],
        "build_customer_dependency": ["customer_dependency_*.json"],
        "extract_customer_dependency_llm": ["customer_dependency_llm_*.json"],
        "import_customer_dependency_external": ["customer_dependency_external_*.json"],
        "import_customer_dependency_reports": ["customer_dependency_external_*.json"],
    }
    return mapping.get(task, [])


def _snapshot_raw_files(patterns: list[str]) -> dict[str, tuple[int, int]]:
    snap: dict[str, tuple[int, int]] = {}
    for pat in patterns:
        for p in RAW_DIR.glob(pat):
            try:
                st = p.stat()
            except OSError:
                continue
            key = str(p.relative_to(ROOT_DIR))
            snap[key] = (st.st_size, st.st_mtime_ns)
    return snap


def _preview_raw_file(rel_path: str, include_full_payload: bool = False) -> dict[str, Any]:
    p = ROOT_DIR / rel_path
    out: dict[str, Any] = {"path": rel_path}
    if not p.exists() or not p.is_file():
        out["error"] = "file not found"
        return out
    out["size_bytes"] = p.stat().st_size
    out["updated_at"] = _iso_from_mtime(p)
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        out["error"] = "invalid json"
        return out
    if not isinstance(payload, dict):
        out["error"] = "invalid payload"
        return out

    # Common preview fields for admin decision making.
    out["company"] = str(payload.get("company") or payload.get("corp_name") or "").strip()
    out["ticker"] = str(payload.get("ticker") or payload.get("stock_code") or "").strip()
    out["market"] = str(payload.get("market") or payload.get("corp_cls") or "").strip()
    out["corp_code"] = str(payload.get("corp_code") or "").strip()
    out["source"] = str(payload.get("source") or "").strip()
    out["title"] = str(payload.get("title") or "").strip()
    out["summary"] = str(payload.get("summary") or "").strip()
    out["published_at"] = str(payload.get("published_at") or "").strip()
    out["url"] = str(payload.get("url") or "").strip()

    dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
    if dart:
        out["dart_status"] = str(dart.get("status") or "").strip()
        out["dart_message"] = str(dart.get("message") or "").strip()
        if "dart_financials_" in rel_path:
            out["financial_interpretation"] = _interpret_dart_financials_payload(payload, rel_path)
    if include_full_payload:
        out["full_payload"] = payload
    return out


def _to_float(v: Any) -> float | None:
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.replace(",", "").strip()
        if not s or s == "-":
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _pick_account_amount(rows: list[dict[str, Any]], names: list[str], ids: list[str]) -> float | None:
    for rid in ids:
        for row in rows:
            if str(row.get("account_id") or "").strip() == rid:
                v = _to_float(row.get("thstrm_amount"))
                if v is not None:
                    return v
    for nm in names:
        for row in rows:
            if str(row.get("account_nm") or "").strip() == nm:
                v = _to_float(row.get("thstrm_amount"))
                if v is not None:
                    return v
    return None


def _fmt_num(v: float | None) -> str:
    if v is None:
        return "정보 부족"
    return f"{int(v):,}" if float(v).is_integer() else f"{v:,.2f}"


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "정보 부족"
    return f"{v:.2f}%"


def _extract_year_from_rel_path(rel_path: str) -> int | None:
    m = re.search(r"dart_financials_\d+_(\d{4})_[A-Z]+\.json$", rel_path)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _interpret_dart_financials_payload(payload: dict[str, Any], rel_path: str) -> dict[str, Any]:
    dart = payload.get("dart") if isinstance(payload.get("dart"), dict) else {}
    rows = dart.get("list") if isinstance(dart.get("list"), list) else []
    typed_rows = [r for r in rows if isinstance(r, dict)]

    revenue = _pick_account_amount(
        typed_rows,
        names=["매출액", "수익(매출액)", "매출 및 지분법 손익", "영업수익"],
        ids=["ifrs-full_Revenue"],
    )
    op_income = _pick_account_amount(
        typed_rows,
        names=["영업이익", "영업이익(손실)"],
        ids=["ifrs-full_ProfitLossFromOperatingActivities"],
    )
    net_income = _pick_account_amount(
        typed_rows,
        names=["당기순이익", "당기순이익(손실)"],
        ids=["ifrs-full_ProfitLoss"],
    )
    assets = _pick_account_amount(
        typed_rows,
        names=["자산총계"],
        ids=["ifrs-full_Assets"],
    )
    liabilities = _pick_account_amount(
        typed_rows,
        names=["부채총계"],
        ids=["ifrs-full_Liabilities"],
    )
    equity = _pick_account_amount(
        typed_rows,
        names=["자본총계"],
        ids=["ifrs-full_Equity"],
    )
    op_margin = (op_income / revenue * 100.0) if (revenue and op_income is not None) else None
    net_margin = (net_income / revenue * 100.0) if (revenue and net_income is not None) else None
    debt_to_equity = (liabilities / equity * 100.0) if (equity and liabilities is not None) else None
    year = _extract_year_from_rel_path(rel_path)

    summary = (
        f"{year or '해당 연도'} 기준 매출 {_fmt_num(revenue)}, "
        f"영업이익 {_fmt_num(op_income)}, 순이익 {_fmt_num(net_income)}, "
        f"영업이익률 {_fmt_pct(op_margin)}, 순이익률 {_fmt_pct(net_margin)}, "
        f"부채/자본 {_fmt_pct(debt_to_equity)}"
    )

    return {
        "year": year,
        "revenue": revenue,
        "operating_income": op_income,
        "net_income": net_income,
        "assets": assets,
        "liabilities": liabilities,
        "equity": equity,
        "operating_margin_pct": op_margin,
        "net_margin_pct": net_margin,
        "debt_to_equity_pct": debt_to_equity,
        "summary": summary,
    }


def _build_interpretation_notes(previews: list[dict[str, Any]]) -> list[str]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for p in previews:
        fi = p.get("financial_interpretation") if isinstance(p.get("financial_interpretation"), dict) else None
        if not fi:
            continue
        company = str(p.get("company") or "").strip() or "unknown"
        grouped.setdefault(company, []).append({"preview": p, "fi": fi})

    notes: list[str] = []
    for company, items in grouped.items():
        items = sorted(items, key=lambda x: x["fi"].get("year") or 0)
        if not items:
            continue
        latest = items[-1]["fi"]
        notes.append(f"{company}: {latest.get('summary', '해석 정보 부족')}")
        if len(items) >= 2:
            prev = items[-2]["fi"]
            curr = items[-1]["fi"]
            prev_year = prev.get("year")
            curr_year = curr.get("year")
            rev_prev = _to_float(prev.get("revenue"))
            rev_curr = _to_float(curr.get("revenue"))
            op_prev = _to_float(prev.get("operating_income"))
            op_curr = _to_float(curr.get("operating_income"))
            ni_prev = _to_float(prev.get("net_income"))
            ni_curr = _to_float(curr.get("net_income"))

            def yoy(a: float | None, b: float | None) -> str:
                if a is None or b is None or a == 0:
                    return "정보 부족"
                return f"{((b / a) - 1.0) * 100.0:.2f}%"

            notes.append(
                f"{company} {prev_year}->{curr_year}: 매출 YoY {yoy(rev_prev, rev_curr)}, "
                f"영업이익 YoY {yoy(op_prev, op_curr)}, 순이익 YoY {yoy(ni_prev, ni_curr)}"
            )

    # 뉴스 파일 해석: 회사별 수집 건수 + 최신 기사 한줄
    news_grouped: dict[str, list[dict[str, Any]]] = {}
    for p in previews:
        path = str(p.get("path") or "")
        source = str(p.get("source") or "")
        if ("news_" not in path) and ("news" not in source.lower()):
            continue
        company = str(p.get("company") or "").strip() or "기업 미지정"
        news_grouped.setdefault(company, []).append(p)

    for company, items in news_grouped.items():
        items_sorted = sorted(items, key=lambda x: str(x.get("published_at") or x.get("updated_at") or ""), reverse=True)
        latest = items_sorted[0] if items_sorted else {}
        title = str(latest.get("title") or "").strip() or "제목 정보 없음"
        published_at = str(latest.get("published_at") or latest.get("updated_at") or "").strip() or "시각 정보 없음"
        summary = str(latest.get("summary") or "").strip()
        if summary:
            summary = summary[:90] + ("..." if len(summary) > 90 else "")
            notes.append(
                f"{company} 뉴스 {len(items)}건 수집. 최신({published_at}) '{title}', 요약: {summary}"
            )
        else:
            notes.append(f"{company} 뉴스 {len(items)}건 수집. 최신({published_at}) '{title}'")
    return notes


class DataAdminService:
    def status(self, health_meta: dict[str, Any]) -> dict[str, Any]:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        PROC_DIR.mkdir(parents=True, exist_ok=True)
        INDEX_DIR.mkdir(parents=True, exist_ok=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)

        raw_files = sorted(RAW_DIR.glob("*.json"))
        yahoo = sum(1 for p in raw_files if p.name.startswith("yahoo_"))
        dart = sum(1 for p in raw_files if p.name.startswith("dart_"))
        news = sum(1 for p in raw_files if p.name.startswith("news_"))
        valuation = sum(1 for p in raw_files if p.name.startswith("valuation_"))
        valuation_case = sum(1 for p in raw_files if p.name.startswith("valuation_case_"))
        synergy_case = sum(1 for p in raw_files if p.name.startswith("synergy_case_"))
        due_diligence_case = sum(1 for p in raw_files if p.name.startswith("due_diligence_case_"))
        strategic_case = sum(1 for p in raw_files if p.name.startswith("strategic_case_"))
        tam = sum(1 for p in raw_files if p.name.startswith("tam_"))
        commodity = sum(1 for p in raw_files if p.name.startswith("commodity_"))
        dart_notes = sum(1 for p in raw_files if p.name.startswith("dart_notes_"))
        dart_financials = sum(1 for p in raw_files if p.name.startswith("dart_financials_"))
        financials_5y = sum(1 for p in raw_files if p.name.startswith("financials_5y_"))
        customer_dependency_external = sum(1 for p in raw_files if p.name.startswith("customer_dependency_external_"))
        customer_dependency_llm = sum(1 for p in raw_files if p.name.startswith("customer_dependency_llm_"))
        customer_dependency = sum(
            1
            for p in raw_files
            if p.name.startswith("customer_dependency_")
            and not p.name.startswith("customer_dependency_external_")
            and not p.name.startswith("customer_dependency_llm_")
        )
        mna = sum(1 for p in raw_files if p.name.startswith("mna_"))
        market_share = sum(1 for p in raw_files if p.name.startswith("market_share_"))
        patent = sum(1 for p in raw_files if p.name.startswith("patent_"))
        esg = sum(1 for p in raw_files if p.name.startswith("esg_"))

        processed_targets = [
            PROC_DIR / "korea_universe.json",
            PROC_DIR / "korea_tickers_all.txt",
            PROC_DIR / "dart_corp_codes_listed.json",
            PROC_DIR / "company_master.json",
            PROC_DIR / "company_financials_5y.jsonl",
            PROC_DIR / "customer_dependency_facts.jsonl",
            PROC_DIR / "normalized_manifest.jsonl",
            PROC_DIR / "normalized_manifest_report.json",
        ]
        index_targets = [
            INDEX_DIR / "chunks.jsonl",
            INDEX_DIR / "index_state.json",
        ]

        recent_raw = [
            {
                "name": p.name,
                "size_bytes": p.stat().st_size,
                "updated_at": _iso_from_mtime(p),
            }
            for p in sorted(raw_files, key=lambda x: x.stat().st_mtime, reverse=True)[:10]
        ]

        recent_logs = [
            {
                "name": p.name,
                "size_bytes": p.stat().st_size,
                "updated_at": _iso_from_mtime(p),
            }
            for p in sorted(LOG_DIR.glob("*.log"), key=lambda x: x.stat().st_mtime, reverse=True)[:10]
        ]

        return {
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "health": health_meta,
            "raw": {
                "total_files": len(raw_files),
                "yahoo_files": yahoo,
                "dart_files": dart,
                "news_files": news,
                "valuation_files": valuation,
                "valuation_case_files": valuation_case,
                "synergy_case_files": synergy_case,
                "due_diligence_case_files": due_diligence_case,
                "strategic_case_files": strategic_case,
                "tam_files": tam,
                "commodity_files": commodity,
                "dart_notes_files": dart_notes,
                "dart_financials_files": dart_financials,
                "financials_5y_files": financials_5y,
                "customer_dependency_files": customer_dependency,
                "customer_dependency_external_files": customer_dependency_external,
                "customer_dependency_llm_files": customer_dependency_llm,
                "mna_files": mna,
                "market_share_files": market_share,
                "patent_files": patent,
                "esg_files": esg,
                "recent_files": recent_raw,
            },
            "processed": {
                "files": [_file_meta(p) for p in processed_targets],
            },
            "index": {
                "files": [_file_meta(p) for p in index_targets],
            },
            "logs": {
                "recent": recent_logs,
            },
        }

    def run_task(self, task: str, options: dict[str, Any] | None = None) -> dict[str, Any]:
        opts = options or {}
        company_filter = _resolve_company_filters(opts.get("companies"))
        companies = company_filter["companies"]
        tickers = company_filter["tickers"]
        corp_codes = company_filter["corp_codes"]

        cmd: list[str] | None = None
        if task == "normalize_manifest":
            cmd = [".venv/bin/python", "scripts/normalize_manifest.py"]
        elif task == "fetch_dart_bulk":
            try:
                sleep = float(opts.get("sleep", 0.25))
            except (TypeError, ValueError):
                return {"ok": False, "task": task, "error": "invalid option type for fetch_dart_bulk"}
            resume = _as_bool(opts.get("resume"), default=True)
            if sleep < 0 or sleep > 5:
                return {"ok": False, "task": task, "error": "sleep must be between 0 and 5"}
            cmd = [".venv/bin/python", "scripts/fetch_dart_bulk.py", "--sleep", str(sleep)]
            if resume:
                cmd.append("--resume")
            if corp_codes:
                cmd.extend(["--corp-codes", *corp_codes])
        elif task == "sync_index_state":
            cmd = [".venv/bin/python", "scripts/sync_index_state.py"]
        elif task == "build_company_master":
            cmd = [".venv/bin/python", "scripts/build_company_master.py"]
        elif task == "fetch_dart_financials":
            try:
                years = int(opts.get("years", 5))
                sleep = float(opts.get("sleep", 0.25))
                fs_div = str(opts.get("fs_div", "CFS")).strip().upper() or "CFS"
            except (TypeError, ValueError):
                return {"ok": False, "task": task, "error": "invalid option type for fetch_dart_financials"}
            resume = bool(opts.get("resume", True))

            if years < 1 or years > 10:
                return {"ok": False, "task": task, "error": "years must be between 1 and 10"}
            if sleep < 0 or sleep > 5:
                return {"ok": False, "task": task, "error": "sleep must be between 0 and 5"}
            if fs_div not in {"CFS", "OFS"}:
                return {"ok": False, "task": task, "error": "fs_div must be CFS or OFS"}

            cmd = [
                ".venv/bin/python",
                "scripts/fetch_dart_financials.py",
                "--years",
                str(years),
                "--sleep",
                str(sleep),
                "--fs-div",
                fs_div,
            ]
            if resume:
                cmd.append("--resume")
            if corp_codes:
                cmd.extend(["--corp-codes", *corp_codes])
            elif tickers:
                cmd.extend(["--tickers", *tickers])
        elif task == "build_company_financials_5y":
            try:
                min_years = int(opts.get("min_years", 3))
            except (TypeError, ValueError):
                return {
                    "ok": False,
                    "task": task,
                    "error": "invalid option type for build_company_financials_5y",
                }
            write_raw = bool(opts.get("write_raw", True))
            if min_years < 1 or min_years > 5:
                return {"ok": False, "task": task, "error": "min_years must be between 1 and 5"}
            cmd = [
                ".venv/bin/python",
                "scripts/build_company_financials_5y.py",
                "--min-years",
                str(min_years),
            ]
            if write_raw:
                cmd.append("--write-raw")
            if companies:
                cmd.extend(["--companies", *companies])
        elif task == "import_customer_dependency_external":
            input_csv = str(opts.get("input_csv", "data/external/customer_dependency.csv")).strip() or "data/external/customer_dependency.csv"
            resume = bool(opts.get("resume", True))
            cmd = [
                ".venv/bin/python",
                "scripts/import_customer_dependency_external.py",
                "--input-csv",
                input_csv,
            ]
            if resume:
                cmd.append("--resume")
        elif task == "import_customer_dependency_reports":
            input_dir = str(opts.get("input_dir", "data/external/customer_reports")).strip() or "data/external/customer_reports"
            resume = bool(opts.get("resume", True))
            cmd = [
                ".venv/bin/python",
                "scripts/import_customer_dependency_reports.py",
                "--input-dir",
                input_dir,
            ]
            if resume:
                cmd.append("--resume")
        elif task == "build_customer_dependency":
            try:
                min_customers = int(opts.get("min_customers", 1))
            except (TypeError, ValueError):
                return {
                    "ok": False,
                    "task": task,
                    "error": "invalid option type for build_customer_dependency",
                }
            write_raw = bool(opts.get("write_raw", True))
            if min_customers < 0 or min_customers > 10:
                return {"ok": False, "task": task, "error": "min_customers must be between 0 and 10"}
            cmd = [
                ".venv/bin/python",
                "scripts/build_customer_dependency.py",
                "--min-customers",
                str(min_customers),
            ]
            if write_raw:
                cmd.append("--write-raw")
            if companies:
                cmd.extend(["--companies", *companies])
        elif task == "extract_customer_dependency_llm":
            provider = str(opts.get("provider", "openai")).strip().lower() or "openai"
            model = str(opts.get("model", "")).strip()
            companies_raw = opts.get("companies")
            try:
                limit = int(opts.get("limit", 50))
                max_context_chars = int(opts.get("max_context_chars", 10000))
                min_confidence = float(opts.get("min_confidence", 0.3))
                timeout = int(opts.get("timeout", 60))
            except (TypeError, ValueError):
                return {"ok": False, "task": task, "error": "invalid option type for extract_customer_dependency_llm"}
            resume = bool(opts.get("resume", True))
            allow_empty_context = bool(opts.get("allow_empty_context", False))
            if provider not in {"openai", "gemini"}:
                return {"ok": False, "task": task, "error": "provider must be openai or gemini"}
            if limit < 1 or limit > 5000:
                return {"ok": False, "task": task, "error": "limit must be between 1 and 5000"}
            if max_context_chars < 1000 or max_context_chars > 50000:
                return {"ok": False, "task": task, "error": "max_context_chars must be between 1000 and 50000"}
            if min_confidence < 0 or min_confidence > 1:
                return {"ok": False, "task": task, "error": "min_confidence must be in [0, 1]"}
            if timeout < 10 or timeout > 300:
                return {"ok": False, "task": task, "error": "timeout must be between 10 and 300"}
            cmd = [
                ".venv/bin/python",
                "scripts/extract_customer_dependency_llm.py",
                "--provider",
                provider,
                "--limit",
                str(limit),
                "--max-context-chars",
                str(max_context_chars),
                "--min-confidence",
                str(min_confidence),
                "--timeout",
                str(timeout),
            ]
            if model:
                cmd.extend(["--model", model])
            companies: list[str] = []
            if isinstance(companies_raw, str):
                companies = [x.strip() for x in companies_raw.split(",") if x.strip()]
            elif isinstance(companies_raw, list):
                companies = [str(x).strip() for x in companies_raw if str(x).strip()]
            for c in companies[:100]:
                cmd.extend(["--company", c])
            if resume:
                cmd.append("--resume")
            if allow_empty_context:
                cmd.append("--allow-empty-context")
        elif task == "fetch_news":
            try:
                limit_company = int(opts.get("limit_company", 80))
                per_company = int(opts.get("per_company", 2))
                sleep = float(opts.get("sleep", 0.2))
            except (TypeError, ValueError):
                return {
                    "ok": False,
                    "task": task,
                    "error": "invalid option type for fetch_news",
                }
            resume = bool(opts.get("resume", True))

            if limit_company < 1 or limit_company > 5000:
                return {
                    "ok": False,
                    "task": task,
                    "error": "limit_company must be between 1 and 5000",
                }
            if per_company < 1 or per_company > 20:
                return {
                    "ok": False,
                    "task": task,
                    "error": "per_company must be between 1 and 20",
                }
            if sleep < 0 or sleep > 5:
                return {
                    "ok": False,
                    "task": task,
                    "error": "sleep must be between 0 and 5",
                }

            cmd = [
                ".venv/bin/python",
                "scripts/fetch_news.py",
                "--universe-file",
                "data/processed/korea_universe.json",
                "--limit-company",
                str(limit_company),
                "--per-company",
                str(per_company),
                "--sleep",
                str(sleep),
            ]
            if resume:
                cmd.append("--resume")
            if companies:
                cmd.extend(["--companies", *companies])
        elif task == "incremental_index":
            cmd = ["./scripts/run_index_incremental.sh"]
        elif task == "full_index":
            cmd = ["./scripts/run_index_full.sh"]
        elif task == "eval_baseline":
            try:
                limit = int(opts.get("limit", 10))
            except (TypeError, ValueError):
                return {
                    "ok": False,
                    "task": task,
                    "error": "invalid option type for eval_baseline",
                }
            if limit < 1 or limit > 500:
                return {
                    "ok": False,
                    "task": task,
                    "error": "limit must be between 1 and 500",
                }
            cmd = [".venv/bin/python", "scripts/eval_search_baseline.py", "--limit", str(limit)]
        elif task == "industry_special_pipeline":
            industries = str(
                opts.get(
                    "industries",
                    "반도체,바이오,2차전지,자동차,방산,조선,클라우드,에너지",
                )
            ).strip()
            commodity_file = str(opts.get("commodity_file", "")).strip()

            try:
                min_samples = int(opts.get("min_samples", 3))
                tam_multiplier = float(opts.get("tam_multiplier", 2.0))
                sam_ratio = float(opts.get("sam_ratio", 0.35))
                som_ratio = float(opts.get("som_ratio", 0.1))
            except (TypeError, ValueError):
                return {
                    "ok": False,
                    "task": task,
                    "error": "invalid numeric option for industry_special_pipeline",
                }

            if min_samples < 1 or min_samples > 20:
                return {"ok": False, "task": task, "error": "min_samples must be between 1 and 20"}
            if tam_multiplier <= 0 or tam_multiplier > 20:
                return {"ok": False, "task": task, "error": "tam_multiplier must be in (0, 20]"}
            if sam_ratio <= 0 or sam_ratio > 1:
                return {"ok": False, "task": task, "error": "sam_ratio must be in (0, 1]"}
            if som_ratio <= 0 or som_ratio > 1:
                return {"ok": False, "task": task, "error": "som_ratio must be in (0, 1]"}

            cmd = [
                "env",
                f"INDUSTRIES={industries}",
                f"MIN_SAMPLES={min_samples}",
                f"TAM_MULTIPLIER={tam_multiplier}",
                f"SAM_RATIO={sam_ratio}",
                f"SOM_RATIO={som_ratio}",
                f"COMMODITY_FILE={commodity_file}",
                "RESUME=1",
                "./scripts/run_industry_special_pipeline.sh",
            ]
        elif task == "build_valuation_cases":
            cmd = [".venv/bin/python", "scripts/build_valuation_cases.py", "--resume"]
        elif task == "build_synergy_cases":
            cmd = [".venv/bin/python", "scripts/build_synergy_cases.py", "--resume"]
        elif task == "build_due_diligence_cases":
            cmd = [".venv/bin/python", "scripts/build_due_diligence_cases.py", "--resume"]
        elif task == "build_strategic_cases":
            cmd = [".venv/bin/python", "scripts/build_strategic_cases.py", "--resume"]
        elif task == "parse_dart_notes":
            cmd = [".venv/bin/python", "scripts/parse_dart_notes.py", "--resume"]
            if companies:
                cmd.extend(["--companies", *companies])
        elif task == "external_enrichment":
            external_dir = str(opts.get("external_dir", "data/external")).strip() or "data/external"
            cmd = [
                "env",
                f"EXTERNAL_DIR={external_dir}",
                "RESUME=1",
                "./scripts/run_external_enrichment.sh",
            ]
        elif task == "eval_target_analysis":
            cmd = [
                ".venv/bin/python",
                "scripts/eval_target_analysis.py",
                "--cases",
                "eval/target_analysis_questions_v1.jsonl",
                "--out",
                "logs/eval_target_analysis_latest.json",
            ]
        elif task == "eval_valuation_analysis":
            cmd = [
                ".venv/bin/python",
                "scripts/eval_valuation_analysis.py",
                "--cases",
                "eval/valuation_analysis_21_30_v1.jsonl",
                "--out",
                "logs/eval_valuation_analysis_latest.json",
            ]
        elif task == "eval_synergy_analysis":
            cmd = [
                ".venv/bin/python",
                "scripts/eval_synergy_analysis.py",
                "--cases",
                "eval/synergy_analysis_31_40_v1.jsonl",
                "--out",
                "logs/eval_synergy_analysis_latest.json",
            ]
        elif task == "eval_due_diligence_analysis":
            cmd = [
                ".venv/bin/python",
                "scripts/eval_due_diligence_analysis.py",
                "--cases",
                "eval/due_diligence_analysis_41_50_v1.jsonl",
                "--out",
                "logs/eval_due_diligence_analysis_latest.json",
            ]
        elif task == "eval_strategic_analysis":
            cmd = [
                ".venv/bin/python",
                "scripts/eval_strategic_analysis.py",
                "--cases",
                "eval/strategic_analysis_51_60_v1.jsonl",
                "--out",
                "logs/eval_strategic_analysis_latest.json",
            ]

        if not cmd:
            return {
                "ok": False,
                "task": task,
                "error": f"unsupported task: {task}",
            }

        raw_patterns = _task_raw_glob_patterns(task)
        before_raw = _snapshot_raw_files(raw_patterns) if raw_patterns else {}

        try:
            proc = subprocess.run(
                cmd,
                cwd=str(ROOT_DIR),
                capture_output=True,
                text=True,
                timeout=1800,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return {
                "ok": False,
                "task": task,
                "error": "task timeout (1800s)",
            }

        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        after_raw = _snapshot_raw_files(raw_patterns) if raw_patterns else {}

        collection: dict[str, Any] | None = None
        if raw_patterns:
            added = sorted([k for k in after_raw.keys() if k not in before_raw])
            updated = sorted([k for k, v in after_raw.items() if k in before_raw and before_raw[k] != v])
            touched = added + updated
            include_full_payload = task in {"fetch_dart_bulk"}
            collection = {
                "patterns": raw_patterns,
                "added_count": len(added),
                "updated_count": len(updated),
                "total_touched": len(added) + len(updated),
                "added_files": added[:30],
                "updated_files": updated[:30],
                "file_previews": [_preview_raw_file(x, include_full_payload=include_full_payload) for x in touched[:10]],
            }
            previews = collection.get("file_previews") if isinstance(collection.get("file_previews"), list) else []
            collection["interpretation_notes"] = _build_interpretation_notes([x for x in previews if isinstance(x, dict)])

        out = {
            "ok": proc.returncode == 0,
            "task": task,
            "command": " ".join(shlex.quote(x) for x in cmd),
            "exit_code": proc.returncode,
            "stdout_tail": "\n".join(stdout.splitlines()[-80:]) if stdout else "",
            "stderr_tail": "\n".join(stderr.splitlines()[-80:]) if stderr else "",
            "finished_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }
        if companies or tickers or corp_codes:
            out["resolved_filter"] = {
                "companies": companies,
                "tickers": tickers,
                "corp_codes": corp_codes,
            }
        if collection is not None:
            out["collection"] = collection
        return out
