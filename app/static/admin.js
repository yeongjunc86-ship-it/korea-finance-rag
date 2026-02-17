async function getJson(url) {
  const resp = await fetch(url);
  if (!resp.ok) {
    if (resp.status === 401 || resp.status === 403) {
      location.href = "/login";
      throw new Error("로그인이 필요합니다.");
    }
    const text = await resp.text();
    throw new Error(text || `HTTP ${resp.status}`);
  }
  return resp.json();
}

async function postJson(url, payload) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    if (resp.status === 401 || resp.status === 403) {
      location.href = "/login";
      throw new Error("로그인이 필요합니다.");
    }
    const text = await resp.text();
    throw new Error(text || `HTTP ${resp.status}`);
  }
  return resp.json();
}

const statusView = document.getElementById("statusView");
const taskView = document.getElementById("taskView");
const exportTxtBtn = document.getElementById("exportTxtBtn");
const exportPdfBtn = document.getElementById("exportPdfBtn");

const taskNameMap = {
  normalize_manifest: "매니페스트 정규화",
  fetch_dart_bulk: "DART 대량 수집",
  fetch_news: "최신 뉴스 수집",
  build_company_master: "회사 마스터 생성",
  fetch_dart_financials: "DART 재무(5Y) 수집",
  build_company_financials_5y: "5개년 재무 팩트 생성",
  import_customer_dependency_external: "고객의존도 외부 Import",
  import_customer_dependency_reports: "고객의존도 보고서 Import",
  extract_customer_dependency_llm: "고객의존도 LLM 추출",
  build_customer_dependency: "고객의존도 프로파일 생성",
  sync_index_state: "인덱스 상태 동기화",
  incremental_index: "새 데이터 만들기",
  full_index: "전체 인덱스 실행",
  reload_index: "서비스 반영하기",
  eval_baseline: "기준 평가 실행",
  industry_special_pipeline: "산업(13/16/19) 데이터 생성",
  build_valuation_cases: "밸류에이션 케이스 생성",
  parse_dart_notes: "DART 주석 파싱",
  external_enrichment: "외부 데이터 병합",
  eval_target_analysis: "타겟 10문항 평가",
  eval_valuation_analysis: "밸류 10문항 평가",
  build_synergy_cases: "시너지 케이스 생성",
  eval_synergy_analysis: "시너지 10문항 평가",
  build_due_diligence_cases: "실사 케이스 생성",
  eval_due_diligence_analysis: "실사 10문항 평가",
  build_strategic_cases: "전략 케이스 생성",
  eval_strategic_analysis: "전략 10문항 평가",
};

function formatStatus(resp) {
  const data = resp?.data || {};
  const health = data.health || {};
  const raw = data.raw || {};

  const lines = [];
  lines.push(`생성 시각: ${data.generated_at || "정보 없음"}`);
  lines.push("");
  lines.push("[서비스 상태]");
  lines.push(`- 인덱스 로드: ${health.index_loaded ? "예" : "아니오"}`);
  lines.push(`- 청크 수: ${health.chunk_count ?? 0}`);
  lines.push(`- 인덱스 버전: ${health.index_version ?? "정보 없음"}`);
  lines.push(`- 인덱싱 문서 수: ${health.indexed_doc_count ?? 0}`);
  lines.push(`- 인덱스 갱신 시각: ${health.index_updated_at || "정보 없음"}`);
  lines.push("");
  lines.push("[원천 데이터]");
  lines.push(`- 전체 파일: ${raw.total_files ?? 0}`);
  lines.push(`- Yahoo 파일: ${raw.yahoo_files ?? 0}`);
  lines.push(`- DART 파일: ${raw.dart_files ?? 0}`);
  lines.push(`- 뉴스 파일: ${raw.news_files ?? 0}`);
  lines.push(`- 멀티플 파일: ${raw.valuation_files ?? 0}`);
  lines.push(`- 밸류케이스 파일: ${raw.valuation_case_files ?? 0}`);
  lines.push(`- 시너지케이스 파일: ${raw.synergy_case_files ?? 0}`);
  lines.push(`- 실사케이스 파일: ${raw.due_diligence_case_files ?? 0}`);
  lines.push(`- 전략케이스 파일: ${raw.strategic_case_files ?? 0}`);
  lines.push(`- TAM/SAM/SOM 파일: ${raw.tam_files ?? 0}`);
  lines.push(`- 원자재 민감도 파일: ${raw.commodity_files ?? 0}`);
  lines.push(`- DART 주석 파일: ${raw.dart_notes_files ?? 0}`);
  lines.push(`- DART 재무 파일: ${raw.dart_financials_files ?? 0}`);
  lines.push(`- 재무 5Y 파일: ${raw.financials_5y_files ?? 0}`);
  lines.push(`- 고객의존도 파일: ${raw.customer_dependency_files ?? 0}`);
  lines.push(`- 고객의존도 외부 파일: ${raw.customer_dependency_external_files ?? 0}`);
  lines.push(`- 고객의존도 LLM 파일: ${raw.customer_dependency_llm_files ?? 0}`);
  lines.push(`- M&A comps 파일: ${raw.mna_files ?? 0}`);
  lines.push(`- 시장점유율 파일: ${raw.market_share_files ?? 0}`);
  lines.push(`- 특허 파일: ${raw.patent_files ?? 0}`);
  lines.push(`- ESG 파일: ${raw.esg_files ?? 0}`);
  lines.push("");
  lines.push("[최근 원천 파일 5개]");
  const recent = Array.isArray(raw.recent_files) ? raw.recent_files.slice(0, 5) : [];
  if (!recent.length) {
    lines.push("- 없음");
  } else {
    recent.forEach((r) => {
      lines.push(`- ${r.name} (${r.updated_at || "시간 없음"})`);
    });
  }

  return lines.join("\n");
}

function updateKpi(resp) {
  const data = resp?.data || {};
  const health = data.health || {};
  const raw = data.raw || {};
  document.getElementById("kpiIndexLoaded").textContent = health.index_loaded ? "정상" : "미로드";
  document.getElementById("kpiChunkCount").textContent = String(health.chunk_count ?? 0);
  document.getElementById("kpiNewsCount").textContent = String(raw.news_files ?? 0);
  document.getElementById("kpiUpdatedAt").textContent = String(data.generated_at || "-");
}

function formatTaskResult(data) {
  if (data?.task === "reload_index") {
    return [
      `작업: ${taskNameMap.reload_index}`,
      `결과: ${data.ok ? "성공" : "실패"}`,
      `청크 수: ${data.chunk_count ?? "정보 없음"}`,
    ].join("\n");
  }

  const taskLabel = taskNameMap[data?.task] || data?.task || "알 수 없는 작업";
  const lines = [];
  lines.push(`작업: ${taskLabel}`);
  lines.push(`결과: ${data?.ok ? "성공" : "실패"}`);
  lines.push(`종료 코드: ${data?.exit_code ?? "정보 없음"}`);
  lines.push(`완료 시각: ${data?.finished_at || "정보 없음"}`);
  lines.push(`실행 명령: ${data?.command || "정보 없음"}`);
  const rf = data?.resolved_filter;
  if (rf && typeof rf === "object") {
    const c = Array.isArray(rf.companies) ? rf.companies.join(", ") : "";
    const t = Array.isArray(rf.tickers) ? rf.tickers.join(", ") : "";
    const cc = Array.isArray(rf.corp_codes) ? rf.corp_codes.join(", ") : "";
    lines.push(`대상 회사 해석: ${c || "없음"}`);
    lines.push(`대상 티커 해석: ${t || "없음"}`);
    lines.push(`대상 corp_code 해석: ${cc || "없음"}`);
  }
  const c = data?.collection;
  if (c && typeof c === "object") {
    lines.push("");
    lines.push("[수집 결과]");
    lines.push(`- 신규 파일: ${Number(c.added_count) || 0}건`);
    lines.push(`- 갱신 파일: ${Number(c.updated_count) || 0}건`);
    lines.push(`- 총 반영: ${Number(c.total_touched) || 0}건`);
    const addedFiles = Array.isArray(c.added_files) ? c.added_files : [];
    const updatedFiles = Array.isArray(c.updated_files) ? c.updated_files : [];
    if (addedFiles.length) {
      lines.push("- 신규 파일 목록:");
      addedFiles.slice(0, 10).forEach((name) => lines.push(`  · ${name}`));
    }
    if (updatedFiles.length) {
      lines.push("- 갱신 파일 목록:");
      updatedFiles.slice(0, 10).forEach((name) => lines.push(`  · ${name}`));
    }
    if (!addedFiles.length && !updatedFiles.length) {
      lines.push("- 이번 실행에서 변경된 대상 파일이 없습니다.");
    }
    const previews = Array.isArray(c.file_previews) ? c.file_previews : [];
    const notes = Array.isArray(c.interpretation_notes) ? c.interpretation_notes : [];
    if (notes.length) {
      lines.push("");
      lines.push("[자동 해석]");
      notes.slice(0, 10).forEach((n) => lines.push(`- ${n}`));
    }
    if (previews.length) {
      lines.push("");
      lines.push("[파일 내용 미리보기]");
      previews.forEach((p, idx) => {
        lines.push(`${idx + 1}. ${p.path || "경로 없음"}`);
        lines.push(`   회사: ${p.company || "정보 없음"} / 티커: ${p.ticker || "정보 없음"} / 시장: ${p.market || "정보 없음"}`);
        lines.push(`   corp_code: ${p.corp_code || "정보 없음"} / source: ${p.source || "정보 없음"}`);
        if (p.dart_status || p.dart_message) {
          lines.push(`   DART 상태: ${p.dart_status || "정보 없음"} (${p.dart_message || "정보 없음"})`);
        }
        if (p.title) {
          lines.push(`   제목: ${p.title}`);
        }
        if (p.summary) {
          lines.push(`   요약: ${String(p.summary).slice(0, 180)}`);
        }
        if (p.financial_interpretation && typeof p.financial_interpretation === "object") {
          lines.push(`   재무 해석: ${p.financial_interpretation.summary || "정보 없음"}`);
        }
        if (p.full_payload && typeof p.full_payload === "object") {
          lines.push("   전체 필드(JSON):");
          const pretty = JSON.stringify(p.full_payload, null, 2) || "{}";
          pretty.split("\n").forEach((ln) => lines.push(`   ${ln}`));
        }
      });
    }
  }
  lines.push("");
  lines.push("[표준 출력]");
  lines.push(data?.stdout_tail || "없음");
  lines.push("");
  lines.push("[표준 오류]");
  lines.push(data?.stderr_tail || "없음");
  return lines.join("\n");
}

function nowCompact() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${y}${m}${day}_${hh}${mm}${ss}`;
}

function downloadText(filename, text) {
  const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function exportTaskResultTxt() {
  const text = String(taskView.textContent || "").trim();
  if (!text) {
    alert("내보낼 작업 결과가 없습니다.");
    return;
  }
  downloadText(`admin_task_result_${nowCompact()}.txt`, text);
}

function exportTaskResultPdf() {
  const text = String(taskView.textContent || "").trim();
  if (!text) {
    alert("내보낼 작업 결과가 없습니다.");
    return;
  }
  const esc = (s) =>
    String(s)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  const html = `<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <title>작업 결과</title>
  <style>
    body { font-family: 'Noto Sans KR', sans-serif; margin: 24px; }
    h1 { font-size: 18px; margin: 0 0 12px; }
    pre { white-space: pre-wrap; word-break: break-word; font-size: 12px; line-height: 1.5; }
    @media print { body { margin: 12mm; } }
  </style>
</head>
<body>
  <h1>작업 결과 (${nowCompact()})</h1>
  <pre>${esc(text)}</pre>
</body>
</html>`;
  const win = window.open("", "_blank");
  if (!win) {
    alert("팝업이 차단되어 PDF 저장 창을 열 수 없습니다.");
    return;
  }
  win.document.open();
  win.document.write(html);
  win.document.close();
  setTimeout(() => {
    win.focus();
    win.print();
  }, 200);
}

async function refreshStatus() {
  statusView.textContent = "조회 중...";
  try {
    const data = await getJson("/api/admin/status");
    updateKpi(data);
    statusView.textContent = formatStatus(data);
  } catch (err) {
    statusView.textContent = `오류: ${err.message}`;
  }
}

async function loadCompanySearchSettings() {
  try {
    const data = await getJson("/api/admin/company-search-settings");
    const openaiEl = document.getElementById("companySearchEnableOpenai");
    const geminiEl = document.getElementById("companySearchEnableGemini");
    if (openaiEl) openaiEl.checked = Boolean(data.enable_openai);
    if (geminiEl) geminiEl.checked = Boolean(data.enable_gemini);
  } catch (_) {
    // Keep defaults on error.
  }
}

async function saveCompanySearchSettings() {
  const openaiEl = document.getElementById("companySearchEnableOpenai");
  const geminiEl = document.getElementById("companySearchEnableGemini");
  const payload = {
    enable_openai: Boolean(openaiEl?.checked),
    enable_gemini: Boolean(geminiEl?.checked),
  };
  try {
    const data = await postJson("/api/admin/company-search-settings", payload);
    const m = `업체검색 설정 저장 완료 (openai=${data.enable_openai}, gemini=${data.enable_gemini})`;
    taskView.textContent = m;
  } catch (err) {
    taskView.textContent = `업체검색 설정 저장 오류: ${err.message}`;
  }
}

async function runTask(task) {
  taskView.textContent = `실행 중: ${taskNameMap[task] || task}`;
  try {
    const payload = { task };
    const options = {};
    const commonCompanies = String(document.getElementById("commonCompanies")?.value || "").trim();
    const companyScopedTasks = new Set([
      "fetch_dart_bulk",
      "fetch_news",
      "fetch_dart_financials",
      "parse_dart_notes",
      "build_company_financials_5y",
      "build_customer_dependency",
      "extract_customer_dependency_llm",
    ]);
    if (commonCompanies && companyScopedTasks.has(task)) {
      options.companies = commonCompanies;
    }

    if (task === "fetch_news") {
      const limitCompany = Number(document.getElementById("newsLimitCompany")?.value || 0);
      const perCompany = Number(document.getElementById("newsPerCompany")?.value || 0);
      const sleep = Number(document.getElementById("newsSleep")?.value || 0);
      const resume = Boolean(document.getElementById("newsResume")?.checked);
      options.limit_company = limitCompany;
      options.per_company = perCompany;
      options.sleep = sleep;
      options.resume = resume;
    }

    if (task === "fetch_dart_bulk") {
      options.sleep = Number(document.getElementById("dartBulkSleep")?.value || 0.25);
      options.resume = Boolean(document.getElementById("dartBulkResume")?.checked);
    }

    if (task === "fetch_dart_financials") {
      options.years = Number(document.getElementById("dartFinancialYears")?.value || 0);
      options.sleep = Number(document.getElementById("dartFinancialSleep")?.value || 0);
      options.fs_div = String(document.getElementById("dartFinancialFsDiv")?.value || "CFS").trim();
      options.resume = Boolean(document.getElementById("dartFinancialResume")?.checked);
    }

    if (task === "build_company_financials_5y") {
      options.min_years = Number(document.getElementById("financial5yMinYears")?.value || 0);
      options.write_raw = Boolean(document.getElementById("financial5yWriteRaw")?.checked);
    }

    if (task === "import_customer_dependency_external") {
      options.input_csv = String(document.getElementById("customerDependencyCsv")?.value || "").trim();
      options.resume = Boolean(document.getElementById("customerDependencyResume")?.checked);
    }

    if (task === "import_customer_dependency_reports") {
      options.input_dir = String(document.getElementById("customerDependencyReportDir")?.value || "").trim();
      options.resume = Boolean(document.getElementById("customerDependencyResume")?.checked);
    }

    if (task === "build_customer_dependency") {
      options.min_customers = Number(document.getElementById("customerDependencyMinCustomers")?.value || 0);
      options.write_raw = Boolean(document.getElementById("customerDependencyWriteRaw")?.checked);
    }

    if (task === "extract_customer_dependency_llm") {
      options.companies = commonCompanies;
      options.provider = String(document.getElementById("customerDependencyLlmProvider")?.value || "openai").trim();
      options.model = String(document.getElementById("customerDependencyLlmModel")?.value || "").trim();
      options.limit = Number(document.getElementById("customerDependencyLlmLimit")?.value || 0);
      options.min_confidence = Number(document.getElementById("customerDependencyLlmMinConfidence")?.value || 0);
      options.resume = Boolean(document.getElementById("customerDependencyResume")?.checked);
      options.allow_empty_context = Boolean(document.getElementById("customerDependencyAllowEmptyContext")?.checked);
    }

    if (task === "eval_baseline") {
      const limit = Number(document.getElementById("evalLimit")?.value || 0);
      options.limit = limit;
    }

    if (task === "industry_special_pipeline") {
      options.industries = String(document.getElementById("industryList")?.value || "").trim();
      options.min_samples = Number(document.getElementById("industryMinSamples")?.value || 0);
      options.tam_multiplier = Number(document.getElementById("industryTamMultiplier")?.value || 0);
      options.sam_ratio = Number(document.getElementById("industrySamRatio")?.value || 0);
      options.som_ratio = Number(document.getElementById("industrySomRatio")?.value || 0);
    }

    if (task === "external_enrichment") {
      options.external_dir = String(document.getElementById("externalDir")?.value || "").trim();
    }

    if (Object.keys(options).length > 0) {
      payload.options = options;
    }

    const data = await postJson("/api/admin/run-task", payload);
    taskView.textContent = formatTaskResult(data);
    await refreshStatus();
  } catch (err) {
    taskView.textContent = `오류: ${err.message}`;
  }
}

document.getElementById("refreshBtn").addEventListener("click", refreshStatus);
document.querySelectorAll(".task-btn").forEach((btn) => {
  btn.addEventListener("click", () => runTask(btn.dataset.task));
});
document.getElementById("logoutBtn").addEventListener("click", async () => {
  try {
    await postJson("/api/auth/logout", {});
  } finally {
    location.href = "/login";
  }
});
if (exportTxtBtn) {
  exportTxtBtn.addEventListener("click", exportTaskResultTxt);
}
if (exportPdfBtn) {
  exportPdfBtn.addEventListener("click", exportTaskResultPdf);
}
const saveCompanySearchSettingsBtn = document.getElementById("saveCompanySearchSettingsBtn");
if (saveCompanySearchSettingsBtn) {
  saveCompanySearchSettingsBtn.addEventListener("click", saveCompanySearchSettings);
}

refreshStatus();
loadCompanySearchSettings();
