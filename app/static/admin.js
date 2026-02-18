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
const disclosureStatusView = document.getElementById("disclosureStatusView");
const disclosureProgressBar = document.getElementById("disclosureProgressBar");
let adminLastAiQuery = "";
let adminLastAiRows = [];
let adminLastAiProvider = "";
let adminLastInternetQuery = "";
let adminLastInternetRows = [];
let disclosurePollTimer = null;

async function readTxtFile(file) {
  if (!file) return "";
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("TXT 파일 읽기 실패"));
    reader.readAsText(file, "utf-8");
  });
}

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

function renderDisclosureStatus(data) {
  if (!disclosureStatusView || !disclosureProgressBar) return;
  const running = Boolean(data?.running);
  const pct = Number(data?.progress_percent || 0);
  const cur = Number(data?.progress_current || 0);
  const total = Number(data?.progress_total || 0);
  const rc = data?.return_code;
  disclosureProgressBar.value = Math.max(0, Math.min(100, pct));

  const lines = [];
  lines.push(`상태: ${running ? "실행 중" : (rc === 0 ? "완료" : (rc == null ? "대기/미실행" : "실패/중지"))}`);
  lines.push(`진행률: ${pct}% (${cur}/${total || "?"})`);
  lines.push(`경과: ${Number(data?.elapsed_sec || 0)}초`);
  if (data?.log_path) lines.push(`로그: ${data.log_path}`);
  if (data?.reloaded) lines.push(`메모리 반영: 완료 (청크 수 ${data?.chunk_count ?? "정보 없음"})`);
  lines.push("");
  lines.push("[최근 로그]");
  lines.push(String(data?.log_tail || "로그 없음"));
  disclosureStatusView.textContent = lines.join("\n");
}

async function refreshDisclosureStatus() {
  if (!disclosureStatusView) return;
  try {
    const data = await getJson("/api/admin/disclosure/status");
    renderDisclosureStatus(data);
    const running = Boolean(data?.running);
    if (!running && disclosurePollTimer) {
      clearInterval(disclosurePollTimer);
      disclosurePollTimer = null;
      await refreshStatus();
    }
  } catch (err) {
    disclosureStatusView.textContent = `오류: ${err.message}`;
  }
}

async function startDisclosureBulk() {
  try {
    const data = await postJson("/api/admin/disclosure/start", {});
    renderDisclosureStatus({ running: true, ...data, progress_percent: 0, progress_current: 0, progress_total: 4, log_tail: "" });
    if (disclosurePollTimer) clearInterval(disclosurePollTimer);
    disclosurePollTimer = setInterval(refreshDisclosureStatus, 2000);
    await refreshDisclosureStatus();
  } catch (err) {
    disclosureStatusView.textContent = `오류: ${err.message}`;
  }
}

async function stopDisclosureBulk() {
  try {
    await postJson("/api/admin/disclosure/stop", {});
    await refreshDisclosureStatus();
  } catch (err) {
    disclosureStatusView.textContent = `오류: ${err.message}`;
  }
}

async function runAdminAiSearch() {
  const commonCompanies = String(document.getElementById("commonCompanies")?.value || "").trim();
  const promptEl = document.getElementById("adminAiSearchPrompt");
  const txtFileEl = document.getElementById("adminAiSearchTxtFile");
  const templateEl = document.getElementById("adminAiSearchTemplate");
  const providerEl = document.getElementById("adminAiSearchProvider");
  const topKEl = document.getElementById("adminAiSearchTopK");
  const resultView = document.getElementById("adminAiSearchResultView");
  const promptView = document.getElementById("adminAiSearchPromptView");
  const rawView = document.getElementById("adminAiSearchRawView");

  const userPrompt = String(promptEl?.value || "").trim();
  const txt = await readTxtFile(txtFileEl?.files && txtFileEl.files[0]);
  if (!userPrompt && !commonCompanies && !txt.trim()) {
    alert("업체명 또는 검색 프롬프트 또는 TXT 파일 중 하나를 입력해 주세요.");
    return;
  }
  const topK = Math.max(1, Math.min(30, Number(topKEl?.value || 10)));
  const templateId = String(templateEl?.value || "company_overview").trim();
  const basePrompt = userPrompt || (commonCompanies ? `${commonCompanies} 관련 업체 분석` : "");
  const aiProvider = String(providerEl?.value || "gemini").trim().toLowerCase();

  resultView.textContent = "AI 업체검색 실행 중...";
  try {
    const data = await postJson("/api/company-overview-search", {
      prompt: basePrompt,
      template_id: templateId,
      top_k: topK,
      txt_content: txt || null,
      include_ai: true,
      ai_provider: aiProvider,
    });
    resultView.textContent = String(data.ai_answer_text || "AI 결과 없음");
    promptView.textContent = String(data.ai_prompt_used || "AI 프롬프트 없음");
    rawView.textContent =
      data.ai_raw_response && typeof data.ai_raw_response === "object"
        ? JSON.stringify(data.ai_raw_response, null, 2)
        : "AI 원본 응답 없음";

    adminLastAiQuery = basePrompt || "admin-ai-search";
    adminLastAiRows = Array.isArray(data.ai_similar_results) ? data.ai_similar_results : [];
    adminLastAiProvider = String(data.provider || "").trim();
  } catch (err) {
    resultView.textContent = `오류: ${err.message}`;
  }
}

async function registerAdminAiSearchResult() {
  const resultView = document.getElementById("adminAiSearchResultView");
  const stamp = new Date().toLocaleString("ko-KR");
  const appendLog = (line) => {
    if (!resultView) return;
    const current = String(resultView.textContent || "").trim();
    resultView.textContent = current ? `${current}\n\n${line}` : line;
  };
  appendLog(`[${stamp}] AI 결과 DB 등록 시도`);
  if (!adminLastAiRows.length) {
    appendLog(`[${stamp}] 등록 중단: 등록할 AI 검색 결과가 없습니다.`);
    alert("등록할 AI 검색 결과가 없습니다.");
    return;
  }
  if (!adminLastAiProvider) {
    appendLog(`[${stamp}] 등록 중단: AI provider 정보가 없습니다.`);
    alert("AI provider 정보가 없어 등록할 수 없습니다.");
    return;
  }
  if (!confirm("현재 AI 검색 결과를 벡터 DB에 등록하시겠습니까?")) {
    appendLog(`[${stamp}] 등록 취소: 사용자가 취소했습니다.`);
    return;
  }
  try {
    const data = await postJson("/api/company-search/register-ai-results", {
      query: adminLastAiQuery,
      provider: adminLastAiProvider,
      items: adminLastAiRows,
    });
    if (data.ok) {
      appendLog(
        `[${stamp}] 등록 완료: 추가 청크 ${data.added_chunks}개`
        + (data.source ? `, source=${data.source}` : "")
      );
      await refreshStatus();
    } else {
      appendLog(`[${stamp}] 등록 실패: ${data.message || "알 수 없는 오류"}`);
    }
  } catch (err) {
    appendLog(`[${stamp}] 등록 오류: ${err.message}`);
  }
}

async function runAdminInternetSearch() {
  const companyName = String(document.getElementById("commonCompanies")?.value || "").trim();
  const promptEl = document.getElementById("adminInternetSearchPrompt");
  const txtFileEl = document.getElementById("adminInternetSearchTxtFile");
  const templateEl = document.getElementById("adminInternetSearchTemplate");
  const includeWebEl = document.getElementById("adminInternetIncludeWeb");
  const topKEl = document.getElementById("adminInternetSearchTopK");
  const resultView = document.getElementById("adminInternetSearchResultView");
  const promptView = document.getElementById("adminInternetSearchPromptView");
  const rawView = document.getElementById("adminInternetSearchRawView");

  const userPrompt = String(promptEl?.value || "").trim();
  const txt = await readTxtFile(txtFileEl?.files && txtFileEl.files[0]);
  if (!userPrompt && !companyName && !txt.trim()) {
    alert("업체명 또는 인터넷 검색 프롬프트 또는 TXT 파일 중 하나를 입력해 주세요.");
    return;
  }

  const topK = Math.max(1, Math.min(30, Number(topKEl?.value || 10)));
  const templateId = String(templateEl?.value || "company_overview").trim();
  resultView.textContent = "인터넷 검색 실행 중...";
  try {
    const data = await postJson("/api/admin/internet-company-search", {
      prompt: userPrompt,
      company_name: companyName,
      template_id: templateId,
      top_k: topK,
      include_web: Boolean(includeWebEl?.checked),
      txt_content: txt || null,
    });
    const newsCount = Array.isArray(data.news_items) ? data.news_items.length : 0;
    const webCount = Array.isArray(data.web_items) ? data.web_items.length : 0;
    resultView.textContent =
      `[수집 건수] 뉴스 ${newsCount}건, 웹 ${webCount}건\n\n` +
      String(data.internet_answer_text || "인터넷 검색 결과 없음");
    promptView.textContent = String(data.internet_prompt_used || "프롬프트 정보 없음");
    rawView.textContent =
      data.internet_raw_response && typeof data.internet_raw_response === "object"
        ? JSON.stringify(data.internet_raw_response, null, 2)
        : "원본 결과 없음";

    adminLastInternetQuery = String(data.query || userPrompt || companyName || "internet-search").trim();
    adminLastInternetRows = Array.isArray(data.internet_similar_results) ? data.internet_similar_results : [];
  } catch (err) {
    resultView.textContent = `오류: ${err.message}`;
  }
}

async function registerAdminInternetSearchResult() {
  const resultView = document.getElementById("adminInternetSearchResultView");
  if (!adminLastInternetRows.length) {
    alert("등록할 인터넷 검색 결과가 없습니다.");
    return;
  }
  if (!confirm("현재 인터넷 검색 결과를 벡터 DB에 등록하시겠습니까?")) return;
  try {
    const data = await postJson("/api/company-search/register-internet-results", {
      query: adminLastInternetQuery,
      items: adminLastInternetRows,
    });
    if (data.ok) {
      resultView.textContent += `\n\n[등록 완료] 추가 청크 ${data.added_chunks}개`;
      await refreshStatus();
    } else {
      resultView.textContent += `\n\n[등록 실패] ${data.message || "알 수 없는 오류"}`;
    }
  } catch (err) {
    resultView.textContent += `\n\n[등록 오류] ${err.message}`;
  }
}

document.getElementById("refreshBtn").addEventListener("click", refreshStatus);
document.getElementById("logoutBtn").addEventListener("click", async () => {
  try {
    await postJson("/api/auth/logout", {});
  } finally {
    location.href = "/login";
  }
});
const disclosureStartBtn = document.getElementById("disclosureStartBtn");
if (disclosureStartBtn) {
  disclosureStartBtn.addEventListener("click", startDisclosureBulk);
}
const disclosureStopBtn = document.getElementById("disclosureStopBtn");
if (disclosureStopBtn) {
  disclosureStopBtn.addEventListener("click", stopDisclosureBulk);
}
const adminAiSearchRunBtn = document.getElementById("adminAiSearchRunBtn");
if (adminAiSearchRunBtn) {
  adminAiSearchRunBtn.addEventListener("click", runAdminAiSearch);
}
const adminAiSearchRegisterBtn = document.getElementById("adminAiSearchRegisterBtn");
if (adminAiSearchRegisterBtn) {
  adminAiSearchRegisterBtn.addEventListener("click", registerAdminAiSearchResult);
}
const adminInternetSearchRunBtn = document.getElementById("adminInternetSearchRunBtn");
if (adminInternetSearchRunBtn) {
  adminInternetSearchRunBtn.addEventListener("click", runAdminInternetSearch);
}
const adminInternetSearchRegisterBtn = document.getElementById("adminInternetSearchRegisterBtn");
if (adminInternetSearchRegisterBtn) {
  adminInternetSearchRegisterBtn.addEventListener("click", registerAdminInternetSearchResult);
}

refreshStatus();
refreshDisclosureStatus();
