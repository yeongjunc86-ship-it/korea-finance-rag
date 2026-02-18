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
let disclosurePollTimer = null;

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

refreshStatus();
refreshDisclosureStatus();
