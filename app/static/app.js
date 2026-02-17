async function postJson(url, payload) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `HTTP ${resp.status}`);
  }
  return resp.json();
}

const questionEl = document.getElementById("question");
const resultEl = document.getElementById("result");
const sourcesBoxEl = document.getElementById("sourcesBox");
const peerGridEl = document.getElementById("peerGrid");
const exportReportTxtBtn = document.getElementById("exportReportTxtBtn");
const exportReportPdfBtn = document.getElementById("exportReportPdfBtn");

function esc(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function setLoading(text) {
  resultEl.textContent = text;
  sourcesBoxEl.textContent = text;
  peerGridEl.textContent = text;
}

function sourceLink(path) {
  const href = `/api/source?path=${encodeURIComponent(path)}`;
  return `<a href="${href}" target="_blank" rel="noopener">${esc(path)}</a>`;
}

function renderSources(answer, retrievedChunks) {
  const aSrc = Array.isArray(answer?.sources) ? answer.sources : [];
  const rSrc = Array.isArray(retrievedChunks) ? retrievedChunks.map((r) => r.source) : [];
  const merged = [...aSrc, ...rSrc].filter((x) => typeof x === "string" && x.trim());
  const dedup = [...new Set(merged)];
  if (!dedup.length) {
    sourcesBoxEl.innerHTML = "<span class='muted'>근거 출처 없음</span>";
    return;
  }
  sourcesBoxEl.innerHTML = dedup.map((s, idx) => `<div>${idx + 1}. ${sourceLink(s)}</div>`).join("");
}

function renderSimilarCards(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) {
    peerGridEl.innerHTML = "<span class='muted'>유사 업체 결과 없음</span>";
    return;
  }

  const cards = list.map((r) => {
    const reason = r.reason || "정보 부족";
    const score = Number.isFinite(r.strategic_fit_score) ? Number(r.strategic_fit_score) : null;
    const scoreText = score === null ? "N/A" : `${score}점`;
    const src = r.source ? sourceLink(r.source) : "정보 부족";
    return `
      <article class="peer-card">
        <h3>${esc(r.company || "정보 부족")}</h3>
        <p class="peer-market">${esc(r.market || "정보 부족")}</p>
        <p class="peer-score">전략 적합성: <strong>${esc(scoreText)}</strong></p>
        <p class="peer-reason">${esc(reason)}</p>
        <p class="peer-source">근거: ${src}</p>
      </article>
    `;
  });
  peerGridEl.innerHTML = cards.join("");
}

function formatSimilarAsReport(data) {
  const rows = Array.isArray(data.results) ? data.results : [];
  const requestedCount = Number.isFinite(data.requested_count) ? Number(data.requested_count) : rows.length;
  const returnedCount = Number.isFinite(data.returned_count) ? Number(data.returned_count) : rows.length;
  const notice =
    typeof data.notice === "string" && data.notice.trim()
      ? data.notice.trim()
      : (returnedCount < requestedCount ? `근거 부족으로 ${returnedCount}개만 반환` : "");
  if (!rows.length) {
    return notice ? `유사 업체를 찾지 못했습니다.\n${notice}` : "유사 업체를 찾지 못했습니다.";
  }
  const lines = ["유사 업체 검색 결과", ""];
  if (notice) {
    lines.push(notice);
    lines.push("");
  }
  rows.forEach((r, idx) => {
    const score = Number.isFinite(r.strategic_fit_score) ? `${r.strategic_fit_score}점` : "N/A";
    lines.push(`${idx + 1}. ${r.company || "정보 부족"} (${r.market || "정보 부족"})`);
    lines.push(`   전략 적합성 점수: ${score}`);
    lines.push(`   사유: ${r.reason || "정보 부족"}`);
    lines.push(`   출처: ${r.source || "정보 부족"}`);
    lines.push("");
  });
  return lines.join("\n").trim();
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

function buildReportExportText() {
  const q = String(questionEl.value || "").trim();
  const report = String(resultEl.textContent || "").trim();
  const sources = String(sourcesBoxEl.textContent || "").trim();
  const lines = [];
  lines.push("전략 분석 보고서");
  lines.push(`생성 시각: ${new Date().toISOString()}`);
  lines.push("");
  lines.push("[질문]");
  lines.push(q || "질문 없음");
  lines.push("");
  lines.push("[보고서]");
  lines.push(report || "보고서 없음");
  lines.push("");
  lines.push("[근거 출처]");
  lines.push(sources || "출처 없음");
  return lines.join("\n");
}

function exportReportTxt() {
  const text = buildReportExportText();
  downloadText(`strategy_report_${nowCompact()}.txt`, text);
}

function exportReportPdf() {
  const text = buildReportExportText();
  const escText = esc(text).replaceAll("\n", "<br/>");
  const html = `<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <title>전략 분석 보고서</title>
  <style>
    body { font-family: 'Noto Sans KR', sans-serif; margin: 24px; line-height: 1.6; }
    h1 { font-size: 18px; margin: 0 0 12px; }
    .content { font-size: 12px; white-space: normal; word-break: break-word; }
    @media print { body { margin: 12mm; } }
  </style>
</head>
<body>
  <h1>전략 분석 보고서</h1>
  <div class="content">${escText}</div>
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

function clearView() {
  resultEl.textContent = "아직 요청 없음";
  sourcesBoxEl.innerHTML = "<span class='muted'>아직 요청 없음</span>";
  peerGridEl.innerHTML = "<span class='muted'>아직 요청 없음</span>";
}

const runBtn = document.getElementById("runBtn");
if (runBtn) {
  runBtn.addEventListener("click", async () => {
    const q = questionEl.value.trim();
    if (!q) return;
    setLoading("요청 중...");
    try {
      const data = await postJson("/api/query", { question: q });
      const text = data.answer_text || "응답 텍스트 없음";
      resultEl.textContent = text;
      renderSources(data.answer, data.retrieved_chunks || []);

      const detailedRows = Array.isArray(data.answer?.similar_companies_detail)
        ? data.answer.similar_companies_detail
        : [];
      const similarRows = detailedRows.length
        ? detailedRows
        : (
            Array.isArray(data.answer?.similar_companies)
              ? data.answer.similar_companies.map((name) => ({
                  company: String(name),
                  market: data.answer?.market || "정보 부족",
                  reason: "보고서 내 추천 유사 기업",
                  source: (Array.isArray(data.answer?.sources) && data.answer.sources[0]) || "",
                }))
              : []
          );
      renderSimilarCards(similarRows);
    } catch (err) {
      setLoading(`오류: ${err.message}`);
    }
  });
}

const clearBtn = document.getElementById("clearBtn");
if (clearBtn) {
  clearBtn.addEventListener("click", () => {
    questionEl.value = "";
    clearView();
  });
}
if (exportReportTxtBtn) {
  exportReportTxtBtn.addEventListener("click", exportReportTxt);
}
if (exportReportPdfBtn) {
  exportReportPdfBtn.addEventListener("click", exportReportPdf);
}

clearView();
