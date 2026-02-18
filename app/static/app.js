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

function esc(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

const questionEl = document.getElementById("question");
const ownedCompanyResultViewEl = document.getElementById("ownedCompanyResultView");
const peerGridEl = document.getElementById("peerGrid");

function renderCompanyCards(el, rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (!el) return;
  if (!list.length) {
    el.innerHTML = "<span class='muted'>결과 없음</span>";
    return;
  }
  el.innerHTML = list
    .map((r) => {
      const score = Number.isFinite(r.strategic_fit_score) ? `${r.strategic_fit_score}점` : "N/A";
      return `
      <article class="peer-card">
        <h3>${esc(r.company || "정보 부족")}</h3>
        <p class="peer-market">${esc(r.market || "정보 부족")}</p>
        <p class="peer-score">전략 적합성: <strong>${esc(score)}</strong></p>
        <p class="peer-reason">${esc(r.reason || "정보 부족")}</p>
        <p class="peer-source">출처: ${esc(r.source || "정보 부족")}</p>
      </article>`;
    })
    .join("");
}

function clearView() {
  if (ownedCompanyResultViewEl) ownedCompanyResultViewEl.textContent = "아직 요청 없음";
  if (peerGridEl) peerGridEl.innerHTML = "<span class='muted'>아직 요청 없음</span>";
}

const runBtn = document.getElementById("runBtn");
if (runBtn) {
  runBtn.addEventListener("click", async () => {
    const q = questionEl.value.trim();
    if (!q) return;
    if (ownedCompanyResultViewEl) ownedCompanyResultViewEl.textContent = "전략 분석 수행 중...";
    try {
      const data = await postJson("/api/query", { question: q });
      if (ownedCompanyResultViewEl) ownedCompanyResultViewEl.textContent = data.answer_text || "응답 텍스트 없음";
      const detailedRows = Array.isArray(data.answer?.similar_companies_detail)
        ? data.answer.similar_companies_detail
        : [];
      renderCompanyCards(peerGridEl, detailedRows);
    } catch (err) {
      if (ownedCompanyResultViewEl) ownedCompanyResultViewEl.textContent = `오류: ${err.message}`;
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

clearView();
