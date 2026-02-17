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
const aiCompanyResultViewEl = document.getElementById("aiCompanyResultView");
const peerGridEl = document.getElementById("peerGrid");
const ownedCompanyResultGridEl = document.getElementById("ownedCompanyResultGrid");
const aiCompanyResultGridEl = document.getElementById("aiCompanyResultGrid");

let lastCompanySearchQuery = "";
let lastCompanySearchAiRows = [];
let lastCompanySearchProvider = "";

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
  if (aiCompanyResultViewEl) aiCompanyResultViewEl.textContent = "아직 요청 없음";
  if (peerGridEl) peerGridEl.innerHTML = "<span class='muted'>아직 요청 없음</span>";
  if (ownedCompanyResultGridEl) ownedCompanyResultGridEl.innerHTML = "<span class='muted'>아직 요청 없음</span>";
  if (aiCompanyResultGridEl) aiCompanyResultGridEl.innerHTML = "<span class='muted'>아직 요청 없음</span>";
  lastCompanySearchQuery = "";
  lastCompanySearchAiRows = [];
  lastCompanySearchProvider = "";
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
      renderCompanyCards(ownedCompanyResultGridEl, detailedRows);
      if (aiCompanyResultViewEl) aiCompanyResultViewEl.textContent = "업체 검색 버튼으로 AI 템플릿 결과를 생성하세요.";
      if (aiCompanyResultGridEl) aiCompanyResultGridEl.innerHTML = "<span class='muted'>업체 검색 전</span>";
    } catch (err) {
      if (ownedCompanyResultViewEl) ownedCompanyResultViewEl.textContent = `오류: ${err.message}`;
    }
  });
}

const searchCompaniesBtn = document.getElementById("searchCompaniesBtn");
if (searchCompaniesBtn) {
  searchCompaniesBtn.addEventListener("click", async () => {
    const q = questionEl.value.trim();
    if (!q) return;
    if (ownedCompanyResultViewEl) ownedCompanyResultViewEl.textContent = "기존 데이터 회사 개요 생성 중...";
    if (aiCompanyResultViewEl) aiCompanyResultViewEl.textContent = "AI 회사 개요 생성 중...";
    try {
      const data = await postJson("/api/company-overview-search", { prompt: q, top_k: 10 });
      if (ownedCompanyResultViewEl) {
        ownedCompanyResultViewEl.textContent = data.local_answer_text || "기존 데이터 결과 없음";
      }
      if (aiCompanyResultViewEl) {
        aiCompanyResultViewEl.textContent = data.ai_answer_text || "AI 결과 없음";
      }
      renderCompanyCards(peerGridEl, data.local_similar_results || []);
      renderCompanyCards(ownedCompanyResultGridEl, data.local_similar_results || []);
      renderCompanyCards(aiCompanyResultGridEl, data.ai_similar_results || []);

      lastCompanySearchQuery = q;
      lastCompanySearchAiRows = Array.isArray(data.ai_similar_results) ? data.ai_similar_results : [];
      lastCompanySearchProvider = String(data.provider || "").trim();
    } catch (err) {
      if (ownedCompanyResultViewEl) ownedCompanyResultViewEl.textContent = `오류: ${err.message}`;
      if (aiCompanyResultViewEl) aiCompanyResultViewEl.textContent = `오류: ${err.message}`;
    }
  });
}

const registerAiToDbBtn = document.getElementById("registerAiToDbBtn");
if (registerAiToDbBtn) {
  registerAiToDbBtn.addEventListener("click", async () => {
    if (!lastCompanySearchAiRows.length) {
      alert("등록할 AI 검색 결과가 없습니다.");
      return;
    }
    if (!lastCompanySearchProvider) {
      alert("AI provider 정보가 없어 등록할 수 없습니다.");
      return;
    }
    try {
      const data = await postJson("/api/company-search/register-ai-results", {
        query: lastCompanySearchQuery,
        provider: lastCompanySearchProvider,
        items: lastCompanySearchAiRows,
      });
      if (data.ok) {
        alert(`등록 완료: 벡터 청크 ${data.added_chunks}개 추가`);
      } else {
        alert(`등록 실패: ${data.message || "알 수 없는 오류"}`);
      }
    } catch (err) {
      alert(`등록 오류: ${err.message}\\n(관리자 로그인 필요)`);
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
