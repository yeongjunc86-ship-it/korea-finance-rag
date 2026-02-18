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

async function readTxtFile(file) {
  if (!file) return "";
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("TXT 파일 읽기 실패"));
    reader.readAsText(file, "utf-8");
  });
}

const promptEl = document.getElementById("searchPrompt");
const txtFileEl = document.getElementById("txtFile");
const topKEl = document.getElementById("topK");
const searchStatusEl = document.getElementById("searchStatus");
const allTemplateResultViewEl = document.getElementById("allTemplateResultView");
const searchBtn = document.getElementById("searchBtn");
const clearBtn = document.getElementById("clearBtn");

function clearView() {
  allTemplateResultViewEl.textContent = "아직 요청 없음";
  searchStatusEl.textContent = "아직 요청 없음";
}

const TEMPLATE_RUN_ORDER = [
  { id: "company_overview", title: "기업 개요 분석" },
  { id: "industry_market_analysis", title: "산업 및 시장 분석" },
  { id: "valuation_analysis", title: "밸류에이션 관련" },
  { id: "due_diligence_risk_analysis", title: "리스크 및 실사" },
  { id: "strategic_decision_analysis", title: "전략적 의사결정" },
  { id: "synergy_pair_analysis", title: "두 회사간 시너지 분석" },
];

searchBtn?.addEventListener("click", async () => {
  const prompt = String(promptEl.value || "").trim();
  const txt = await readTxtFile(txtFileEl.files && txtFileEl.files[0]);
  if (!prompt && !txt.trim()) {
    alert("검색 프롬프트 또는 TXT 파일 중 하나를 입력해 주세요.");
    return;
  }

  const topK = Math.max(1, Math.min(30, Number(topKEl.value || 10)));
  searchStatusEl.textContent = "업체 검색 수행 중...";
  allTemplateResultViewEl.textContent = "기존 데이터 전체 템플릿 생성 중...";

  try {
    const parts = [];
    let inferredCompany = "";

    for (let i = 0; i < TEMPLATE_RUN_ORDER.length; i += 1) {
      const t = TEMPLATE_RUN_ORDER[i];
      searchStatusEl.textContent = `업체 검색 수행 중... (${i + 1}/${TEMPLATE_RUN_ORDER.length}) ${t.title}`;
      const data = await postJson("/api/company-overview-search", {
        prompt,
        template_id: t.id,
        top_k: topK,
        txt_content: txt || null,
        include_ai: false,
      });
      if (!inferredCompany) {
        inferredCompany = String(data.inferred_company || "").trim();
      }
      parts.push(data.local_answer_text || `[${t.title}] 결과 없음`);
    }

    allTemplateResultViewEl.textContent = parts.join("\n\n==================================================\n\n");
    searchStatusEl.textContent = inferredCompany
      ? `완료. 유추 회사: ${inferredCompany}`
      : "완료. 유추 회사 없음";
  } catch (err) {
    const msg = `오류: ${err.message}`;
    allTemplateResultViewEl.textContent = msg;
    searchStatusEl.textContent = msg;
  }
});

clearBtn?.addEventListener("click", () => {
  promptEl.value = "";
  topKEl.value = "10";
  txtFileEl.value = "";
  clearView();
});

clearView();
