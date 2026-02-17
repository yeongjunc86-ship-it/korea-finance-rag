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
const localResultViewEl = document.getElementById("localResultView");
const aiResultViewEl = document.getElementById("aiResultView");
const aiPromptViewEl = document.getElementById("aiPromptView");
const aiRawResponseViewEl = document.getElementById("aiRawResponseView");
const searchBtn = document.getElementById("searchBtn");
const clearBtn = document.getElementById("clearBtn");
const registerAiBtn = document.getElementById("registerAiBtn");

let lastQuery = "";
let lastAiRows = [];
let lastProvider = "";

function clearView() {
  localResultViewEl.textContent = "아직 요청 없음";
  aiResultViewEl.textContent = "아직 요청 없음";
  if (aiPromptViewEl) aiPromptViewEl.textContent = "아직 요청 없음";
  if (aiRawResponseViewEl) aiRawResponseViewEl.textContent = "아직 요청 없음";
  searchStatusEl.textContent = "아직 요청 없음";
  lastQuery = "";
  lastAiRows = [];
  lastProvider = "";
}

searchBtn?.addEventListener("click", async () => {
  const prompt = String(promptEl.value || "").trim();
  const txt = await readTxtFile(txtFileEl.files && txtFileEl.files[0]);
  if (!prompt && !txt.trim()) {
    alert("검색 프롬프트 또는 TXT 파일 중 하나를 입력해 주세요.");
    return;
  }
  const topK = Math.max(1, Math.min(30, Number(topKEl.value || 10)));
  searchStatusEl.textContent = "업체 검색 수행 중...";
  localResultViewEl.textContent = "기존 데이터 회사 개요 생성 중...";
  aiResultViewEl.textContent = "AI 회사 개요 생성 중...";
  try {
    const data = await postJson("/api/company-overview-search", {
      prompt,
      top_k: topK,
      txt_content: txt || null,
    });

    localResultViewEl.textContent = data.local_answer_text || "기존 데이터 결과 없음";
    aiResultViewEl.textContent = data.ai_answer_text || "AI 결과 없음";
    if (aiPromptViewEl) {
      aiPromptViewEl.textContent = String(data.ai_prompt_used || "AI 프롬프트 없음");
    }
    if (aiRawResponseViewEl) {
      const raw = data.ai_raw_response && typeof data.ai_raw_response === "object"
        ? JSON.stringify(data.ai_raw_response, null, 2)
        : "AI 원본 응답 없음";
      aiRawResponseViewEl.textContent = raw;
    }

    lastQuery = prompt;
    lastAiRows = Array.isArray(data.ai_similar_results) ? data.ai_similar_results : [];
    lastProvider = String(data.provider || "").trim();

    const providerText = lastProvider || "none";
    const inferred = String(data.inferred_company || "").trim();
    searchStatusEl.textContent = inferred
      ? `완료. 유추 회사: ${inferred} / AI provider: ${providerText}`
      : `완료. AI provider: ${providerText}`;
  } catch (err) {
    const msg = `오류: ${err.message}`;
    localResultViewEl.textContent = msg;
    aiResultViewEl.textContent = msg;
    searchStatusEl.textContent = msg;
  }
});

registerAiBtn?.addEventListener("click", async () => {
  if (!lastAiRows.length) {
    alert("등록할 AI 검색 결과가 없습니다. (AI 템플릿의 유사기업 항목 필요)");
    return;
  }
  if (!lastProvider) {
    alert("AI provider 정보가 없어 등록할 수 없습니다.");
    return;
  }
  if (!confirm("현재 AI 결과를 벡터 DB에 등록하시겠습니까?")) return;
  try {
    const payload = {
      query: lastQuery,
      provider: lastProvider,
      items: lastAiRows,
    };
    const data = await postJson("/api/company-search/register-ai-results", payload);
    if (data.ok) {
      alert(`등록 완료: 추가 청크 ${data.added_chunks}개`);
    } else {
      alert(`등록 실패: ${data.message || "알 수 없는 오류"}`);
    }
  } catch (err) {
    if (String(err.message || "").includes("403")) {
      alert("관리자 로그인 후 등록할 수 있습니다.");
      return;
    }
    alert(`등록 오류: ${err.message}`);
  }
});

clearBtn?.addEventListener("click", () => {
  promptEl.value = "";
  topKEl.value = "10";
  txtFileEl.value = "";
  clearView();
});

clearView();
