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
const searchBtn = document.getElementById("searchBtn");
const clearBtn = document.getElementById("clearBtn");

function clearView() {
  localResultViewEl.textContent = "아직 요청 없음";
  searchStatusEl.textContent = "아직 요청 없음";
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

  try {
    const data = await postJson("/api/company-overview-search", {
      prompt,
      top_k: topK,
      txt_content: txt || null,
      include_ai: false,
    });

    localResultViewEl.textContent = data.local_answer_text || "기존 데이터 결과 없음";
    const inferred = String(data.inferred_company || "").trim();
    searchStatusEl.textContent = inferred
      ? `완료. 유추 회사: ${inferred}`
      : "완료. 유추 회사 없음";
  } catch (err) {
    const msg = `오류: ${err.message}`;
    localResultViewEl.textContent = msg;
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
