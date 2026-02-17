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

const emailEl = document.getElementById("email");
const passwordEl = document.getElementById("password");
const resultEl = document.getElementById("loginResult");

document.getElementById("loginBtn").addEventListener("click", async () => {
  const email = emailEl.value.trim();
  const password = passwordEl.value;
  if (!email || !password) {
    resultEl.textContent = "이메일과 비밀번호를 입력하세요.";
    return;
  }

  resultEl.textContent = "로그인 처리 중...";
  try {
    await postJson("/api/auth/login", { email, password });
    resultEl.textContent = "로그인 성공. 관리자 화면으로 이동합니다.";
    location.href = "/admin";
  } catch (err) {
    resultEl.textContent = `로그인 실패: ${err.message}`;
  }
});
