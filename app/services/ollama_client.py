from __future__ import annotations

import json
import requests


class OllamaClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def _post_json(self, path: str, payload: dict) -> dict:
        url = f"{self.base_url}{path}"
        resp = requests.post(url, json=payload, timeout=120)
        resp.raise_for_status()
        return resp.json()

    def embed(self, model: str, text: str) -> list[float]:
        payload = {"model": model, "input": text}
        try:
            out = self._post_json("/api/embed", payload)
            embeddings = out.get("embeddings") or []
            if embeddings and isinstance(embeddings[0], list):
                return embeddings[0]
        except requests.HTTPError:
            pass

        # Backward compatible endpoint
        fallback = self._post_json("/api/embeddings", {"model": model, "prompt": text})
        emb = fallback.get("embedding")
        if not emb:
            raise RuntimeError("Failed to get embedding from Ollama")
        return emb

    def generate_json(self, model: str, prompt: str) -> dict:
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.1},
        }
        out = self._post_json("/api/generate", payload)
        raw = out.get("response", "").strip()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # Guardrail: return wrapped raw text when model fails strict JSON.
            return {"raw": raw, "note": "Model output was not valid JSON."}
