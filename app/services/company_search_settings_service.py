from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class CompanySearchSettingsService:
    def __init__(self) -> None:
        root = Path(__file__).resolve().parents[2]
        self._path = root / "data" / "admin" / "company_search_settings.json"

    def load(self) -> dict[str, bool]:
        defaults = {"enable_openai": True, "enable_gemini": True}
        if not self._path.exists():
            return defaults
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return defaults
        if not isinstance(payload, dict):
            return defaults
        return {
            "enable_openai": bool(payload.get("enable_openai", True)),
            "enable_gemini": bool(payload.get("enable_gemini", True)),
        }

    def save(self, data: dict[str, Any]) -> dict[str, bool]:
        payload = {
            "enable_openai": bool(data.get("enable_openai", True)),
            "enable_gemini": bool(data.get("enable_gemini", True)),
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload
