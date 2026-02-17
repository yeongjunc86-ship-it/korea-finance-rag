from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    ollama_base_url: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    ollama_chat_model: str = os.getenv("OLLAMA_CHAT_MODEL", "llama3")
    ollama_embed_model: str = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")
    dart_api_key: str = os.getenv("DART_API_KEY", "")
    top_k: int = int(os.getenv("TOP_K", "5"))
    index_path: str = os.getenv("INDEX_PATH", "data/index/chunks.jsonl")


settings = Settings()
