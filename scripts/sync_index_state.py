#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

RAW_DIR = Path("data/raw")
STATE_PATH = Path("data/index/index_state.json")


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(RAW_DIR.glob("*.json"))
    state: dict[str, dict[str, int]] = {}
    for p in files:
        st = p.stat()
        state[str(p)] = {"mtime_ns": st.st_mtime_ns, "size": st.st_size}

    payload = {
        "version": 1,
        "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "files": state,
    }
    STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"done. files={len(files)}, state={STATE_PATH}")


if __name__ == "__main__":
    main()
