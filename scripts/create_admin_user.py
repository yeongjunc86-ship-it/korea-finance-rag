#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.auth_service import AuthService


def main() -> None:
    parser = argparse.ArgumentParser(description="Create or update admin user")
    parser.add_argument("--email", required=True)
    parser.add_argument("--name", default="관리자")
    parser.add_argument("--password", default="")
    args = parser.parse_args()

    password = args.password.strip()
    if not password:
        password = getpass.getpass("관리자 비밀번호 입력: ").strip()
    if len(password) < 8:
        raise SystemExit("비밀번호는 최소 8자 이상이어야 합니다.")

    svc = AuthService()
    result = svc.create_or_update_admin(args.email, password, args.name)
    status = "created" if result.get("created") else "updated"
    print(f"done. {status} admin={result['email']}")


if __name__ == "__main__":
    main()
