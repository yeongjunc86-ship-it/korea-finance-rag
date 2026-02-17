from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import time

ROOT_DIR = Path(__file__).resolve().parents[2]
ADMIN_DIR = ROOT_DIR / "data" / "admin"
USERS_PATH = ADMIN_DIR / "users.json"

PBKDF2_ITERATIONS = 260_000
SESSION_COOKIE_NAME = "aidome_session"
SESSION_TTL_SECONDS = 60 * 60 * 12  # 12 hours


@dataclass
class AuthUser:
    user_id: str
    email: str
    role: str
    is_active: bool


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return (
        "pbkdf2_sha256"
        f"${PBKDF2_ITERATIONS}"
        f"${base64.b64encode(salt).decode('ascii')}"
        f"${base64.b64encode(digest).decode('ascii')}"
    )


def verify_password(password: str, encoded: str) -> bool:
    try:
        algo, iters_str, salt_b64, digest_b64 = encoded.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iters = int(iters_str)
        salt = base64.b64decode(salt_b64.encode("ascii"))
        expected = base64.b64decode(digest_b64.encode("ascii"))
    except Exception:
        return False

    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iters)
    return hmac.compare_digest(actual, expected)


def _load_users() -> list[dict[str, Any]]:
    ADMIN_DIR.mkdir(parents=True, exist_ok=True)
    if not USERS_PATH.exists():
        return []
    try:
        payload = json.loads(USERS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    users = payload.get("users") if isinstance(payload, dict) else None
    return users if isinstance(users, list) else []


def _save_users(users: list[dict[str, Any]]) -> None:
    ADMIN_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "updated_at": _now_iso(),
        "users": users,
    }
    USERS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class AuthService:
    def ensure_seed_admin(self) -> None:
        users = _load_users()
        if users:
            return

        admin_email = _normalize_email(os.getenv("ADMIN_EMAIL", "admin@local"))
        admin_password = os.getenv("ADMIN_PASSWORD", "admin1234!")
        admin_name = os.getenv("ADMIN_NAME", "관리자")
        now = _now_iso()

        users.append(
            {
                "user_id": "u_admin",
                "email": admin_email,
                "display_name": admin_name,
                "password_hash": hash_password(admin_password),
                "role": "admin",
                "is_active": True,
                "created_at": now,
                "updated_at": now,
                "last_login_at": None,
            }
        )
        _save_users(users)

    def authenticate(self, email: str, password: str) -> AuthUser | None:
        target = _normalize_email(email)
        users = _load_users()
        for u in users:
            if _normalize_email(str(u.get("email", ""))) != target:
                continue
            if not bool(u.get("is_active", True)):
                return None
            encoded = str(u.get("password_hash", ""))
            if not verify_password(password, encoded):
                return None

            u["last_login_at"] = _now_iso()
            u["updated_at"] = _now_iso()
            _save_users(users)

            return AuthUser(
                user_id=str(u.get("user_id") or ""),
                email=str(u.get("email") or target),
                role=str(u.get("role") or "viewer"),
                is_active=bool(u.get("is_active", True)),
            )
        return None

    def get_user_by_id(self, user_id: str) -> AuthUser | None:
        users = _load_users()
        for u in users:
            if str(u.get("user_id") or "") != user_id:
                continue
            if not bool(u.get("is_active", True)):
                return None
            return AuthUser(
                user_id=str(u.get("user_id") or ""),
                email=str(u.get("email") or ""),
                role=str(u.get("role") or "viewer"),
                is_active=bool(u.get("is_active", True)),
            )
        return None

    def create_or_update_admin(self, email: str, password: str, display_name: str = "관리자") -> dict[str, Any]:
        target = _normalize_email(email)
        users = _load_users()
        now = _now_iso()

        for u in users:
            if _normalize_email(str(u.get("email", ""))) == target:
                u["password_hash"] = hash_password(password)
                u["display_name"] = display_name
                u["role"] = "admin"
                u["is_active"] = True
                u["updated_at"] = now
                _save_users(users)
                return {"ok": True, "created": False, "email": target}

        user_id = f"u_{secrets.token_hex(6)}"
        users.append(
            {
                "user_id": user_id,
                "email": target,
                "display_name": display_name,
                "password_hash": hash_password(password),
                "role": "admin",
                "is_active": True,
                "created_at": now,
                "updated_at": now,
                "last_login_at": None,
            }
        )
        _save_users(users)
        return {"ok": True, "created": True, "email": target}

    @staticmethod
    def session_cookie_name() -> str:
        return SESSION_COOKIE_NAME

    @staticmethod
    def _session_secret() -> bytes:
        return os.getenv("SESSION_SECRET", "change-me-session-secret").encode("utf-8")

    def issue_session_token(self, user: AuthUser) -> str:
        payload = {
            "uid": user.user_id,
            "email": user.email,
            "role": user.role,
            "exp": int(time.time()) + SESSION_TTL_SECONDS,
        }
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        body = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
        sig = hmac.new(self._session_secret(), body.encode("ascii"), hashlib.sha256).hexdigest()
        return f"{body}.{sig}"

    def parse_session_token(self, token: str) -> dict[str, Any] | None:
        try:
            body, sig = token.rsplit(".", 1)
        except ValueError:
            return None
        expected = hmac.new(self._session_secret(), body.encode("ascii"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            return None

        pad = "=" * (-len(body) % 4)
        try:
            raw = base64.urlsafe_b64decode((body + pad).encode("ascii"))
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            return None

        if not isinstance(payload, dict):
            return None
        exp = payload.get("exp")
        if not isinstance(exp, int) or exp < int(time.time()):
            return None
        return payload
