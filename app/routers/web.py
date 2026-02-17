from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.services.auth_service import AuthService

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
auth_service = AuthService()


def _current_role(request: Request) -> str | None:
    token = request.cookies.get(auth_service.session_cookie_name(), "")
    payload = auth_service.parse_session_token(token) if token else None
    role = payload.get("role") if isinstance(payload, dict) else None
    return role if isinstance(role, str) else None


@router.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/admin", response_class=HTMLResponse)
def admin(request: Request):
    role = _current_role(request)
    if role != "admin":
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("admin.html", {"request": request})


@router.get("/login", response_class=HTMLResponse)
def login(request: Request):
    if _current_role(request) == "admin":
        return RedirectResponse(url="/admin", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request})
