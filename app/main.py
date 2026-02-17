from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.routers.api import router as api_router
from app.routers.web import router as web_router
from app.services.auth_service import AuthService

app = FastAPI(title="AIDOME M&A Agent")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(web_router)
app.include_router(api_router)


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> RedirectResponse:
    return RedirectResponse(url="/static/favicon.svg")


@app.on_event("startup")
def startup_seed_admin() -> None:
    AuthService().ensure_seed_admin()
