from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse


router = APIRouter(prefix="/live", tags=["live"])

_STATIC_DIR = Path(__file__).resolve().parent / "static"
_INDEX_HTML = _STATIC_DIR / "index.html"


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse, include_in_schema=False)
def live_page():
    return HTMLResponse(_INDEX_HTML.read_text(encoding="utf-8"))


@router.get("/api/latest")
def live_latest(request: Request):
    return jsonable_encoder({
        "th": request.app.state.db.get_web_sensor_th(),
        "watch": request.app.state.db.get_web_sensor_hb(),
    })
