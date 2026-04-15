from __future__ import annotations

import mimetypes
from pathlib import Path

from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response


STATIC_DIR = Path(__file__).resolve().parent / "static"


def create_frontend_router(*, static_dir: Path | None = None) -> APIRouter:
    resolved_static_dir = (static_dir or STATIC_DIR).resolve()
    index_file = (resolved_static_dir / "index.html").resolve()

    router = APIRouter()

    async def frontend_entrypoint() -> HTMLResponse:
        if not index_file.is_file():
            raise HTTPException(status_code=404, detail="frontend not found")
        return HTMLResponse(index_file.read_text(encoding="utf-8"))

    async def static_asset(asset_path: str) -> Response:
        candidate = (resolved_static_dir / asset_path).resolve()
        if resolved_static_dir not in candidate.parents:
            raise HTTPException(status_code=404, detail="asset not found")
        if not candidate.is_file():
            raise HTTPException(status_code=404, detail="asset not found")
        media_type, _ = mimetypes.guess_type(str(candidate))
        return Response(
            content=candidate.read_bytes(),
            media_type=media_type or "application/octet-stream",
        )

    router.add_api_route("/", frontend_entrypoint, include_in_schema=False)
    router.add_api_route(
        "/static/{asset_path:path}",
        static_asset,
        include_in_schema=False,
    )
    setattr(router, "static_dir", resolved_static_dir)
    setattr(router, "index_file", index_file)
    return router


def mount_frontend(app: FastAPI, *, static_dir: Path | None = None) -> None:
    resolved_static_dir = static_dir or STATIC_DIR
    frontend_router = create_frontend_router(static_dir=resolved_static_dir)

    app.include_router(frontend_router)
    app.state.frontend_static_dir = resolved_static_dir
    app.state.frontend_index_file = getattr(frontend_router, "index_file")
