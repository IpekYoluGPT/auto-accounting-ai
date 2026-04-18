"""
FastAPI app factory helpers.
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse


def create_app(
    *,
    lifespan,
    webhook_router,
    groups_router,
    periskope_router,
    setup_router,
    health_handler: Callable[[], JSONResponse],
    export_csv_handler: Callable[[], FileResponse],
    export_xlsx_handler: Callable[[], StreamingResponse],
) -> FastAPI:
    app = FastAPI(
        title="Auto Accounting AI",
        description="WhatsApp → Gemini invoice extraction and accounting preparation backend.",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.include_router(webhook_router)
    app.include_router(groups_router)
    app.include_router(periskope_router)
    app.include_router(setup_router)

    app.get("/health", tags=["health"])(health_handler)
    app.get("/export.csv", tags=["export"])(export_csv_handler)
    app.get("/export.xlsx", tags=["export"])(export_xlsx_handler)

    return app
