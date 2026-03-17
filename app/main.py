"""
FastAPI application entry point.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.routes.webhooks import router as webhook_router
from app.utils.logging import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Auto Accounting AI started.")
    yield


app = FastAPI(
    title="Auto Accounting AI",
    description=(
        "WhatsApp → Gemini invoice extraction and accounting preparation backend."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(webhook_router)


@app.get("/health", tags=["health"])
async def health_check() -> JSONResponse:
    """Liveness probe for Railway and other deployment platforms."""
    return JSONResponse({"status": "ok"})
