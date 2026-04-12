"""
FastAPI application entry point.
"""

import csv
import os
import threading
from contextlib import asynccontextmanager
from io import BytesIO
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from app.config import settings
from app.routes.groups import router as groups_router
from app.routes.periskope import router as periskope_router
from app.routes.setup import router as setup_router
from app.services.accounting.exporter import TURKISH_HEADERS, tabular_rows_to_xlsx_bytes
from app.services.providers import google_sheets
from app.routes.webhooks import router as webhook_router
from app.utils.logging import get_logger

logger = get_logger(__name__)


def _run_google_sheets_startup_tasks() -> None:
    startup_steps = (
        ("prepare_current_month_sheet", google_sheets.ensure_current_month_spreadsheet_ready),
        ("process_pending_sheet_appends", google_sheets.process_pending_sheet_appends),
        ("process_pending_document_uploads", google_sheets.process_pending_document_uploads),
    )
    for step_name, step in startup_steps:
        try:
            step()
        except Exception as exc:
            logger.warning("Startup bootstrap step %s failed: %s", step_name, exc)


def _start_google_sheets_bootstrap() -> threading.Thread:
    thread = threading.Thread(
        target=_run_google_sheets_startup_tasks,
        name="google-sheets-startup-bootstrap",
        daemon=True,
    )
    thread.start()
    return thread


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Auto Accounting AI started.")
    railway_volume_mount_path = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
    if os.getenv("RAILWAY_SERVICE_ID"):
        storage_path = Path(settings.storage_dir).resolve()
        if not railway_volume_mount_path:
            logger.warning(
                "Railway volume mount path is not configured; storage at %s will be ephemeral.",
                storage_path,
            )
        else:
            volume_path = Path(railway_volume_mount_path).resolve()
            if storage_path != volume_path and volume_path not in storage_path.parents:
                logger.warning(
                    "STORAGE_DIR=%s is outside Railway volume mount path %s; queue persistence will not survive redeploys.",
                    storage_path,
                    volume_path,
                )
            else:
                logger.info("Using Railway volume-backed storage at %s.", storage_path)
    configured_models = {
        "classifier": settings.gemini_classifier_model,
        "extractor": settings.gemini_extractor_model,
        "validation": settings.gemini_validation_model,
    }
    non_pro_models = {name: model for name, model in configured_models.items() if model != "gemini-2.5-pro"}
    if non_pro_models:
        logger.warning("Gemini model override detected; expected gemini-2.5-pro but got %s", non_pro_models)
    if not settings.periskope_signing_key:
        logger.warning("PERISKOPE_SIGNING_KEY is not configured; webhook signature verification will be skipped.")
    google_sheets.start_pending_sheet_append_worker()
    google_sheets.start_pending_drive_upload_worker()
    google_sheets.start_monthly_rollover_scheduler()
    _start_google_sheets_bootstrap()
    try:
        yield
    finally:
        google_sheets.stop_monthly_rollover_scheduler()


app = FastAPI(
    title="Auto Accounting AI",
    description=(
        "WhatsApp → Gemini invoice extraction and accounting preparation backend."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(webhook_router)
app.include_router(groups_router)
app.include_router(periskope_router)
app.include_router(setup_router)


@app.get("/health", tags=["health"])
async def health_check() -> JSONResponse:
    """Liveness probe for Railway and other deployment platforms."""
    return JSONResponse({"status": "ok"})


def _latest_export_path() -> Path:
    export_dir = Path(settings.storage_dir) / "exports"
    export_files = sorted(export_dir.glob("records_*.csv"))
    if not export_files:
        raise HTTPException(status_code=404, detail="No export file available yet.")
    return export_files[-1]


@app.get("/export.csv", tags=["export"])
async def export_csv() -> FileResponse:
    """Download the latest CSV export."""
    filepath = _latest_export_path()
    return FileResponse(
        path=filepath,
        media_type="text/csv; charset=utf-8",
        filename=filepath.name,
    )


@app.get("/export.xlsx", tags=["export"])
async def export_xlsx() -> StreamingResponse:
    """Download the latest export as an XLSX workbook."""
    filepath = _latest_export_path()
    with filepath.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    workbook_bytes = tabular_rows_to_xlsx_bytes(rows, headers=TURKISH_HEADERS)
    filename = filepath.name.replace(".csv", ".xlsx")
    return StreamingResponse(
        BytesIO(workbook_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
