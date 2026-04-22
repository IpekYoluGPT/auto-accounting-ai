"""
FastAPI application entry point.
"""

import csv
import threading
from io import BytesIO
from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from app.bootstrap.app_factory import create_app
from app.bootstrap.lifespan import WorkerCoordinator, build_lifespan
from app.config import settings
from app.routes.groups import router as groups_router
from app.routes.periskope import router as periskope_router
from app.routes.setup import router as setup_router
from app.services.accounting import inbound_queue
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
        ("bootstrap_inbound_queue", inbound_queue.bootstrap_inbound_queue),
        ("process_pending_inbound_jobs", inbound_queue.process_pending_inbound_jobs),
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


def _start_background_workers() -> None:
    google_sheets.start_pending_sheet_append_worker()
    google_sheets.start_pending_drive_upload_worker()
    google_sheets.start_monthly_rollover_scheduler()
    inbound_queue.start_pending_inbound_job_worker()


def _stop_background_workers() -> None:
    inbound_queue.stop_pending_inbound_job_worker()
    google_sheets.stop_monthly_rollover_scheduler()


lifespan = build_lifespan(
    worker_coordinator=WorkerCoordinator(
        start_workers=lambda: _start_background_workers(),
        stop_workers=lambda: _stop_background_workers(),
    ),
    start_bootstrap=lambda: _start_google_sheets_bootstrap(),
)


async def health_check() -> JSONResponse:
    """Liveness probe for Railway and other deployment platforms."""
    return JSONResponse({"status": "ok"})


def _latest_export_path() -> Path:
    export_dir = Path(settings.storage_dir) / "exports"
    export_files = sorted(export_dir.glob("records_*.csv"))
    if not export_files:
        raise HTTPException(status_code=404, detail="No export file available yet.")
    return export_files[-1]


async def export_csv() -> FileResponse:
    """Download the latest CSV export."""
    filepath = _latest_export_path()
    return FileResponse(
        path=filepath,
        media_type="text/csv; charset=utf-8",
        filename=filepath.name,
    )


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


app = create_app(
    lifespan=lifespan,
    webhook_router=webhook_router,
    groups_router=groups_router,
    periskope_router=periskope_router,
    setup_router=setup_router,
    health_handler=health_check,
    export_csv_handler=export_csv,
    export_xlsx_handler=export_xlsx,
)
