"""
FastAPI application entry point.
"""

import csv
from contextlib import asynccontextmanager
from io import BytesIO
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from app.config import settings
from app.routes.groups import router as groups_router
from app.routes.periskope import router as periskope_router
from app.services.accounting.exporter import TURKISH_HEADERS, tabular_rows_to_xlsx_bytes
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
app.include_router(groups_router)
app.include_router(periskope_router)


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
