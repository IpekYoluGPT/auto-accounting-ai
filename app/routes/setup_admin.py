from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

from app.config import settings
from app.services.accounting import inbound_queue
from app.services.providers import google_sheets
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["setup"])


class ResetSheetRequest(BaseModel):
    spreadsheet_id: str | None = None
    clear_storage: bool = True


class RepairSheetRequest(BaseModel):
    spreadsheet_id: str | None = None
    refresh_formatting: bool = False
    tab_name: list[str] | None = None


class RewriteBelgeLinksRequest(BaseModel):
    spreadsheet_id: str | None = None
    tab_name: list[str] | None = None


class HideHiddenTabsRequest(BaseModel):
    spreadsheet_id: str | None = None


class DrainQueuesRequest(BaseModel):
    max_rounds: int = 10


def _verify_admin_token(request: Request) -> None:
    expected = settings.periskope_tool_token.strip()
    if not expected:
        return

    auth_header = request.headers.get("authorization", "")
    api_key_header = request.headers.get("x-api-key", "")
    if auth_header == f"Bearer {expected}" or api_key_header == expected:
        return

    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid tool token.")


def _sandbox_sheet_url(spreadsheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"


@router.post("/reset-sheet")
async def reset_sheet(request: Request, payload: ResetSheetRequest) -> dict[str, object]:
    """Authenticated helper to clear test rows from the target spreadsheet."""
    _verify_admin_token(request)

    try:
        queue_before = google_sheets.queue_status() if payload.clear_storage else None
        reset_count = google_sheets.reset_current_month_spreadsheet_data(
            spreadsheet_id=payload.spreadsheet_id,
        )
        queue_cleared = google_sheets.clear_current_namespace_storage() if payload.clear_storage else None

        response = {
            "status": "ok",
            "spreadsheet_id": payload.spreadsheet_id or settings.google_sheets_spreadsheet_id,
            "tabs_reset": reset_count,
        }
        if payload.clear_storage:
            response["queue_before"] = queue_before
            response["queue_cleared"] = queue_cleared
        return response
    except Exception as exc:
        logger.error("Sheet reset failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/repair-sheet")
async def repair_sheet(request: Request, payload: RepairSheetRequest) -> dict[str, object]:
    """Authenticated helper to repair the live spreadsheet layout and formulas."""
    _verify_admin_token(request)

    try:
        target_tabs = {tab for tab in (payload.tab_name or []) if tab}
        report = google_sheets.audit_current_month_spreadsheet(
            spreadsheet_id=payload.spreadsheet_id,
            repair=True,
            target_tabs=target_tabs or None,
            refresh_formatting=payload.refresh_formatting,
        )
        response = {
            "status": "ok",
            **report,
            "sheet_url": _sandbox_sheet_url(report["spreadsheet_id"]),
        }
        if target_tabs:
            response["audited_tabs"] = sorted(target_tabs)
        return response
    except Exception as exc:
        logger.error("Sheet repair failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/rewrite-belge-links")
async def rewrite_belge_links(request: Request, payload: RewriteBelgeLinksRequest) -> dict[str, object]:
    """Authenticated helper to force-rewrite visible Belge formulas on the live spreadsheet."""
    _verify_admin_token(request)

    try:
        target_tabs = {tab for tab in (payload.tab_name or []) if tab}
        rewritten = google_sheets.force_rewrite_drive_links(
            spreadsheet_id=payload.spreadsheet_id,
            target_tabs=target_tabs or None,
        )
        spreadsheet_id = payload.spreadsheet_id or settings.google_sheets_spreadsheet_id
        response = {
            "status": "ok",
            "spreadsheet_id": spreadsheet_id,
            "rewritten_tabs": rewritten,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
        }
        if target_tabs:
            response["audited_tabs"] = sorted(target_tabs)
        return response
    except Exception as exc:
        logger.error("Belge link rewrite failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/hide-hidden-tabs")
async def hide_hidden_tabs(request: Request, payload: HideHiddenTabsRequest) -> dict[str, object]:
    """Authenticated helper to re-hide technical and ignored orphan worksheets."""
    _verify_admin_token(request)

    try:
        hidden_counts = google_sheets.hide_nonvisible_tabs(spreadsheet_id=payload.spreadsheet_id)
        spreadsheet_id = payload.spreadsheet_id or settings.google_sheets_spreadsheet_id
        return {
            "status": "ok",
            "spreadsheet_id": spreadsheet_id,
            "hidden_tabs": hidden_counts,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
        }
    except Exception as exc:
        logger.error("Hide hidden tabs failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/drain-queues")
async def drain_queues(request: Request, payload: DrainQueuesRequest) -> dict[str, object]:
    """Authenticated helper to drain production queue workers on demand."""
    _verify_admin_token(request)

    try:
        queue_before = google_sheets.queue_status()
        processed_inbound = 0
        processed_sheet = 0
        processed_drive = 0
        rounds = max(1, min(int(payload.max_rounds or 10), 20))

        for _ in range(rounds):
            inbound_count = inbound_queue.process_pending_inbound_jobs()
            sheet_count = google_sheets.process_pending_sheet_appends()
            drive_count = google_sheets.process_pending_document_uploads()
            processed_inbound += inbound_count
            processed_sheet += sheet_count
            processed_drive += drive_count
            if inbound_count == 0 and sheet_count == 0 and drive_count == 0:
                break

        queue_after = google_sheets.queue_status()
        return {
            "status": "ok",
            "queue_before": queue_before,
            "drain": {
                "pending_inbound_jobs_processed": processed_inbound,
                "pending_sheet_appends_processed": processed_sheet,
                "pending_drive_uploads_processed": processed_drive,
            },
            "queue_after": queue_after,
            "rounds": rounds,
        }
    except Exception as exc:
        logger.error("Queue drain failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/retry-inbound")
async def retry_inbound(request: Request) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        result = inbound_queue.retry_pending_inbound_jobs()
        return {
            "status": "ok",
            **result,
            "queue": google_sheets.queue_status(),
        }
    except Exception as exc:
        logger.error("Retry inbound failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/reset-inbound-queue")
async def reset_inbound_queue(request: Request) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        result = inbound_queue.reset_inbound_queue()
        return {
            "status": "ok",
            **result,
            "queue": google_sheets.queue_status(),
        }
    except Exception as exc:
        logger.error("Reset inbound queue failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/storage-status")
async def storage_status(request: Request) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        return {
            "status": "ok",
            **inbound_queue.storage_status(),
            "queue": google_sheets.queue_status(),
        }
    except Exception as exc:
        logger.error("Storage status failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
