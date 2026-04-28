from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

from app.config import settings
from app.services.accounting import inbound_queue, record_store
from app.services.providers import google_sheets, google_sheets_scheduler
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


class UpdateSheetRegistryRequest(BaseModel):
    month: str
    spreadsheet_id: str


class ReprocessMessageRequest(BaseModel):
    message_id: str
    media_sha256: str | None = None


class PatchRecordDateRequest(BaseModel):
    message_id: str
    new_date: str


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

    restart_workers = False
    try:
        queue_before = None
        queue_cleared = None
        deleted_paths: list[str] = []
        workers_restarted = False

        if payload.clear_storage:
            inbound_queue.stop_pending_inbound_job_worker(timeout_seconds=5.0)
            restart_workers = True
            queue_before = google_sheets.queue_status()
            clear_result = google_sheets.clear_current_namespace_storage()
            queue_cleared = clear_result.get("queue_status", clear_result)
            deleted_paths = list(clear_result.get("deleted_paths", []))

        reset_count = google_sheets.reset_current_month_spreadsheet_data(
            spreadsheet_id=payload.spreadsheet_id,
        )

        if payload.clear_storage:
            inbound_queue.start_pending_inbound_job_worker()
            google_sheets_scheduler.start_monthly_rollover_scheduler(google_sheets)
            workers_restarted = True
            restart_workers = False

        response = {
            "status": "ok",
            "spreadsheet_id": payload.spreadsheet_id or settings.google_sheets_spreadsheet_id,
            "tabs_reset": reset_count,
        }
        if payload.clear_storage:
            response["queue_before"] = queue_before
            response["queue_cleared"] = queue_cleared
            response["deleted_paths"] = deleted_paths
            response["workers_restarted"] = workers_restarted
        return response
    except Exception as exc:
        logger.error("Sheet reset failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        if restart_workers:
            try:
                inbound_queue.start_pending_inbound_job_worker()
                google_sheets_scheduler.start_monthly_rollover_scheduler(google_sheets)
            except Exception as exc:
                logger.warning("Could not restart workers after failed sheet reset: %s", exc, exc_info=True)


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


@router.post("/update-sheet-registry")
async def update_sheet_registry(request: Request, payload: UpdateSheetRegistryRequest) -> dict[str, object]:
    """Update the sheets registry so a month points to the correct spreadsheet ID."""
    _verify_admin_token(request)

    month = payload.month.strip()
    spreadsheet_id = payload.spreadsheet_id.strip()

    import re as _re
    if not _re.match(r"^\d{4}-\d{2}$", month):
        raise HTTPException(status_code=400, detail="month must be in YYYY-MM format (e.g. '2026-04')")
    if not spreadsheet_id:
        raise HTTPException(status_code=400, detail="spreadsheet_id must not be empty")

    try:
        google_sheets.update_registry_entry(month, spreadsheet_id)
        return {
            "status": "ok",
            "month": month,
            "spreadsheet_id": spreadsheet_id,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
        }
    except Exception as exc:
        logger.error("Registry update failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/patch-record-date")
async def patch_record_date(request: Request, payload: PatchRecordDateRequest) -> dict[str, object]:
    """Update document_date in the CSV source-of-truth for a specific message ID.

    Use this when the sheet keeps reverting a manually-corrected date because
    the periodic projection sync reads from the CSV which still has the wrong date.
    """
    _verify_admin_token(request)

    import re as _re
    message_id = payload.message_id.strip()
    new_date = payload.new_date.strip()

    if not message_id:
        raise HTTPException(status_code=400, detail="message_id must not be empty")
    if not _re.match(r"^\d{4}-\d{2}-\d{2}$", new_date):
        raise HTTPException(status_code=400, detail="new_date must be in YYYY-MM-DD format (e.g. '2026-04-22')")

    try:
        result = record_store.patch_record_date(message_id, new_date)
        return {"status": "ok", **result}
    except Exception as exc:
        logger.error("Patch record date failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/reprocess-message")
async def reprocess_message(request: Request, payload: ReprocessMessageRequest) -> dict[str, object]:
    """Clear dedup entries for a message ID so the customer can resend and have it reprocessed."""
    _verify_admin_token(request)

    message_id = payload.message_id.strip()
    if not message_id:
        raise HTTPException(status_code=400, detail="message_id must not be empty")

    try:
        result = record_store.clear_message_dedup(
            message_id,
            media_sha256=payload.media_sha256,
        )
        return {"status": "ok", **result}
    except Exception as exc:
        logger.error("Reprocess message failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
