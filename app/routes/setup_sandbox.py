from __future__ import annotations

import base64
import binascii
from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from app.services.accounting import intake, record_store
from app.services.accounting.pipeline_context import sandbox_context, pipeline_context_scope
from app.services.providers import google_sheets
from app.utils.logging import get_logger

from .setup_admin import _sandbox_sheet_url, _verify_admin_token

logger = get_logger(__name__)

router = APIRouter(tags=["setup"])


class EnsureSandboxRequest(BaseModel):
    session_id: str | None = None


class SandboxSessionRequest(BaseModel):
    session_id: str


class SandboxIntakeRequest(BaseModel):
    session_id: str
    message_id: str | None = None
    msg_type: Literal["text", "image", "document"]
    sender_id: str | None = None
    sender_name: str | None = None
    chat_id: str | None = None
    chat_type: Literal["individual", "group"] = "group"
    group_id: str | None = None
    text: str | None = None
    media_base64: str | None = None
    mime_type: str | None = None
    filename: str | None = None
    source_type: str | None = None
    attachment_url: str | None = None


class SandboxDriftRequest(BaseModel):
    session_id: str
    action: Literal[
        "reorder_rows",
        "delete_summary_tab",
        "rename_data_tab",
        "corrupt_total_row",
        "corrupt_header_row",
        "clear_hidden_row_ids",
    ]
    tab_name: str | None = None
    replacement_name: str | None = None
    row_count: int = 5


def _normalize_sandbox_session_id(raw_session_id: str | None) -> str:
    session_id = (raw_session_id or uuid4().hex[:12]).strip()
    if not session_id:
        session_id = uuid4().hex[:12]
    return session_id


def _resolve_existing_sandbox_spreadsheet_id(session_id: str) -> str | None:
    context = sandbox_context(session_id=session_id)
    with pipeline_context_scope(context):
        return google_sheets._registered_spreadsheet_id_for_month(google_sheets._month_key())


def _require_existing_sandbox_context(session_id: str):
    normalized_session_id = _normalize_sandbox_session_id(session_id)
    spreadsheet_id = _resolve_existing_sandbox_spreadsheet_id(normalized_session_id)
    if not spreadsheet_id:
        raise HTTPException(status_code=404, detail="Sandbox session not found.")
    return sandbox_context(session_id=normalized_session_id), spreadsheet_id


def _ensure_sandbox_context(session_id: str | None):
    normalized_session_id = _normalize_sandbox_session_id(session_id)
    context = sandbox_context(session_id=normalized_session_id)
    existing_spreadsheet_id = _resolve_existing_sandbox_spreadsheet_id(normalized_session_id)
    with pipeline_context_scope(context):
        spreadsheet_id = google_sheets.ensure_current_month_spreadsheet_ready()
    if not spreadsheet_id:
        raise HTTPException(status_code=500, detail="Sandbox spreadsheet could not be prepared.")
    return context, spreadsheet_id, existing_spreadsheet_id is None


def _drain_sandbox_queues(context) -> dict[str, int]:
    total_sheet_appends = 0
    total_drive_uploads = 0
    with pipeline_context_scope(context):
        for _ in range(10):
            processed_sheet = google_sheets.process_pending_sheet_appends()
            processed_drive = google_sheets.process_pending_document_uploads()
            total_sheet_appends += processed_sheet
            total_drive_uploads += processed_drive
            if processed_sheet == 0 and processed_drive == 0:
                break
    return {
        "pending_sheet_appends_processed": total_sheet_appends,
        "pending_drive_uploads_processed": total_drive_uploads,
    }


@router.post("/sandbox/ensure")
async def ensure_sandbox(request: Request, payload: EnsureSandboxRequest) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        context, spreadsheet_id, created = _ensure_sandbox_context(payload.session_id)
        return {
            "status": "ok",
            "session_id": context.session_id,
            "namespace": context.normalized_namespace,
            "spreadsheet_id": spreadsheet_id,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
            "month_key": google_sheets._month_key(),
            "created": created,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Sandbox ensure failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sandbox/intake")
async def sandbox_intake(request: Request, payload: SandboxIntakeRequest) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        context, spreadsheet_id, _ = _ensure_sandbox_context(payload.session_id)
        session_id = context.session_id or _normalize_sandbox_session_id(payload.session_id)
        chat_type = payload.chat_type
        default_chat_id = f"sandbox-{session_id}@g.us" if chat_type == "group" else f"sandbox-{session_id}@c.us"
        chat_id = (payload.chat_id or default_chat_id).strip()
        sender_id = (payload.sender_id or "sandbox-user@c.us").strip()
        message_id = (payload.message_id or f"sandbox-{session_id}-{uuid4().hex[:12]}").strip()
        source_type = (payload.source_type or ("sandbox_text" if payload.msg_type == "text" else "sandbox_media")).strip()

        media_bytes: bytes | None = None
        if payload.msg_type == "text":
            if not (payload.text or "").strip():
                raise HTTPException(status_code=422, detail="text is required when msg_type=text")
        else:
            if not payload.media_base64 or not payload.mime_type or not payload.filename:
                raise HTTPException(status_code=422, detail="media_base64, mime_type, and filename are required for media intake")
            try:
                media_bytes = base64.b64decode(payload.media_base64, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise HTTPException(status_code=400, detail="Invalid media_base64 payload.") from exc

        route = intake.MessageRoute(
            platform="periskope",
            sender_id=sender_id,
            sender_name=payload.sender_name,
            chat_id=chat_id,
            chat_type=chat_type,
            recipient_type="sandbox",
            group_id=payload.group_id if chat_type == "group" else None,
        )

        with pipeline_context_scope(context):
            before_rows = len(record_store.find_export_rows(source_message_id=message_id, limit=1000, context=context))

        outcome = intake.process_incoming_message(
            message_id=message_id,
            msg_type=payload.msg_type,
            route=route,
            text=payload.text,
            fetch_media=(lambda: media_bytes) if media_bytes is not None else None,
            mime_type=payload.mime_type,
            filename=payload.filename,
            source_type=source_type,
            attachment_url=payload.attachment_url,
            send_text=lambda route, text: None,
            send_reaction=lambda route, emoji: None,
            context=context,
        )

        drain = _drain_sandbox_queues(context)
        with pipeline_context_scope(context):
            after_rows = record_store.find_export_rows(source_message_id=message_id, limit=1000, context=context)
            queue = google_sheets.queue_status()

        return {
            "status": "ok",
            "session_id": context.session_id,
            "spreadsheet_id": spreadsheet_id,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
            "message_id": message_id,
            "outcome": outcome,
            "record_count": max(len(after_rows) - before_rows, 0),
            "recent_rows": after_rows[:5],
            "queue": queue,
            "drain": drain,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Sandbox intake failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sandbox/audit")
async def sandbox_audit(
    request: Request,
    session_id: str = Query(...),
    repair: bool = Query(default=True),
    tab_name: list[str] | None = Query(default=None),
) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        context, spreadsheet_id = _require_existing_sandbox_context(session_id)
        target_tabs = {tab for tab in (tab_name or []) if tab}
        with pipeline_context_scope(context):
            report = google_sheets.audit_current_month_spreadsheet(
                spreadsheet_id=spreadsheet_id,
                repair=repair,
                target_tabs=target_tabs or None,
                refresh_formatting=repair,
            )
        return {
            "status": "ok",
            "session_id": context.session_id,
            **report,
            "audited_tabs": sorted(target_tabs) if target_tabs else None,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Sandbox audit failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sandbox/drift")
async def sandbox_drift(request: Request, payload: SandboxDriftRequest) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        context, spreadsheet_id = _require_existing_sandbox_context(payload.session_id)
        with pipeline_context_scope(context):
            details = google_sheets.apply_test_drift(
                action=payload.action,
                spreadsheet_id=spreadsheet_id,
                tab_name=payload.tab_name,
                replacement_name=payload.replacement_name,
                row_count=payload.row_count,
            )
            recommended_audit_tabs = google_sheets.recommended_audit_tabs_for_test_drift(
                action=payload.action,
                tab_name=payload.tab_name,
            )
        return {
            "status": "ok",
            "session_id": context.session_id,
            **details,
            "recommended_audit_tabs": recommended_audit_tabs,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Sandbox drift failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sandbox/reset")
async def sandbox_reset(request: Request, payload: SandboxSessionRequest) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        context, spreadsheet_id = _require_existing_sandbox_context(payload.session_id)
        with pipeline_context_scope(context):
            queue_before = google_sheets.queue_status()
            tabs_reset = google_sheets.reset_current_month_spreadsheet_data(spreadsheet_id=spreadsheet_id)
            cleared = google_sheets.clear_current_namespace_storage()

        reseed_context = sandbox_context(session_id=context.session_id or payload.session_id, spreadsheet_id_override=spreadsheet_id)
        with pipeline_context_scope(reseed_context):
            google_sheets.ensure_current_month_spreadsheet_ready()

        return {
            "status": "ok",
            "session_id": context.session_id,
            "spreadsheet_id": spreadsheet_id,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
            "tabs_reset": tabs_reset,
            "queue_before": queue_before,
            "queue_cleared": cleared,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Sandbox reset failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
