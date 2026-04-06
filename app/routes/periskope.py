"""
Periskope webhook and AI tool integration routes.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, status

from app.config import settings
from app.models.schemas import (
    BillRecord,
    PeriskopeAssignToHumanRequest,
    PeriskopeCreateAccountingRecordRequest,
    PeriskopeMessage,
    PeriskopeSubmissionStatusRequest,
)
from app.services.providers import periskope
from app.services.accounting import record_store
from app.services.accounting.intake import MessageRoute, process_incoming_message
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/integrations/periskope", tags=["periskope"])


@router.post("/webhook")
async def receive_periskope_webhook(
    request: Request, background_tasks: BackgroundTasks
) -> dict[str, str]:
    raw_body = await request.body()
    signature = request.headers.get("x-periskope-signature", "")
    _verify_periskope_signature(raw_body, signature)

    try:
        event_name, message_payload = _parse_periskope_webhook(raw_body)
    except Exception as exc:
        logger.error("Ignoring malformed Periskope webhook payload: %s", exc)
        return {"status": "ignored"}

    if event_name != "message.created":
        logger.info("Ignoring unsupported Periskope event=%s", event_name)
        return {"status": "ignored"}

    try:
        message = PeriskopeMessage.model_validate(message_payload)
    except Exception as exc:
        logger.error(
            "Ignoring malformed Periskope message payload for event=%s keys=%s: %s",
            event_name,
            sorted(message_payload.keys()),
            exc,
        )
        return {"status": "ignored"}

    if message.from_me or message.is_private_note:
        logger.info("Ignoring self-authored/private Periskope message id=%s", message.message_id)
        return {"status": "ignored"}

    background_tasks.add_task(_process_periskope_message, message)
    return {"status": "ok"}


@router.post("/tools/create_accounting_record")
async def create_accounting_record_tool(
    request: Request, payload: PeriskopeCreateAccountingRecordRequest
) -> dict[str, Any]:
    _verify_tool_token(request)

    source_chat_type, source_group_id, source_sender_id = _infer_tool_source_context(
        chat_id=payload.chat_id,
        source_chat_type=payload.source_chat_type,
        source_group_id=payload.source_group_id,
        source_sender_id=payload.source_sender_id,
    )

    record = BillRecord(
        company_name=payload.company_name,
        tax_number=payload.tax_number,
        tax_office=payload.tax_office,
        document_number=payload.document_number,
        invoice_number=payload.invoice_number,
        receipt_number=payload.receipt_number,
        document_date=payload.document_date,
        document_time=payload.document_time,
        currency=payload.currency,
        subtotal=payload.subtotal,
        vat_rate=payload.vat_rate,
        vat_amount=payload.vat_amount,
        total_amount=payload.total_amount,
        payment_method=payload.payment_method,
        expense_category=payload.expense_category,
        description=payload.description,
        notes=payload.notes,
        source_message_id=payload.source_message_id,
        source_filename=payload.source_filename,
        source_type=payload.source_type or "periskope_tool",
        source_sender_id=source_sender_id,
        source_group_id=source_group_id,
        source_chat_type=source_chat_type,
        confidence=payload.confidence,
    )

    persisted = record_store.persist_record_once(record)
    return {
        "status": "recorded" if persisted else "duplicate",
        "duplicate_protection": bool(payload.source_message_id),
        "record": record.model_dump(exclude_none=True),
    }


@router.post("/tools/get_submission_status")
async def get_submission_status_tool(
    request: Request, payload: PeriskopeSubmissionStatusRequest
) -> dict[str, Any]:
    _verify_tool_token(request)

    if not payload.source_message_id and not payload.chat_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide source_message_id or chat_id.",
        )

    rows = record_store.find_export_rows(
        source_message_id=payload.source_message_id,
        chat_id=payload.chat_id,
        limit=payload.limit,
    )
    return {
        "found": bool(rows),
        "match_count": len(rows),
        "rows": rows,
    }


@router.post("/tools/assign_to_human")
async def assign_to_human_tool(
    request: Request, payload: PeriskopeAssignToHumanRequest
) -> dict[str, Any]:
    _verify_tool_token(request)
    response = periskope.send_private_note(
        payload.chat_id,
        payload.message,
        reply_to=payload.reply_to,
    )
    return {
        "status": "queued",
        "provider_response": response,
    }


def _process_periskope_message(message: PeriskopeMessage) -> None:
    route = _resolve_periskope_route(message)

    if not _is_allowed_periskope_chat(route.chat_id):
        logger.info("Ignoring Periskope message from non-allowed chat_id=%s", route.chat_id)
        return

    if message.message_type in {"text", "chat"}:
        process_incoming_message(
            message_id=message.message_id,
            msg_type="text",
            route=route,
            send_text=_send_periskope_text_message,
            text=message.body or "",
        )
        return

    if message.message_type in {"image", "document"}:
        media = message.media
        process_incoming_message(
            message_id=message.message_id,
            msg_type=message.message_type,
            route=route,
            send_text=_send_periskope_text_message,
            fetch_media=(
                lambda: periskope.fetch_media(media.path, message_id=message.message_id)
            )
            if media and media.path
            else None,
            mime_type=(media.mimetype if media and media.mimetype else _default_mime_type(message.message_type)),
            filename=(media.filename if media and media.filename else f"{message.message_id}.{_default_extension(message.message_type)}"),
            source_type=message.message_type,
        )
        return

    process_incoming_message(
        message_id=message.message_id,
        msg_type=message.message_type,
        route=route,
        send_text=_send_periskope_text_message,
    )


def _resolve_periskope_route(message: PeriskopeMessage) -> MessageRoute:
    is_group = message.chat_id.endswith("@g.us")
    sender_id = message.sender_phone or message.author or message.from_ or message.chat_id
    return MessageRoute(
        platform="periskope",
        sender_id=sender_id,
        chat_id=message.chat_id,
        chat_type="group" if is_group else "individual",
        recipient_type="periskope",
        group_id=message.chat_id if is_group else None,
        reply_to_message_id=message.message_id,
    )


def _send_periskope_text_message(route: MessageRoute, text: str) -> None:
    periskope.send_text_message(route.chat_id, text, reply_to=route.reply_to_message_id)


def _is_allowed_periskope_chat(chat_id: str) -> bool:
    raw_allowlist = settings.periskope_allowed_chat_ids.strip()
    if not raw_allowlist:
        return True

    allowed = {
        item.strip()
        for item in raw_allowlist.split(",")
        if item.strip()
    }
    return chat_id in allowed


def _verify_periskope_signature(raw_body: bytes, signature: str) -> None:
    secret = settings.periskope_signing_key.strip()
    if not secret:
        logger.warning(
            "PERISKOPE_SIGNING_KEY is not configured; accepting webhook without signature verification."
        )
        return

    if not signature:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing signature.")

    try:
        parsed = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Malformed JSON.") from exc

    canonical_payload = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
    digest = hmac.new(secret.encode("utf-8"), canonical_payload.encode("utf-8"), hashlib.sha256)
    if not hmac.compare_digest(digest.hexdigest(), signature):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature.")


def _parse_periskope_webhook(raw_body: bytes) -> tuple[str, dict[str, Any]]:
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise ValueError("Malformed JSON.") from exc

    if not isinstance(payload, dict):
        raise ValueError("Webhook payload must be a JSON object.")

    event_name = payload.get("event") or payload.get("event_type")
    if not event_name:
        raise ValueError("Missing event/event_type.")

    message_payload = (
        payload.get("data")
        or payload.get("current_attributes")
        or payload.get("attributes")
        or payload.get("message")
    )
    if not isinstance(message_payload, dict):
        raise ValueError("Missing message payload under data/current_attributes/attributes.")

    return str(event_name), message_payload


def _verify_tool_token(request: Request) -> None:
    expected = settings.periskope_tool_token.strip()
    if not expected:
        return

    auth_header = request.headers.get("authorization", "")
    api_key_header = request.headers.get("x-api-key", "")
    if auth_header == f"Bearer {expected}" or api_key_header == expected:
        return

    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid tool token.")


def _infer_tool_source_context(
    *,
    chat_id: str | None,
    source_chat_type: str | None,
    source_group_id: str | None,
    source_sender_id: str | None,
) -> tuple[str | None, str | None, str | None]:
    inferred_chat_type = source_chat_type
    inferred_group_id = source_group_id
    inferred_sender_id = source_sender_id

    if chat_id and not inferred_chat_type:
        inferred_chat_type = "group" if chat_id.endswith("@g.us") else "individual"

    if inferred_chat_type == "group" and chat_id and not inferred_group_id:
        inferred_group_id = chat_id

    if inferred_chat_type == "individual" and chat_id and not inferred_sender_id:
        inferred_sender_id = chat_id

    return inferred_chat_type, inferred_group_id, inferred_sender_id


def _default_extension(message_type: str) -> str:
    if message_type == "document":
        return "pdf"
    return "jpg"


def _default_mime_type(message_type: str) -> str:
    if message_type == "document":
        return "application/pdf"
    return "image/jpeg"
