"""
Periskope webhook and AI tool integration routes.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone
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
from app.services.accounting import inbound_queue, ingress_service, record_store
from app.services.accounting.intake import MessageRoute
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

    if _is_stale_periskope_message(message):
        logger.info(
            "Ignoring stale Periskope message id=%s timestamp=%s max_age_minutes=%s",
            message.message_id,
            message.timestamp,
            settings.periskope_max_message_age_minutes,
        )
        return {"status": "ignored"}

    if message.message_type in {"image", "document"}:
        _process_periskope_message(message)
    else:
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
        sender_name=payload.sender_name,
        payment_method=payload.payment_method,
        expense_category=payload.expense_category,
        description=payload.description,
        notes=payload.notes,
        source_message_id=payload.source_message_id,
        source_filename=payload.source_filename,
        source_type=payload.source_type or "periskope_tool",
        source_sender_id=source_sender_id,
        source_sender_name=payload.source_sender_name,
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

    media = message.media
    ingress_service.process_or_enqueue_message(
        message_id=message.message_id,
        msg_type=message.message_type,
        route=route,
        send_text=_send_periskope_text_message,
        send_reaction=_send_periskope_reaction,
        text=message.body or "",
        mime_type=(media.mimetype if media and media.mimetype else _default_mime_type(message.message_type))
        if message.message_type in {"image", "document"} else None,
        filename=(media.filename if media and media.filename else f"{message.message_id}.{_default_extension(message.message_type)}")
        if message.message_type in {"image", "document"} else None,
        source_type=message.message_type if message.message_type in {"image", "document"} else None,
        media_path=(media.path if media and media.path else None),
        attachment_url=(media.path if media and media.path and media.path.startswith("http") else None),
    )


def _resolve_periskope_route(message: PeriskopeMessage) -> MessageRoute:
    is_group = message.chat_id.endswith("@g.us")
    sender_id = message.sender_phone or message.author or message.from_ or message.chat_id
    sender_name = _best_sender_name(message)
    return MessageRoute(
        platform="periskope",
        sender_id=sender_id,
        chat_id=message.chat_id,
        chat_type="group" if is_group else "individual",
        recipient_type="periskope",
        sender_name=sender_name,
        group_id=message.chat_id if is_group else None,
        reply_to_message_id=message.message_id,
    )


def _best_sender_name(message: PeriskopeMessage) -> str | None:
    for candidate in (message.sender_name, message.contact_name, message.push_name, message.notify_name):
        if not candidate:
            continue
        normalized = str(candidate).strip()
        if normalized and not normalized.endswith("@c.us") and not normalized.endswith("@g.us"):
            return normalized
    return None


def _send_periskope_text_message(route: MessageRoute, text: str) -> None:
    periskope.send_text_message(route.chat_id, text, reply_to=route.reply_to_message_id)


def _send_periskope_reaction(route: MessageRoute, emoji: str) -> None:
    if not route.reply_to_message_id:
        return
    periskope.react_to_message(route.reply_to_message_id, emoji)


def _safe_periskope_text_message(route: MessageRoute, text: str) -> None:
    try:
        _send_periskope_text_message(route, text)
    except Exception as exc:
        logger.error("Failed to send queued Periskope text to %s: %s", route.chat_id, exc, exc_info=True)


def _safe_periskope_reaction(route: MessageRoute, emoji: str) -> None:
    try:
        _send_periskope_reaction(route, emoji)
    except Exception as exc:
        logger.error("Failed to send queued Periskope reaction to %s: %s", route.chat_id, exc, exc_info=True)


def _is_allowed_periskope_chat(chat_id: str) -> bool:
    raw_allowlist = settings.periskope_allowed_chat_ids.strip()
    if not raw_allowlist:
        # Safety: if no allowlist is configured, reject all chats.
        # This prevents the bot from responding in every group.
        logger.warning(
            "PERISKOPE_ALLOWED_CHAT_IDS is empty — rejecting chat_id=%s. "
            "Set this env var to allow specific chats.",
            chat_id,
        )
        return False

    allowed = {
        item.strip()
        for item in raw_allowlist.split(",")
        if item.strip()
    }
    return chat_id in allowed


def _is_stale_periskope_message(message: PeriskopeMessage) -> bool:
    max_age_minutes = max(settings.periskope_max_message_age_minutes, 0)
    if max_age_minutes <= 0:
        return False

    message_timestamp = _parse_periskope_timestamp(message.timestamp)
    if message_timestamp is None:
        return False

    age = _now_utc() - message_timestamp
    return age > timedelta(minutes=max_age_minutes)


def _parse_periskope_timestamp(raw_timestamp: str | int | float | None) -> datetime | None:
    if raw_timestamp is None:
        return None

    if isinstance(raw_timestamp, (int, float)):
        return _parse_unix_timestamp(float(raw_timestamp))

    normalized = str(raw_timestamp).strip()
    if not normalized:
        return None

    try:
        return _parse_unix_timestamp(float(normalized))
    except ValueError:
        pass

    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        logger.warning("Ignoring unparseable Periskope message timestamp=%s", raw_timestamp)
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_unix_timestamp(value: float) -> datetime | None:
    if value <= 0:
        return None
    if value > 10_000_000_000:
        value = value / 1000
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


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

    if "timestamp" not in message_payload:
        for timestamp_key in ("created_at", "createdAt", "message_timestamp", "messageTimestamp"):
            if timestamp_key in message_payload:
                message_payload = dict(message_payload)
                message_payload["timestamp"] = message_payload[timestamp_key]
                break

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
