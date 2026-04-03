"""
WhatsApp webhook router.

Handles:
- GET  /webhook  -> Meta verification challenge
- POST /webhook  -> Incoming message events
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, Response

from app.config import settings
from app.models.schemas import WhatsAppWebhookPayload
from app.services import bill_classifier, gemini_extractor, record_store, whatsapp
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/webhook", tags=["webhook"])

MSG_ACCEPTED = (
    "\u2705 Belgeniz al\u0131nd\u0131 ve muhasebe kayd\u0131na eklendi.\n"
    "Firma: {company}\nToplam: {total} {currency}"
)
MSG_TEXT_NEEDS_PHOTO = (
    "\U0001F4C4 Fatura/fi\u015f metin olarak alg\u0131land\u0131. "
    "L\u00fctfen belge foto\u011fraf\u0131n\u0131 g\u00f6nderin."
)
MSG_PROCESSING = (
    "\u23f3 Fatura i\u015fleniyor, bu 5-10 saniye s\u00fcrebilir. "
    "Bitti\u011finde haber verece\u011fim."
)
MSG_ERROR = "\u26a0\ufe0f Belgeniz i\u015flenirken bir hata olu\u015ftu. L\u00fctfen daha sonra tekrar deneyin."


@router.get("")
async def verify_webhook(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
) -> Response:
    """Meta webhook verification handshake."""
    if hub_mode == "subscribe" and hub_verify_token == settings.whatsapp_verify_token:
        logger.info("Webhook verified successfully.")
        return Response(content=hub_challenge, media_type="text/plain")
    logger.warning("Webhook verification failed: invalid token.")
    raise HTTPException(status_code=403, detail="Forbidden")


@router.post("")
async def receive_webhook(
    request: Request, background_tasks: BackgroundTasks
) -> dict[str, str]:
    """Receive and enqueue incoming WhatsApp message events."""
    try:
        body: dict[str, Any] = await request.json()
        logger.debug("Received webhook payload: %s", str(body)[:400])
        payload = WhatsAppWebhookPayload.model_validate(body)
    except Exception as exc:
        logger.error("Ignoring malformed webhook payload: %s", exc)
        return {"status": "ignored"}

    for entry in payload.entry:
        for change in entry.changes:
            messages = change.value.messages or []
            for message in messages:
                background_tasks.add_task(_process_message, message)

    return {"status": "ok"}


def _process_message(message) -> None:
    """Process a single WhatsApp message in the background."""
    sender = message.from_
    msg_id = message.id
    msg_type = message.type

    logger.info("Processing message id=%s type=%s from=%s", msg_id, msg_type, sender)

    if not record_store.claim_message_processing(msg_id):
        logger.info("Message id=%s already completed or in-flight; skipping duplicate.", msg_id)
        return

    try:
        if msg_type == "text":
            outcome = _handle_text(message, sender)
            record_store.mark_message_handled(msg_id, outcome=outcome)
        elif msg_type in ("image", "document"):
            outcome = _handle_media(message, sender)
            if outcome != "exported":
                record_store.mark_message_handled(msg_id, outcome=outcome)
        else:
            logger.info("Unsupported message type '%s'; skipping.", msg_type)
            record_store.mark_message_handled(msg_id, outcome="unsupported_message_type")
    except Exception as exc:
        logger.error("Unhandled error processing message %s: %s", msg_id, exc, exc_info=True)
        record_store.release_message_processing(msg_id)
        _safe_send_text_message(sender, MSG_ERROR, context="fatal processing error")


def _handle_text(message, sender: str) -> str:
    text = message.text.body if message.text else ""
    result = bill_classifier.classify_text(text)
    logger.info("Text classification: is_bill=%s confidence=%.2f", result.is_bill, result.confidence)

    if not result.is_bill:
        logger.info("Text message is not a bill; ignoring.")
        return "ignored_non_bill_text"

    _safe_send_text_message(sender, MSG_TEXT_NEEDS_PHOTO, context="text needs photo prompt")
    return "prompted_for_photo"


def _handle_media(message, sender: str) -> str:
    """Download, classify, extract, and reply for image/document messages."""
    if message.type == "image" and message.image:
        media = message.image
        mime_type = media.mime_type or "image/jpeg"
        filename = f"{media.id}.jpg"
        source_type = "image"
    elif message.type == "document" and message.document:
        media = message.document
        mime_type = media.mime_type or "application/pdf"
        filename = media.filename or f"{media.id}.pdf"
        source_type = "document"
    else:
        logger.warning("Media message has no media payload; skipping.")
        return "missing_media_payload"

    try:
        _safe_send_text_message(sender, MSG_PROCESSING, context="processing notice")
        raw_bytes = whatsapp.fetch_media(media.id)

        classification = bill_classifier.classify_image(raw_bytes, mime_type=mime_type)
        logger.info(
            "Image classification: is_bill=%s confidence=%.2f reason=%s",
            classification.is_bill,
            classification.confidence,
            classification.reason,
        )

        if not classification.is_bill:
            logger.info("Image is not a bill; ignoring.")
            return "ignored_non_bill_media"

        record = gemini_extractor.extract_bill(
            image_bytes=raw_bytes,
            mime_type=mime_type,
            source_message_id=message.id,
            source_filename=filename,
            source_type=source_type,
        )

        persisted = record_store.persist_record_once(record)
        if not persisted:
            return "already_exported"

        reply = MSG_ACCEPTED.format(
            company=record.company_name or "Bilinmiyor",
            total=record.total_amount or "?",
            currency=record.currency or "TRY",
        )
        _safe_send_text_message(sender, reply, context="success confirmation")
        return "exported"

    except Exception as exc:
        logger.error("Media processing failed for message %s: %s", message.id, exc, exc_info=True)
        raise RuntimeError("media processing failed") from exc


def _safe_send_text_message(to: str, text: str, *, context: str) -> None:
    """Send a WhatsApp text message and log failures without crashing the worker."""
    try:
        whatsapp.send_text_message(to, text)
    except Exception as exc:
        logger.error("Failed to send %s to %s: %s", context, to, exc, exc_info=True)
