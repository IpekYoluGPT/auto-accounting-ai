"""
WhatsApp webhook router.

Handles:
- GET  /webhook  → Meta verification challenge
- POST /webhook  → Incoming message events
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, Response

from app.config import settings
from app.models.schemas import WhatsAppWebhookPayload
from app.services import bill_classifier, gemini_extractor, whatsapp
from app.utils.file_storage import cleanup_file, save_temp_file
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/webhook", tags=["webhook"])

# ─── Turkish user-facing messages ────────────────────────────────────────────

MSG_ACCEPTED = (
    "✅ Belgeniz alındı ve muhasebe kaydına eklendi.\n"
    "Firma: {company}\nToplam: {total} {currency}"
)
MSG_IGNORED = "ℹ️ Bu mesaj fatura/fiş içermiyor, atlandı."
MSG_ERROR = (
    "⚠️ Belgeniz işlenirken bir hata oluştu. Lütfen daha sonra tekrar deneyin."
)

# ─── Verification ─────────────────────────────────────────────────────────────


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


# ─── Incoming events ──────────────────────────────────────────────────────────


@router.post("")
async def receive_webhook(
    request: Request, background_tasks: BackgroundTasks
) -> dict[str, str]:
    """Receive and enqueue incoming WhatsApp message events."""
    body: dict[str, Any] = await request.json()
    logger.debug("Received webhook payload: %s", str(body)[:400])

    try:
        payload = WhatsAppWebhookPayload.model_validate(body)
    except Exception as exc:
        logger.error("Invalid webhook payload: %s", exc)
        # Always return 200 to prevent Meta from retrying indefinitely
        return {"status": "ignored"}

    for entry in payload.entry:
        for change in entry.changes:
            messages = change.value.messages or []
            for message in messages:
                background_tasks.add_task(_process_message, message)

    return {"status": "ok"}


# ─── Background processing ────────────────────────────────────────────────────


def _process_message(message) -> None:
    """Process a single WhatsApp message in the background."""
    sender = message.from_
    msg_id = message.id
    msg_type = message.type

    logger.info("Processing message id=%s type=%s from=%s", msg_id, msg_type, sender)

    try:
        if msg_type == "text":
            _handle_text(message, sender)
        elif msg_type in ("image", "document"):
            _handle_media(message, sender)
        else:
            logger.info("Unsupported message type '%s'; skipping.", msg_type)
    except Exception as exc:
        logger.error("Unhandled error processing message %s: %s", msg_id, exc, exc_info=True)
        whatsapp.send_text_message(sender, MSG_ERROR)


def _handle_text(message, sender: str) -> None:
    text = message.text.body if message.text else ""
    result = bill_classifier.classify_text(text)
    logger.info("Text classification: is_bill=%s confidence=%.2f", result.is_bill, result.confidence)

    if not result.is_bill:
        logger.info("Text message is not a bill; ignoring.")
        return

    # Text-only messages are unlikely to contain extractable bill images; inform user
    whatsapp.send_text_message(
        sender,
        "📄 Fatura/fiş metin olarak algılandı. Lütfen belge fotoğrafını gönderin.",
    )


def _handle_media(message, sender: str) -> None:
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
        return

    extension = mime_type.split("/")[-1].replace("jpeg", "jpg")
    temp_path: Path | None = None

    try:
        raw_bytes = whatsapp.fetch_media(media.id)
        temp_path = save_temp_file(raw_bytes, extension=extension)

        # Classify before running expensive extraction
        classification = bill_classifier.classify_image(raw_bytes, mime_type=mime_type)
        logger.info(
            "Image classification: is_bill=%s confidence=%.2f reason=%s",
            classification.is_bill,
            classification.confidence,
            classification.reason,
        )

        if not classification.is_bill:
            logger.info("Image is not a bill; ignoring.")
            return

        record = gemini_extractor.extract_bill(
            image_bytes=raw_bytes,
            mime_type=mime_type,
            source_message_id=message.id,
            source_filename=filename,
            source_type=source_type,
        )

        # Persist the extracted record
        _persist_record(record)

        reply = MSG_ACCEPTED.format(
            company=record.company_name or "Bilinmiyor",
            total=record.total_amount or "?",
            currency=record.currency or "TRY",
        )
        whatsapp.send_text_message(sender, reply)

    except Exception as exc:
        logger.error("Media processing failed for message %s: %s", message.id, exc, exc_info=True)
        whatsapp.send_text_message(sender, MSG_ERROR)
    finally:
        if temp_path:
            cleanup_file(temp_path)


def _persist_record(record) -> None:
    """Append the record to the daily CSV export file."""
    from datetime import date

    from app.services.exporter import TURKISH_HEADERS, record_to_row
    import csv

    exports_dir = Path(settings.storage_dir) / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    filepath = exports_dir / f"records_{date.today().isoformat()}.csv"

    write_header = not filepath.exists()
    with filepath.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=TURKISH_HEADERS)
        if write_header:
            writer.writeheader()
        writer.writerow(record_to_row(record))
    logger.info("Record appended to %s", filepath)
