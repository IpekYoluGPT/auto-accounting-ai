"""
Shared inbound accounting intake pipeline for Meta and Periskope messages.

Flow for media messages:
  1. bill_classifier  — is this a financial document at all?
  2. doc_classifier   — which of the 6 categories?
  3. gemini_extractor — extract structured fields
  4. record_store     — persist to daily CSV (dedup by message_id)
  5. google_sheets    — append to the correct Sheets tab

Special path for the company manager:
  - Text messages from MANAGER_PHONE_NUMBER are treated as elden ödeme entries.
  - Gemini extracts the amount and description from the free-form text.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Literal

from app.config import settings
from app.models.schemas import BillRecord, DocumentCategory
from app.services.accounting import (
    bill_classifier,
    doc_classifier,
    gemini_extractor,
    record_store,
)
from app.services.providers import google_sheets
from app.utils.logging import get_logger

logger = get_logger(__name__)

# ─── User-facing messages ─────────────────────────────────────────────────────

MSG_ACCEPTED = (
    "✅ Belgeniz alındı ve muhasebe kaydına eklendi.\n"
    "Kategori: {category}\n"
    "Firma: {company}\n"
    "Toplam: {total} {currency}"
)
MSG_ELDEN_ODEME_ACCEPTED = (
    "✅ Elden ödeme kaydedildi.\n"
    "Tutar: {total} {currency}\n"
    "Açıklama: {description}"
)
MSG_TEXT_NEEDS_PHOTO = (
    "📄 Fatura/fiş metin olarak algılandı. "
    "Lütfen belge fotoğrafını gönderin."
)
MSG_UNRELATED_TEXT = (
    "Bu hat yalnızca fatura ve fiş işlemleri için kullanılır. "
    "Lütfen belge fotoğrafı gönderin."
)
MSG_UNRELATED_IMAGE = (
    "Bu görsel muhasebe belgesi olarak algılanmadı. "
    "Lütfen fatura veya fiş fotoğrafı gönderin."
)
MSG_PROCESSING = (
    "⏳ Belge işleniyor, bu 5-10 saniye sürebilir. "
    "Bittiğinde haber vereceğim."
)
MSG_GROUPS_ONLY = (
    "🔒 Bu bot şimdilik yalnızca muhasebe grubunda çalışıyor. "
    "Lütfen belgeyi grup içinden gönderin."
)
MSG_ERROR = "⚠️ Belgeniz işlenirken bir hata oluştu. Lütfen daha sonra tekrar deneyin."

# Human-readable category labels for confirmation messages
_CATEGORY_LABELS: dict[DocumentCategory, str] = {
    DocumentCategory.FATURA: "Fatura",
    DocumentCategory.ODEME_DEKONTU: "Ödeme Dekontu",
    DocumentCategory.HARCAMA_FISI: "Harcama Fişi",
    DocumentCategory.CEK: "Çek",
    DocumentCategory.ELDEN_ODEME: "Elden Ödeme",
    DocumentCategory.MALZEME: "Malzeme / İrsaliye",
    DocumentCategory.IADE: "İade Belgesi",
    DocumentCategory.BELIRSIZ: "Genel Belge",
}


# ─── Route descriptor ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class MessageRoute:
    platform: Literal["meta_whatsapp", "periskope"]
    sender_id: str
    chat_id: str
    chat_type: Literal["individual", "group"]
    recipient_type: str
    group_id: str | None = None
    reply_to_message_id: str | None = None


SendTextFn = Callable[[MessageRoute, str], None]
FetchMediaFn = Callable[[], bytes]


# ─── Manager phone helper ─────────────────────────────────────────────────────


def _is_manager(sender_id: str) -> bool:
    """Return True when the sender is the configured company manager."""
    if not settings.manager_phone_number:
        return False
    # Normalise: strip @c.us suffix for comparison
    def _bare(s: str) -> str:
        return s.replace("@c.us", "").replace("+", "").strip()

    return _bare(sender_id) == _bare(settings.manager_phone_number)


# ─── Main entry point ─────────────────────────────────────────────────────────


def process_incoming_message(
    *,
    message_id: str,
    msg_type: str,
    route: MessageRoute,
    send_text: SendTextFn,
    text: str | None = None,
    fetch_media: FetchMediaFn | None = None,
    mime_type: str | None = None,
    filename: str | None = None,
    source_type: str | None = None,
    attachment_url: str | None = None,
) -> None:
    """Run the full intake flow for one inbound message."""
    logger.info(
        "Processing %s message id=%s type=%s sender=%s chat_id=%s chat_type=%s",
        route.platform,
        message_id,
        msg_type,
        route.sender_id,
        route.chat_id,
        route.chat_type,
    )

    if not record_store.claim_message_processing(message_id):
        logger.info("Message id=%s already completed or in-flight; skipping duplicate.", message_id)
        return

    try:
        if settings.whatsapp_groups_only and route.chat_type != "group":
            outcome = _handle_disabled_individual_chat(route, send_text)
            record_store.mark_message_handled(message_id, outcome=outcome)
            return

        if msg_type == "text":
            # Manager text → attempt elden ödeme extraction first
            if _is_manager(route.sender_id):
                outcome = _handle_manager_text(
                    text=text or "",
                    route=route,
                    send_text=send_text,
                    message_id=message_id,
                )
            else:
                outcome = _handle_text(text or "", route, send_text)
            record_store.mark_message_handled(message_id, outcome=outcome)
            return

        if msg_type in {"image", "document"}:
            if fetch_media is None or not mime_type or not filename or not source_type:
                record_store.mark_message_handled(message_id, outcome="missing_media_configuration")
                logger.warning("Missing media configuration for message id=%s", message_id)
                return

            outcome = _handle_media(
                message_id=message_id,
                route=route,
                send_text=send_text,
                fetch_media=fetch_media,
                mime_type=mime_type,
                filename=filename,
                source_type=source_type,
                attachment_url=attachment_url,
            )
            if outcome != "exported":
                record_store.mark_message_handled(message_id, outcome=outcome)
            return

        logger.info("Unsupported message type '%s'; skipping.", msg_type)
        record_store.mark_message_handled(message_id, outcome="unsupported_message_type")

    except Exception as exc:
        logger.error("Unhandled error processing message %s: %s", message_id, exc, exc_info=True)
        record_store.release_message_processing(message_id)
        _safe_send_text_message(route, MSG_ERROR, reason="fatal processing error", send_text=send_text)


# ─── Text handlers ────────────────────────────────────────────────────────────


def _handle_text(text: str, route: MessageRoute, send_text: SendTextFn) -> str:
    result = bill_classifier.classify_text(text)
    logger.info("Text classification: is_bill=%s confidence=%.2f", result.is_bill, result.confidence)

    if not result.is_bill:
        if route.chat_type == "group":
            logger.info("Ignoring non-bill text in group chat_id=%s without warning.", route.chat_id)
            return "ignored_non_bill_group_text"
        if _send_throttled_warning(
            route,
            MSG_UNRELATED_TEXT,
            warning_key="unrelated_text",
            reason="unrelated text warning",
            send_text=send_text,
        ):
            return "warned_non_bill_text"
        logger.info("Text message is not a bill; warning suppressed by throttle.")
        return "ignored_non_bill_text"

    _safe_send_text_message(route, MSG_TEXT_NEEDS_PHOTO, reason="text needs photo prompt", send_text=send_text)
    return "prompted_for_photo"


def _handle_manager_text(
    text: str,
    route: MessageRoute,
    send_text: SendTextFn,
    message_id: str,
) -> str:
    """
    Process a text message from the company manager.

    If Gemini extracts a payment amount → create an elden ödeme record.
    Otherwise fall back to the regular text handler.
    """
    logger.info("Manager text message received from %s; attempting elden ödeme extraction.", route.sender_id)

    total, currency, recipient, description = doc_classifier.extract_elden_odeme_from_text(text)

    if not total:
        logger.info("No payment amount found in manager text; treating as regular text.")
        return _handle_text(text, route, send_text)

    # Build a minimal BillRecord for the elden ödeme entry
    now = datetime.now(timezone.utc)
    record = BillRecord(
        company_name=recipient,
        document_date=now.strftime("%Y-%m-%d"),
        document_time=now.strftime("%H:%M"),
        currency=currency,
        total_amount=total,
        payment_method="Nakit",
        expense_category="Elden Ödeme",
        description=description or text[:200],
        source_message_id=message_id,
        source_sender_id=route.sender_id,
        source_group_id=route.group_id,
        source_chat_type=route.chat_type,
        source_type="manager_text",
        confidence=0.9,
    )

    persisted = record_store.persist_record_once(record)
    if persisted:
        google_sheets.append_record(record, DocumentCategory.ELDEN_ODEME, is_return=False)

    reply = MSG_ELDEN_ODEME_ACCEPTED.format(
        total=total,
        currency=currency,
        description=description or text[:100],
    )
    _safe_send_text_message(route, reply, reason="elden odeme confirmation", send_text=send_text)
    return "exported_elden_odeme"


def _handle_disabled_individual_chat(route: MessageRoute, send_text: SendTextFn) -> str:
    logger.info("Skipping direct 1:1 chat because groups-only mode is enabled.")
    if _send_throttled_warning(
        route,
        MSG_GROUPS_ONLY,
        warning_key="groups_only_disabled",
        reason="groups-only warning",
        send_text=send_text,
    ):
        return "warned_groups_only_disabled"
    return "ignored_groups_only_disabled"


# ─── Media handler ────────────────────────────────────────────────────────────


def _handle_media(
    *,
    message_id: str,
    route: MessageRoute,
    send_text: SendTextFn,
    fetch_media: FetchMediaFn,
    mime_type: str,
    filename: str,
    source_type: str,
    attachment_url: str | None = None,
) -> str:
    _safe_send_text_message(route, MSG_PROCESSING, reason="processing notice", send_text=send_text)
    raw_bytes = fetch_media()

    # Use Periskope's GCS attachment URL directly as the document link.
    # Drive upload is intentionally skipped: service accounts have no storage quota
    # and concurrent httplib2 calls are not thread-safe (causes segfaults under load).
    drive_link: str | None = attachment_url

    # Step 1: Is this a financial document at all?
    classification = bill_classifier.classify_image(raw_bytes, mime_type=mime_type)
    logger.info(
        "Image classification: is_bill=%s confidence=%.2f reason=%s",
        classification.is_bill,
        classification.confidence,
        classification.reason,
    )

    if not classification.is_bill:
        _safe_send_text_message(route, MSG_UNRELATED_IMAGE, reason="unrelated image warning", send_text=send_text)
        return "warned_non_bill_media"

    # Step 2: Which category?
    category, is_return = doc_classifier.classify_document_type(raw_bytes, mime_type=mime_type)

    # Step 3: Extract structured fields
    record = gemini_extractor.extract_bill(
        image_bytes=raw_bytes,
        mime_type=mime_type,
        source_message_id=message_id,
        source_filename=filename,
        source_type=source_type,
        source_sender_id=route.sender_id,
        source_group_id=route.group_id,
        source_chat_type=route.chat_type,
    )

    # Step 4: Persist to CSV (dedup guard)
    persisted = record_store.persist_record_once(record)
    if not persisted:
        return "already_exported"

    # Step 5: Write to Google Sheets (with Drive link)
    google_sheets.append_record(record, category, is_return=is_return, drive_link=drive_link)

    # Confirmation message includes category label
    category_label = _CATEGORY_LABELS.get(category, "Belge")
    reply = MSG_ACCEPTED.format(
        category=category_label,
        company=record.company_name or "Bilinmiyor",
        total=record.total_amount or "?",
        currency=record.currency or "TRY",
    )
    _safe_send_text_message(route, reply, reason="success confirmation", send_text=send_text)
    return "exported"


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _safe_send_text_message(
    route: MessageRoute, text: str, *, reason: str, send_text: SendTextFn
) -> None:
    try:
        send_text(route, text)
    except Exception as exc:
        logger.error(
            "Failed to send %s to chat_id=%s (chat_type=%s platform=%s): %s",
            reason,
            route.chat_id,
            route.chat_type,
            route.platform,
            exc,
            exc_info=True,
        )


def _send_throttled_warning(
    route: MessageRoute,
    text: str,
    *,
    warning_key: str,
    reason: str,
    send_text: SendTextFn,
) -> bool:
    if not record_store.should_send_warning(route.chat_id, warning_key):
        return False
    _safe_send_text_message(route, text, reason=reason, send_text=send_text)
    return True
