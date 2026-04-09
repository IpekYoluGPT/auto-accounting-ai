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
    ocr,
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
MSG_MULTI_ACCEPTED = (
    "✅ {count} adet belge algılandı ve muhasebe kaydına eklendi.\n"
    "{details}"
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
MSG_GROUPS_ONLY = (
    "🔒 Bu bot şimdilik yalnızca muhasebe grubunda çalışıyor. "
    "Lütfen belgeyi grup içinden gönderin."
)
MSG_ERROR = "⚠️ Belgeniz işlenirken bir hata oluştu. Lütfen daha sonra tekrar deneyin."
MSG_MEDIA_FETCH_ERROR = (
    "Belge indirilemedi, bu yüzden işlenemedi. "
    "Lütfen görüntüyü tekrar gönderin."
)
MSG_MEDIA_CLASSIFICATION_ERROR = (
    "Belgenin muhasebe evrakı olup olmadığı doğrulanamadı. "
    "Lütfen aynı görseli tekrar gönderin."
)
MSG_MEDIA_CATEGORY_ERROR = (
    "Belgenin türü belirlenemedi. "
    "Lütfen tek belge içeren daha net bir görsel gönderin."
)
MSG_MEDIA_EXTRACTION_ERROR = (
    "Belgedeki bilgiler çıkarılamadı. "
    "Lütfen tek belge içeren daha net bir görsel gönderin."
)
MSG_MEDIA_EMPTY_EXTRACTION = (
    "Belge algılandı ama okunabilir bilgi çıkarılamadı. "
    "Lütfen daha net bir fotoğraf gönderin."
)
MSG_MEDIA_TEMPORARY_UPSTREAM_ERROR = (
    "Belge alındı ancak OCR/AI servisi şu anda yoğun veya geçici olarak erişilemiyor. "
    "Lütfen 1-2 dakika sonra aynı görseli tekrar gönderin."
)

REACTION_PROCESSING = "⌛"
REACTION_SUCCESS = "✅"
REACTION_WARNING = "⚠️"

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

_RETRYABLE_MEDIA_OUTCOMES = {
    "media_fetch_failed",
    "classification_failed",
    "category_failed",
    "extraction_failed",
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
SendReactionFn = Callable[[MessageRoute, str], None]
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
    send_reaction: SendReactionFn | None = None,
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
                _handle_media_failure(
                    route,
                    send_text,
                    send_reaction,
                    message=MSG_MEDIA_FETCH_ERROR,
                    reason="missing media configuration",
                    outcome="missing_media_configuration",
                )
                record_store.mark_message_handled(message_id, outcome="missing_media_configuration")
                logger.warning("Missing media configuration for message id=%s", message_id)
                return

            outcome = _handle_media(
                message_id=message_id,
                route=route,
                send_text=send_text,
                send_reaction=send_reaction,
                fetch_media=fetch_media,
                mime_type=mime_type,
                filename=filename,
                source_type=source_type,
                attachment_url=attachment_url,
            )
            if outcome in _RETRYABLE_MEDIA_OUTCOMES:
                record_store.release_message_processing(message_id)
            elif outcome != "exported":
                record_store.mark_message_handled(message_id, outcome=outcome)
            return

        logger.info("Unsupported message type '%s'; skipping.", msg_type)
        record_store.mark_message_handled(message_id, outcome="unsupported_message_type")

    except Exception as exc:
        logger.error("Unhandled error processing message %s: %s", message_id, exc, exc_info=True)
        record_store.release_message_processing(message_id)
        if msg_type in {"image", "document"}:
            _handle_media_failure(
                route,
                send_text,
                send_reaction,
                message=MSG_ERROR,
                reason="fatal processing error",
            )
        else:
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
    send_reaction: SendReactionFn | None,
    fetch_media: FetchMediaFn,
    mime_type: str,
    filename: str,
    source_type: str,
    attachment_url: str | None = None,
) -> str:
    _safe_send_reaction(route, REACTION_PROCESSING, reason="processing reaction", send_reaction=send_reaction)
    try:
        raw_bytes = fetch_media()
    except Exception as exc:
        logger.warning("Failed to fetch media for message id=%s: %s", message_id, exc)
        return _handle_media_failure(
            route,
            send_text,
            send_reaction,
            message=MSG_MEDIA_FETCH_ERROR,
            reason="media fetch failed",
            outcome="media_fetch_failed",
        )

    prepared = ocr.prepare_document(raw_bytes, mime_type=mime_type)
    working_bytes = prepared.media_bytes
    working_mime_type = prepared.mime_type
    ocr_bundle = prepared.ocr_bundle
    if prepared.warnings:
        logger.info(
            "Media prepared for OCR: message_id=%s warnings=%s",
            message_id,
            prepared.warnings,
        )

    # Step 1: Is this a financial document at all?
    try:
        if ocr_bundle is not None:
            classification = bill_classifier.classify_image(
                working_bytes,
                mime_type=working_mime_type,
                ocr_bundle=ocr_bundle,
            )
        else:
            classification = bill_classifier.classify_image(working_bytes, mime_type=working_mime_type)
    except Exception as exc:
        logger.warning("Failed to classify media for message id=%s: %s", message_id, exc)
        return _handle_media_failure(
            route,
            send_text,
            send_reaction,
            message=_message_for_media_exception(exc, MSG_MEDIA_CLASSIFICATION_ERROR),
            reason="media classification failed",
            outcome="classification_failed",
        )
    logger.info(
        "Image classification: is_bill=%s confidence=%.2f reason=%s",
        classification.is_bill,
        classification.confidence,
        (classification.reason or "")[:120],
    )

    if not classification.is_bill:
        return _handle_media_failure(
            route,
            send_text,
            send_reaction,
            message=MSG_UNRELATED_IMAGE,
            reason="unrelated image warning",
            outcome="warned_non_bill_media",
        )

    # Step 2: Which category?
    try:
        if ocr_bundle is not None:
            category, is_return = doc_classifier.classify_document_type(
                working_bytes,
                mime_type=working_mime_type,
                ocr_bundle=ocr_bundle,
            )
        else:
            category, is_return = doc_classifier.classify_document_type(working_bytes, mime_type=working_mime_type)
    except Exception as exc:
        logger.warning("Failed to classify document type for message id=%s: %s", message_id, exc)
        return _handle_media_failure(
            route,
            send_text,
            send_reaction,
            message=_message_for_media_exception(exc, MSG_MEDIA_CATEGORY_ERROR),
            reason="document type classification failed",
            outcome="category_failed",
        )

    # Step 3: Extract structured fields (may return multiple documents)
    try:
        extract_kwargs = {
            "image_bytes": working_bytes,
            "mime_type": working_mime_type,
            "source_message_id": message_id,
            "source_filename": filename,
            "source_type": source_type,
            "source_sender_id": route.sender_id,
            "source_group_id": route.group_id,
            "source_chat_type": route.chat_type,
        }
        if ocr_bundle is not None:
            extract_kwargs["ocr_bundle"] = ocr_bundle
            extract_kwargs["category_hint"] = category
        records = gemini_extractor.extract_bills(**extract_kwargs)
    except Exception as exc:
        logger.warning("Failed to extract bills for message id=%s: %s", message_id, exc)
        return _handle_media_failure(
            route,
            send_text,
            send_reaction,
            message=_message_for_media_exception(exc, MSG_MEDIA_EXTRACTION_ERROR),
            reason="bill extraction failed",
            outcome="extraction_failed",
        )

    if not records:
        logger.warning("Gemini returned zero documents for message id=%s", message_id)
        return _handle_media_failure(
            route,
            send_text,
            send_reaction,
            message=MSG_MEDIA_EMPTY_EXTRACTION,
            reason="empty extraction",
            outcome="empty_extraction",
        )

    # Store the source document in Drive so accounting can verify the original.
    drive_link = google_sheets.upload_document(raw_bytes, filename=filename, mime_type=mime_type)

    # Step 4 & 5: Persist and write each record to Sheets
    persisted_count = 0
    details_lines: list[str] = []
    for record in records:
        persisted = record_store.persist_record_once(record)
        if not persisted:
            continue
        persisted_count += 1
        google_sheets.append_record(record, category, is_return=is_return, drive_link=drive_link)
        details_lines.append(
            f"  • {record.company_name or 'Bilinmiyor'}: "
            f"{record.total_amount or '?'} {record.currency or 'TRY'}"
        )

    if persisted_count == 0:
        _safe_send_reaction(route, REACTION_SUCCESS, reason="already exported reaction", send_reaction=send_reaction)
        return "already_exported"

    _safe_send_reaction(route, REACTION_SUCCESS, reason="success reaction", send_reaction=send_reaction)
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


def _safe_send_reaction(
    route: MessageRoute,
    emoji: str,
    *,
    reason: str,
    send_reaction: SendReactionFn | None,
) -> None:
    if send_reaction is None:
        return
    try:
        send_reaction(route, emoji)
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


def _handle_media_failure(
    route: MessageRoute,
    send_text: SendTextFn,
    send_reaction: SendReactionFn | None,
    *,
    message: str,
    reason: str,
    outcome: str = "media_failure",
) -> str:
    _safe_send_reaction(route, REACTION_WARNING, reason=f"{reason} reaction", send_reaction=send_reaction)
    _safe_send_text_message(route, message, reason=reason, send_text=send_text)
    return outcome


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


def _message_for_media_exception(exc: Exception, default_message: str) -> str:
    error = str(exc).lower()
    if any(
        token in error
        for token in (
            "503",
            "429",
            "unavailable",
            "resource_exhausted",
            "overload",
            "timed out",
            "timeout",
        )
    ):
        return MSG_MEDIA_TEMPORARY_UPSTREAM_ERROR
    return default_message
