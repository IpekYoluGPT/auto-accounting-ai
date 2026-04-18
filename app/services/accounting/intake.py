"""
Shared inbound accounting intake pipeline for Meta and Periskope messages.

Flow for media messages:
  1. doc_classifier   — financial-doc triage + category + return detection
  2. gemini_extractor — extract structured fields
  4. record_store     — persist to daily CSV (dedup by message_id + strong content fingerprints)
  5. google_sheets    — append to the correct Sheets tab

Special path for the company manager:
  - Text messages from MANAGER_PHONE_NUMBER are treated as elden ödeme entries.
  - Gemini extracts the amount and description from the free-form text.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from app.config import settings
from app.models.schemas import BillRecord, DocumentCategory
from app.services.accounting import (
    bill_classifier,
    doc_classifier,
    gemini_extractor,
    media_prep,
    record_store,
)
from app.services.accounting.intake_messages import (
    handle_media_failure as _messages_handle_media_failure,
    maybe_send_sheet_backlog_notice as _messages_maybe_send_sheet_backlog_notice,
    safe_send_reaction as _messages_safe_send_reaction,
    safe_send_text_message as _messages_safe_send_text_message,
    send_throttled_warning as _messages_send_throttled_warning,
)
from app.services.accounting.intake_types import (
    FetchMediaFn,
    MediaPayload,
    MediaProcessingResult,
    MessageRoute,
    SendReactionFn,
    SendTextFn,
)
from app.services.accounting.media_pipeline import (
    is_temporary_media_exception as _pipeline_is_temporary_media_exception,
    message_for_media_exception as _pipeline_message_for_media_exception,
    process_media_payload as _pipeline_process_media_payload,
    record_meets_minimum_fields as _pipeline_record_meets_minimum_fields,
)
from app.services.accounting.pipeline_context import PipelineContext, current_pipeline_context, pipeline_context_scope
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
    "Belge alındı ancak AI servisi şu anda yoğun veya geçici olarak erişilemiyor. "
    "Lütfen 1-2 dakika sonra aynı görseli tekrar gönderin."
)
MSG_MEDIA_RETRY_QUALITY = (
    "Belge çok belirsiz veya eksik görünüyor. "
    "Lütfen tek belge içeren daha net ve tam bir fotoğraf/PDF gönderin."
)
MSG_MEDIA_MULTI_DOCUMENT_RETRY = (
    "Aynı fotoğrafta birden fazla belge var ama hepsini güvenle ayıramadım. "
    "Lütfen daha net bir görsel veya belgeleri ayrı ayrı gönderin."
)
MSG_SHEET_BACKLOG_NOTICE = (
    "Belge işlendi. Görünür satırlar önce, detaylar sonra yazılıyor. "
    "✅ geldiğinde ana tabloda görünmüş olur; yoğunlukta birkaç dakika sürebilir."
)

REACTION_PROCESSING = "⌛"
REACTION_SHEET_PENDING = "📝"
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
    "multi_document_retry_required",
}

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
    context: PipelineContext | None = None,
) -> str:
    """Run the full intake flow for one inbound message."""
    with pipeline_context_scope(context):
        logger.info(
            "Processing %s message id=%s type=%s sender=%s chat_id=%s chat_type=%s namespace=%s",
            route.platform,
            message_id,
            msg_type,
            route.sender_id,
            route.chat_id,
            route.chat_type,
            current_pipeline_context().normalized_namespace,
        )

        if not record_store.claim_message_processing(message_id):
            logger.info("Message id=%s already completed or in-flight; skipping duplicate.", message_id)
            return "duplicate_message"

        try:
            if settings.whatsapp_groups_only and route.chat_type != "group":
                outcome = _handle_disabled_individual_chat(route, send_text)
                record_store.mark_message_handled(message_id, outcome=outcome)
                return outcome

            if msg_type == "text":
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
                return outcome

            if msg_type in {"image", "document"}:
                if fetch_media is None or not mime_type or not filename or not source_type:
                    outcome = _handle_media_failure(
                        route,
                        send_text,
                        send_reaction,
                        message=MSG_MEDIA_FETCH_ERROR,
                        reason="missing media configuration",
                        outcome="missing_media_configuration",
                    )
                    record_store.mark_message_handled(message_id, outcome=outcome)
                    logger.warning("Missing media configuration for message id=%s", message_id)
                    return outcome

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
                else:
                    record_store.mark_message_handled(message_id, outcome=outcome)
                return outcome

            logger.info("Unsupported message type '%s'; skipping.", msg_type)
            record_store.mark_message_handled(message_id, outcome="unsupported_message_type")
            return "unsupported_message_type"

        except Exception as exc:
            logger.error("Unhandled error processing message %s: %s", message_id, exc, exc_info=True)
            record_store.release_message_processing(message_id)
            if msg_type in {"image", "document"}:
                return _handle_media_failure(
                    route,
                    send_text,
                    send_reaction,
                    message=MSG_ERROR,
                    reason="fatal processing error",
                    outcome="fatal_processing_error",
                )
            _safe_send_text_message(route, MSG_ERROR, reason="fatal processing error", send_text=send_text)
            return "fatal_processing_error"


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
        source_sender_name=route.sender_name,
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

    try:
        result = process_media_payload(
            message_id=message_id,
            route=route,
            raw_bytes=raw_bytes,
            mime_type=mime_type,
            filename=filename,
            source_type=source_type,
            attachment_url=attachment_url,
        )
    except Exception as exc:
        logger.error("Unhandled media processing error for message id=%s: %s", message_id, exc, exc_info=True)
        return _handle_media_failure(
            route,
            send_text,
            send_reaction,
            message=_message_for_media_exception(exc, MSG_ERROR),
            reason="media processing crash",
            outcome="fatal_processing_error",
        )

    if result.retryable:
        return _handle_media_failure(
            route,
            send_text,
            send_reaction,
            message=result.user_message or MSG_MEDIA_TEMPORARY_UPSTREAM_ERROR,
            reason="media temporary retry required",
            outcome=result.outcome,
        )

    if result.exported_count > 0:
        maybe_send_sheet_backlog_notice(route, send_text=send_text)

    if result.outcome in {"exported", "already_exported"}:
        _safe_send_reaction(route, REACTION_SUCCESS, reason="success reaction", send_reaction=send_reaction)
        return result.outcome

    return _handle_media_failure(
        route,
        send_text,
        send_reaction,
        message=result.user_message or MSG_ERROR,
        reason=result.outcome,
        outcome=result.outcome,
    )


def process_media_payload(
    *,
    message_id: str,
    route: MessageRoute,
    raw_bytes: bytes,
    mime_type: str,
    filename: str,
    source_type: str,
    attachment_url: str | None = None,
) -> MediaProcessingResult:
    return _pipeline_process_media_payload(
        intake_module=sys.modules[__name__],
        payload=MediaPayload(
            message_id=message_id,
            route=route,
            raw_bytes=raw_bytes,
            mime_type=mime_type,
            filename=filename,
            source_type=source_type,
            attachment_url=attachment_url,
        ),
    )


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _safe_send_text_message(
    route: MessageRoute, text: str, *, reason: str, send_text: SendTextFn
) -> None:
    _messages_safe_send_text_message(route, text, reason=reason, send_text=send_text)


def _safe_send_reaction(
    route: MessageRoute,
    emoji: str,
    *,
    reason: str,
    send_reaction: SendReactionFn | None,
) -> None:
    _messages_safe_send_reaction(route, emoji, reason=reason, send_reaction=send_reaction)


def _handle_media_failure(
    route: MessageRoute,
    send_text: SendTextFn,
    send_reaction: SendReactionFn | None,
    *,
    message: str,
    reason: str,
    outcome: str = "media_failure",
) -> str:
    return _messages_handle_media_failure(
        route,
        send_text,
        send_reaction,
        message=message,
        reason=reason,
        outcome=outcome,
    )


def _send_throttled_warning(
    route: MessageRoute,
    text: str,
    *,
    warning_key: str,
    reason: str,
    send_text: SendTextFn,
) -> bool:
    return _messages_send_throttled_warning(
        route,
        text,
        warning_key=warning_key,
        reason=reason,
        send_text=send_text,
    )


def maybe_send_sheet_backlog_notice(route: MessageRoute, *, send_text: SendTextFn) -> None:
    _messages_maybe_send_sheet_backlog_notice(route, send_text=send_text)


def _record_meets_minimum_fields(record: BillRecord, category: DocumentCategory) -> bool:
    return _pipeline_record_meets_minimum_fields(record, category)


def _message_for_media_exception(exc: Exception, default_message: str) -> str:
    return _pipeline_message_for_media_exception(exc, default_message)


def is_temporary_media_exception(exc: Exception) -> bool:
    return _pipeline_is_temporary_media_exception(exc)
