"""
Shared inbound accounting intake pipeline for Meta and Periskope messages.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal

from app.config import settings
from app.services.accounting import bill_classifier, gemini_extractor, record_store
from app.utils.logging import get_logger

logger = get_logger(__name__)

MSG_ACCEPTED = (
    "\u2705 Belgeniz al\u0131nd\u0131 ve muhasebe kayd\u0131na eklendi.\n"
    "Firma: {company}\nToplam: {total} {currency}"
)
MSG_TEXT_NEEDS_PHOTO = (
    "\U0001F4C4 Fatura/fi\u015f metin olarak alg\u0131land\u0131. "
    "L\u00fctfen belge foto\u011fraf\u0131n\u0131 g\u00f6nderin."
)
MSG_UNRELATED_TEXT = (
    "Bu hat yaln\u0131zca fatura ve fi\u015f i\u015flemleri i\u00e7in kullan\u0131l\u0131r. "
    "L\u00fctfen belge foto\u011fraf\u0131 g\u00f6nderin."
)
MSG_UNRELATED_IMAGE = (
    "Bu g\u00f6rsel muhasebe belgesi olarak alg\u0131lanmad\u0131. "
    "L\u00fctfen fatura veya fi\u015f foto\u011fraf\u0131 g\u00f6nderin."
)
MSG_PROCESSING = (
    "\u23f3 Fatura i\u015fleniyor, bu 5-10 saniye s\u00fcrebilir. "
    "Bitti\u011finde haber verece\u011fim."
)
MSG_GROUPS_ONLY = (
    "\U0001F512 Bu bot \u015fimdilik yaln\u0131zca muhasebe grubunda \u00e7al\u0131\u015f\u0131yor. "
    "L\u00fctfen belgeyi grup i\u00e7inden g\u00f6nderin."
)
MSG_ERROR = "\u26a0\ufe0f Belgeniz i\u015flenirken bir hata olu\u015ftu. L\u00fctfen daha sonra tekrar deneyin."


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


def _handle_text(text: str, route: MessageRoute, send_text: SendTextFn) -> str:
    result = bill_classifier.classify_text(text)
    logger.info("Text classification: is_bill=%s confidence=%.2f", result.is_bill, result.confidence)

    if not result.is_bill:
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


def _handle_media(
    *,
    message_id: str,
    route: MessageRoute,
    send_text: SendTextFn,
    fetch_media: FetchMediaFn,
    mime_type: str,
    filename: str,
    source_type: str,
) -> str:
    _safe_send_text_message(route, MSG_PROCESSING, reason="processing notice", send_text=send_text)
    raw_bytes = fetch_media()

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

    persisted = record_store.persist_record_once(record)
    if not persisted:
        return "already_exported"

    reply = MSG_ACCEPTED.format(
        company=record.company_name or "Bilinmiyor",
        total=record.total_amount or "?",
        currency=record.currency or "TRY",
    )
    _safe_send_text_message(route, reply, reason="success confirmation", send_text=send_text)
    return "exported"


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
