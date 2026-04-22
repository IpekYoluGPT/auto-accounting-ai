"""
User-facing intake copy and outbound helpers.
"""

from __future__ import annotations

from app.services.accounting import record_store
from app.services.accounting.intake_types import MessageRoute, SendReactionFn, SendTextFn
from app.services.accounting.pipeline_context import current_pipeline_context
from app.services.providers import google_sheets
from app.utils.logging import get_logger

logger = get_logger(__name__)

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

_BACKLOG_NOTICE_THRESHOLD = 5


def safe_send_text_message(
    route: MessageRoute, text: str, *, reason: str, send_text: SendTextFn
) -> None:
    if current_pipeline_context().disable_outbound_messages:
        logger.info(
            "Skipping outbound text in namespace=%s for chat_id=%s reason=%s",
            current_pipeline_context().normalized_namespace,
            route.chat_id,
            reason,
        )
        return
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


def safe_send_reaction(
    route: MessageRoute,
    emoji: str,
    *,
    reason: str,
    send_reaction: SendReactionFn | None,
) -> None:
    if current_pipeline_context().disable_outbound_messages:
        logger.info(
            "Skipping outbound reaction in namespace=%s for chat_id=%s reason=%s",
            current_pipeline_context().normalized_namespace,
            route.chat_id,
            reason,
        )
        return
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


def handle_media_failure(
    route: MessageRoute,
    send_text: SendTextFn,
    send_reaction: SendReactionFn | None,
    *,
    message: str,
    reason: str,
    outcome: str = "media_failure",
) -> str:
    safe_send_reaction(route, REACTION_WARNING, reason=f"{reason} reaction", send_reaction=send_reaction)
    safe_send_text_message(route, message, reason=reason, send_text=send_text)
    return outcome


def send_throttled_warning(
    route: MessageRoute,
    text: str,
    *,
    warning_key: str,
    reason: str,
    send_text: SendTextFn,
) -> bool:
    if not record_store.should_send_warning(route.chat_id, warning_key):
        return False
    safe_send_text_message(route, text, reason=reason, send_text=send_text)
    return True


def maybe_send_sheet_backlog_notice(route: MessageRoute, *, send_text: SendTextFn) -> None:
    try:
        backlog = google_sheets.queue_status().get("pending_sheet_appends", 0)
    except Exception as exc:
        logger.warning("Could not inspect Google Sheets backlog for chat_id=%s: %s", route.chat_id, exc)
        return

    if backlog < _BACKLOG_NOTICE_THRESHOLD:
        return

    send_throttled_warning(
        route,
        MSG_SHEET_BACKLOG_NOTICE,
        warning_key="sheet_backlog_notice",
        reason="sheet backlog notice",
        send_text=send_text,
    )
