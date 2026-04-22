"""
Provider-neutral ingress orchestration for direct text processing and media queueing.
"""

from __future__ import annotations

from app.services.accounting import inbound_queue, intake
from app.services.accounting.intake_types import MessageRoute, SendReactionFn, SendTextFn


def process_or_enqueue_message(
    *,
    message_id: str,
    msg_type: str,
    route: MessageRoute,
    send_text: SendTextFn,
    send_reaction: SendReactionFn | None = None,
    text: str | None = None,
    mime_type: str | None = None,
    filename: str | None = None,
    source_type: str | None = None,
    media_id: str | None = None,
    media_path: str | None = None,
    attachment_url: str | None = None,
) -> None:
    if msg_type == "text":
        intake.process_incoming_message(
            message_id=message_id,
            msg_type="text",
            route=route,
            send_text=send_text,
            text=text or "",
        )
        return

    if msg_type in {"image", "document"} and mime_type and filename and source_type:
        result = inbound_queue.enqueue_media_job(
            message_id=message_id,
            msg_type=msg_type,
            route=route,
            mime_type=mime_type,
            filename=filename,
            source_type=source_type,
            media_id=media_id,
            media_path=media_path,
            attachment_url=attachment_url,
        )
        if result.status == "enqueued":
            intake._safe_send_reaction(
                route,
                intake.REACTION_PROCESSING,
                reason="queued processing reaction",
                send_reaction=send_reaction,
            )
        elif result.status == "rejected_due_to_storage":
            intake._safe_send_reaction(
                route,
                intake.REACTION_WARNING,
                reason="storage pressure reaction",
                send_reaction=send_reaction,
            )
            intake._safe_send_text_message(
                route,
                result.message or inbound_queue.MSG_STORAGE_PRESSURE,
                reason="storage pressure warning",
                send_text=send_text,
            )
        return

    intake.process_incoming_message(
        message_id=message_id,
        msg_type=msg_type,
        route=route,
        send_text=send_text,
    )
