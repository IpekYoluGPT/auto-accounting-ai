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
from app.services.accounting import inbound_queue, ingress_service
from app.services.providers import whatsapp
from app.services.accounting.intake import MessageRoute
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/webhook", tags=["webhook"])


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
            contact_names = {
                contact.wa_id: (contact.profile or {}).get("name")
                for contact in (change.value.contacts or [])
                if contact.wa_id
            }
            messages = change.value.messages or []
            for message in messages:
                if message.type in {"image", "document"}:
                    _process_message(message, contact_names.get(message.from_))
                else:
                    background_tasks.add_task(_process_message, message, contact_names.get(message.from_))

    return {"status": "ok"}


def _process_message(message, sender_name: str | None = None) -> None:
    """Process a single WhatsApp message in the background."""
    route = _resolve_message_route(message, sender_name=sender_name)
    ingress_service.process_or_enqueue_message(
        message_id=message.id,
        msg_type=message.type,
        route=route,
        send_text=_send_meta_text_message,
        send_reaction=_send_meta_reaction,
        text=message.text.body if message.text else "",
        mime_type=(
            message.image.mime_type if message.type == "image" and message.image else
            message.document.mime_type if message.type == "document" and message.document else
            None
        ) or ("image/jpeg" if message.type == "image" else "application/pdf" if message.type == "document" else None),
        filename=(
            f"{message.image.id}.jpg" if message.type == "image" and message.image else
            message.document.filename or f"{message.document.id}.pdf" if message.type == "document" and message.document else
            None
        ),
        source_type=message.type if message.type in {"image", "document"} else None,
        media_id=(
            message.image.id if message.type == "image" and message.image else
            message.document.id if message.type == "document" and message.document else
            None
        ),
    )


def _resolve_message_route(message, sender_name: str | None = None) -> MessageRoute:
    """Map an inbound message to the outbound chat target and export metadata."""
    group_id = (message.group_id or "").strip() or None
    if group_id:
        return MessageRoute(
            platform="meta_whatsapp",
            sender_id=message.from_,
            chat_id=group_id,
            chat_type="group",
            recipient_type="group",
            sender_name=sender_name,
            group_id=group_id,
            reply_to_message_id=message.id,
        )

    return MessageRoute(
        platform="meta_whatsapp",
        sender_id=message.from_,
        chat_id=message.from_,
        chat_type="individual",
        recipient_type="individual",
        sender_name=sender_name,
        reply_to_message_id=message.id,
    )

def _send_meta_text_message(route: MessageRoute, text: str) -> None:
    whatsapp.send_text_message(
        route.chat_id,
        text,
        recipient_type=route.recipient_type,
        reply_to_message_id=route.reply_to_message_id,
    )


def _send_meta_reaction(route: MessageRoute, emoji: str) -> None:
    if not route.reply_to_message_id:
        return
    whatsapp.send_reaction_message(
        route.chat_id,
        route.reply_to_message_id,
        emoji,
        recipient_type=route.recipient_type,
    )


def _safe_meta_text_message(route: MessageRoute, text: str) -> None:
    try:
        _send_meta_text_message(route, text)
    except Exception as exc:
        logger.error("Failed to send queued Meta text to %s: %s", route.chat_id, exc, exc_info=True)


def _safe_meta_reaction(route: MessageRoute, emoji: str) -> None:
    try:
        _send_meta_reaction(route, emoji)
    except Exception as exc:
        logger.error("Failed to send queued Meta reaction to %s: %s", route.chat_id, exc, exc_info=True)
