"""
WhatsApp Cloud API client.

Handles media metadata retrieval and media download.
"""

from __future__ import annotations

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings
from app.utils.logging import get_logger

logger = get_logger(__name__)

_GRAPH_BASE = "https://graph.facebook.com/v19.0"


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.whatsapp_access_token}"}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def get_media_url(media_id: str) -> str:
    """Resolve a WhatsApp media ID to a download URL."""
    url = f"{_GRAPH_BASE}/{media_id}"
    with httpx.Client(timeout=15) as client:
        resp = client.get(url, headers=_auth_headers())
        resp.raise_for_status()
        data = resp.json()
        download_url: str = data["url"]
        logger.debug("Resolved media %s -> %s", media_id, download_url)
        return download_url


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def download_media(download_url: str) -> bytes:
    """Download raw bytes from a WhatsApp media URL."""
    with httpx.Client(timeout=60) as client:
        resp = client.get(download_url, headers=_auth_headers())
        resp.raise_for_status()
        logger.debug("Downloaded %d bytes from %s", len(resp.content), download_url)
        return resp.content


def fetch_media(media_id: str) -> bytes:
    """Convenience wrapper: resolve ID → download → return bytes."""
    url = get_media_url(media_id)
    return download_media(url)


def send_text_message(to: str, text: str) -> None:
    """Send a plain-text WhatsApp message to *to*."""
    if not settings.whatsapp_access_token or not settings.whatsapp_phone_number_id:
        logger.warning("WhatsApp credentials not configured; skipping outbound message.")
        return

    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text},
    }
    url = f"{_GRAPH_BASE}/{settings.whatsapp_phone_number_id}/messages"
    with httpx.Client(timeout=15) as client:
        resp = client.post(url, json=payload, headers=_auth_headers())
        if resp.is_error:
            logger.error("Failed to send message to %s: %s", to, resp.text)
        else:
            logger.debug("Sent message to %s", to)
