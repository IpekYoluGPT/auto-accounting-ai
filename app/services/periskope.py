"""
Periskope API client helpers for webhook-driven integrations and tool actions.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings
from app.utils.logging import get_logger

logger = get_logger(__name__)


def _require_periskope_credentials() -> None:
    if not settings.periskope_api_key or not settings.periskope_phone:
        raise RuntimeError("Periskope credentials are not configured.")


def _api_base_url() -> str:
    return settings.periskope_api_base_url.rstrip("/")


def _media_base_url() -> str:
    base = settings.periskope_media_base_url.strip()
    if base:
        return base.rstrip("/")

    api_base = _api_base_url()
    if api_base.endswith("/v1"):
        return api_base[:-3]
    return api_base


def _auth_headers() -> dict[str, str]:
    _require_periskope_credentials()
    return {
        "Authorization": f"Bearer {settings.periskope_api_key}",
        "x-phone": settings.periskope_phone,
    }


def _absolute_media_url(media_path: str) -> str:
    if media_path.startswith("http://") or media_path.startswith("https://"):
        return media_path
    return urljoin(f"{_media_base_url()}/", media_path.lstrip("/"))


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def fetch_media(media_path: str) -> bytes:
    """Download bytes from a Periskope media path or absolute media URL."""
    url = _absolute_media_url(media_path)
    with httpx.Client(timeout=60) as client:
        resp = client.get(url, headers=_auth_headers())
        resp.raise_for_status()
        logger.debug("Downloaded %d bytes from Periskope media %s", len(resp.content), url)
        return resp.content


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def send_text_message(chat_id: str, text: str, *, reply_to: str | None = None) -> dict[str, Any]:
    """Queue a text reply through Periskope."""
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message": text,
    }
    if reply_to:
        payload["reply_to"] = reply_to

    with httpx.Client(timeout=20) as client:
        resp = client.post(f"{_api_base_url()}/message/send", json=payload, headers=_auth_headers())
        resp.raise_for_status()
        return resp.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def send_private_note(chat_id: str, message: str, *, reply_to: str | None = None) -> dict[str, Any]:
    """Create a private note in a Periskope chat for human follow-up."""
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message": message,
    }
    if reply_to:
        payload["reply_to"] = reply_to

    with httpx.Client(timeout=20) as client:
        resp = client.post(f"{_api_base_url()}/note/create", json=payload, headers=_auth_headers())
        resp.raise_for_status()
        return resp.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def get_message(message_id: str) -> dict[str, Any]:
    """Fetch one message object from Periskope by message or queue ID."""
    with httpx.Client(timeout=20) as client:
        resp = client.get(f"{_api_base_url()}/message/{message_id}", headers=_auth_headers())
        resp.raise_for_status()
        return resp.json()
