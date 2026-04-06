"""
WhatsApp Cloud API client.

Handles media metadata retrieval, media download, and outbound messages.
"""

from __future__ import annotations

from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings
from app.utils.logging import get_logger

logger = get_logger(__name__)

_GRAPH_BASE = "https://graph.facebook.com/v19.0"


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.whatsapp_access_token}"}


def _require_whatsapp_credentials() -> None:
    if not settings.whatsapp_access_token or not settings.whatsapp_phone_number_id:
        raise RuntimeError("WhatsApp credentials are not configured.")


def _groups_collection_url() -> str:
    _require_whatsapp_credentials()
    return f"{_GRAPH_BASE}/{settings.whatsapp_phone_number_id}/groups"


def _group_object_url(group_id: str) -> str:
    _require_whatsapp_credentials()
    return f"{_GRAPH_BASE}/{group_id}"


def _group_subresource_url(group_id: str, resource: str) -> str:
    return f"{_group_object_url(group_id)}/{resource}"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
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


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
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


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def create_group(
    *,
    subject: str,
    description: str | None = None,
    join_approval_mode: str = "auto_approve",
) -> dict[str, Any]:
    """
    Create an official WhatsApp group.

    Endpoint shape is inferred from Meta Groups API conventions and matches the
    provider documentation used during implementation.
    """
    payload: dict[str, Any] = {
        "messaging_product": "whatsapp",
        "subject": subject,
        "join_approval_mode": join_approval_mode,
    }
    if description:
        payload["description"] = description

    with httpx.Client(timeout=20) as client:
        resp = client.post(_groups_collection_url(), json=payload, headers=_auth_headers())
        resp.raise_for_status()
        data = resp.json()
        logger.info("Created WhatsApp group subject=%s response=%s", subject, str(data)[:300])
        return data


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def list_groups(
    *, limit: int = 25, before: str | None = None, after: str | None = None
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": limit}
    if before:
        params["before"] = before
    if after:
        params["after"] = after

    with httpx.Client(timeout=20) as client:
        resp = client.get(_groups_collection_url(), params=params, headers=_auth_headers())
        resp.raise_for_status()
        return resp.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def get_group_info(group_id: str, *, fields: str | None = None) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if fields:
        params["fields"] = fields

    with httpx.Client(timeout=20) as client:
        resp = client.get(_group_object_url(group_id), params=params, headers=_auth_headers())
        resp.raise_for_status()
        return resp.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def get_group_invite_link(group_id: str) -> dict[str, Any]:
    with httpx.Client(timeout=20) as client:
        resp = client.get(_group_subresource_url(group_id, "invite_link"), headers=_auth_headers())
        resp.raise_for_status()
        return resp.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def reset_group_invite_link(group_id: str) -> dict[str, Any]:
    with httpx.Client(timeout=20) as client:
        resp = client.post(_group_subresource_url(group_id, "invite_link"), headers=_auth_headers())
        resp.raise_for_status()
        return resp.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def list_group_join_requests(
    group_id: str, *, before: str | None = None, after: str | None = None
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if before:
        params["before"] = before
    if after:
        params["after"] = after

    with httpx.Client(timeout=20) as client:
        resp = client.get(
            _group_subresource_url(group_id, "join_requests"),
            params=params,
            headers=_auth_headers(),
        )
        resp.raise_for_status()
        return resp.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def approve_group_join_requests(group_id: str, *, join_request_ids: list[str]) -> dict[str, Any]:
    payload = {"join_request_ids": join_request_ids}
    with httpx.Client(timeout=20) as client:
        resp = client.post(
            _group_subresource_url(group_id, "join_requests"),
            json=payload,
            headers=_auth_headers(),
        )
        resp.raise_for_status()
        return resp.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
def send_text_message(to: str, text: str, *, recipient_type: str = "individual") -> None:
    """Send a plain-text WhatsApp message to an individual chat or a group."""
    if not settings.whatsapp_access_token or not settings.whatsapp_phone_number_id:
        logger.warning("WhatsApp credentials not configured; skipping outbound message.")
        return

    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text},
    }
    if recipient_type != "individual":
        payload["recipient_type"] = recipient_type
    url = f"{_GRAPH_BASE}/{settings.whatsapp_phone_number_id}/messages"
    with httpx.Client(timeout=15) as client:
        resp = client.post(url, json=payload, headers=_auth_headers())
        resp.raise_for_status()
        logger.debug("Sent %s message to %s", recipient_type, to)
