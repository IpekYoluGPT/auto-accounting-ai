"""
Official WhatsApp group onboarding and management endpoints.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from app.models.schemas import GroupJoinRequestDecisionRequest, GroupOnboardingRequest
from app.services.providers import whatsapp
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/groups", tags=["groups"])

_DEFAULT_GROUP_FIELDS = (
    "subject,description,participants,join_approval_mode,"
    "total_participant_count,suspended,creation_timestamp"
)


def _find_first_string(payload: Any, *keys: str) -> str | None:
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
        for value in payload.values():
            found = _find_first_string(value, *keys)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_first_string(item, *keys)
            if found:
                return found
    return None


@router.get("")
async def get_active_groups(
    limit: int = Query(25, ge=1, le=1024),
    before: str | None = None,
    after: str | None = None,
) -> dict[str, Any]:
    """List active official WhatsApp groups."""
    return whatsapp.list_groups(limit=limit, before=before, after=after)


@router.post("/onboard")
async def onboard_group(payload: GroupOnboardingRequest) -> dict[str, Any]:
    """
    Create a new official WhatsApp group and return the most useful metadata we
    can fetch synchronously for onboarding.
    """
    create_response = whatsapp.create_group(
        subject=payload.subject,
        description=payload.description,
        join_approval_mode=payload.join_approval_mode,
    )
    group_id = _find_first_string(create_response, "group_id", "id")

    result: dict[str, Any] = {
        "group_id": group_id,
        "subject": payload.subject,
        "description": payload.description,
        "join_approval_mode": payload.join_approval_mode,
        "invite_link": None,
        "create_response": create_response,
    }

    if not group_id:
        logger.warning("Group create response did not contain an obvious group ID.")
        return result

    try:
        result["group"] = whatsapp.get_group_info(group_id, fields=_DEFAULT_GROUP_FIELDS)
    except Exception as exc:
        logger.warning("Failed to fetch group info for %s after create: %s", group_id, exc)
        result["group"] = None

    try:
        invite_payload = whatsapp.get_group_invite_link(group_id)
        result["invite_link"] = _find_first_string(invite_payload, "invite_link", "link")
        result["invite_link_response"] = invite_payload
    except Exception as exc:
        logger.warning("Failed to fetch invite link for group %s: %s", group_id, exc)
        result["invite_link_response"] = None

    return result


@router.get("/{group_id}")
async def get_group(group_id: str, fields: str | None = Query(_DEFAULT_GROUP_FIELDS)) -> dict[str, Any]:
    """Fetch metadata for a single official WhatsApp group."""
    return whatsapp.get_group_info(group_id, fields=fields)


@router.get("/{group_id}/invite-link")
async def get_group_invite_link(group_id: str) -> dict[str, Any]:
    """Return the current invite link for a group."""
    return whatsapp.get_group_invite_link(group_id)


@router.post("/{group_id}/invite-link/reset")
async def reset_group_invite_link(group_id: str) -> dict[str, Any]:
    """Reset and return the invite link for a group."""
    return whatsapp.reset_group_invite_link(group_id)


@router.get("/{group_id}/join-requests")
async def get_group_join_requests(
    group_id: str, before: str | None = None, after: str | None = None
) -> dict[str, Any]:
    """List open join requests for a group."""
    return whatsapp.list_group_join_requests(group_id, before=before, after=after)


@router.post("/{group_id}/join-requests/approve")
async def approve_group_join_requests(
    group_id: str, payload: GroupJoinRequestDecisionRequest
) -> dict[str, Any]:
    """Approve one or more open join requests."""
    return whatsapp.approve_group_join_requests(
        group_id,
        join_request_ids=payload.join_request_ids,
    )
