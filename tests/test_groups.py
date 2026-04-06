"""
Tests for the official WhatsApp groups management endpoints.
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app


def test_onboard_group_creates_group_and_fetches_invite_link():
    client = TestClient(app)
    with patch(
        "app.routes.groups.whatsapp.create_group",
        return_value={"id": "group-123", "messaging_product": "whatsapp"},
    ) as create_mock, patch(
        "app.routes.groups.whatsapp.get_group_info",
        return_value={"id": "group-123", "subject": "Acme Muhasebe", "join_approval_mode": "auto_approve"},
    ) as info_mock, patch(
        "app.routes.groups.whatsapp.get_group_invite_link",
        return_value={"invite_link": "https://chat.whatsapp.com/abc123"},
    ) as invite_mock:
        response = client.post(
            "/groups/onboard",
            json={
                "subject": "Acme Muhasebe",
                "description": "Sadece fatura ve fis",
                "join_approval_mode": "auto_approve",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["group_id"] == "group-123"
    assert body["invite_link"] == "https://chat.whatsapp.com/abc123"
    create_mock.assert_called_once_with(
        subject="Acme Muhasebe",
        description="Sadece fatura ve fis",
        join_approval_mode="auto_approve",
    )
    info_mock.assert_called_once()
    invite_mock.assert_called_once_with("group-123")


def test_onboard_group_returns_partial_response_when_invite_link_lookup_fails():
    client = TestClient(app)
    with patch(
        "app.routes.groups.whatsapp.create_group",
        return_value={"group_id": "group-456"},
    ), patch(
        "app.routes.groups.whatsapp.get_group_info",
        return_value={"id": "group-456", "subject": "Beta Group"},
    ), patch(
        "app.routes.groups.whatsapp.get_group_invite_link",
        side_effect=RuntimeError("invite lookup failed"),
    ):
        response = client.post(
            "/groups/onboard",
            json={"subject": "Beta Group", "join_approval_mode": "approval_required"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["group_id"] == "group-456"
    assert body["invite_link"] is None
    assert body["invite_link_response"] is None


def test_approve_group_join_requests_passes_ids_to_service():
    client = TestClient(app)
    with patch(
        "app.routes.groups.whatsapp.approve_group_join_requests",
        return_value={"data": [{"id": "jr-1"}]},
    ) as approve_mock:
        response = client.post(
            "/groups/group-123/join-requests/approve",
            json={"join_request_ids": ["jr-1", "jr-2"]},
        )

    assert response.status_code == 200
    assert response.json() == {"data": [{"id": "jr-1"}]}
    approve_mock.assert_called_once_with("group-123", join_request_ids=["jr-1", "jr-2"])
