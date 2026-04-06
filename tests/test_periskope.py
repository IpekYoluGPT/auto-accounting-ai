"""
Integration tests for Periskope webhook and tool routes.
"""

from __future__ import annotations

import csv
import hashlib
import hmac
import json
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from app.main import app
from app.models.schemas import BillRecord, ClassificationResult
from app.services import periskope as periskope_service


def _periskope_event(data: dict, *, event: str = "message.created") -> dict:
    return {
        "event": event,
        "org_id": "org-1",
        "timestamp": "2026-04-06T10:00:00Z",
        "data": data,
    }


def _periskope_event_type_payload(data: dict, *, event_type: str = "message.created") -> dict:
    return {
        "event_type": event_type,
        "org_id": "org-1",
        "current_attributes": data,
        "previous_attributes": {},
        "timestamp": "2026-04-06T10:00:00Z",
    }


def _periskope_image_message(
    *,
    message_id: str = "peri-msg-1",
    chat_id: str = "120363410789660631@g.us",
    sender_phone: str = "905456952965@c.us",
    from_me: bool = False,
) -> dict:
    return {
        "message_id": message_id,
        "org_id": "org-1",
        "org_phone": "905516419175@c.us",
        "chat_id": chat_id,
        "message_type": "image",
        "body": "",
        "from_me": from_me,
        "has_media": True,
        "sender_phone": sender_phone,
        "author": sender_phone,
        "media": {
            "path": "/storage/v1/object/public/message-media/org-1/group/receipt-1",
            "filename": "receipt-1.jpg",
            "mimetype": "image/jpeg",
        },
    }


def _sign_payload(payload: dict, secret: str) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    digest = hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256)
    return digest.hexdigest()


def _read_export_rows(storage_dir: str) -> list[dict[str, str]]:
    export_dir = Path(storage_dir) / "exports"
    if not export_dir.exists():
        return []
    export_files = list(export_dir.glob("records_*.csv"))
    if not export_files:
        return []
    with export_files[0].open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


@contextmanager
def _patch_runtime_settings(
    tmpdir: str,
    *,
    signing_key: str = "periskope-secret",
    groups_only: bool = True,
    tool_token: str = "tool-secret",
):
    with patch("app.routes.periskope.settings.periskope_signing_key", signing_key), patch(
        "app.routes.periskope.settings.periskope_tool_token", tool_token
    ), patch("app.routes.periskope.settings.storage_dir", tmpdir), patch(
        "app.services.record_store.settings.storage_dir", tmpdir
    ), patch("app.services.intake.settings.whatsapp_groups_only", groups_only):
        yield


def test_periskope_group_image_webhook_exports_and_replies():
    payload = _periskope_event(_periskope_image_message())
    signature = _sign_payload(payload, "periskope-secret")
    record = BillRecord(
        company_name="ABC Market",
        total_amount=245.5,
        currency="TRY",
        source_message_id="peri-msg-1",
        source_filename="receipt-1.jpg",
        source_type="image",
        source_sender_id="905456952965@c.us",
        source_group_id="120363410789660631@g.us",
        source_chat_type="group",
        confidence=0.93,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.periskope.periskope.fetch_media",
            return_value=b"fake-image",
        ), patch(
            "app.services.intake.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.96),
        ), patch(
            "app.services.intake.gemini_extractor.extract_bill",
            return_value=record,
        ), patch(
            "app.routes.periskope.periskope.send_text_message",
        ) as send_mock:
            response = client.post(
                "/integrations/periskope/webhook",
                json=payload,
                headers={"x-periskope-signature": signature},
            )

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        assert rows[0]["Kaynak Mesaj ID"] == "peri-msg-1"
        assert rows[0]["Kaynak Gönderen ID"] == "905456952965@c.us"
        assert rows[0]["Kaynak Grup ID"] == "120363410789660631@g.us"
        assert send_mock.call_count == 2
        assert send_mock.call_args_list[0].args[0] == "120363410789660631@g.us"
        assert send_mock.call_args_list[0].kwargs["reply_to"] == "peri-msg-1"


def test_periskope_webhook_rejects_invalid_signature():
    payload = _periskope_event(_periskope_image_message())
    client = TestClient(app)
    with _patch_runtime_settings("/tmp/unused"):
        response = client.post(
            "/integrations/periskope/webhook",
            json=payload,
            headers={"x-periskope-signature": "wrong"},
        )

    assert response.status_code == 401


def test_periskope_webhook_ignores_self_messages():
    payload = _periskope_event(_periskope_image_message(from_me=True))
    signature = _sign_payload(payload, "periskope-secret")
    client = TestClient(app)
    with _patch_runtime_settings("/tmp/unused"), patch(
        "app.routes.periskope.periskope.fetch_media"
    ) as fetch_mock:
        response = client.post(
            "/integrations/periskope/webhook",
            json=payload,
            headers={"x-periskope-signature": signature},
        )

    assert response.status_code == 200
    assert response.json() == {"status": "ignored"}
    fetch_mock.assert_not_called()


def test_periskope_webhook_accepts_event_type_with_current_attributes():
    payload = _periskope_event_type_payload(_periskope_image_message(message_id="peri-msg-2"))
    signature = _sign_payload(payload, "periskope-secret")
    record = BillRecord(
        company_name="ABC Market",
        total_amount=125.0,
        currency="TRY",
        source_message_id="peri-msg-2",
        source_filename="receipt-1.jpg",
        source_type="image",
        source_sender_id="905456952965@c.us",
        source_group_id="120363410789660631@g.us",
        source_chat_type="group",
        confidence=0.91,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.periskope.periskope.fetch_media",
            return_value=b"fake-image",
        ), patch(
            "app.services.intake.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.96),
        ), patch(
            "app.services.intake.gemini_extractor.extract_bill",
            return_value=record,
        ), patch(
            "app.routes.periskope.periskope.send_text_message",
        ) as send_mock:
            response = client.post(
                "/integrations/periskope/webhook",
                json=payload,
                headers={"x-periskope-signature": signature},
            )

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        assert rows[0]["Kaynak Mesaj ID"] == "peri-msg-2"
        assert send_mock.call_count == 2


def test_periskope_webhook_accepts_null_has_media():
    payload = _periskope_event_type_payload(
        _periskope_image_message(message_id="peri-msg-3") | {"has_media": None}
    )
    signature = _sign_payload(payload, "periskope-secret")
    record = BillRecord(
        company_name="ABC Market",
        total_amount=88.0,
        currency="TRY",
        source_message_id="peri-msg-3",
        source_filename="receipt-1.jpg",
        source_type="image",
        source_sender_id="905456952965@c.us",
        source_group_id="120363410789660631@g.us",
        source_chat_type="group",
        confidence=0.89,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.periskope.periskope.fetch_media",
            return_value=b"fake-image",
        ), patch(
            "app.services.intake.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
        ), patch(
            "app.services.intake.gemini_extractor.extract_bill",
            return_value=record,
        ), patch(
            "app.routes.periskope.periskope.send_text_message",
        ) as send_mock:
            response = client.post(
                "/integrations/periskope/webhook",
                json=payload,
                headers={"x-periskope-signature": signature},
            )

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        assert rows[0]["Kaynak Mesaj ID"] == "peri-msg-3"
        assert send_mock.call_count == 2


def test_fetch_media_falls_back_to_canonical_message_path_after_401():
    request = httpx.Request("GET", "https://storage.googleapis.com/private-object")
    response = httpx.Response(401, request=request)
    attempts: list[str] = []

    def _download(path: str) -> bytes:
        attempts.append(path)
        if path == "https://storage.googleapis.com/private-object":
            raise httpx.HTTPStatusError("unauthorized", request=request, response=response)
        if path == "/storage/v1/object/public/message-media/org-1/group/receipt-1":
            return b"image-bytes"
        raise AssertionError(f"Unexpected path {path}")

    with patch("app.services.periskope._download_media", side_effect=_download), patch(
        "app.services.periskope.get_message",
        return_value={
            "message_id": "peri-msg-4",
            "media": {"path": "/storage/v1/object/public/message-media/org-1/group/receipt-1"},
        },
    ):
        raw = periskope_service.fetch_media(
            "https://storage.googleapis.com/private-object",
            message_id="peri-msg-4",
        )

    assert raw == b"image-bytes"
    assert attempts == [
        "https://storage.googleapis.com/private-object",
        "/storage/v1/object/public/message-media/org-1/group/receipt-1",
    ]


def test_create_accounting_record_tool_persists_manual_record():
    client = TestClient(app)
    with TemporaryDirectory() as tmpdir:
        with _patch_runtime_settings(tmpdir):
            response = client.post(
                "/integrations/periskope/tools/create_accounting_record",
                json={
                    "chat_id": "120363410789660631@g.us",
                    "company_name": "Cafe Test",
                    "document_date": "2026-04-06",
                    "total_amount": 180.0,
                    "currency": "TRY",
                    "description": "Kahve toplantısı",
                },
                headers={"Authorization": "Bearer tool-secret"},
            )

        assert response.status_code == 200
        assert response.json()["status"] == "recorded"
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        assert rows[0]["Firma Adı"] == "Cafe Test"
        assert rows[0]["Kaynak Grup ID"] == "120363410789660631@g.us"
        assert rows[0]["Sohbet Türü"] == "group"


def test_get_submission_status_tool_returns_matching_rows():
    client = TestClient(app)
    with TemporaryDirectory() as tmpdir:
        with _patch_runtime_settings(tmpdir):
            client.post(
                "/integrations/periskope/tools/create_accounting_record",
                json={
                    "chat_id": "905456952965@c.us",
                    "source_message_id": "peri-status-1",
                    "company_name": "Market",
                    "total_amount": 99.9,
                },
                headers={"Authorization": "Bearer tool-secret"},
            )
            response = client.post(
                "/integrations/periskope/tools/get_submission_status",
                json={"source_message_id": "peri-status-1"},
                headers={"Authorization": "Bearer tool-secret"},
            )

        assert response.status_code == 200
        body = response.json()
        assert body["found"] is True
        assert body["match_count"] == 1
        assert body["rows"][0]["Kaynak Mesaj ID"] == "peri-status-1"


def test_assign_to_human_tool_creates_private_note():
    client = TestClient(app)
    with _patch_runtime_settings("/tmp/unused"), patch(
        "app.routes.periskope.periskope.send_private_note",
        return_value={"success": True, "message": "queued"},
    ) as note_mock:
        response = client.post(
            "/integrations/periskope/tools/assign_to_human",
            json={
                "chat_id": "120363410789660631@g.us",
                "message": "Lutfen muhasebeci kontrol etsin.",
                "reply_to": "peri-msg-1",
            },
            headers={"Authorization": "Bearer tool-secret"},
        )

    assert response.status_code == 200
    note_mock.assert_called_once_with(
        "120363410789660631@g.us",
        "Lutfen muhasebeci kontrol etsin.",
        reply_to="peri-msg-1",
    )
