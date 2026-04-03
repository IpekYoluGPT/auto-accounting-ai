"""
Integration tests for the webhook route.
"""

from __future__ import annotations

import csv
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app
from app.models.schemas import BillRecord, ClassificationResult


def _image_payload(message_id: str = "wamid-1") -> dict:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "entry-1",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "messaging_product": "whatsapp",
                            "messages": [
                                {
                                    "id": message_id,
                                    "from": "905551112233",
                                    "timestamp": "1710000000",
                                    "type": "image",
                                    "image": {"id": "media-1", "mime_type": "image/jpeg"},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def _document_payload(message_id: str = "wamid-doc-1") -> dict:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "entry-1",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "messaging_product": "whatsapp",
                            "messages": [
                                {
                                    "id": message_id,
                                    "from": "905551112233",
                                    "timestamp": "1710000000",
                                    "type": "document",
                                    "document": {"id": "media-doc-1"},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def _text_payload(text: str, message_id: str = "wamid-text-1") -> dict:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "entry-1",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "messaging_product": "whatsapp",
                            "messages": [
                                {
                                    "id": message_id,
                                    "from": "905551112233",
                                    "timestamp": "1710000000",
                                    "type": "text",
                                    "text": {"body": text},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def _read_export_rows(storage_dir: str) -> list[dict[str, str]]:
    export_dir = Path(storage_dir) / "exports"
    if not export_dir.exists():
        return []
    export_files = list(export_dir.glob("records_*.csv"))
    if not export_files:
        return []
    with export_files[0].open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_verify_webhook_success_returns_challenge():
    client = TestClient(app)
    with patch("app.routes.webhooks.settings.whatsapp_verify_token", "verify-token"):
        response = client.get(
            "/webhook",
            params={
                "hub.mode": "subscribe",
                "hub.verify_token": "verify-token",
                "hub.challenge": "challenge-123",
            },
        )

    assert response.status_code == 200
    assert response.text == "challenge-123"
    assert response.headers["content-type"].startswith("text/plain")


def test_verify_webhook_invalid_token_returns_403():
    client = TestClient(app)
    with patch("app.routes.webhooks.settings.whatsapp_verify_token", "verify-token"):
        response = client.get(
            "/webhook",
            params={
                "hub.mode": "subscribe",
                "hub.verify_token": "wrong-token",
                "hub.challenge": "challenge-123",
            },
        )

    assert response.status_code == 403


def test_happy_path_image_webhook_writes_csv_and_replies():
    record = BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-1",
        source_filename="media-1.jpg",
        source_type="image",
        confidence=0.91,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
            "app.services.record_store.settings.storage_dir", tmpdir
        ), patch("app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"), patch(
            "app.routes.webhooks.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
        ), patch(
            "app.routes.webhooks.gemini_extractor.extract_bill",
            return_value=record,
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock:
            response = client.post("/webhook", json=_image_payload())

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        assert rows[0]["Genel Toplam"] == "100.0"
        assert send_mock.call_count == 2
        assert "s\u00fcrebilir" in send_mock.call_args_list[0].args[1].lower()
        assert "muhasebe kayd\u0131na eklendi" in send_mock.call_args_list[1].args[1].lower()


def test_bill_like_text_prompts_for_photo():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
            "app.services.record_store.settings.storage_dir", tmpdir
        ), patch(
            "app.routes.webhooks.bill_classifier.classify_text",
            return_value=ClassificationResult(is_bill=True, reason="bill text", confidence=0.81),
        ), patch("app.routes.webhooks.whatsapp.send_text_message") as send_mock:
            response = client.post("/webhook", json=_text_payload("Toplam 150 TL, KDV 27 TL"))

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    send_mock.assert_called_once()
    assert "foto" in send_mock.call_args.args[1].lower()


def test_non_bill_text_is_ignored_without_reply():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
            "app.services.record_store.settings.storage_dir", tmpdir
        ), patch(
            "app.routes.webhooks.bill_classifier.classify_text",
            return_value=ClassificationResult(is_bill=False, reason="chat", confidence=0.92),
        ), patch("app.routes.webhooks.whatsapp.send_text_message") as send_mock:
            response = client.post("/webhook", json=_text_payload("Merhaba nasilsin"))

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    send_mock.assert_not_called()


def test_text_classification_failure_sends_error_message():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
            "app.services.record_store.settings.storage_dir", tmpdir
        ), patch(
            "app.routes.webhooks.bill_classifier.classify_text",
            side_effect=RuntimeError("classifier down"),
        ), patch("app.routes.webhooks.whatsapp.send_text_message") as send_mock:
            response = client.post("/webhook", json=_text_payload("Toplam 150 TL"))

    assert response.status_code == 200
    send_mock.assert_called_once()
    assert "hata" in send_mock.call_args.args[1].lower()


def test_malformed_json_returns_ignored():
    client = TestClient(app, raise_server_exceptions=False)
    response = client.post(
        "/webhook",
        content="not-json",
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 200
    assert response.json() == {"status": "ignored"}


def test_duplicate_delivery_writes_only_one_export_row():
    record = BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-1",
        source_filename="media-1.jpg",
        source_type="image",
        confidence=0.91,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
            "app.services.record_store.settings.storage_dir", tmpdir
        ), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ) as fetch_mock, patch(
            "app.routes.webhooks.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
        ) as classify_mock, patch(
            "app.routes.webhooks.gemini_extractor.extract_bill",
            return_value=record,
        ) as extract_mock, patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock:
            response_one = client.post("/webhook", json=_image_payload())
            response_two = client.post("/webhook", json=_image_payload())

        assert response_one.status_code == 200
        assert response_two.status_code == 200
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        assert send_mock.call_count == 2
        fetch_mock.assert_called_once()
        classify_mock.assert_called_once()
        extract_mock.assert_called_once()


def test_classification_failure_sends_error_and_writes_no_row():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
            "app.services.record_store.settings.storage_dir", tmpdir
        ), patch("app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"), patch(
            "app.routes.webhooks.bill_classifier.classify_image",
            side_effect=RuntimeError("classifier down"),
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock:
            response = client.post("/webhook", json=_image_payload())

        assert response.status_code == 200
        assert _read_export_rows(tmpdir) == []
        assert send_mock.call_count == 2
        assert "s\u00fcrebilir" in send_mock.call_args_list[0].args[1].lower()
        assert "hata" in send_mock.call_args_list[1].args[1].lower()


def test_extraction_failure_sends_error_and_writes_no_row():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
            "app.services.record_store.settings.storage_dir", tmpdir
        ), patch("app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"), patch(
            "app.routes.webhooks.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
        ), patch(
            "app.routes.webhooks.gemini_extractor.extract_bill",
            side_effect=RuntimeError("extractor down"),
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock:
            response = client.post("/webhook", json=_image_payload())

        assert response.status_code == 200
        assert _read_export_rows(tmpdir) == []
        assert send_mock.call_count == 2
        assert "s\u00fcrebilir" in send_mock.call_args_list[0].args[1].lower()
        assert "hata" in send_mock.call_args_list[1].args[1].lower()


def test_non_bill_image_is_skipped_without_export():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
            "app.services.record_store.settings.storage_dir", tmpdir
        ), patch("app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"), patch(
            "app.routes.webhooks.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=False, reason="meme", confidence=0.97),
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock:
            response = client.post("/webhook", json=_image_payload())

        assert response.status_code == 200
        assert _read_export_rows(tmpdir) == []
        send_mock.assert_called_once()
        assert "s\u00fcrebilir" in send_mock.call_args.args[1].lower()


def test_document_webhook_defaults_pdf_metadata():
    record = BillRecord(
        company_name="PDF Supplier",
        total_amount=240.0,
        currency="TRY",
        source_message_id="wamid-doc-1",
        source_filename="media-doc-1.pdf",
        source_type="document",
        confidence=0.89,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
            "app.services.record_store.settings.storage_dir", tmpdir
        ), patch("app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-pdf"), patch(
            "app.routes.webhooks.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="document", confidence=0.94),
        ) as classify_mock, patch(
            "app.routes.webhooks.gemini_extractor.extract_bill",
            return_value=record,
        ) as extract_mock, patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock:
            response = client.post("/webhook", json=_document_payload())

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        assert len(_read_export_rows(tmpdir)) == 1
        classify_mock.assert_called_once_with(b"fake-pdf", mime_type="application/pdf")
        extract_mock.assert_called_once_with(
            image_bytes=b"fake-pdf",
            mime_type="application/pdf",
            source_message_id="wamid-doc-1",
            source_filename="media-doc-1.pdf",
            source_type="document",
        )
        assert send_mock.call_count == 2
        assert "s\u00fcrebilir" in send_mock.call_args_list[0].args[1].lower()


def test_send_failure_does_not_abort_export():
    record = BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-1",
        source_filename="media-1.jpg",
        source_type="image",
        confidence=0.91,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
            "app.services.record_store.settings.storage_dir", tmpdir
        ), patch("app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"), patch(
            "app.routes.webhooks.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
        ), patch(
            "app.routes.webhooks.gemini_extractor.extract_bill",
            return_value=record,
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message",
            side_effect=RuntimeError("send failed"),
        ) as send_mock:
            response = client.post("/webhook", json=_image_payload())

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        assert len(_read_export_rows(tmpdir)) == 1
        assert send_mock.call_count == 2


def test_failed_attempt_releases_claim_and_allows_retry():
    record = BillRecord(
        company_name="Retry Market",
        total_amount=55.0,
        currency="TRY",
        source_message_id="wamid-retry-1",
        source_filename="media-1.jpg",
        source_type="image",
        confidence=0.9,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
            "app.services.record_store.settings.storage_dir", tmpdir
        ), patch("app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"), patch(
            "app.routes.webhooks.bill_classifier.classify_image",
            side_effect=[
                RuntimeError("classifier down"),
                ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
            ],
        ), patch(
            "app.routes.webhooks.gemini_extractor.extract_bill",
            return_value=record,
        ), patch("app.routes.webhooks.whatsapp.send_text_message") as send_mock:
            response_one = client.post("/webhook", json=_image_payload("wamid-retry-1"))
            response_two = client.post("/webhook", json=_image_payload("wamid-retry-1"))

        assert response_one.status_code == 200
        assert response_two.status_code == 200
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        assert send_mock.call_count == 4
