"""
Integration tests for the webhook route.
"""

from __future__ import annotations

import csv
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app
from app.models.schemas import BillRecord, ClassificationResult, DocumentCategory


def _image_payload(
    message_id: str = "wamid-1",
    *,
    sender: str = "905551112233",
    group_id: str | None = None,
) -> dict:
    message = {
        "id": message_id,
        "from": sender,
        "timestamp": "1710000000",
        "type": "image",
        "image": {"id": "media-1", "mime_type": "image/jpeg"},
    }
    if group_id:
        message["group_id"] = group_id

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
                            "messages": [message],
                        },
                    }
                ],
            }
        ],
    }


def _document_payload(
    message_id: str = "wamid-doc-1",
    *,
    sender: str = "905551112233",
    group_id: str | None = None,
) -> dict:
    message = {
        "id": message_id,
        "from": sender,
        "timestamp": "1710000000",
        "type": "document",
        "document": {"id": "media-doc-1"},
    }
    if group_id:
        message["group_id"] = group_id

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
                            "messages": [message],
                        },
                    }
                ],
            }
        ],
    }


def _text_payload(
    text: str,
    message_id: str = "wamid-text-1",
    *,
    sender: str = "905551112233",
    group_id: str | None = None,
) -> dict:
    message = {
        "id": message_id,
        "from": sender,
        "timestamp": "1710000000",
        "type": "text",
        "text": {"body": text},
    }
    if group_id:
        message["group_id"] = group_id

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
                            "messages": [message],
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


@contextmanager
def _patch_runtime_settings(tmpdir: str, *, groups_only: bool = False):
    with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
        "app.routes.webhooks.settings.whatsapp_groups_only", groups_only
    ), patch("app.services.accounting.record_store.settings.storage_dir", tmpdir):
        yield


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


def test_happy_path_image_webhook_writes_csv_and_reacts():
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
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
        ), patch(
            "app.services.accounting.intake.doc_classifier.classify_document_type",
            return_value=(DocumentCategory.FATURA, False),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            return_value=[record],
        ), patch(
            "app.services.accounting.intake.google_sheets.upload_document",
            return_value="https://drive.google.com/file/d/test/view",
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response = client.post("/webhook", json=_image_payload())

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        assert rows[0]["Genel Toplam"] == "100.0"
        send_mock.assert_not_called()
        assert reaction_mock.call_count == 2
        assert reaction_mock.call_args_list[0].args == ("905551112233", "wamid-1", "⌛")
        assert reaction_mock.call_args_list[1].args == ("905551112233", "wamid-1", "✅")
        assert reaction_mock.call_args_list[0].kwargs["recipient_type"] == "individual"


def test_group_image_webhook_reacts_to_group_and_exports_group_metadata():
    record = BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-group-1",
        source_filename="media-1.jpg",
        source_type="image",
        source_sender_id="905551112233",
        source_group_id="group-123",
        source_chat_type="group",
        confidence=0.91,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir, groups_only=True), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
        ), patch(
            "app.services.accounting.intake.doc_classifier.classify_document_type",
            return_value=(DocumentCategory.FATURA, False),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            return_value=[record],
        ) as extract_mock, patch(
            "app.services.accounting.intake.google_sheets.upload_document",
            return_value="https://drive.google.com/file/d/test/view",
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response = client.post("/webhook", json=_image_payload("wamid-group-1", group_id="group-123"))

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        assert rows[0]["Kaynak Gönderen ID"] == "905551112233"
        assert rows[0]["Kaynak Grup ID"] == "group-123"
        assert rows[0]["Sohbet Türü"] == "group"
        extract_mock.assert_called_once_with(
            image_bytes=b"fake-image",
            mime_type="image/jpeg",
            source_message_id="wamid-group-1",
            source_filename="media-1.jpg",
            source_type="image",
            source_sender_id="905551112233",
            source_group_id="group-123",
            source_chat_type="group",
        )
        send_mock.assert_not_called()
        assert reaction_mock.call_count == 2
        assert reaction_mock.call_args_list[0].args == ("group-123", "wamid-group-1", "⌛")
        assert reaction_mock.call_args_list[1].args == ("group-123", "wamid-group-1", "✅")
        assert reaction_mock.call_args_list[0].kwargs["recipient_type"] == "group"


def test_bill_like_text_prompts_for_photo():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.services.accounting.intake.bill_classifier.classify_text",
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
        with _patch_runtime_settings(tmpdir), patch(
            "app.services.accounting.intake.bill_classifier.classify_text",
            return_value=ClassificationResult(is_bill=False, reason="chat", confidence=0.92),
        ), patch("app.routes.webhooks.whatsapp.send_text_message") as send_mock:
            response = client.post("/webhook", json=_text_payload("Merhaba nasilsin"))

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    send_mock.assert_called_once()
    assert "yaln\u0131zca fatura" in send_mock.call_args.args[1].lower()


def test_group_non_bill_text_is_ignored_without_reply():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir, groups_only=True), patch(
            "app.services.accounting.intake.bill_classifier.classify_text",
            return_value=ClassificationResult(is_bill=False, reason="chat", confidence=0.92),
        ), patch("app.routes.webhooks.whatsapp.send_text_message") as send_mock:
            response_one = client.post(
                "/webhook",
                json=_text_payload("Merhaba", "wamid-text-group-1", sender="905551112233", group_id="group-123"),
            )
            response_two = client.post(
                "/webhook",
                json=_text_payload("Selam", "wamid-text-group-2", sender="905559998877", group_id="group-123"),
            )

    assert response_one.status_code == 200
    assert response_two.status_code == 200
    send_mock.assert_not_called()


def test_direct_messages_are_blocked_when_groups_only_mode_is_enabled():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir, groups_only=True), patch(
            "app.services.accounting.intake.bill_classifier.classify_text"
        ) as classify_mock, patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock:
            response = client.post("/webhook", json=_text_payload("Toplam 150 TL", "wamid-direct-disabled-1"))

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    classify_mock.assert_not_called()
    send_mock.assert_called_once()
    assert "grup" in send_mock.call_args.args[1].lower()
    assert send_mock.call_args.kwargs["recipient_type"] == "individual"


def test_text_classification_failure_sends_error_message():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.services.accounting.intake.bill_classifier.classify_text",
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
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ) as fetch_mock, patch(
            "app.services.accounting.intake.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
        ), patch(
            "app.services.accounting.intake.doc_classifier.classify_document_type",
            return_value=(DocumentCategory.FATURA, False),
        ) as classify_mock, patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            return_value=[record],
        ) as extract_mock, patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.services.accounting.intake.google_sheets.upload_document",
            return_value="https://drive.google.com/file/d/test/view",
        ), patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response_one = client.post("/webhook", json=_image_payload())
            response_two = client.post("/webhook", json=_image_payload())

        assert response_one.status_code == 200
        assert response_two.status_code == 200
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        send_mock.assert_not_called()
        assert reaction_mock.call_count == 2
        fetch_mock.assert_called_once()
        classify_mock.assert_called_once()
        extract_mock.assert_called_once()


def test_classification_failure_sends_error_and_writes_no_row():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.bill_classifier.classify_image",
            side_effect=RuntimeError("classifier down"),
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response = client.post("/webhook", json=_image_payload())

        assert response.status_code == 200
        assert _read_export_rows(tmpdir) == []
        send_mock.assert_called_once()
        assert "doğrulanamadı" in send_mock.call_args.args[1].lower()
        assert send_mock.call_args.kwargs["reply_to_message_id"] == "wamid-1"
        assert reaction_mock.call_count == 2
        assert [call.args[2] for call in reaction_mock.call_args_list] == ["⌛", "⚠️"]


def test_classification_failure_503_sends_temporary_upstream_message():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.bill_classifier.classify_image",
            side_effect=RuntimeError("503 UNAVAILABLE"),
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response = client.post("/webhook", json=_image_payload("wamid-503-1"))

        assert response.status_code == 200
        assert _read_export_rows(tmpdir) == []
        send_mock.assert_called_once()
        assert "ocr/ai servisi" in send_mock.call_args.args[1].lower()
        assert reaction_mock.call_count == 2
        assert [call.args[2] for call in reaction_mock.call_args_list] == ["⌛", "⚠️"]


def test_extraction_failure_sends_error_and_writes_no_row():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
        ), patch(
            "app.services.accounting.intake.doc_classifier.classify_document_type",
            return_value=(DocumentCategory.FATURA, False),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            side_effect=RuntimeError("extractor down"),
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response = client.post("/webhook", json=_image_payload())

        assert response.status_code == 200
        assert _read_export_rows(tmpdir) == []
        send_mock.assert_called_once()
        assert "bilgiler çıkarılamadı" in send_mock.call_args.args[1].lower()
        assert reaction_mock.call_count == 2
        assert [call.args[2] for call in reaction_mock.call_args_list] == ["⌛", "⚠️"]


def test_non_bill_image_is_skipped_without_export():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=False, reason="meme", confidence=0.97),
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response = client.post("/webhook", json=_image_payload())

        assert response.status_code == 200
        assert _read_export_rows(tmpdir) == []
        send_mock.assert_called_once()
        assert "muhasebe belgesi olarak alg\u0131lanmad\u0131" in send_mock.call_args.args[1].lower()
        assert reaction_mock.call_count == 2
        assert [call.args[2] for call in reaction_mock.call_args_list] == ["⌛", "⚠️"]


def test_repeated_non_bill_text_warning_is_throttled():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.services.accounting.intake.bill_classifier.classify_text",
            return_value=ClassificationResult(is_bill=False, reason="chat", confidence=0.92),
        ), patch("app.routes.webhooks.whatsapp.send_text_message") as send_mock:
            response_one = client.post("/webhook", json=_text_payload("Merhaba", "wamid-text-1"))
            response_two = client.post("/webhook", json=_text_payload("Selam", "wamid-text-2"))

    assert response_one.status_code == 200
    assert response_two.status_code == 200
    send_mock.assert_called_once()


def test_repeated_non_bill_images_always_get_terminal_reply():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=False, reason="meme", confidence=0.97),
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response_one = client.post("/webhook", json=_image_payload("wamid-image-1"))
            response_two = client.post("/webhook", json=_image_payload("wamid-image-2"))

    assert response_one.status_code == 200
    assert response_two.status_code == 200
    assert send_mock.call_count == 2
    assert "muhasebe belgesi olarak alg\u0131lanmad\u0131" in send_mock.call_args_list[0].args[1].lower()
    assert "muhasebe belgesi olarak alg\u0131lanmad\u0131" in send_mock.call_args_list[1].args[1].lower()
    assert reaction_mock.call_count == 4
    assert [call.args[2] for call in reaction_mock.call_args_list] == ["⌛", "⚠️", "⌛", "⚠️"]


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
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-pdf"
        ), patch(
            "app.services.accounting.intake.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="document", confidence=0.94),
        ), patch(
            "app.services.accounting.intake.doc_classifier.classify_document_type",
            return_value=(DocumentCategory.FATURA, False),
        ) as classify_mock, patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            return_value=[record],
        ) as extract_mock, patch(
            "app.services.accounting.intake.google_sheets.upload_document",
            return_value="https://drive.google.com/file/d/test/view",
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
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
            source_sender_id="905551112233",
            source_group_id=None,
            source_chat_type="individual",
        )
        send_mock.assert_not_called()
        assert reaction_mock.call_count == 2
        assert reaction_mock.call_args_list[0].kwargs["recipient_type"] == "individual"
        assert [call.args[2] for call in reaction_mock.call_args_list] == ["⌛", "✅"]


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
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.bill_classifier.classify_image",
            return_value=ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
        ), patch(
            "app.services.accounting.intake.doc_classifier.classify_document_type",
            return_value=(DocumentCategory.FATURA, False),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            return_value=[record],
        ), patch(
            "app.services.accounting.intake.google_sheets.upload_document",
            return_value="https://drive.google.com/file/d/test/view",
        ), patch(
            "app.routes.webhooks.whatsapp.send_reaction_message",
            side_effect=RuntimeError("send failed"),
        ) as reaction_mock, patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock:
            response = client.post("/webhook", json=_image_payload())

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        assert len(_read_export_rows(tmpdir)) == 1
        assert reaction_mock.call_count == 2
        send_mock.assert_not_called()


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
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.bill_classifier.classify_image",
            side_effect=[
                RuntimeError("classifier down"),
                ClassificationResult(is_bill=True, reason="ok", confidence=0.95),
            ],
        ), patch(
            "app.services.accounting.intake.doc_classifier.classify_document_type",
            return_value=(DocumentCategory.FATURA, False),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            return_value=[record],
        ), patch(
            "app.services.accounting.intake.google_sheets.upload_document",
            return_value="https://drive.google.com/file/d/test/view",
        ), patch("app.routes.webhooks.whatsapp.send_text_message") as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response_one = client.post("/webhook", json=_image_payload("wamid-retry-1"))
            response_two = client.post("/webhook", json=_image_payload("wamid-retry-1"))

        assert response_one.status_code == 200
        assert response_two.status_code == 200
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        assert send_mock.call_count == 1
        assert reaction_mock.call_count == 4
