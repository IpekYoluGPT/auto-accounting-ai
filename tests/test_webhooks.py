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
from app.services.accounting import inbound_queue
from app.services.accounting.intake import process_incoming_message
from app.services.accounting.doc_classifier import DocumentAnalysis


def _image_payload(
    message_id: str = "wamid-1",
    *,
    sender: str = "905551112233",
    sender_name: str | None = None,
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

    value = {
        "messaging_product": "whatsapp",
        "messages": [message],
    }
    if sender_name:
        value["contacts"] = [{"wa_id": sender, "profile": {"name": sender_name}}]

    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "entry-1",
                "changes": [
                    {
                        "field": "messages",
                        "value": value,
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


def _analysis(
    category: DocumentCategory = DocumentCategory.FATURA,
    *,
    is_financial_document: bool = True,
    is_return: bool = False,
    document_count: int | None = None,
    quality: str = "usable",
    needs_retry: bool = False,
    confidence: float = 0.95,
    reason: str = "ok",
) -> DocumentAnalysis:
    return DocumentAnalysis(
        is_financial_document=is_financial_document,
        category=category,
        is_return=is_return,
        document_count=document_count if document_count is not None else (1 if is_financial_document else 0),
        quality=quality,
        needs_retry=needs_retry,
        confidence=confidence,
        reason=reason,
    )


@contextmanager
def _patch_runtime_settings(tmpdir: str, *, groups_only: bool = False):
    def _enqueue_media_immediately(**kwargs):
        message_id = kwargs["message_id"]
        route = kwargs["route"]
        media_id = kwargs.get("media_id")
        mime_type = kwargs["mime_type"]
        filename = kwargs["filename"]
        source_type = kwargs["source_type"]
        attachment_url = kwargs.get("attachment_url")

        process_incoming_message(
            message_id=message_id,
            msg_type=kwargs["msg_type"],
            route=route,
            send_text=_send_meta_text_message_placeholder,
            send_reaction=_send_meta_reaction_placeholder,
            fetch_media=lambda: whatsapp_fetch_media_placeholder(media_id),
            mime_type=mime_type,
            filename=filename,
            source_type=source_type,
            attachment_url=attachment_url,
        )
        return inbound_queue.EnqueueResult(status="duplicate")

    with patch("app.routes.webhooks.settings.storage_dir", tmpdir), patch(
        "app.routes.webhooks.settings.whatsapp_groups_only", groups_only
    ), patch("app.services.accounting.record_store.settings.storage_dir", tmpdir), patch(
        "app.services.accounting.inbound_queue.settings.storage_dir", tmpdir
    ), patch(
        "app.services.accounting.storage_guard.settings.storage_dir", tmpdir
    ), patch(
        "app.routes.webhooks.inbound_queue.enqueue_media_job"
    ) as enqueue_mock:
        from app.routes import webhooks as webhooks_route

        def whatsapp_fetch_media_placeholder(media_id: str | None):
            if not media_id:
                raise RuntimeError("missing media id")
            return webhooks_route.whatsapp.fetch_media(media_id)

        def _send_meta_text_message_placeholder(route, text: str):
            return webhooks_route._send_meta_text_message(route, text)

        def _send_meta_reaction_placeholder(route, emoji: str):
            return webhooks_route._send_meta_reaction(route, emoji)

        enqueue_mock.side_effect = _enqueue_media_immediately
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


def test_image_webhook_enqueues_media_without_inline_fetch():
    client = TestClient(app)
    with patch(
        "app.routes.webhooks.inbound_queue.enqueue_media_job",
        return_value=inbound_queue.EnqueueResult(status="enqueued"),
    ) as enqueue_mock, patch(
        "app.routes.webhooks.whatsapp.fetch_media"
    ) as fetch_mock, patch(
        "app.routes.webhooks.whatsapp.send_reaction_message"
    ) as reaction_mock:
        response = client.post("/webhook", json=_image_payload())

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    fetch_mock.assert_not_called()
    enqueue_mock.assert_called_once()
    assert reaction_mock.call_args.args == ("905551112233", "wamid-1", "⌛")


def test_happy_path_image_webhook_writes_csv_and_reacts():
    record = BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-1",
        source_filename="media-1.jpg",
        source_type="image",
        document_date="2026-04-09",
        confidence=0.91,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.FATURA, confidence=0.95),
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


def test_meta_contact_name_is_forwarded_to_extractor():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.ODEME_DEKONTU, confidence=0.95),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            return_value=[
                BillRecord(
                    company_name="Yapi Kredi",
                    total_amount=100.0,
                    currency="TRY",
                    source_message_id="wamid-1",
                )
            ],
        ) as extract_mock, patch(
            "app.services.accounting.intake.google_sheets.upload_document",
            return_value="https://drive.google.com/file/d/test/view",
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ), patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ):
            response = client.post(
                "/webhook",
                json=_image_payload(sender_name="Ahmet Yılmaz"),
            )

        assert response.status_code == 200
        assert extract_mock.call_args.kwargs["source_sender_name"] == "Ahmet Yılmaz"


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
        document_date="2026-04-09",
        confidence=0.91,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir, groups_only=True), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.FATURA, confidence=0.95),
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
            source_sender_name=None,
            source_group_id="group-123",
            source_chat_type="group",
            category_hint=DocumentCategory.FATURA,
            document_count_hint=None,
            is_return_hint=False,
            strict_document_count=None,
            split_retry=False,
        )
        send_mock.assert_not_called()
        assert reaction_mock.call_count == 2
        assert reaction_mock.call_args_list[0].args == ("group-123", "wamid-group-1", "⌛")
        assert reaction_mock.call_args_list[1].args == ("group-123", "wamid-group-1", "✅")
        assert reaction_mock.call_args_list[0].kwargs["recipient_type"] == "group"


def test_image_webhook_passes_pending_document_payload_to_sheet_outbox_when_upload_fails():
    record = BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-1",
        source_filename="media-1.jpg",
        source_type="image",
        document_date="2026-04-09",
        confidence=0.91,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.FATURA, confidence=0.95),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            return_value=[record],
        ), patch(
            "app.services.accounting.intake.google_sheets.upload_document",
            return_value=None,
        ), patch(
            "app.services.accounting.intake.google_sheets.append_record",
            return_value=[],
        ) as append_mock, patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response = client.post("/webhook", json=_image_payload())

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        assert len(_read_export_rows(tmpdir)) == 1
        send_mock.assert_not_called()
        assert reaction_mock.call_count == 2
        append_mock.assert_called_once()
        assert append_mock.call_args.kwargs["drive_link"] is None
        assert append_mock.call_args.kwargs["pending_document_bytes"] == b"fake-image"
        assert append_mock.call_args.kwargs["pending_document_filename"] == "media-1.jpg"
        assert append_mock.call_args.kwargs["pending_document_mime_type"] == "image/jpeg"


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


def test_multi_document_image_exports_all_records_when_counts_match():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)

        records = [
            BillRecord(
                company_name="Yapı Kredi",
                document_number="CHK-001",
                document_date="2026-03-30",
                total_amount=444000.0,
                currency="TRY",
                source_message_id="wamid-checks-1__doc1",
                source_filename="checks.jpg",
                source_type="image",
                confidence=0.91,
            ),
            BillRecord(
                company_name="Yapı Kredi",
                document_number="CHK-002",
                document_date="2026-04-20",
                total_amount=444000.0,
                currency="TRY",
                source_message_id="wamid-checks-1__doc2",
                source_filename="checks.jpg",
                source_type="image",
                confidence=0.91,
            ),
            BillRecord(
                company_name="Yapı Kredi",
                document_number="CHK-003",
                document_date="2026-04-30",
                total_amount=444000.0,
                currency="TRY",
                source_message_id="wamid-checks-1__doc3",
                source_filename="checks.jpg",
                source_type="image",
                confidence=0.91,
            ),
        ]

        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"same-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.CEK, confidence=0.95, document_count=3),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            return_value=records,
        ) as extract_mock, patch(
            "app.services.accounting.intake.google_sheets.upload_document",
            return_value="https://drive.google.com/file/d/test/view",
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response = client.post("/webhook", json=_image_payload("wamid-checks-1"))

        assert response.status_code == 200
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 3
        assert extract_mock.call_count == 1
        send_mock.assert_not_called()
        assert [call.args[2] for call in reaction_mock.call_args_list] == ["⌛", "✅"]



def test_non_cheque_multi_document_image_requires_all_or_retry_when_split_stays_incomplete():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)

        partial_records = [
            BillRecord(
                company_name="Yapı Kredi",
                document_number="CHK-001",
                document_date="2026-03-30",
                total_amount=444000.0,
                currency="TRY",
                source_message_id="wamid-checks-2__doc1",
                source_filename="checks.jpg",
                source_type="image",
                confidence=0.91,
            ),
            BillRecord(
                company_name="Yapı Kredi",
                document_number="CHK-002",
                document_date="2026-04-20",
                total_amount=444000.0,
                currency="TRY",
                source_message_id="wamid-checks-2__doc2",
                source_filename="checks.jpg",
                source_type="image",
                confidence=0.91,
            ),
        ]

        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"same-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.FATURA, confidence=0.95, document_count=3),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            side_effect=[partial_records, partial_records],
        ) as extract_mock, patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response = client.post("/webhook", json=_image_payload("wamid-checks-2"))

        assert response.status_code == 200
        assert _read_export_rows(tmpdir) == []
        assert extract_mock.call_count == 2
        assert extract_mock.call_args_list[0].kwargs["split_retry"] is False
        assert extract_mock.call_args_list[1].kwargs["split_retry"] is True
        assert extract_mock.call_args_list[1].kwargs["strict_document_count"] == 3
        send_mock.assert_called_once()
        assert "birden fazla belge var" in send_mock.call_args.args[1].lower()
        assert [call.args[2] for call in reaction_mock.call_args_list] == ["⌛", "⚠️"]


def test_cheque_multi_document_image_exports_usable_records_when_split_stays_incomplete():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)

        partial_records = [
            BillRecord(
                company_name="HALKBANK",
                document_number="3941468",
                document_date="2027-04-30",
                total_amount=125000.0,
                currency="TRY",
                source_message_id="wamid-four-checks__doc1",
                source_filename="checks.jpg",
                source_type="image",
                confidence=0.91,
            ),
            BillRecord(
                company_name="QNB FİNANSBANK",
                document_number="0448596",
                document_date="2023-12-30",
                total_amount=380000.0,
                currency="TRY",
                source_message_id="wamid-four-checks__doc2",
                source_filename="checks.jpg",
                source_type="image",
                confidence=0.91,
            ),
            BillRecord(
                company_name="Garanti BBVA",
                document_number="0205893",
                document_date="2023-12-05",
                total_amount=200000.0,
                currency="TRY",
                source_message_id="wamid-four-checks__doc3",
                source_filename="checks.jpg",
                source_type="image",
                confidence=0.91,
            ),
        ]

        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"same-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.CEK, confidence=0.95, document_count=4),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            side_effect=[partial_records, partial_records],
        ) as extract_mock, patch(
            "app.services.accounting.intake.google_sheets.append_record",
            return_value=[],
        ) as append_mock, patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response = client.post("/webhook", json=_image_payload("wamid-four-checks"))

        assert response.status_code == 200
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 3
        assert extract_mock.call_count == 2
        assert extract_mock.call_args_list[1].kwargs["split_retry"] is True
        assert extract_mock.call_args_list[1].kwargs["strict_document_count"] == 4
        assert append_mock.call_count == 3
        send_mock.assert_not_called()
        assert [call.args[2] for call in reaction_mock.call_args_list] == ["⌛", "✅"]



def test_sheet_backlog_notice_is_throttled_per_chat():
    record = BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-backlog-1",
        source_filename="media-1.jpg",
        source_type="image",
        document_date="2026-04-09",
        confidence=0.91,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.FATURA, confidence=0.95),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            side_effect=[[record], [record.model_copy(update={"source_message_id": "wamid-backlog-2"})]],
        ), patch(
            "app.services.accounting.intake.google_sheets.upload_document",
            return_value="https://drive.google.com/file/d/test/view",
        ), patch(
            "app.services.accounting.intake.google_sheets.queue_status",
            return_value={"pending_sheet_appends": 7, "pending_drive_uploads": 0},
        ), patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ) as send_mock, patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ) as reaction_mock:
            response_one = client.post("/webhook", json=_image_payload("wamid-backlog-1"))
            response_two = client.post("/webhook", json=_image_payload("wamid-backlog-2"))

        assert response_one.status_code == 200
        assert response_two.status_code == 200
        assert send_mock.call_count == 1
        assert "görünür satırlar önce" in send_mock.call_args.args[1].lower()
        assert [call.args[2] for call in reaction_mock.call_args_list] == ["⌛", "✅", "⌛", "✅"]


def test_duplicate_content_with_new_message_id_writes_only_one_export_row():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)

        def _record_for(message_id: str) -> BillRecord:
            return BillRecord(
                company_name="ABC Market",
                tax_number="1234567890",
                invoice_number="INV-2026-15",
                document_date="2026-04-09",
                document_time="10:15",
                total_amount=100.0,
                currency="TRY",
                source_message_id=message_id,
                source_filename="media-1.jpg",
                source_type="image",
                confidence=0.91,
            )

        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"same-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.FATURA, confidence=0.95),
        ), patch(
            "app.services.accounting.intake.gemini_extractor.extract_bills",
            side_effect=[[_record_for("wamid-1")], [_record_for("wamid-2")]],
        ), patch(
            "app.services.accounting.intake.google_sheets.upload_document",
        ) as upload_mock, patch(
            "app.services.accounting.intake.google_sheets.append_record",
            return_value=[],
        ) as append_mock, patch(
            "app.routes.webhooks.whatsapp.send_text_message"
        ), patch(
            "app.routes.webhooks.whatsapp.send_reaction_message"
        ):
            response_one = client.post("/webhook", json=_image_payload("wamid-1"))
            response_two = client.post("/webhook", json=_image_payload("wamid-2"))

        assert response_one.status_code == 200
        assert response_two.status_code == 200
        rows = _read_export_rows(tmpdir)
        assert len(rows) == 1
        upload_mock.assert_not_called()
        append_mock.assert_called_once()


def test_duplicate_delivery_writes_only_one_export_row():
    record = BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-1",
        source_filename="media-1.jpg",
        source_type="image",
        document_date="2026-04-09",
        confidence=0.91,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ) as fetch_mock, patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.FATURA, confidence=0.95),
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
            "app.services.accounting.intake.doc_classifier.analyze_document",
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
            "app.services.accounting.intake.doc_classifier.analyze_document",
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
        assert "ai servisi" in send_mock.call_args.args[1].lower()
        assert reaction_mock.call_count == 2
        assert [call.args[2] for call in reaction_mock.call_args_list] == ["⌛", "⚠️"]


def test_extraction_failure_sends_error_and_writes_no_row():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.FATURA, confidence=0.95),
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
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(
                DocumentCategory.BELIRSIZ,
                is_financial_document=False,
                document_count=0,
                confidence=0.97,
                reason="meme",
            ),
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
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(
                DocumentCategory.BELIRSIZ,
                is_financial_document=False,
                document_count=0,
                confidence=0.97,
                reason="meme",
            ),
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
        document_date="2026-04-09",
        confidence=0.89,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-pdf"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.FATURA, confidence=0.94, reason="document"),
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
            source_sender_name=None,
            source_group_id=None,
            source_chat_type="individual",
            category_hint=DocumentCategory.FATURA,
            document_count_hint=None,
            is_return_hint=False,
            strict_document_count=None,
            split_retry=False,
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
        document_date="2026-04-09",
        confidence=0.91,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            return_value=_analysis(DocumentCategory.FATURA, confidence=0.95),
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
        document_date="2026-04-09",
        confidence=0.9,
    )

    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with _patch_runtime_settings(tmpdir), patch(
            "app.routes.webhooks.whatsapp.fetch_media", return_value=b"fake-image"
        ), patch(
            "app.services.accounting.intake.doc_classifier.analyze_document",
            side_effect=[
                RuntimeError("classifier down"),
                _analysis(DocumentCategory.FATURA, confidence=0.95),
            ],
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
