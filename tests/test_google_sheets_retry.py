"""
Unit tests for Google Sheets retry-on-rate-limit logic.
"""

from __future__ import annotations

import ssl
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.services.providers import google_sheets
from app.services.providers.google_sheets import _retry_on_rate_limit


class FakeApiError(Exception):
    pass


def test_retry_succeeds_after_rate_limit():
    """Function should retry and eventually succeed on 429."""
    call_count = 0

    def flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise FakeApiError("[429]: Quota exceeded for quota metric 'Write requests'")
        return "ok"

    with patch("app.services.providers.google_sheets.time.sleep") as mock_sleep:
        result = _retry_on_rate_limit(flaky, base_delay=1.0)

    assert result == "ok"
    assert call_count == 3
    assert mock_sleep.call_count == 2  # slept twice before 3rd attempt


def test_retry_raises_non_429_immediately():
    """Non-rate-limit errors should propagate without retry."""
    call_count = 0

    def always_fail():
        nonlocal call_count
        call_count += 1
        raise ValueError("Something else went wrong")

    with patch("app.services.providers.google_sheets.time.sleep") as mock_sleep:
        with pytest.raises(ValueError, match="Something else"):
            _retry_on_rate_limit(always_fail, base_delay=1.0)

    assert call_count == 1
    mock_sleep.assert_not_called()


def test_retry_exhausts_max_retries():
    """Should raise after max_retries are exhausted."""
    call_count = 0

    def always_429():
        nonlocal call_count
        call_count += 1
        raise FakeApiError("[429]: RATE_LIMIT exceeded")

    with patch("app.services.providers.google_sheets.time.sleep"):
        with pytest.raises(FakeApiError, match="429"):
            _retry_on_rate_limit(always_429, max_retries=3, base_delay=1.0)

    assert call_count == 4  # initial + 3 retries


def test_retry_exponential_backoff_delays():
    """Verify exponential backoff timing."""
    call_count = 0

    def fail_three_times():
        nonlocal call_count
        call_count += 1
        if call_count <= 3:
            raise FakeApiError("Quota exceeded for 429")
        return "done"

    with patch("app.services.providers.google_sheets.time.sleep") as mock_sleep:
        _retry_on_rate_limit(fail_three_times, base_delay=5.0)

    delays = [call.args[0] for call in mock_sleep.call_args_list]
    assert delays == [5.0, 10.0, 20.0]  # 5 * 2^0, 5 * 2^1, 5 * 2^2


def test_upload_document_retries_transient_ssl_error(monkeypatch):
    call_state = {"count": 0}

    class FakeCreateRequest:
        def execute(self):
            call_state["count"] += 1
            if call_state["count"] == 1:
                raise ssl.SSLError("record layer failure")
            return {"webViewLink": "https://drive.google.com/file/d/test/view"}

    class FakeFilesResource:
        def create(self, **kwargs):
            return FakeCreateRequest()

    class FakeDriveService:
        def files(self):
            return FakeFilesResource()

    monkeypatch.setattr(google_sheets.settings, "google_drive_parent_folder_id", "folder-1")
    monkeypatch.setattr(
        google_sheets,
        "_get_oauth_drive_service",
        lambda force_refresh=False: None,
    )
    monkeypatch.setattr(
        google_sheets,
        "_get_drive_service",
        lambda force_refresh=False: FakeDriveService(),
    )
    monkeypatch.setattr(
        google_sheets,
        "_get_or_create_month_drive_folder",
        lambda: "folder-1",
    )
    monkeypatch.setattr("app.services.providers.google_sheets.time.sleep", lambda seconds: None)

    link = google_sheets.upload_document(
        b"fake-image",
        filename="receipt.jpg",
        mime_type="image/jpeg",
    )

    assert link == "https://drive.google.com/file/d/test/view"
    assert call_state["count"] == 2


def test_upload_document_serializes_concurrent_drive_requests(monkeypatch):
    state = {"active": 0, "max_active": 0, "entered": 0}
    state_lock = threading.Lock()
    first_entered = threading.Event()
    release_first = threading.Event()

    class FakeCreateRequest:
        def execute(self):
            with state_lock:
                state["entered"] += 1
                state["active"] += 1
                state["max_active"] = max(state["max_active"], state["active"])
                current_entry = state["entered"]

            if current_entry == 1:
                first_entered.set()
                release_first.wait(timeout=2.0)

            time.sleep(0.05)
            with state_lock:
                state["active"] -= 1
            return {"webViewLink": "https://drive.google.com/file/d/test/view"}

    class FakeFilesResource:
        def create(self, **kwargs):
            return FakeCreateRequest()

    class FakeDriveService:
        def files(self):
            return FakeFilesResource()

    monkeypatch.setattr(google_sheets.settings, "google_drive_parent_folder_id", "folder-1")
    monkeypatch.setattr(
        google_sheets,
        "_get_oauth_drive_service",
        lambda force_refresh=False: None,
    )
    monkeypatch.setattr(
        google_sheets,
        "_get_drive_service",
        lambda force_refresh=False: FakeDriveService(),
    )
    monkeypatch.setattr(
        google_sheets,
        "_get_or_create_month_drive_folder",
        lambda: "folder-1",
    )

    results: list[str | None] = []

    def _run_upload(name: str) -> None:
        results.append(
            google_sheets.upload_document(
                b"fake-image",
                filename=name,
                mime_type="image/jpeg",
            )
        )

    thread1 = threading.Thread(target=_run_upload, args=("one.jpg",))
    thread2 = threading.Thread(target=_run_upload, args=("two.jpg",))

    thread1.start()
    assert first_entered.wait(timeout=1.0)
    thread2.start()
    time.sleep(0.1)
    release_first.set()
    thread1.join(timeout=2.0)
    thread2.join(timeout=2.0)

    assert results == [
        "https://drive.google.com/file/d/test/view",
        "https://drive.google.com/file/d/test/view",
    ]
    assert state["max_active"] == 1


def test_process_pending_document_uploads_backfills_missing_drive_links(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))
    monkeypatch.setattr(
        google_sheets,
        "start_pending_drive_upload_worker",
        lambda: None,
    )

    target = {
        "spreadsheet_id": "sheet-123",
        "tab_name": "💳 Dekontlar",
        "row_number": 7,
    }
    google_sheets.queue_pending_document_upload(
        file_bytes=b"pending-payload",
        filename="Dekont.pdf",
        mime_type="application/pdf",
        targets=[target],
        source_message_id="wamid-pending-1",
    )

    fake_ws = MagicMock()
    fake_sheet = MagicMock()
    fake_sheet.worksheet.return_value = fake_ws
    fake_client = MagicMock()
    fake_client.open_by_key.return_value = fake_sheet

    monkeypatch.setattr(
        google_sheets,
        "_get_client",
        lambda: fake_client,
    )
    monkeypatch.setattr(
        google_sheets,
        "upload_document",
        lambda file_bytes, filename, mime_type: "https://drive.google.com/file/d/pending/view",
    )

    processed = google_sheets.process_pending_document_uploads()

    assert processed == 1
    fake_client.open_by_key.assert_called_once_with("sheet-123")
    fake_sheet.worksheet.assert_called_once_with("💳 Dekontlar")
    fake_ws.update.assert_called_once_with(
        [['=HYPERLINK("https://drive.google.com/file/d/pending/view";"📄 Görüntüle")']],
        "K7",
        value_input_option="USER_ENTERED",
    )
    assert google_sheets._load_pending_drive_uploads() == []
    payload_files = list((Path(tmp_path) / "state" / "pending_drive_uploads").glob("*"))
    assert payload_files == []



def test_queue_pending_document_upload_merges_targets_for_same_message(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))
    monkeypatch.setattr(google_sheets, "start_pending_drive_upload_worker", lambda: None)

    google_sheets.queue_pending_document_upload(
        file_bytes=b"doc-bytes",
        filename="Dekont.pdf",
        mime_type="application/pdf",
        targets=[{"spreadsheet_id": "sheet-1", "tab_name": "💳 Dekontlar", "row_number": 3}],
        source_message_id="wamid-merge-1",
    )
    google_sheets.queue_pending_document_upload(
        file_bytes=b"doc-bytes",
        filename="Dekont.pdf",
        mime_type="application/pdf",
        targets=[{"spreadsheet_id": "sheet-1", "tab_name": "↩️ İadeler", "row_number": 5}],
        source_message_id="wamid-merge-1",
    )

    items = google_sheets._load_pending_drive_uploads()
    assert len(items) == 1
    assert items[0]["source_message_id"] == "wamid-merge-1"
    assert items[0]["targets"] == [
        {"spreadsheet_id": "sheet-1", "tab_name": "💳 Dekontlar", "row_number": 3},
        {"spreadsheet_id": "sheet-1", "tab_name": "↩️ İadeler", "row_number": 5},
    ]


def test_append_record_queues_pending_sheet_appends_and_starts_worker(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))
    monkeypatch.setattr(google_sheets.settings, "google_sheets_spreadsheet_id", "sheet-queue-1")
    monkeypatch.setattr(google_sheets, "_get_client", lambda: object())

    worker_started = {"count": 0}

    def _start_worker():
        worker_started["count"] += 1

    monkeypatch.setattr(google_sheets, "start_pending_sheet_append_worker", _start_worker)

    record = google_sheets.BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-sheet-1",
        document_date="2026-04-11",
        confidence=0.91,
    )

    google_sheets.append_record(
        record,
        google_sheets.DocumentCategory.FATURA,
        is_return=True,
        drive_link=None,
        pending_document_bytes=b"fake-image",
        pending_document_filename="media-1.jpg",
        pending_document_mime_type="image/jpeg",
    )

    items = google_sheets._load_pending_sheet_appends()
    assert len(items) == 2
    assert worker_started["count"] == 1
    assert [item["tab_name"] for item in items] == ["🧾 Faturalar", "↩️ İadeler"]
    assert [item["category"] for item in items] == ["fatura", "iade"]
    assert all(item["spreadsheet_id"] == "sheet-queue-1" for item in items)
    assert all(Path(item["document_payload_path"]).exists() for item in items)


def test_process_pending_sheet_appends_batches_rows_and_queues_drive_backfill(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))
    monkeypatch.setattr(google_sheets.settings, "google_sheets_spreadsheet_id", "sheet-batch-1")

    record = google_sheets.BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-sheet-2",
        document_date="2026-04-11",
        confidence=0.91,
    )

    monkeypatch.setattr(google_sheets, "_get_client", lambda: object())
    monkeypatch.setattr(google_sheets, "start_pending_sheet_append_worker", lambda: None)
    google_sheets.append_record(
        record,
        google_sheets.DocumentCategory.FATURA,
        drive_link=None,
        pending_document_bytes=b"fake-image",
        pending_document_filename="media-1.jpg",
        pending_document_mime_type="image/jpeg",
    )

    queued_items = google_sheets._load_pending_sheet_appends()
    assert len(queued_items) == 1
    pending_id = queued_items[0]["id"]

    fake_ws = MagicMock()
    fake_ws.get.return_value = []
    fake_ws.col_values.return_value = ["#", "TOPLAM"]
    fake_sheet = MagicMock()
    fake_sheet.worksheet.return_value = fake_ws
    fake_client = MagicMock()
    fake_client.open_by_key.return_value = fake_sheet

    queue_mock = MagicMock()
    monkeypatch.setattr(google_sheets, "_get_client", lambda: fake_client)
    monkeypatch.setattr(google_sheets, "queue_pending_document_upload", queue_mock)

    processed = google_sheets.process_pending_sheet_appends()

    assert processed == 1
    fake_client.open_by_key.assert_called_once_with("sheet-batch-1")
    fake_ws.append_rows.assert_called_once()
    appended_rows = fake_ws.append_rows.call_args.args[0]
    assert len(appended_rows) == 1
    assert appended_rows[0][-1] == pending_id
    queue_mock.assert_called_once()
    assert queue_mock.call_args.kwargs["targets"] == [
        {"spreadsheet_id": "sheet-batch-1", "tab_name": "🧾 Faturalar", "row_number": 3}
    ]
    assert queue_mock.call_args.kwargs["source_message_id"] == "wamid-sheet-2"
    assert google_sheets._load_pending_sheet_appends() == []
    assert list((Path(tmp_path) / "state" / "pending_sheet_appends").glob("*")) == []



def test_process_pending_sheet_appends_retries_rate_limited_batch(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))
    monkeypatch.setattr(google_sheets.settings, "google_sheets_spreadsheet_id", "sheet-batch-2")

    record = google_sheets.BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-sheet-3",
        document_date="2026-04-11",
        confidence=0.91,
    )

    monkeypatch.setattr(google_sheets, "_get_client", lambda: object())
    monkeypatch.setattr(google_sheets, "start_pending_sheet_append_worker", lambda: None)
    google_sheets.append_record(
        record,
        google_sheets.DocumentCategory.FATURA,
        drive_link="https://drive.google.com/file/d/test/view",
    )

    fake_ws = MagicMock()
    fake_ws.get.return_value = []
    fake_ws.col_values.return_value = ["#", "TOPLAM"]
    fake_ws.append_rows.side_effect = FakeApiError("429 rate limit")
    fake_sheet = MagicMock()
    fake_sheet.worksheet.return_value = fake_ws
    fake_client = MagicMock()
    fake_client.open_by_key.return_value = fake_sheet

    monkeypatch.setattr(google_sheets, "_get_client", lambda: fake_client)
    monkeypatch.setattr(google_sheets, "_retry_on_rate_limit", lambda fn, **kwargs: fn())

    processed = google_sheets.process_pending_sheet_appends()

    assert processed == 0
    items = google_sheets._load_pending_sheet_appends()
    assert len(items) == 1
    assert items[0]["attempts"] == 1
    assert "429" in items[0]["last_error"]
    assert float(items[0]["next_attempt_at"]) > 0
