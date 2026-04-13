"""
Unit tests for Google Sheets retry-on-rate-limit logic.
"""

from __future__ import annotations

import json
import ssl
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.services.accounting.pipeline_context import pipeline_context_scope, sandbox_context
from app.services.providers import google_sheets
from app.services.providers.google_sheets import _retry_on_rate_limit


class FakeApiError(Exception):
    pass


def _banka_odemeleri_lookup_row(row_id: str) -> list[str]:
    row = [""] * 12
    row[9] = row_id
    return row


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


def test_force_rewrite_drive_links_falls_back_to_raw_document_links(monkeypatch):
    fake_sheet = MagicMock()
    fake_client = MagicMock()
    target_ws = MagicMock()
    target_ws.spreadsheet.locale = 'tr_TR'

    monkeypatch.setattr(google_sheets, '_get_client', lambda: fake_client)
    monkeypatch.setattr(google_sheets, '_open_spreadsheet_by_key', lambda client, spreadsheet_id: fake_sheet)
    monkeypatch.setattr(google_sheets, '_raw_document_drive_link_map', lambda sh: {'doc-1': 'https://drive.google.com/file/d/raw/view'})
    monkeypatch.setattr(google_sheets, '_iter_visible_row_maps', lambda ws, tab_name, value_render_option=None: [(3, {google_sheets._VISIBLE_DRIVE_LINK_HEADER: 'Görüntüle', google_sheets._HIDDEN_SOURCE_DOC_ID_HEADER: 'doc-1'})])
    monkeypatch.setattr(google_sheets, '_ensure_tab_exists', lambda sh, tab_name, lightweight=True: target_ws)
    rewrite_calls = []
    monkeypatch.setattr(google_sheets, '_rewrite_drive_cells', lambda ws, tab_name, row_formulas: rewrite_calls.append((tab_name, row_formulas)) or len(row_formulas))

    rewritten = google_sheets.force_rewrite_drive_links(spreadsheet_id='sheet-1', target_tabs={'Sevk Fişleri'})

    assert rewritten == {'Sevk Fişleri': 1}
    assert rewrite_calls == [('Sevk Fişleri', [(3, '=HYPERLINK("https://drive.google.com/file/d/raw/view";"Görüntüle")')])]



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
    fake_sheet.worksheet.assert_called_once_with("Banka Ödemeleri")
    fake_ws.update.assert_called_once_with(
        [['=HYPERLINK("https://drive.google.com/file/d/pending/view";"Görüntüle")']],
        "I7",
        value_input_option="USER_ENTERED",
    )
    assert google_sheets._load_pending_drive_uploads() == []
    payload_files = list((Path(tmp_path) / "state" / "pending_drive_uploads").glob("*"))
    assert payload_files == []


def test_process_pending_document_uploads_resolves_row_id_after_row_reorder(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))
    monkeypatch.setattr(google_sheets, "start_pending_drive_upload_worker", lambda: None)

    google_sheets.queue_pending_document_upload(
        file_bytes=b"pending-payload",
        filename="Dekont.pdf",
        mime_type="application/pdf",
        targets=[
            {
                "spreadsheet_id": "sheet-123",
                "tab_name": "💳 Dekontlar",
                "row_number": 7,
                "row_id": "pending-row-id",
            }
        ],
        source_message_id="wamid-pending-row-id",
    )

    fake_ws = MagicMock()
    fake_ws.get.return_value = [_banka_odemeleri_lookup_row("other-row"), _banka_odemeleri_lookup_row("pending-row-id")]
    fake_sheet = MagicMock()
    fake_sheet.worksheet.return_value = fake_ws
    fake_client = MagicMock()
    fake_client.open_by_key.return_value = fake_sheet

    monkeypatch.setattr(google_sheets, "_get_client", lambda: fake_client)
    monkeypatch.setattr(
        google_sheets,
        "upload_document",
        lambda file_bytes, filename, mime_type: "https://drive.google.com/file/d/pending/view",
    )

    processed = google_sheets.process_pending_document_uploads()

    assert processed == 1
    fake_ws.get.assert_called_once_with("A3:L")
    fake_ws.update.assert_called_once_with(
        [['=HYPERLINK("https://drive.google.com/file/d/pending/view";"Görüntüle")']],
        "I4",
        value_input_option="USER_ENTERED",
    )



def test_process_pending_document_uploads_reuses_cached_drive_link_after_row_lookup_retry(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))
    monkeypatch.setattr(google_sheets, "start_pending_drive_upload_worker", lambda: None)

    google_sheets.queue_pending_document_upload(
        file_bytes=b"pending-payload",
        filename="Dekont.pdf",
        mime_type="application/pdf",
        targets=[
            {
                "spreadsheet_id": "sheet-123",
                "tab_name": "💳 Dekontlar",
                "row_number": 7,
                "row_id": "pending-row-id",
            }
        ],
        source_message_id="wamid-pending-retry",
    )

    fake_ws = MagicMock()
    fake_sheet = MagicMock()
    fake_sheet.worksheet.return_value = fake_ws
    fake_client = MagicMock()
    fake_client.open_by_key.return_value = fake_sheet
    monkeypatch.setattr(google_sheets, "_get_client", lambda: fake_client)

    upload_calls = {"count": 0}

    def _upload_document(file_bytes, filename, mime_type):
        upload_calls["count"] += 1
        return "https://drive.google.com/file/d/pending/view"

    monkeypatch.setattr(google_sheets, "upload_document", _upload_document)

    fake_ws.get.return_value = []
    processed = google_sheets.process_pending_document_uploads()
    assert processed == 0
    assert upload_calls["count"] == 1
    pending_items = google_sheets._load_pending_drive_uploads()
    assert len(pending_items) == 1
    assert pending_items[0]["drive_link"] == "https://drive.google.com/file/d/pending/view"

    fake_ws.get.return_value = [_banka_odemeleri_lookup_row("pending-row-id")]
    processed = google_sheets.process_pending_document_uploads()

    assert processed == 1
    assert upload_calls["count"] == 1
    fake_ws.update.assert_called_once_with(
        [['=HYPERLINK("https://drive.google.com/file/d/pending/view";"Görüntüle")']],
        "I3",
        value_input_option="USER_ENTERED",
    )
    assert google_sheets._load_pending_drive_uploads() == []



def test_queue_pending_document_upload_merges_targets_for_same_message(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))
    monkeypatch.setattr(google_sheets, "start_pending_drive_upload_worker", lambda: None)

    google_sheets.queue_pending_document_upload(
        file_bytes=b"doc-bytes",
        filename="Dekont.pdf",
        mime_type="application/pdf",
        targets=[
            {"spreadsheet_id": "sheet-1", "tab_name": "💳 Dekontlar", "row_number": 3, "row_id": "row-dekont-1"}
        ],
        source_message_id="wamid-merge-1",
    )
    google_sheets.queue_pending_document_upload(
        file_bytes=b"doc-bytes",
        filename="Dekont.pdf",
        mime_type="application/pdf",
        targets=[
            {"spreadsheet_id": "sheet-1", "tab_name": "🏗️ Malzeme", "row_number": 5, "row_id": "row-malzeme-1"}
        ],
        source_message_id="wamid-merge-1",
    )

    items = google_sheets._load_pending_drive_uploads()
    assert len(items) == 1
    assert items[0]["source_message_id"] == "wamid-merge-1"
    assert items[0]["targets"] == [
        {"spreadsheet_id": "sheet-1", "tab_name": "Banka Ödemeleri", "row_number": 3, "row_id": "row-dekont-1"},
        {"spreadsheet_id": "sheet-1", "tab_name": "Sevk Fişleri", "row_number": 5, "row_id": "row-malzeme-1"},
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
    assert len(items) == 3
    assert worker_started["count"] == 1
    assert [item["tab_name"] for item in items] == ["__Raw Belgeler", "Faturalar", "Masraf Kayıtları"]
    assert [item["category"] for item in items] == ["fatura", "fatura", "fatura"]
    assert all(item["spreadsheet_id"] == "sheet-queue-1" for item in items)
    payload_paths = {item["document_payload_path"] for item in items}
    assert len(payload_paths) == 1
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
    assert len(queued_items) == 3
    pending_ids_by_tab = {item["tab_name"]: item["id"] for item in queued_items}

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
    monkeypatch.setattr(google_sheets, "_audit_spreadsheet_layout", lambda sh, repair=False, target_tabs=None: [])
    monkeypatch.setattr(google_sheets, "_upsert_party_cards", lambda sh, cards: None)

    processed = google_sheets.process_pending_sheet_appends()

    assert processed == 3
    assert fake_client.open_by_key.call_count == 3
    assert {call.args[0] for call in fake_client.open_by_key.call_args_list} == {"sheet-batch-1"}
    assert fake_ws.append_rows.call_count == 3
    assert queue_mock.call_count == 3
    queued_target_tabs = {call.kwargs["targets"][0]["tab_name"] for call in queue_mock.call_args_list}
    queued_target_row_ids = {call.kwargs["targets"][0]["row_id"] for call in queue_mock.call_args_list}
    assert queued_target_tabs == {"__Raw Belgeler", "Faturalar", "Masraf Kayıtları"}
    assert queued_target_row_ids == set(pending_ids_by_tab.values())
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
    assert len(items) == 3
    items_by_tab = {item["tab_name"]: item for item in items}
    assert items_by_tab["__Raw Belgeler"]["attempts"] == 1
    assert "429" in items_by_tab["__Raw Belgeler"]["last_error"]
    assert float(items_by_tab["__Raw Belgeler"]["next_attempt_at"]) > 0
    assert items_by_tab["Faturalar"]["attempts"] == 0
    assert items_by_tab["Masraf Kayıtları"]["attempts"] == 0



def test_select_pending_sheet_batch_defers_banka_odemeleri_until_other_ready_tabs_are_processed(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))

    items = [
        {
            "id": "banka-1",
            "spreadsheet_id": "sheet-1",
            "month_key": "2026-04",
            "tab_name": "Banka Ödemeleri",
            "next_attempt_at": 0,
        },
        {
            "id": "masraf-1",
            "spreadsheet_id": "sheet-1",
            "month_key": "2026-04",
            "tab_name": "Masraf Kayıtları",
            "next_attempt_at": 0,
        },
    ]
    monkeypatch.setattr(google_sheets, "_load_pending_sheet_appends", lambda: items)

    batch = google_sheets._select_pending_sheet_batch(batch_size=10)

    assert [item["id"] for item in batch] == ["masraf-1"]


def test_reset_current_month_spreadsheet_data_clears_only_data_rows(monkeypatch):
    fake_summary_ws = MagicMock()
    fake_summary_ws.row_count = 1000
    fake_tab_wss = {}
    for tab_name in google_sheets._TABS:
        if tab_name == "📊 Özet":
            continue
        ws = MagicMock()
        ws.row_count = 1234
        fake_tab_wss[tab_name] = ws

    fake_sheet = MagicMock()
    fake_sheet.id = "sheet-reset-1"
    fake_client = MagicMock()
    fake_client.open_by_key.return_value = fake_sheet

    def _ensure_tab_exists(sh, tab_name, base_name=None):
        assert sh is fake_sheet
        if tab_name == "📊 Özet":
            return fake_summary_ws
        return fake_tab_wss[tab_name]

    recent_marks = []
    monkeypatch.setattr(google_sheets, "_get_client", lambda: fake_client)
    monkeypatch.setattr(google_sheets, "_ensure_tab_exists", _ensure_tab_exists)
    monkeypatch.setattr(google_sheets, "_mark_recently_prepared", lambda sh: recent_marks.append(sh.id))

    touched_tabs = google_sheets.reset_current_month_spreadsheet_data(spreadsheet_id="sheet-reset-1")

    assert touched_tabs == len(google_sheets._TABS)
    fake_client.open_by_key.assert_called_once_with("sheet-reset-1")
    fake_summary_ws.batch_clear.assert_not_called()
    for tab_name, ws in fake_tab_wss.items():
        ws.batch_clear.assert_called_once_with(
            [f"A3:{google_sheets._internal_row_id_column_letter(tab_name)}1234"]
        )
    assert recent_marks == ["sheet-reset-1"]


def test_append_record_skips_pending_payload_when_storage_budget_is_exceeded(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))
    monkeypatch.setattr(google_sheets.settings, "google_sheets_spreadsheet_id", "sheet-budget-1")
    monkeypatch.setattr(google_sheets.settings, "pending_payload_storage_limit_mb", 1)
    monkeypatch.setattr(google_sheets, "_get_client", lambda: object())
    monkeypatch.setattr(google_sheets, "start_pending_sheet_append_worker", lambda: None)

    record = google_sheets.BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-budget-1",
        document_date="2026-04-11",
        confidence=0.91,
    )

    google_sheets.append_record(
        record,
        google_sheets.DocumentCategory.FATURA,
        drive_link=None,
        pending_document_bytes=b"x" * (2 * 1024 * 1024),
        pending_document_filename="big.jpg",
        pending_document_mime_type="image/jpeg",
    )

    items = google_sheets._load_pending_sheet_appends()
    assert len(items) == 3
    assert all(item["document_payload_path"] == "" for item in items)
    assert list((Path(tmp_path) / "state" / "pending_sheet_appends").glob("*.bin")) == []


def test_load_pending_sheet_appends_skips_legacy_iade_items(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))
    queue_path = Path(tmp_path) / "state" / "pending_sheet_appends.json"
    queue_path.parent.mkdir(parents=True, exist_ok=True)
    queue_path.write_text(
        json.dumps(
            [
                {"id": "legacy-iade", "tab_name": "↩️ İadeler", "category": "iade"},
                {"id": "normal-1", "tab_name": "🧾 Faturalar", "category": "fatura"},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    items = google_sheets._load_pending_sheet_appends()

    assert [item["id"] for item in items] == ["normal-1"]



def test_archive_legacy_iade_tabs_renames_old_tab_once(monkeypatch):
    legacy_ws = MagicMock()
    legacy_ws.title = "↩️ İadeler"
    canonical_ws = MagicMock()
    canonical_ws.title = "🧾 Faturalar"

    monkeypatch.setattr(google_sheets, "_list_worksheets", lambda _sh: [legacy_ws, canonical_ws])

    archived = google_sheets._archive_legacy_iade_tabs(MagicMock())

    assert archived == ["↩️ İadeler LEGACY"]
    legacy_ws.update_title.assert_called_once_with("↩️ İadeler LEGACY")
    assert google_sheets._is_ignored_orphan_title("↩️ İadeler LEGACY") is True


def test_google_sheets_paths_are_namespaced_for_sandbox(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))

    production_registry = google_sheets._registry_path()
    assert production_registry == Path(tmp_path) / "state" / "sheets_registry.json"

    with pipeline_context_scope(sandbox_context(session_id="alpha")):
        sandbox_registry = google_sheets._registry_path()
        sandbox_sheet_queue = google_sheets._pending_sheet_appends_state_path()
        sandbox_drive_folder = google_sheets._month_drive_folder_name()

    assert sandbox_registry == Path(tmp_path) / "sandboxes" / "sandbox-alpha" / "state" / "sheets_registry.json"
    assert sandbox_sheet_queue == Path(tmp_path) / "sandboxes" / "sandbox-alpha" / "state" / "pending_sheet_appends.json"
    assert sandbox_drive_folder.startswith("[SANDBOX alpha] Fişler — ")


def test_start_pending_sheet_worker_is_noop_in_sandbox(monkeypatch):
    thread_started = {"count": 0}

    class FakeThread:
        def __init__(self, *args, **kwargs):
            thread_started["count"] += 1

        def start(self):
            thread_started["count"] += 100

        def is_alive(self):
            return False

    monkeypatch.setattr(google_sheets, "_pending_sheet_worker_thread", None)
    monkeypatch.setattr(google_sheets.threading, "Thread", FakeThread)

    with pipeline_context_scope(sandbox_context(session_id="alpha")):
        google_sheets.start_pending_sheet_append_worker()

    assert thread_started["count"] == 0


def test_process_pending_sheet_appends_repairs_target_tabs_before_append(tmp_path, monkeypatch):
    monkeypatch.setattr(google_sheets.settings, "storage_dir", str(tmp_path))
    monkeypatch.setattr(google_sheets.settings, "google_sheets_spreadsheet_id", "sheet-batch-audit")

    record = google_sheets.BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id="wamid-sheet-audit",
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
    fake_sheet = MagicMock()
    fake_sheet.worksheet.return_value = fake_ws
    fake_client = MagicMock()
    fake_client.open_by_key.return_value = fake_sheet

    audit_calls = []
    monkeypatch.setattr(google_sheets, "_get_client", lambda: fake_client)
    monkeypatch.setattr(google_sheets, "_audit_spreadsheet_layout", lambda sh, repair=False, target_tabs=None: audit_calls.append((sh, repair, target_tabs)) or [])

    monkeypatch.setattr(google_sheets, "_upsert_party_cards", lambda sh, cards: None)

    processed = google_sheets.process_pending_sheet_appends()

    assert processed == 3
    assert audit_calls == [
        (fake_sheet, True, {"__Raw Belgeler", "📊 Özet"}),
        (fake_sheet, True, {"Faturalar", "📊 Özet"}),
        (fake_sheet, True, {"Masraf Kayıtları", "📊 Özet"}),
    ]


def test_ensure_tab_total_row_rewrites_in_place_without_inserting_rows():
    fake_ws = MagicMock()

    google_sheets._ensure_tab_total_row(fake_ws, "🧾 Faturalar")

    fake_ws.update.assert_called_once_with(
        [google_sheets._total_row_values("🧾 Faturalar")],
        "A2",
        value_input_option="USER_ENTERED",
    )
    fake_ws.insert_row.assert_not_called()



def test_audit_missing_tab_repair_uses_lightweight_creation(monkeypatch):
    import gspread

    ensure_calls = []

    def fake_ensure_tab_exists(_sh, tab_name, base_name=None, *, lightweight=False):
        ensure_calls.append((tab_name, base_name, lightweight))
        return object()

    monkeypatch.setattr(google_sheets, "_ensure_tab_exists", fake_ensure_tab_exists)
    monkeypatch.setattr(
        google_sheets,
        "_get_worksheet",
        lambda _sh, _title: (_ for _ in ()).throw(gspread.WorksheetNotFound("missing")),
    )

    findings = []
    google_sheets._audit_data_tab(object(), "🧾 Faturalar", findings, repair=True)
    google_sheets._audit_summary_tab(object(), findings, repair=True)

    assert ("🧾 Faturalar", None, True) in ensure_calls
    assert ("📊 Özet", None, True) in ensure_calls
    assert findings[0]["repaired"] is True
    assert findings[1]["repaired"] is True


def test_audit_data_tab_uses_visible_schema_migration_for_legacy_headers(monkeypatch):
    ws = MagicMock()

    monkeypatch.setattr(google_sheets, "_get_worksheet", lambda _sh, _title: ws)
    monkeypatch.setattr(google_sheets, "_tab_headers_match", lambda _ws, _tab_name: False)
    monkeypatch.setattr(google_sheets, "_tab_headers_can_migrate_in_place", lambda _ws, _tab_name: True)
    monkeypatch.setattr(google_sheets, "_tab_total_row_is_valid", lambda _ws, _tab_name: True)
    monkeypatch.setattr(google_sheets, "_repair_drive_link_formulas", lambda _ws, _tab_name: None)
    monkeypatch.setattr(google_sheets, "_backfill_internal_row_ids", lambda _ws, _tab_name: 0)
    monkeypatch.setattr(google_sheets, "_restore_archived_drifted_visible_schema", lambda _sh, _ws, _tab_name: False)
    setup_calls = []
    migrate_calls = []
    monkeypatch.setattr(google_sheets, "_setup_worksheet", lambda _ws, _tab_name, *, lightweight=False: setup_calls.append((_tab_name, lightweight)))
    monkeypatch.setattr(google_sheets, "_rewrite_legacy_visible_schema_in_place", lambda _sh, _ws, _tab_name: migrate_calls.append(_tab_name) or True)
    monkeypatch.setattr(google_sheets, "_worksheet_has_visible_data", lambda _ws, _tab_name: True)
    monkeypatch.setattr(google_sheets, "_archive_drifted_tab", lambda _sh, _ws, _tab_name: (_ for _ in ()).throw(AssertionError("should not archive legacy visible schema")))

    findings = []
    google_sheets._audit_data_tab(object(), "Sevk Fişleri", findings, repair=True)

    assert migrate_calls == ["Sevk Fişleri"]
    assert setup_calls == []
    assert findings[0]["code"] == "header_drift"
    assert findings[0]["repaired"] is True



def test_restore_archived_drifted_visible_schema_uses_latest_archived_tab(monkeypatch):
    canonical_ws = MagicMock()
    canonical_ws.title = "Sevk Fişleri"
    archived_old = MagicMock()
    archived_old.title = "Sevk Fişleri MANUAL_DRIFT 20260412090000"
    archived_new = MagicMock()
    archived_new.title = "Sevk Fişleri MANUAL_DRIFT 20260413115348"

    monkeypatch.setattr(google_sheets, "_list_worksheets", lambda _sh: [canonical_ws, archived_old, archived_new])
    monkeypatch.setattr(google_sheets, "_remapped_legacy_visible_rows", lambda _sh, ws, tab_name: [[ws.title, tab_name]])
    applied = []
    monkeypatch.setattr(google_sheets, "_apply_remapped_visible_rows", lambda ws, tab_name, rows: applied.append((ws, tab_name, rows)))

    restored = google_sheets._restore_archived_drifted_visible_schema(object(), canonical_ws, "Sevk Fişleri")

    assert restored is True
    assert applied == [(canonical_ws, "Sevk Fişleri", [["Sevk Fişleri MANUAL_DRIFT 20260413115348", "Sevk Fişleri"]])]



def test_audit_data_tab_repair_does_not_reapply_formatting_by_default(monkeypatch):
    ws = MagicMock()

    monkeypatch.setattr(google_sheets, "_get_worksheet", lambda _sh, _title: ws)
    monkeypatch.setattr(google_sheets, "_tab_headers_match", lambda _ws, _tab_name: True)
    monkeypatch.setattr(google_sheets, "_tab_total_row_is_valid", lambda _ws, _tab_name: True)
    monkeypatch.setattr(google_sheets, "_repair_drive_link_formulas", lambda _ws, _tab_name: None)
    monkeypatch.setattr(google_sheets, "_backfill_internal_row_ids", lambda _ws, _tab_name: 0)
    monkeypatch.setattr(google_sheets, "_restore_archived_drifted_visible_schema", lambda _sh, _ws, _tab_name: False)
    setup_calls = []
    monkeypatch.setattr(google_sheets, "_setup_worksheet", lambda _ws, _tab_name, *, lightweight=False: setup_calls.append((_tab_name, lightweight)))

    findings = []
    google_sheets._audit_data_tab(object(), "Faturalar", findings, repair=True)

    assert findings == []
    assert setup_calls == []


def test_audit_data_tab_repair_reapplies_hidden_state_for_hidden_tabs(monkeypatch):
    ws = MagicMock()

    monkeypatch.setattr(google_sheets, "_get_worksheet", lambda _sh, _title: ws)
    monkeypatch.setattr(google_sheets, "_tab_headers_match", lambda _ws, _tab_name: True)
    monkeypatch.setattr(google_sheets, "_tab_total_row_is_valid", lambda _ws, _tab_name: True)
    monkeypatch.setattr(google_sheets, "_repair_drive_link_formulas", lambda _ws, _tab_name: None)
    monkeypatch.setattr(google_sheets, "_backfill_internal_row_ids", lambda _ws, _tab_name: 0)
    monkeypatch.setattr(google_sheets, "_restore_archived_drifted_visible_schema", lambda _sh, _ws, _tab_name: False)
    hidden_calls = []
    monkeypatch.setattr(google_sheets, "_set_worksheet_hidden", lambda _ws, *, hidden=True: hidden_calls.append(hidden))

    findings = []
    google_sheets._audit_data_tab(object(), "__Raw Belgeler", findings, repair=True)

    assert findings == []
    assert hidden_calls == [True]



def test_audit_data_tab_reapplies_formatting_when_explicitly_requested(monkeypatch):
    ws = MagicMock()

    monkeypatch.setattr(google_sheets, "_get_worksheet", lambda _sh, _title: ws)
    monkeypatch.setattr(google_sheets, "_tab_headers_match", lambda _ws, _tab_name: True)
    monkeypatch.setattr(google_sheets, "_tab_total_row_is_valid", lambda _ws, _tab_name: True)
    monkeypatch.setattr(google_sheets, "_repair_drive_link_formulas", lambda _ws, _tab_name: None)
    monkeypatch.setattr(google_sheets, "_backfill_internal_row_ids", lambda _ws, _tab_name: 0)
    monkeypatch.setattr(google_sheets, "_restore_archived_drifted_visible_schema", lambda _sh, _ws, _tab_name: False)
    setup_calls = []
    monkeypatch.setattr(google_sheets, "_setup_worksheet", lambda _ws, _tab_name, *, lightweight=False: setup_calls.append((_tab_name, lightweight)))

    findings = []
    google_sheets._audit_data_tab(object(), "Faturalar", findings, repair=True, refresh_formatting=True)

    assert findings == []
    assert setup_calls == [("Faturalar", False)]


def test_audit_summary_tab_repair_does_not_reapply_formatting_by_default(monkeypatch):
    ws = MagicMock()

    monkeypatch.setattr(google_sheets, "_get_worksheet", lambda _sh, _title: ws)
    monkeypatch.setattr(google_sheets, "_summary_tab_is_valid", lambda _ws: True)
    setup_calls = []
    monkeypatch.setattr(google_sheets, "_setup_summary_tab", lambda _ws, month_label, *, lightweight=False: setup_calls.append((month_label, lightweight)))
    monkeypatch.setattr(google_sheets, "_month_label", lambda: "Nisan 2026")

    findings = []
    google_sheets._audit_summary_tab(object(), findings, repair=True)

    assert findings == []
    assert setup_calls == []



def test_audit_summary_tab_reapplies_formatting_when_explicitly_requested(monkeypatch):
    ws = MagicMock()

    monkeypatch.setattr(google_sheets, "_get_worksheet", lambda _sh, _title: ws)
    monkeypatch.setattr(google_sheets, "_summary_tab_is_valid", lambda _ws: True)
    setup_calls = []
    monkeypatch.setattr(google_sheets, "_setup_summary_tab", lambda _ws, month_label, *, lightweight=False: setup_calls.append((month_label, lightweight)))
    monkeypatch.setattr(google_sheets, "_month_label", lambda: "Nisan 2026")

    findings = []
    google_sheets._audit_summary_tab(object(), findings, repair=True, refresh_formatting=True)

    assert findings == []
    assert setup_calls == [("Nisan 2026", False)]
