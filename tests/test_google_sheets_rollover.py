"""
Tests for Google Sheets monthly rollover behavior.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from app.main import app
from app.models.schemas import BillRecord, DocumentCategory
from app.services.providers import google_sheets


def test_next_month_rollover_boundary_uses_configured_timezone():
    tz = ZoneInfo("Europe/Istanbul")
    now = datetime(2026, 4, 30, 23, 59, 30, tzinfo=tz)

    rollover_at = google_sheets._next_month_rollover_at(now)
    seconds = google_sheets._seconds_until_next_month_rollover(now)

    assert rollover_at == datetime(2026, 5, 1, 0, 0, 0, tzinfo=tz)
    assert seconds == 30.0


def test_tab_total_and_summary_formulas_use_dedicated_total_cells():
    assert google_sheets._build_tab_total_formula("🧾 Faturalar") == "=IFERROR(SUM(K3:K),0)"
    assert google_sheets._build_summary_formula("🧾 Faturalar") == "=IFERROR('🧾 Faturalar'!K2,0)"


def test_month_drive_folder_name_uses_fisler_prefix():
    with patch(
        "app.services.providers.google_sheets._month_label",
        return_value="Nisan 2026",
    ):
        assert google_sheets._month_drive_folder_name() == "Fişler — Nisan 2026"


def test_next_seq_ignores_header_and_total_rows():
    ws = MagicMock()
    ws.col_values.return_value = ["#", "TOPLAM", "1", "2", "3"]

    assert google_sheets._next_seq(ws) == 4


def test_ensure_tab_total_row_inserts_row_when_existing_data_starts_on_row_two():
    ws = MagicMock()
    ws.row_values.return_value = ["1", "2026-04-01", "ABC"]

    google_sheets._ensure_tab_total_row(ws, "🧾 Faturalar")

    ws.insert_row.assert_called_once()
    ws.update.assert_called_once_with(
        [["TOPLAM", "", "", "", "", "", "", "", "", "", "=IFERROR(SUM(K3:K),0)", "", "", "", "", ""]],
        "A2",
        value_input_option="USER_ENTERED",
    )


def test_elden_odeme_row_includes_drive_verification_cell():
    row = google_sheets._build_row(
        BillRecord(
            document_date="2026-04-09",
            document_time="10:30",
            description="Kasadan odeme",
            total_amount=1500.0,
            currency="TRY",
            source_sender_id="905551112233",
        ),
        DocumentCategory.ELDEN_ODEME,
        seq=1,
        drive_link="https://drive.google.com/file/d/example/view",
    )

    assert row[-1] == '=HYPERLINK("https://drive.google.com/file/d/example/view","📄 Görüntüle")'
    assert google_sheets._TABS["💵 Elden Ödemeler"][0][-1] == "📎 Belge"


def test_get_or_create_spreadsheet_creates_new_month_without_overwriting_old_registry():
    client = MagicMock()
    spreadsheet = MagicMock()
    client.open_by_key.return_value = spreadsheet

    with patch(
        "app.services.providers.google_sheets._load_registry",
        return_value={"2026-03": "sheet-march", "permanent": "legacy-sheet"},
    ), patch(
        "app.services.providers.google_sheets._save_registry",
    ) as save_registry_mock, patch(
        "app.services.providers.google_sheets._month_key",
        return_value="2026-04",
    ), patch(
        "app.services.providers.google_sheets._month_label",
        return_value="Nisan 2026",
    ), patch(
        "app.services.providers.google_sheets.settings.google_sheets_spreadsheet_id",
        "",
    ), patch(
        "app.services.providers.google_sheets.settings.google_sheets_owner_email",
        "",
    ), patch(
        "app.services.providers.google_sheets._find_existing_spreadsheet_in_drive",
        return_value=None,
    ), patch(
        "app.services.providers.google_sheets._try_create_spreadsheet_in_drive",
        return_value="sheet-april",
    ), patch(
        "app.services.providers.google_sheets._bootstrap_spreadsheet_tabs",
    ) as bootstrap_mock:
        result = google_sheets._get_or_create_spreadsheet(client)

    assert result is spreadsheet
    save_registry_mock.assert_called_once_with(
        {"2026-03": "sheet-march", "2026-04": "sheet-april"}
    )
    client.open_by_key.assert_called_once_with("sheet-april")
    bootstrap_mock.assert_called_once_with(spreadsheet)


def test_app_startup_prepares_current_month_sheet_and_scheduler():
    with patch(
        "app.main.google_sheets.ensure_current_month_spreadsheet_ready"
    ) as ensure_mock, patch(
        "app.main.google_sheets.process_pending_document_uploads"
    ) as pending_mock, patch(
        "app.main.google_sheets.start_monthly_rollover_scheduler"
    ) as start_mock, patch(
        "app.main.google_sheets.stop_monthly_rollover_scheduler"
    ) as stop_mock:
        with TestClient(app):
            pass

    ensure_mock.assert_called_once_with()
    pending_mock.assert_called_once_with()
    start_mock.assert_called_once_with()
    stop_mock.assert_called_once_with()


def test_ensure_current_month_spreadsheet_ready_skips_immediate_repair_for_recent_bootstrap():
    fake_client = MagicMock()
    fake_sheet = MagicMock()
    fake_sheet.id = "sheet-123"

    google_sheets._recently_prepared_spreadsheets.clear()
    google_sheets._mark_recently_prepared(fake_sheet)

    with patch(
        "app.services.providers.google_sheets._get_client",
        return_value=fake_client,
    ), patch(
        "app.services.providers.google_sheets._get_or_create_spreadsheet",
        return_value=fake_sheet,
    ), patch(
        "app.services.providers.google_sheets._repair_monthly_spreadsheet_layout",
    ) as repair_mock:
        google_sheets.ensure_current_month_spreadsheet_ready()

    repair_mock.assert_not_called()
