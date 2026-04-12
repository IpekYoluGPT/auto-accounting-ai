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
    assert google_sheets._build_tab_total_formula("🧾 Faturalar") == "=IFERROR(SUM(O3:O);0)"
    assert google_sheets._build_summary_formula("🧾 Faturalar") == "=IFERROR('Faturalar'!O2;0)"
    assert [tab_name for _, tab_name in google_sheets._summary_rows()] == [
        "Masraf Kayıtları",
        "Banka Ödemeleri",
        "Faturalar",
    ]



def test_active_layout_excludes_iade_tab_and_keeps_technical_tabs_hidden():
    assert "↩️ İadeler" not in google_sheets._TABS
    assert google_sheets._VISIBLE_TABS == [
        "Masraf Kayıtları",
        "Banka Ödemeleri",
        "Faturalar",
        "Sevk Fişleri",
    ]
    assert {"📊 Özet", "__Raw Belgeler", "__Fatura Kalemleri", "__Çek_Dekont_Detay", "__Cari_Kartlar", "__Ödeme_Dağıtımları"}.issubset(set(google_sheets._TABS))
    assert google_sheets._header_index("Faturalar", google_sheets._HIDDEN_DRIVE_LINK_HEADER) == 17


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


def test_ensure_tab_total_row_rewrites_row_two_when_existing_data_starts_on_row_two():
    ws = MagicMock()
    ws.row_values.return_value = ["1", "2026-04-01", "ABC"]

    google_sheets._ensure_tab_total_row(ws, "🧾 Faturalar")

    ws.insert_row.assert_not_called()
    ws.update.assert_called_once_with(
        [google_sheets._total_row_values("🧾 Faturalar")],
        "A2",
        value_input_option="USER_ENTERED",
    )


def test_elden_odeme_row_includes_drive_verification_cell():
    row = google_sheets._build_row_for_tab(
        BillRecord(
            document_date="2026-04-09",
            document_time="10:30",
            description="Kasadan odeme",
            total_amount=1500.0,
            currency="TRY",
            source_sender_id="905551112233",
        ),
        "Masraf Kayıtları",
        category=DocumentCategory.ELDEN_ODEME,
        row_id="row-1",
        row_number=3,
        drive_link="https://drive.google.com/file/d/example/view",
        source_doc_id="row-1",
    )

    assert row[2] == "Kasadan odeme"
    assert row[5] == 1500.0
    assert row[8] == '=HYPERLINK("https://drive.google.com/file/d/example/view";"Görüntüle")'
    assert row[9] == "row-1"


def test_dekont_row_prefers_sender_name_over_phone_number():
    row = google_sheets._build_row_for_tab(
        BillRecord(
            document_date="2026-04-09",
            document_time="10:30",
            company_name="Yapı Kredi",
            document_number="REF-123",
            description="Alıcı: Mehmet Demir",
            total_amount=2500.0,
            currency="TRY",
            sender_name="Ahmet Yılmaz",
            source_sender_name="Meta Profil",
            source_sender_id="905551112233",
        ),
        "__Çek_Dekont_Detay",
        category=DocumentCategory.ODEME_DEKONTU,
        row_id="row-1",
        row_number=3,
        drive_link=None,
        source_doc_id="row-1",
    )

    assert row[3] == "Ahmet Yılmaz"


def test_dekont_row_falls_back_to_source_sender_name_but_not_phone_number():
    row = google_sheets._build_row_for_tab(
        BillRecord(
            document_date="2026-04-09",
            document_time="10:30",
            company_name="Yapı Kredi",
            document_number="REF-123",
            description="Alıcı: Mehmet Demir",
            total_amount=2500.0,
            currency="TRY",
            source_sender_name="Ayşe Demir",
            source_sender_id="905551112233",
        ),
        "__Çek_Dekont_Detay",
        category=DocumentCategory.ODEME_DEKONTU,
        row_id="row-1",
        row_number=3,
        drive_link=None,
        source_doc_id="row-1",
    )

    assert row[3] == "Ayşe Demir"


def test_repair_drive_link_formulas_rewrites_old_comma_separator():
    ws = MagicMock()
    ws.get.return_value = [
        ['=HYPERLINK("https://drive.google.com/file/d/a/view","Görüntüle")'],
        ['=HYPERLINK("https://drive.google.com/file/d/b/view";"Görüntüle")'],
        [""],
    ]

    google_sheets._repair_drive_link_formulas(ws, "💳 Dekontlar")

    ws.update.assert_called_once_with(
        [['=HYPERLINK("https://drive.google.com/file/d/a/view";"Görüntüle")']],
        "I3",
        value_input_option="USER_ENTERED",
    )


def test_create_spreadsheet_sets_turkish_locale_and_timezone():
    client = MagicMock()
    spreadsheet = MagicMock()
    client.open_by_key.return_value = spreadsheet

    create_execute = MagicMock(return_value={"spreadsheetId": "sheet-123"})
    create_call = MagicMock(return_value=MagicMock(execute=create_execute))
    sheets_service = MagicMock()
    sheets_service.spreadsheets.return_value.create = create_call

    with patch(
        "app.services.providers.google_sheets._get_oauth_sheets_service",
        return_value=sheets_service,
    ), patch(
        "app.services.providers.google_sheets._get_sheets_service",
        return_value=None,
    ), patch(
        "app.services.providers.google_sheets._get_oauth_drive_service",
        return_value=None,
    ), patch(
        "app.services.providers.google_sheets._get_drive_service",
        return_value=None,
    ), patch(
        "app.services.providers.google_sheets.settings.google_drive_parent_folder_id",
        "",
    ), patch(
        "app.services.providers.google_sheets._bootstrap_spreadsheet_tabs",
    ):
        sheet_id = google_sheets._create_and_setup_spreadsheet(client, "Muhasebe — Nisan 2026")

    assert sheet_id == "sheet-123"
    create_call.assert_called_once_with(
        body={
            "properties": {
                "title": "Muhasebe — Nisan 2026",
                "locale": "tr_TR",
                "timeZone": "Europe/Istanbul",
            }
        },
        fields="spreadsheetId",
    )


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


def test_google_sheets_startup_bootstrap_runs_preparation_and_queue_drain_steps():
    with patch(
        "app.main.google_sheets.ensure_current_month_spreadsheet_ready"
    ) as ensure_mock, patch(
        "app.main.google_sheets.process_pending_sheet_appends"
    ) as pending_sheet_mock, patch(
        "app.main.google_sheets.process_pending_document_uploads"
    ) as pending_drive_mock:
        from app.main import _run_google_sheets_startup_tasks

        _run_google_sheets_startup_tasks()

    ensure_mock.assert_called_once_with()
    pending_sheet_mock.assert_called_once_with()
    pending_drive_mock.assert_called_once_with()


def test_app_startup_prepares_current_month_sheet_and_scheduler():
    with patch(
        "app.main._start_google_sheets_bootstrap"
    ) as start_bootstrap_mock, patch(
        "app.main.google_sheets.start_pending_sheet_append_worker"
    ) as start_pending_sheet_mock, patch(
        "app.main.google_sheets.start_pending_drive_upload_worker"
    ) as start_pending_drive_mock, patch(
        "app.main.google_sheets.start_monthly_rollover_scheduler"
    ) as start_mock, patch(
        "app.main.google_sheets.stop_monthly_rollover_scheduler"
    ) as stop_mock:
        with TestClient(app):
            pass

    start_bootstrap_mock.assert_called_once_with()
    start_pending_sheet_mock.assert_called_once_with()
    start_pending_drive_mock.assert_called_once_with()
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
        result = google_sheets.ensure_current_month_spreadsheet_ready()

    assert result == "sheet-123"
    repair_mock.assert_not_called()
