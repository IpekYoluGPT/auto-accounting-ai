"""
Tests for Google Sheets monthly rollover behavior.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from app.main import app
from app.models.schemas import BillRecord, DocumentCategory, InvoiceLineItem
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
    assert google_sheets._header_index("Faturalar", google_sheets._VISIBLE_DRIVE_LINK_HEADER) == 17
    assert google_sheets._header_index("Faturalar", google_sheets._HIDDEN_DRIVE_LINK_HEADER) is None
    assert google_sheets._drive_column_letter("Faturalar") == "R"


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


def test_invoice_row_uses_single_line_item_fallbacks_for_visible_columns():
    row = google_sheets._build_row_for_tab(
        BillRecord(
            document_date="2026-04-03",
            invoice_type="Toptan Satış Faturası",
            company_name="YILMAZ YAPIM",
            total_amount=750.0,
            vat_rate=0.0,
            vat_amount=0.0,
            line_items=[
                InvoiceLineItem(
                    description="ARKİM ARPLAST HAND-EL SIVASI 7111 25 KG",
                    quantity=5.0,
                    unit="TRB",
                    unit_price=150.0,
                    line_amount=750.0,
                )
            ],
        ),
        "Faturalar",
        category=DocumentCategory.FATURA,
        row_id="row-1",
        row_number=3,
        drive_link="https://drive.google.com/file/d/example/view",
        source_doc_id="row-1",
    )

    assert row[6] == "ARKİM ARPLAST HAND-EL SIVASI 7111 25 KG"
    assert row[7] == 5.0
    assert row[8] == 150.0
    assert row[9] == 750.0
    assert row[17] == '=HYPERLINK("https://drive.google.com/file/d/example/view";"Görüntüle")'



def test_invoice_row_uses_line_item_summary_when_description_is_generic():
    row = google_sheets._build_row_for_tab(
        BillRecord(
            document_date="2026-02-28",
            invoice_type="Toptan Satış Faturası",
            company_name="ŞEMSETTİN YILMAZ",
            subtotal=3214.95,
            vat_rate=20.0,
            vat_amount=642.99,
            total_amount=3857.94,
            description="Toptan Satış Faturası",
            line_items=[
                InvoiceLineItem(
                    description="PVC ATIK SU BORUSU TİP1 50/500 MM",
                    quantity=10.0,
                    unit="ADET",
                    unit_price=69.92,
                    line_amount=699.20,
                ),
                InvoiceLineItem(
                    description="PVC ATIK SU BORUSU TİP1 50/250 MM",
                    quantity=10.0,
                    unit="ADET",
                    unit_price=43.09,
                    line_amount=430.90,
                ),
                InvoiceLineItem(
                    description="PVC ATIK SU DİRSEK 87* 50 MM",
                    quantity=20.0,
                    unit="ADET",
                    unit_price=27.56,
                    line_amount=551.20,
                ),
            ],
        ),
        "Faturalar",
        category=DocumentCategory.FATURA,
        row_id="row-2",
        row_number=4,
        drive_link=None,
        source_doc_id="row-2",
    )

    assert row[6] == "PVC ATIK SU BORUSU TİP1 50/500 MM, PVC ATIK SU BORUSU TİP1 50/250 MM +1 kalem"
    assert row[7] == ""
    assert row[8] == ""
    assert row[9] == 3214.95


def test_invoice_row_uses_currency_and_extra_detail_visible_columns():
    row = google_sheets._build_row_for_tab(
        BillRecord(
            document_date="2026-04-03",
            invoice_type="Toptan Satış Faturası",
            company_name="YILMAZ YAPIM",
            total_amount=750.0,
            payable_amount=750.0,
            currency="USD",
            bank_name="Yapı Kredi",
            iban="TR123",
            notes="Vadeli ödeme",
        ),
        "Faturalar",
        category=DocumentCategory.FATURA,
        row_id="row-extra-1",
        row_number=3,
        drive_link=None,
        source_doc_id="row-extra-1",
    )

    assert row[15] == "USD"
    assert row[16] == "Banka: Yapı Kredi | IBAN: TR123 | Not: Vadeli ödeme"



def test_sevk_row_uses_dense_visible_columns_and_summary_detail():
    row = google_sheets._build_row_for_tab(
        BillRecord(
            document_number="9677",
            document_date="2026-03-07",
            company_name="KUM - ÇAKIL - HAFRİYAT",
            recipient_name="H. Karakaya İnş.",
            description="SİYAH KUM",
            product_quantity=18.0,
            shipment_destination="KARAKAYA İNŞ Kuzey Organize",
            shipment_origin="ELAZIĞ",
            vehicle_plate="23ABC123",
            pallet_count=3,
            items_per_pallet=6,
            notes="Saha teslim",
        ),
        "Sevk Fişleri",
        category=DocumentCategory.MALZEME,
        row_id="row-sevk-1",
        row_number=3,
        drive_link=None,
        source_doc_id="row-sevk-1",
    )

    assert row[:8] == [
        "9677",
        "2026-03-07",
        "KUM - ÇAKIL - HAFRİYAT",
        "H. Karakaya İnş.",
        "SİYAH KUM",
        18.0,
        "KARAKAYA İNŞ Kuzey Organize",
        "Çıkış: ELAZIĞ | Plaka: 23ABC123 | Palet: 3 | Adet/Palet: 6 | Not: Saha teslim",
    ]



def test_payment_allocation_row_uses_reference_and_sender_columns():
    row = google_sheets._build_payment_allocation_row(
        party_name="MUZAFFER KARAKAŞ",
        description="GİDEN FAST",
        reference_number="581829",
        sender_name="Yapı Kredi",
        payment_amount=444000.0,
        payment_date="2026-08-30",
        remaining_balance=444000.0,
        status="Eşleşmedi",
        drive_link=None,
        row_id="odeme-1",
        party_key="party-1",
        source_doc_id="doc-1",
        debt_row_id="",
    )

    assert row[:4] == ["MUZAFFER KARAKAŞ", "GİDEN FAST", "581829", "Yapı Kredi"]



def test_remap_legacy_fatura_row_replaces_sparse_bank_columns_with_dense_fields():
    legacy_headers = google_sheets._legacy_header_variants("Faturalar")[0]
    row = [
        "81",
        "2026-03-18",
        "Toptan Satış İade",
        "ŞEMSETTİN YILMAZ",
        "4540007255",
        "H.KARAKAYA",
        "PVC ATIK SU BORUSU",
        "13",
        "1051.86",
        "6837.09",
        "20",
        "1367.42",
        "HAYIR",
        "0",
        "8204.51",
        "TR-OLD",
        "OLD BANK",
        '=HYPERLINK("https://drive.google.com/file/d/test/view";"Görüntüle")',
        "row-1",
        "party-1",
        "doc-1",
        "4540007255",
        "fatura",
    ]

    remapped = google_sheets._remap_legacy_visible_row(
        "Faturalar",
        row,
        legacy_headers=legacy_headers,
        raw_by_doc_id={
            "doc-1": {
                "Para Birimi": "TRY",
                "Banka": "Yapı Kredi",
                "IBAN": "TR123",
                "Notlar": "İade notu",
            }
        },
        payment_detail_by_doc_id={},
    )

    assert remapped[15] == "TRY"
    assert remapped[16] == "Banka: Yapı Kredi | IBAN: TR123 | Not: İade notu"
    assert remapped[17] == '=HYPERLINK("https://drive.google.com/file/d/test/view";"Görüntüle")'



def test_formula_arg_separator_uses_comma_for_english_locale():
    spreadsheet = MagicMock()
    spreadsheet.id = 'sheet-en'
    spreadsheet.locale = 'en_US'

    assert google_sheets._formula_arg_separator(spreadsheet=spreadsheet) == ','



def test_drive_cell_uses_comma_separator_for_english_locale():
    spreadsheet = MagicMock()
    spreadsheet.id = 'sheet-en'
    spreadsheet.locale = 'en_US'

    assert google_sheets._drive_cell('https://drive.google.com/file/d/example/view', spreadsheet=spreadsheet) == '=HYPERLINK("https://drive.google.com/file/d/example/view","Görüntüle")'



def test_repair_drive_link_formulas_rewrites_old_semicolon_separator_for_comma_locale():
    ws = MagicMock()
    ws.spreadsheet.locale = 'en_US'
    ws.get.side_effect = [
        [
            ['=HYPERLINK("https://drive.google.com/file/d/a/view";"Görüntüle")'],
        ],
        [
            ['=HYPERLINK("https://drive.google.com/file/d/a/view";"Görüntüle")'],
        ],
    ]

    google_sheets._repair_drive_link_formulas(ws, 'Faturalar')

    ws.update.assert_called_once_with(
        [['=HYPERLINK("https://drive.google.com/file/d/a/view","Görüntüle")']],
        'R3',
        value_input_option='USER_ENTERED',
    )


def test_repair_drive_link_formulas_rewrites_old_comma_separator():
    ws = MagicMock()
    ws.get.side_effect = [
        [
            ['=HYPERLINK("https://drive.google.com/file/d/a/view","Görüntüle")'],
            ['=HYPERLINK("https://drive.google.com/file/d/b/view";"Görüntüle")'],
            [""],
        ],
        [
            ['=HYPERLINK("https://drive.google.com/file/d/a/view","Görüntüle")'],
            ['Görüntüle'],
            [""],
        ],
    ]

    google_sheets._repair_drive_link_formulas(ws, "💳 Dekontlar")

    ws.update.assert_called_once_with(
        [['=HYPERLINK("https://drive.google.com/file/d/a/view";"Görüntüle")']],
        "I3",
        value_input_option="USER_ENTERED",
    )


def test_repair_drive_link_formulas_rewrites_formula_like_text_even_with_current_separator():
    ws = MagicMock()
    ws.spreadsheet.locale = 'en_US'
    ws.get.side_effect = [
        [
            ['=HYPERLINK("https://drive.google.com/file/d/a/view","Görüntüle")'],
        ],
        [
            ['=HYPERLINK("https://drive.google.com/file/d/a/view","Görüntüle")'],
        ],
    ]

    google_sheets._repair_drive_link_formulas(ws, 'Faturalar')

    ws.update.assert_called_once_with(
        [['=HYPERLINK("https://drive.google.com/file/d/a/view","Görüntüle")']],
        'R3',
        value_input_option='USER_ENTERED',
    )


def test_repair_drive_link_formulas_skips_real_formula_cells():
    ws = MagicMock()
    ws.spreadsheet.locale = 'en_US'
    ws.get.side_effect = [
        [
            ['=HYPERLINK("https://drive.google.com/file/d/a/view","Görüntüle")'],
        ],
        [
            ['Görüntüle'],
        ],
    ]

    google_sheets._repair_drive_link_formulas(ws, 'Faturalar')

    ws.update.assert_not_called()


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


def test_ensure_current_month_spreadsheet_ready_runs_lightweight_repair_for_recent_bootstrap():
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
    ) as repair_mock, patch(
        "app.services.providers.google_sheets._audit_spreadsheet_layout",
    ) as audit_mock:
        result = google_sheets.ensure_current_month_spreadsheet_ready()

    assert result == "sheet-123"
    repair_mock.assert_not_called()
    audit_mock.assert_called_once_with(fake_sheet, repair=True, refresh_formatting=False)


def test_archive_drifted_tab_hides_archived_copy(monkeypatch):
    ws = MagicMock()
    ws.title = "Masraf Kayıtları"
    ws.id = 42
    ws.spreadsheet = MagicMock()

    monkeypatch.setattr(google_sheets, "_list_worksheets", lambda _sh: [])

    archived_title = google_sheets._archive_drifted_tab(MagicMock(), ws, "Masraf Kayıtları")

    assert archived_title.startswith("Masraf Kayıtları MANUAL_DRIFT ")
    ws.update_title.assert_called_once_with(archived_title)
    ws.spreadsheet.batch_update.assert_called_once()
    assert google_sheets._is_ignored_orphan_title(archived_title) is True


def test_archive_legacy_iade_tabs_hides_archived_sheet(monkeypatch):
    legacy_ws = MagicMock()
    legacy_ws.title = "↩️ İadeler"
    legacy_ws.id = 7
    legacy_ws.spreadsheet = MagicMock()
    canonical_ws = MagicMock()
    canonical_ws.title = "Faturalar"

    monkeypatch.setattr(google_sheets, "_list_worksheets", lambda _sh: [legacy_ws, canonical_ws])

    archived = google_sheets._archive_legacy_iade_tabs(MagicMock())

    assert archived == ["↩️ İadeler LEGACY"]
    legacy_ws.update_title.assert_called_once_with("↩️ İadeler LEGACY")
    legacy_ws.spreadsheet.batch_update.assert_called_once()


def test_audit_repair_hides_ignored_orphan_tabs(monkeypatch):
    archived_ws = MagicMock()
    archived_ws.title = "Masraf Kayıtları MANUAL_DRIFT 20260412092926"
    archived_ws.id = 99
    archived_ws.spreadsheet = MagicMock()

    monkeypatch.setattr(google_sheets, "_list_worksheets", lambda _sh: [archived_ws])
    monkeypatch.setattr(google_sheets, "_audit_data_tab", lambda sh, tab_name, findings, repair, refresh_formatting=False: None)
    monkeypatch.setattr(google_sheets, "_audit_summary_tab", lambda sh, findings, repair, refresh_formatting=False: None)
    monkeypatch.setattr(google_sheets, "_archive_legacy_iade_tabs", lambda sh: [])

    findings = google_sheets._audit_spreadsheet_layout(MagicMock(), repair=True)

    assert findings == []
    archived_ws.spreadsheet.batch_update.assert_called_once()
