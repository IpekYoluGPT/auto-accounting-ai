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
        "Çekler",
        "Faturalar",
        "Sevk Fişleri",
    ]
    assert {"📊 Özet", "__Raw Belgeler", "__Fatura Kalemleri", "__Çek_Dekont_Detay", "__Cari_Kartlar", "__Ödeme_Dağıtımları"}.issubset(set(google_sheets._TABS))
    assert google_sheets._header_index("Faturalar", google_sheets._VISIBLE_DRIVE_LINK_HEADER) == 17
    assert google_sheets._header_index("Faturalar", google_sheets._HIDDEN_DRIVE_LINK_HEADER) is None
    assert google_sheets._drive_column_letter("Faturalar") == "R"
    assert google_sheets._canonical_tab_name("📝 Çekler") == "Çekler"
    assert google_sheets._visible_headers("Çekler")[:4] == [
        "Lehdar",
        "Açıklama",
        "Çek No",
        "Çeki Düzenleyen",
    ]


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
    assert row[7] == "5 TRB"
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
    assert row[7] == "3 kalem"
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
            line_unit="m3",
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
        "18m3",
        "KARAKAYA İNŞ Kuzey Organize",
        "Çıkış: ELAZIĞ | Plaka: 23ABC123 | Palet: 3 | Adet/Palet: 6 | Not: Saha teslim",
    ]



def test_payment_allocation_row_uses_reference_and_sender_columns():
    row = google_sheets._build_payment_allocation_row(
        party_name="MUZAFFER KARAKAŞ",
        description="GİDEN FAST",
        reference_number="581829",
        sender_name="Yapı Kredi",
        sender_iban="TR330001100000000000000001",
        recipient_iban="TR440001100000000000000002",
        bank_name="Garanti Bankası",
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

    assert row[:7] == [
        "MUZAFFER KARAKAŞ",
        "GİDEN FAST",
        "581829",
        "Yapı Kredi",
        "TR330001100000000000000001",
        "TR440001100000000000000002",
        "Garanti Bankası",
    ]
    assert row[8] == "2026-08-30"


def test_expense_row_preserves_exact_visible_receipt_line_details():
    row = google_sheets._build_row_for_tab(
        BillRecord(
            document_date="2026-03-05",
            receipt_number="0007",
            company_name="ZAİMOĞLU PETROL",
            total_amount=4700.38,
            description="Akaryakıt alımı",
            line_items=[
                InvoiceLineItem(
                    description="MOT V MAX E Dz10%20",
                    quantity=75.170,
                    unit="LT",
                    unit_price=62.53,
                    line_amount=4700.38,
                ),
            ],
        ),
        "Masraf Kayıtları",
        category=DocumentCategory.HARCAMA_FISI,
        row_id="fuel-1",
        row_number=3,
        drive_link=None,
        source_doc_id="fuel-1",
    )

    assert row[3] == "75.17 LT X 62.53 | MOT V MAX E Dz10%20"


def test_lightweight_layout_formats_date_columns_as_day_month_year():
    ws = MagicMock()
    ws.id = 123
    ws.title = "Çekler"

    google_sheets._apply_lightweight_layout(ws, "Çekler")

    requests = ws.spreadsheet.batch_update.call_args.args[0]["requests"]
    payment_date_index = google_sheets._header_index("Çekler", "Ödeme Tarihi")
    assert any(
        request.get("repeatCell", {}).get("range", {}).get("startColumnIndex") == payment_date_index
        and request["repeatCell"]["cell"]["userEnteredFormat"]["numberFormat"] == {
            "type": "DATE",
            "pattern": "dd-mm-yyyy",
        }
        for request in requests
    )


def test_build_payment_projection_rows_marks_borc_yok_when_party_matches_but_open_debt_missing():
    rows, allocations, cards = google_sheets._build_payment_projection_rows(
        record=BillRecord(
            document_date="2026-03-31",
            total_amount=-8008.37,
            tax_number="4540007255",
            recipient_name="MUZAFFER KARAKAŞ",
            sender_name="H.KARAKAYA İNŞ.TİC.SAN.LTD.ŞTİ.",
            description="GİDEN FAST",
        ),
        category=DocumentCategory.ODEME_DEKONTU,
        item_id="odeme-1",
        debt_state=[
            {
                "row_id": "debt-1",
                "party_key": "tax:4540007255",
                "display_name": "ŞEMSETTİN YILMAZ",
                "tax_number": "4540007255.0",
                "date": "2026-03-18",
                "original_amount": -8204.51,
                "remaining_amount": -8204.51,
                "aliases": ("H.KARAKAYA İNŞAAT TİCARET SAN.TİC.LTD.ŞTİ.",),
                "sort_index": 0,
            },
        ],
        drive_link=None,
    )

    assert allocations == []
    assert len(rows) == 1
    assert cards[0]["party_key"] == "tax:4540007255"
    assert rows[0][9] == 0
    assert rows[0][10] == "Borç Yok"
    assert rows[0][7] == -8008.37
    assert rows[0][13] == "tax:4540007255"


def test_build_payment_projection_rows_uses_receivable_side_for_cheque_matching():
    rows, allocations, cards = google_sheets._build_payment_projection_rows(
        record=BillRecord(
            cheque_due_date="2026-08-30",
            total_amount=4000.0,
            tax_number="4960863229",
            company_name="KANSA GRUP GIDA TİCARET VE SANAYİ LİMİTED ŞİRKETİ",
            sender_name="KANSA GRUP GIDA TİCARET VE SANAYİ LİMİTED ŞİRKETİ",
            recipient_name="H. KARAKAYA İNŞAAT TİC. VE SAN. LTD. ŞTİ.",
            cheque_serial_number="581829",
        ),
        category=DocumentCategory.CEK,
        item_id="cek-1",
        debt_state=[
            {
                "row_id": "recv-1",
                "party_key": "tax:4960863229",
                "display_name": "KANSA GRUP GIDA TİCARET VE SANAYİ LİMİTED ŞİRKETİ",
                "tax_number": "4960863229.0",
                "date": "2026-03-01",
                "original_amount": -8204.51,
                "remaining_amount": -8204.51,
                "aliases": ("KANSA GRUP GIDA",),
                "sort_index": 0,
            },
        ],
        drive_link=None,
    )

    assert len(rows) == 1
    assert len(allocations) == 1
    assert rows[0][0] == "H. KARAKAYA İNŞAAT TİC. VE SAN. LTD. ŞTİ."
    assert rows[0][3] == "KANSA GRUP GIDA TİCARET VE SANAYİ LİMİTED ŞİRKETİ"
    assert cards[0]["party_key"] == "tax:4960863229"
    assert rows[0][4] == 4000.0
    assert rows[0][6] == 4204.51
    assert rows[0][7] == "Kısmi"
    assert rows[0][8] == ""
    assert rows[0][11] == "tax:4960863229"
    assert allocations[0][2] == "receivable:recv-1"
    assert allocations[0][7] == 4000.0


def test_cheque_projection_uses_printed_company_as_drawer_when_sender_missing():
    rows, _, _ = google_sheets._build_payment_projection_rows(
        record=BillRecord(
            cheque_due_date="2026-08-30",
            total_amount=1500000.0,
            company_name="BERAT DEMİRCİ DENBER TAAHHÜT İNŞAAT",
            recipient_name="H. KARAKAYA İNŞAAT TİC. LTD. ŞTİ.",
            cheque_serial_number="0137440",
            cheque_bank_name="Ziraat Bankası",
        ),
        category=DocumentCategory.CEK,
        item_id="cek-drawer-1",
        debt_state=[],
        drive_link=None,
    )

    assert rows[0][0] == "H. KARAKAYA İNŞAAT TİC. LTD. ŞTİ."
    assert rows[0][3] == "BERAT DEMİRCİ DENBER TAAHHÜT İNŞAAT"


def test_build_payment_projection_rows_allocates_negative_outgoing_payment_by_absolute_amount():
    rows, allocations, cards = google_sheets._build_payment_projection_rows(
        record=BillRecord(
            document_date="2026-02-23",
            total_amount=-40.0,
            recipient_name="ABC Market",
            description="HESAPTAN FAST",
        ),
        category=DocumentCategory.ODEME_DEKONTU,
        item_id="odeme-neg-1",
        debt_state=[
            {
                "row_id": "debt-1",
                "party_key": "name:abc market",
                "display_name": "ABC Market",
                "tax_number": "",
                "date": "2026-02-01",
                "original_amount": 100.0,
                "remaining_amount": 100.0,
                "aliases": (),
                "sort_index": 0,
            },
        ],
        drive_link=None,
    )

    assert len(rows) == 1
    assert cards[0]["party_key"] == "name:abc market"
    assert allocations[0][7] == 40.0
    assert rows[0][7] == -40.0
    assert rows[0][9] == 60.0
    assert rows[0][10] == "Kısmi"


def test_build_payment_projection_rows_keeps_single_visible_row_for_multi_debt_allocation():
    rows, allocations, cards = google_sheets._build_payment_projection_rows(
        record=BillRecord(
            document_date="2026-03-31",
            total_amount=8008.37,
            tax_number="4540007255",
            recipient_name="MUZAFFER KARAKAŞ",
            sender_name="H.KARAKAYA İNŞ.TİC.SAN.LTD.ŞTİ.",
            description="GİDEN FAST",
        ),
        category=DocumentCategory.ODEME_DEKONTU,
        item_id="odeme-multi-1",
        debt_state=[
            {
                "row_id": "debt-1",
                "party_key": "tax:4540007255",
                "display_name": "ŞEMSETTİN YILMAZ",
                "tax_number": "4540007255.0",
                "date": "2026-03-18",
                "original_amount": 3857.94,
                "remaining_amount": 3857.94,
                "aliases": (),
                "sort_index": 0,
            },
            {
                "row_id": "debt-2",
                "party_key": "tax:4540007255",
                "display_name": "ŞEMSETTİN YILMAZ",
                "tax_number": "4540007255.0",
                "date": "2026-03-28",
                "original_amount": 8204.51,
                "remaining_amount": 8204.51,
                "aliases": (),
                "sort_index": 1,
            },
        ],
        drive_link=None,
    )

    assert len(rows) == 1
    assert len(allocations) == 2
    assert cards[0]["party_key"] == "tax:4540007255"
    assert rows[0][0] == "ŞEMSETTİN YILMAZ"
    assert rows[0][1] == "GİDEN FAST | 2 borca dağıtıldı"
    assert rows[0][7] == 8008.37
    assert rows[0][9] == 4054.08
    assert rows[0][10] == "Kısmi"
    assert rows[0][13] == "tax:4540007255"
    assert allocations[0][2] == "debt-1"
    assert allocations[1][2] == "debt-2"
    assert allocations[0][7] == 3857.94
    assert allocations[1][7] == 4150.43



def test_build_visible_projection_snapshot_splits_cheques_from_banka_odemeleri(monkeypatch):
    dekont = google_sheets._canonical_store.StoredDocument(
        source_doc_id="dekont-1",
        category=DocumentCategory.ODEME_DEKONTU,
        return_source_category=None,
        source_message_id="wamid-dekont-1",
        record=BillRecord(
            document_date="2026-04-11",
            document_number="REF-123",
            total_amount=1500.0,
            sender_name="Ahmet Yılmaz",
            sender_iban="TR330001100000000000000001",
            recipient_name="Mehmet Demir",
            recipient_iban="TR440001100000000000000002",
            bank_name="Garanti Bankası",
        ),
        drive_link=None,
        feedback_platform=None,
        feedback_chat_id=None,
        feedback_recipient_type=None,
        feedback_message_id=None,
        created_at="2026-04-11T10:00:00+00:00",
        updated_at="2026-04-11T10:00:00+00:00",
    )
    cek = google_sheets._canonical_store.StoredDocument(
        source_doc_id="cek-1",
        category=DocumentCategory.CEK,
        return_source_category=None,
        source_message_id="wamid-cek-1",
        record=BillRecord(
            cheque_due_date="2026-04-12",
            cheque_serial_number="CHK-1",
            total_amount=2500.0,
            sender_name="KANSA",
            recipient_name="H. KARAKAYA",
            cheque_bank_name="Ziraat Bankası",
        ),
        drive_link=None,
        feedback_platform=None,
        feedback_chat_id=None,
        feedback_recipient_type=None,
        feedback_message_id=None,
        created_at="2026-04-11T10:00:00+00:00",
        updated_at="2026-04-11T10:00:00+00:00",
    )

    monkeypatch.setattr(google_sheets._canonical_store, "list_documents", lambda: [dekont, cek])
    monkeypatch.setattr(google_sheets, "_build_projection_debt_state", lambda documents: [])

    rows_by_tab, hashes_by_tab = google_sheets._build_visible_projection_snapshot()

    assert len(rows_by_tab["Banka Ödemeleri"]) == 1
    assert len(rows_by_tab["Çekler"]) == 1
    assert rows_by_tab["Banka Ödemeleri"][0][4] == "TR330001100000000000000001"
    assert rows_by_tab["Banka Ödemeleri"][0][5] == "TR440001100000000000000002"
    assert rows_by_tab["Çekler"][0][8] == "Ziraat Bankası"
    assert set(hashes_by_tab) >= {"Banka Ödemeleri", "Çekler"}


def test_build_visible_projection_snapshot_filters_out_non_current_month_documents(monkeypatch):
    current_doc = google_sheets._canonical_store.StoredDocument(
        source_doc_id="cek-current-month",
        category=DocumentCategory.CEK,
        return_source_category=None,
        source_message_id="wamid-cek-current",
        record=BillRecord(
            cheque_due_date="2026-04-12",
            cheque_serial_number="CHK-CURRENT",
            total_amount=2500.0,
            sender_name="KANSA",
            recipient_name="H. KARAKAYA",
            cheque_bank_name="Ziraat Bankası",
        ),
        drive_link=None,
        feedback_platform=None,
        feedback_chat_id=None,
        feedback_recipient_type=None,
        feedback_message_id=None,
        created_at="2026-04-12T10:00:00+00:00",
        updated_at="2026-04-12T10:00:00+00:00",
    )
    old_doc = google_sheets._canonical_store.StoredDocument(
        source_doc_id="cek-old-month",
        category=DocumentCategory.CEK,
        return_source_category=None,
        source_message_id="wamid-cek-old",
        record=BillRecord(
            cheque_due_date="2017-10-15",
            cheque_serial_number="CHK-OLD",
            total_amount=15000.0,
            sender_name="OLD SENDER",
            recipient_name="OLD RECIPIENT",
            cheque_bank_name="Old Bank",
        ),
        drive_link=None,
        feedback_platform=None,
        feedback_chat_id=None,
        feedback_recipient_type=None,
        feedback_message_id=None,
        created_at="2017-10-15T10:00:00+00:00",
        updated_at="2017-10-15T10:00:00+00:00",
    )

    monkeypatch.setattr(google_sheets._canonical_store, "list_documents", lambda: [old_doc, current_doc])
    monkeypatch.setattr(google_sheets, "_month_key", lambda: "2026-04")
    monkeypatch.setattr(google_sheets, "_build_projection_debt_state", lambda documents: [])

    rows_by_tab, _ = google_sheets._build_visible_projection_snapshot()

    assert len(rows_by_tab["Çekler"]) == 1
    assert rows_by_tab["Çekler"][0][2] == "CHK-CURRENT"


def test_cheque_projection_accepts_legacy_visible_override_names(monkeypatch):
    cek = google_sheets._canonical_store.StoredDocument(
        source_doc_id="cek-legacy-overrides",
        category=DocumentCategory.CEK,
        return_source_category=None,
        source_message_id="wamid-cek-legacy-overrides",
        record=BillRecord(
            cheque_due_date="2026-08-30",
            cheque_serial_number="CHK-1",
            total_amount=1500000.0,
            sender_name="BERAT DEMİRCİ",
            recipient_name="H. Karkaya İnşaat",
            cheque_bank_name="Ziraat Bankası",
        ),
        drive_link=None,
        feedback_platform=None,
        feedback_chat_id=None,
        feedback_recipient_type=None,
        feedback_message_id=None,
        created_at="2026-04-11T10:00:00+00:00",
        updated_at="2026-04-11T10:00:00+00:00",
    )

    monkeypatch.setattr(google_sheets._canonical_store, "list_documents", lambda: [cek])
    monkeypatch.setattr(google_sheets, "_build_projection_debt_state", lambda documents: [])
    monkeypatch.setattr(
        google_sheets._canonical_store,
        "override_map_for_tab",
        lambda tab_name: {
            "cek-legacy-overrides": {
                "Alıcı / Tedarikçi": "Legacy Lehdar",
                "Referans No": "LEGACY-CEK-1",
                "Gönderen": "Legacy Düzenleyen",
            }
        } if tab_name == "Çekler" else {},
    )

    rows_by_tab, _ = google_sheets._build_visible_projection_snapshot()

    assert rows_by_tab["Çekler"][0][:4] == [
        "Legacy Lehdar",
        "",
        "LEGACY-CEK-1",
        "Legacy Düzenleyen",
    ]


def test_remap_legacy_cheque_headers_renames_and_blanks_noisy_description():
    legacy_headers = [
        "Alıcı / Tedarikçi",
        "Açıklama",
        "Referans No",
        "Gönderen",
        "Ödeme Tutarı (TL)",
        "Ödeme Tarihi",
        "Kalan Bakiye (TL)",
        "Durum",
        "Banka",
        google_sheets._VISIBLE_DRIVE_LINK_HEADER,
        google_sheets._HIDDEN_ROW_ID_HEADER,
        google_sheets._HIDDEN_PARTY_KEY_HEADER,
        google_sheets._HIDDEN_SOURCE_DOC_ID_HEADER,
        google_sheets._HIDDEN_PAYMENT_DOC_ID_HEADER,
        google_sheets._HIDDEN_DEBT_ROW_ID_HEADER,
        google_sheets._HIDDEN_ALLOCATION_ID_HEADER,
        google_sheets._HIDDEN_TAX_NUMBER_HEADER,
        google_sheets._HIDDEN_RECORD_KIND_HEADER,
    ]
    row = [
        "H. Karkaya İnşaat",
        "TL#1.500.000#",
        "0137440",
        "BERAT DEMİRCİ",
        1500000.0,
        "2026-08-30",
        0.0,
        "Borç Yok",
        "Ziraat Bankası",
        "",
        "row-1",
        "",
        "doc-1",
        "doc-1",
        "",
        "",
        "",
        "odeme",
    ]

    remapped = google_sheets._remap_legacy_visible_row(
        "Çekler",
        row,
        legacy_headers=legacy_headers,
        raw_by_doc_id={},
        payment_detail_by_doc_id={},
    )

    assert remapped[:4] == ["H. Karkaya İnşaat", "", "0137440", "BERAT DEMİRCİ"]


def test_cheque_projection_suppresses_amount_and_party_description_noise(monkeypatch):
    cek = google_sheets._canonical_store.StoredDocument(
        source_doc_id="cek-noisy-description",
        category=DocumentCategory.CEK,
        return_source_category=None,
        source_message_id="wamid-cek-noisy",
        record=BillRecord(
            cheque_due_date="2026-08-30",
            cheque_serial_number="0137440",
            total_amount=1500000.0,
            sender_name="BERAT DEMİRCİ DENBER TAAHHÜT İNŞAAT MADENCİLİK OTOMOTİV TURİZM SANAYİ TİCARET LİM",
            recipient_name="H. Karkaya İnşaat Ticaret San. Tic. Ltd. Şti.",
            cheque_bank_name="Ziraat Bankası",
            description="H. Karkaya İnşaat Ticaret San. Tic. Ltd. Şti. emrine",
        ),
        drive_link=None,
        feedback_platform=None,
        feedback_chat_id=None,
        feedback_recipient_type=None,
        feedback_message_id=None,
        created_at="2026-04-11T10:00:00+00:00",
        updated_at="2026-04-11T10:00:00+00:00",
    )
    date_noise = google_sheets._canonical_store.StoredDocument(
        source_doc_id="cek-date-description",
        category=DocumentCategory.CEK,
        return_source_category=None,
        source_message_id="wamid-cek-date-noise",
        record=BillRecord(
            cheque_due_date="2026-08-30",
            cheque_serial_number="0137441",
            total_amount=1500000.0,
            sender_name="BERAT DEMİRCİ",
            recipient_name="H. Karkaya İnşaat",
            cheque_bank_name="Ziraat Bankası",
            description="30/08/2026",
        ),
        drive_link=None,
        feedback_platform=None,
        feedback_chat_id=None,
        feedback_recipient_type=None,
        feedback_message_id=None,
        created_at="2026-04-11T10:00:00+00:00",
        updated_at="2026-04-11T10:00:00+00:00",
    )

    monkeypatch.setattr(google_sheets._canonical_store, "list_documents", lambda: [cek, date_noise])
    monkeypatch.setattr(google_sheets, "_build_projection_debt_state", lambda documents: [])
    monkeypatch.setattr(
        google_sheets._canonical_store,
        "override_map_for_tab",
        lambda tab_name: {
            "cek-noisy-description": {"Açıklama": "TL#1.500.000#"}
        } if tab_name == "Çekler" else {},
    )

    rows_by_tab, _ = google_sheets._build_visible_projection_snapshot()

    assert rows_by_tab["Çekler"][0][1] == ""
    assert rows_by_tab["Çekler"][1][1] == ""


def test_cheque_projection_keeps_meaningful_description(monkeypatch):
    cek = google_sheets._canonical_store.StoredDocument(
        source_doc_id="cek-meaningful-description",
        category=DocumentCategory.CEK,
        return_source_category=None,
        source_message_id="wamid-cek-meaningful",
        record=BillRecord(
            cheque_due_date="2026-08-30",
            cheque_serial_number="0137440",
            total_amount=1500000.0,
            sender_name="BERAT DEMİRCİ",
            recipient_name="H. Karkaya İnşaat",
            cheque_bank_name="Ziraat Bankası",
            description="Teminat çeki",
        ),
        drive_link=None,
        feedback_platform=None,
        feedback_chat_id=None,
        feedback_recipient_type=None,
        feedback_message_id=None,
        created_at="2026-04-11T10:00:00+00:00",
        updated_at="2026-04-11T10:00:00+00:00",
    )

    monkeypatch.setattr(google_sheets._canonical_store, "list_documents", lambda: [cek])
    monkeypatch.setattr(google_sheets, "_build_projection_debt_state", lambda documents: [])
    monkeypatch.setattr(google_sheets._canonical_store, "override_map_for_tab", lambda tab_name: {})

    rows_by_tab, _ = google_sheets._build_visible_projection_snapshot()

    assert rows_by_tab["Çekler"][0][1] == "Teminat çeki"


def test_build_visible_projection_snapshot_includes_hidden_payment_allocations(monkeypatch):
    monkeypatch.setattr(google_sheets._canonical_store, "list_documents", lambda: ["doc"])
    monkeypatch.setattr(google_sheets, "_build_projection_debt_state", lambda documents: [{"row_id": "debt-1"}])
    monkeypatch.setattr(
        google_sheets,
        "_build_payment_projection_rows_from_documents",
        lambda documents, debt_state: ([['payment-row']], [['cheque-row']], {'doc-1': 'hash-1'}, {'doc-2': 'hash-2'}, [['alloc-row']]),
    )
    monkeypatch.setattr(google_sheets, "_build_expense_projection_rows", lambda debt_state: ([['expense-row']], {'exp-1': 'hash-exp'}))
    monkeypatch.setattr(google_sheets, "_build_invoice_projection_rows", lambda documents: ([['invoice-row']], {'inv-1': 'hash-inv'}))
    monkeypatch.setattr(google_sheets, "_build_shipment_projection_rows", lambda documents: ([['shipment-row']], {'ship-1': 'hash-ship'}))

    rows_by_tab, hashes_by_tab = google_sheets._build_visible_projection_snapshot()

    assert rows_by_tab['Banka Ödemeleri'] == [['payment-row']]
    assert rows_by_tab['Çekler'] == [['cheque-row']]
    assert rows_by_tab['__Ödeme_Dağıtımları'] == [['alloc-row']]
    assert hashes_by_tab['Banka Ödemeleri'] == {'doc-1': 'hash-1'}
    assert hashes_by_tab['Çekler'] == {'doc-2': 'hash-2'}


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



def test_remap_legacy_banka_odemeleri_row_leaves_split_ibans_blank_when_only_legacy_iban_exists():
    legacy_headers = google_sheets._legacy_header_variants("Banka Ödemeleri")[0]
    row = [
        "MUZAFFER KARAKAŞ",
        "GİDEN FAST",
        "2026-03-31",
        "0",
        "8008.37",
        "2026-03-31",
        "0",
        "Borç Yok",
        '=HYPERLINK("https://drive.google.com/file/d/test/view";"Görüntüle")',
        "row-1",
        "party-1",
        "doc-1",
        "doc-1",
        "",
        "",
        "4540007255",
        "odeme",
    ]

    remapped = google_sheets._remap_legacy_visible_row(
        "Banka Ödemeleri",
        row,
        legacy_headers=legacy_headers,
        raw_by_doc_id={"doc-1": {"Banka": "Garanti Bankası", "IBAN": "TR-OLD"}},
        payment_detail_by_doc_id={"doc-1": {"Referans": "REF-123", "Gönderen": "Ahmet Yılmaz"}},
    )

    assert remapped[:12] == [
        "MUZAFFER KARAKAŞ",
        "GİDEN FAST",
        "REF-123",
        "Ahmet Yılmaz",
        "",
        "",
        "Garanti Bankası",
        "8008.37",
        "2026-03-31",
        "0",
        "Borç Yok",
        '=HYPERLINK("https://drive.google.com/file/d/test/view";"Görüntüle")',
    ]


def test_apply_remapped_visible_rows_rewrites_drive_cells_after_bulk_update(monkeypatch):
    ws = MagicMock()
    ws.spreadsheet.locale = 'tr_TR'
    ws.row_count = 1000

    monkeypatch.setattr(google_sheets, '_setup_worksheet', lambda _ws, _tab_name, *, lightweight=False: None)
    monkeypatch.setattr(google_sheets, '_ensure_tab_total_row', lambda _ws, _tab_name: None)
    rewrite_calls = []
    monkeypatch.setattr(google_sheets, '_rewrite_drive_cells', lambda _ws, _tab_name, row_formulas: rewrite_calls.append((_tab_name, row_formulas)) or len(row_formulas))

    row = [
        '81', '2026-03-18', 'Fatura', 'ŞEMSETTİN YILMAZ', '4540007255', 'H.KARAKAYA',
        'PVC ATIK SU BORUSU', '', '', '6837.09', '20', '1367.42', 'HAYIR', '0', '8204.51',
        'TRY', 'Banka: Yapı Kredi', '=HYPERLINK("https://drive.google.com/file/d/test/view";"Görüntüle")',
        'row-1', 'party-1', 'doc-1', '4540007255', 'fatura'
    ]

    google_sheets._apply_remapped_visible_rows(ws, 'Faturalar', [row])

    assert ws.update.call_args_list[0].args[1] == 'A3'
    assert rewrite_calls == [('Faturalar', [(3, '=HYPERLINK("https://drive.google.com/file/d/test/view";"Görüntüle")')])]



def test_tab_headers_can_migrate_in_place_accepts_visible_only_legacy_headers():
    ws = MagicMock()
    ws.row_values.return_value = [
        "Fiş No",
        "Tarih",
        "Alıcı",
        "Ürün Cinsi",
        "Palet Sayısı",
        "Adet/Palet",
        "Ürün Miktarı",
        "Plaka",
        "Satıcı",
        "Çıkış Yeri",
        "Sevk Yeri",
        "Belge",
    ]

    assert google_sheets._tab_headers_can_migrate_in_place(ws, "Sevk Fişleri") is True



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
        "L3",
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


def test_get_or_create_spreadsheet_reuses_existing_month_sheet_when_registry_missing_and_cache_cold():
    client = MagicMock()
    spreadsheet = MagicMock()
    client.open_by_key.return_value = spreadsheet

    with patch(
        "app.services.providers.google_sheets._load_registry",
        return_value={"2026-03": "sheet-march"},
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
        return_value="sheet-april-existing",
    ) as find_existing_mock, patch(
        "app.services.providers.google_sheets._try_create_spreadsheet_in_drive",
    ) as create_mock, patch(
        "app.services.providers.google_sheets._bootstrap_spreadsheet_tabs",
    ) as bootstrap_mock:
        google_sheets._drive_folder_cache.clear()
        result = google_sheets._get_or_create_spreadsheet(client)

    assert result is spreadsheet
    find_existing_mock.assert_called_once_with("Muhasebe — Nisan 2026")
    save_registry_mock.assert_called_once_with(
        {"2026-03": "sheet-march", "2026-04": "sheet-april-existing"}
    )
    client.open_by_key.assert_called_once_with("sheet-april-existing")
    create_mock.assert_not_called()
    bootstrap_mock.assert_not_called()


def test_get_or_create_spreadsheet_prefers_env_over_stale_registry():
    """GOOGLE_SHEETS_SPREADSHEET_ID must win over sheets_registry.json for the same month."""
    client = MagicMock()
    spreadsheet = MagicMock()
    client.open_by_key.return_value = spreadsheet

    with patch(
        "app.services.providers.google_sheets._load_registry",
        return_value={"2026-04": "sheet-old"},
    ), patch(
        "app.services.providers.google_sheets._save_registry",
    ) as save_registry_mock, patch(
        "app.services.providers.google_sheets._month_key",
        return_value="2026-04",
    ), patch(
        "app.services.providers.google_sheets.settings.google_sheets_spreadsheet_id",
        "sheet-from-env",
    ), patch(
        "app.services.providers.google_sheets._find_existing_spreadsheet_in_drive",
    ) as find_mock:
        result = google_sheets._get_or_create_spreadsheet(client)

    assert result is spreadsheet
    client.open_by_key.assert_called_once_with("sheet-from-env")
    find_mock.assert_not_called()
    save_registry_mock.assert_called_once_with({"2026-04": "sheet-from-env"})


def test_find_existing_spreadsheet_in_drive_searches_month_folder_when_cache_is_cold():
    drive = MagicMock()
    list_mock = drive.files.return_value.list

    def _list_for_query(*, q, **kwargs):
        if "'folder-april' in parents" in q:
            return MagicMock(execute=MagicMock(return_value={"files": [{"id": "sheet-april-existing"}]}))
        if "'parent-folder' in parents" in q:
            return MagicMock(execute=MagicMock(return_value={"files": []}))
        raise AssertionError(q)

    list_mock.side_effect = _list_for_query

    with patch(
        "app.services.providers.google_sheets.settings.google_drive_parent_folder_id",
        "parent-folder",
    ), patch(
        "app.services.providers.google_sheets._get_oauth_drive_service",
        return_value=drive,
    ), patch(
        "app.services.providers.google_sheets._get_drive_service",
        return_value=None,
    ), patch(
        "app.services.providers.google_sheets._get_or_create_month_drive_folder",
        return_value="folder-april",
    ), patch(
        "app.services.providers.google_sheets._month_drive_folder_name",
        return_value="Fişler — Nisan 2026",
    ):
        google_sheets._drive_folder_cache.clear()
        result = google_sheets._find_existing_spreadsheet_in_drive("Muhasebe — Nisan 2026")

    assert result == "sheet-april-existing"


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
