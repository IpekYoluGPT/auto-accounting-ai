"""
Tests for OCR preparation and deterministic extraction helpers.
"""

from unittest.mock import patch

from app.models.ocr import OCRKeyValue, OCRMediaMetadata, OCRParseBundle, OCRTable, OCRTableCell
from app.models.schemas import DocumentCategory
from app.services.accounting import ocr


def _bundle(text: str, *, quality_score: float = 0.9) -> OCRParseBundle:
    return OCRParseBundle(
        text=text,
        lines=[line for line in text.splitlines() if line],
        quality_score=quality_score,
        readability_score=quality_score,
        text_char_count=len(text),
        processor_used="form_parser",
        metadata=OCRMediaMetadata(
            mime_type="image/jpeg",
            original_mime_type="image/jpeg",
            byte_size=1024,
            width=1000,
            height=1400,
            source_hash="hash1",
        ),
    )


def _table(rows: list[list[str]]) -> OCRTable:
    cells: list[OCRTableCell] = []
    for row_index, row in enumerate(rows):
        for column_index, text in enumerate(row):
            cells.append(
                OCRTableCell(
                    row_index=row_index,
                    column_index=column_index,
                    text=text,
                    confidence=0.9,
                )
            )
    return OCRTable(
        page_number=1,
        row_count=len(rows),
        column_count=max((len(row) for row in rows), default=0),
        cells=cells,
        confidence=0.9,
    )


def test_prepare_document_returns_original_bytes_for_invalid_fake_image():
    prepared = ocr.prepare_document(b"not-an-image", "image/jpeg")

    assert prepared.media_bytes == b"not-an-image"
    assert prepared.mime_type == "image/jpeg"


def test_prepare_document_uses_cached_ocr_bundle(monkeypatch):
    bundle = _bundle("Toplam: 150,00 TL")
    with patch("app.services.accounting.ocr.google_document_ai.is_configured", return_value=True), patch(
        "app.services.accounting.ocr.google_document_ai.process_document",
        return_value=bundle,
    ) as process_mock:
        first = ocr.prepare_document(b"same-bytes", "image/jpeg")
        second = ocr.prepare_document(b"same-bytes", "image/jpeg")

    assert first.ocr_bundle == bundle
    assert second.ocr_bundle == bundle
    process_mock.assert_called_once()


def test_assess_extraction_builds_direct_record_for_clean_receipt():
    bundle = _bundle(
        "ÖZTÜRK GIDA LTD. ŞTİ.\n"
        "Tarih: 09.04.2026\n"
        "Saat: 14:32\n"
        "Fiş No: 004218\n"
        "Ara Toplam: 1.250,00 TL\n"
        "KDV %20: 250,00 TL\n"
        "Genel Toplam: 1.500,00 TL\n"
        "Ödeme: Nakit"
    )

    assessment = ocr.assess_extraction(bundle, category_hint=DocumentCategory.HARCAMA_FISI)

    assert assessment.use_direct is True
    assert assessment.record.company_name == "ÖZTÜRK GIDA LTD. ŞTİ."
    assert assessment.record.receipt_number == "004218"
    assert assessment.record.document_date == "2026-04-09"
    assert assessment.record.total_amount == 1500.0
    assert assessment.record.vat_amount == 250.0


def test_assess_extraction_flags_multi_document_images():
    bundle = _bundle(
        "Firma A\nTarih: 09.04.2026\nFiş No: 001\nToplam: 100,00 TL\n"
        "Firma B\nTarih: 09.04.2026\nFiş No: 002\nToplam: 200,00 TL"
    )

    assessment = ocr.assess_extraction(bundle, category_hint=DocumentCategory.HARCAMA_FISI)

    assert assessment.multi_document_suspected is True
    assert assessment.use_direct is False


def test_detect_bill_from_ocr_accepts_material_slip_anchors():
    bundle = _bundle(
        "SOMAY PETROL SAN. VE TİC. LTD. ŞTİ.\n"
        "VERESİYE SATIŞ SENEDİ\n"
        "Miktarı\n"
        "Malın Cinsi\n"
        "Tutarı\n"
        "Teslim Alan\n"
        "1200\n"
        "2300\n"
        "4500"
    )

    assert ocr.detect_bill_from_ocr(bundle) is True


def test_assess_extraction_sums_material_slip_table_total():
    bundle = _bundle(
        "SOMAY PETROL SAN. VE TİC. LTD. ŞTİ.\n"
        "VERESİYE SATIŞ SENEDİ\n"
        "No: 08979\n"
        "Teslim Alan",
    )
    bundle.tables = [
        _table(
            [
                ["Miktarı", "Malın Cinsi", "Fiyat", "Tutarı"],
                ["300", "2m Mastar", "", "1200"],
                ["100", "25 cm Pn16", "", "2300"],
                ["100", "Sac 1.5", "", "1000"],
                ["", "", "", "4500"],
            ]
        )
    ]

    assessment = ocr.assess_extraction(bundle, category_hint=DocumentCategory.MALZEME)

    assert assessment.use_direct is True
    assert assessment.record.company_name == "SOMAY PETROL SAN. VE TİC. LTD. ŞTİ."
    assert assessment.record.document_number == "08979"
    assert assessment.record.total_amount == 4500.0


def test_assess_extraction_accepts_dekont_party_key_value():
    bundle = _bundle(
        "Banka Dekontu\n"
        "Tarih: 09.04.2026\n"
        "İşlem Tutarı: 1.500,00 TL\n"
        "Açıklama: Kira ödemesi"
    )
    bundle.key_values = [
        OCRKeyValue(key="Alıcı Adı", value="ÖZTÜRK GIDA LTD. ŞTİ.", confidence=0.93),
    ]

    assessment = ocr.assess_extraction(bundle, category_hint=DocumentCategory.ODEME_DEKONTU)

    assert assessment.use_direct is True
    assert assessment.record.company_name == "ÖZTÜRK GIDA LTD. ŞTİ."
    assert assessment.record.total_amount == 1500.0
    assert assessment.record.document_date == "2026-04-09"
