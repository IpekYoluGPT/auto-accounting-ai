"""
Tests for the Gemini extractor service.
"""

from unittest.mock import ANY, patch

import pytest

from app.models.schemas import AIExtractionResult, AIMultiExtractionResult, BillRecord
from app.services.accounting.gemini_extractor import _normalize_record, _parse_tr_number, extract_bill, extract_bills


class TestParseTrNumber:
    def test_turkish_format(self):
        assert _parse_tr_number("1.234,56") == 1234.56

    def test_plain_float_string(self):
        assert _parse_tr_number("150.00") == 150.0

    def test_integer_string(self):
        assert _parse_tr_number("250") == 250.0

    def test_none_returns_none(self):
        assert _parse_tr_number(None) is None

    def test_invalid_returns_none(self):
        assert _parse_tr_number("not-a-number") is None

    def test_large_turkish_number(self):
        result = _parse_tr_number("12.345,67")
        assert result == pytest.approx(12345.67)


class TestNormalizeRecord:
    def _raw(self, **overrides) -> dict:
        base = {
            "company_name": "Test \u015eirketi A.\u015e.",
            "tax_number": "1234567890",
            "tax_office": "Kad\u0131k\u00f6y",
            "document_number": "DOC-001",
            "invoice_number": "FTR-2024-001",
            "receipt_number": None,
            "document_date": "2024-01-15",
            "document_time": "14:30",
            "currency": "TRY",
            "subtotal": 100.0,
            "vat_rate": 18.0,
            "vat_amount": 18.0,
            "total_amount": 118.0,
            "payment_method": "Kredi Kart\u0131",
            "expense_category": "Ofis",
            "description": "Ofis malzemeleri",
            "notes": None,
            "confidence": 0.95,
        }
        base.update(overrides)
        return base

    def test_basic_normalization(self):
        record = _normalize_record(self._raw())
        assert record.company_name == "Test \u015eirketi A.\u015e."
        assert record.total_amount == 118.0
        assert record.confidence == 0.95

    def test_currency_uppercased(self):
        record = _normalize_record(self._raw(currency="try"))
        assert record.currency == "TRY"

    def test_invalid_currency_defaults_to_try(self):
        record = _normalize_record(self._raw(currency="GBP"))
        assert record.currency == "TRY"

    def test_missing_currency_defaults_to_try(self):
        record = _normalize_record(self._raw(currency=None))
        assert record.currency == "TRY"

    def test_valid_non_try_currency_is_preserved(self):
        record = _normalize_record(self._raw(currency="EUR"))
        assert record.currency == "EUR"

    def test_date_normalized_from_dotted(self):
        record = _normalize_record(self._raw(document_date="15.01.2024"))
        assert record.document_date == "2024-01-15"

    def test_date_normalized_from_slash(self):
        record = _normalize_record(self._raw(document_date="15/01/2024"))
        assert record.document_date == "2024-01-15"

    def test_time_normalized(self):
        record = _normalize_record(self._raw(document_time="9.05"))
        assert record.document_time == "09:05"

    def test_none_fields_handled(self):
        record = _normalize_record(self._raw(company_name=None, total_amount=None))
        assert record.company_name is None
        assert record.total_amount is None

    def test_turkish_number_float_converted(self):
        record = _normalize_record(self._raw(total_amount="1.234,56"))
        assert record.total_amount == pytest.approx(1234.56)

    def test_returns_bill_record(self):
        record = _normalize_record(self._raw())
        assert isinstance(record, BillRecord)

    def test_empty_string_becomes_none(self):
        record = _normalize_record(self._raw(company_name=""))
        assert record.company_name is None


class TestExtractBill:
    def test_no_api_key_raises(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "")
        with pytest.raises(RuntimeError, match="GEMINI_API_KEY"):
            extract_bill(b"fake", mime_type="image/jpeg")

    def test_successful_extraction(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        monkeypatch.setattr(
            "app.services.accounting.gemini_extractor.settings.gemini_extractor_model",
            "gemini-test-extractor",
        )
        single_doc = AIExtractionResult(
            company_name="ABC Market",
            tax_number="9876543210",
            tax_office="Be\u015fikta\u015f",
            document_number="FIS-001",
            receipt_number="FIS-001",
            document_date="2024-03-10",
            document_time="10:15",
            currency="TRY",
            subtotal=84.75,
            vat_rate=18.0,
            vat_amount=15.25,
            total_amount=100.0,
            payment_method="Nakit",
            expense_category="Yemek",
            description="Market al\u0131\u015fveri\u015fi",
            confidence=0.91,
        )
        expected = AIMultiExtractionResult(documents=[single_doc])

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate:
            record = extract_bill(
                b"fake_image",
                mime_type="application/pdf",
                source_message_id="msg_123",
                source_filename="receipt.jpg",
                source_type="image",
            )

        assert isinstance(record, BillRecord)
        assert record.company_name == "ABC Market"
        assert record.total_amount == 100.0
        assert record.expense_category == "Yemek"
        assert record.source_message_id == "msg_123"
        assert record.confidence == pytest.approx(0.91)
        mock_generate.assert_called_once_with(
            model="gemini-test-extractor",
            prompt=ANY,
            response_schema=AIMultiExtractionResult,
            thinking_level="low",
            media_bytes=b"fake_image",
            mime_type="application/pdf",
        )

    def test_multi_document_extraction(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        monkeypatch.setattr(
            "app.services.accounting.gemini_extractor.settings.gemini_extractor_model",
            "gemini-test-extractor",
        )
        doc1 = AIExtractionResult(
            company_name="Firma A",
            total_amount=100.0,
            currency="TRY",
            confidence=0.9,
        )
        doc2 = AIExtractionResult(
            company_name="Firma B",
            total_amount=200.0,
            currency="TRY",
            confidence=0.88,
        )
        doc3 = AIExtractionResult(
            company_name="Firma C",
            total_amount=300.0,
            currency="TRY",
            confidence=0.85,
        )
        expected = AIMultiExtractionResult(documents=[doc1, doc2, doc3])

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=expected,
        ):
            records = extract_bills(
                b"fake_image_3_cheques",
                mime_type="image/jpeg",
                source_message_id="msg_456",
                source_filename="cheques.jpg",
                source_type="image",
            )

        assert len(records) == 3
        assert records[0].company_name == "Firma A"
        assert records[1].company_name == "Firma B"
        assert records[2].company_name == "Firma C"
        # Sub-indices for dedup
        assert records[0].source_message_id == "msg_456__doc1"
        assert records[1].source_message_id == "msg_456__doc2"
        assert records[2].source_message_id == "msg_456__doc3"

    def test_generation_error_propagates(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            side_effect=RuntimeError("Gemini unavailable"),
        ):
            with pytest.raises(RuntimeError, match="Gemini unavailable"):
                extract_bill(b"fake_image")
