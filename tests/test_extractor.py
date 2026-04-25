"""
Tests for the Gemini extractor service.
"""

from types import SimpleNamespace
from unittest.mock import ANY, patch

import pytest

from app.models.schemas import AIExtractionResult, AIMultiExtractionResult, BillRecord, DocumentCategory, InvoiceLineItem
from app.services.accounting import unit_dictionary
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
            "company_name": "Test Şirketi A.Ş.",
            "tax_number": "1234567890",
            "tax_office": "Kadıköy",
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
            "sender_name": None,
            "payment_method": "Kredi Kartı",
            "expense_category": "Ofis",
            "description": "Ofis malzemeleri",
            "notes": None,
            "confidence": 0.95,
        }
        base.update(overrides)
        return base

    def test_basic_normalization(self):
        record = _normalize_record(self._raw())
        assert record.company_name == "Test Şirketi A.Ş."
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

    def test_sender_name_is_preserved(self):
        record = _normalize_record(self._raw(sender_name="Ahmet Yılmaz"))
        assert record.sender_name == "Ahmet Yılmaz"

    def test_identifier_fields_preserve_leading_zeroes_and_strip_numeric_punctuation(self):
        record = _normalize_record(
            self._raw(
                document_number="1.031,00",
                invoice_number="00123",
                receipt_number="00 81",
                cheque_serial_number="00 45",
                cheque_account_ref="7,00",
            )
        )

        assert record.document_number == "103100"
        assert record.invoice_number == "00123"
        assert record.receipt_number == "0081"
        assert record.cheque_serial_number == "0045"
        assert record.cheque_account_ref == "700"

    def test_new_structured_fields_are_normalized(self):
        record = _normalize_record(
            self._raw(
                recipient_name="Mehmet Kaya",
                sender_iban="TR330001100000000000000001",
                recipient_iban="TR440001100000000000000002",
                buyer_name="Kaya İnşaat",
                invoice_type="E-Arşiv Fatura",
                line_quantity="2,5",
                line_unit="ton",
                unit_price="150,00",
                line_amount="375,00",
                withholding_present="true",
                withholding_rate="20",
                withholding_amount="75,00",
                payable_amount="300,00",
                iban="TR120006200000000123456789",
                bank_name="Yapı Kredi",
                shipment_origin="Elazığ",
                shipment_destination="Karakaya",
                pallet_count="4",
                items_per_pallet="12",
                product_quantity="48",
                vehicle_plate="23 ABC 123",
                cheque_issue_place="Elazığ",
                cheque_issue_date="15.03.2026",
                cheque_due_date="30.03.2026",
                cheque_serial_number="CHQ-7788",
                cheque_bank_name="Yapı Kredi",
                cheque_branch="Elazığ Şubesi",
                cheque_account_ref="123-456-789",
                line_items=[
                    {
                        "description": "Arp Lastik",
                        "quantity": "3",
                        "unit": "adet",
                        "unit_price": "25,50",
                        "line_amount": "76,50",
                    }
                ],
            )
        )

        assert record.recipient_name == "Mehmet Kaya"
        assert record.buyer_name == "Kaya İnşaat"
        assert record.invoice_type == "E-Arşiv Fatura"
        assert record.line_quantity == pytest.approx(2.5)
        assert record.line_unit == "ton"
        assert record.unit_price == pytest.approx(150.0)
        assert record.line_amount == pytest.approx(375.0)
        assert record.withholding_present is True
        assert record.withholding_rate == pytest.approx(20.0)
        assert record.withholding_amount == pytest.approx(75.0)
        assert record.payable_amount == pytest.approx(300.0)
        assert record.sender_iban == "TR330001100000000000000001"
        assert record.recipient_iban == "TR440001100000000000000002"
        assert record.iban == "TR120006200000000123456789"
        assert record.bank_name == "Yapı Kredi"
        assert record.shipment_origin == "Elazığ"
        assert record.shipment_destination == "Karakaya"
        assert record.pallet_count == pytest.approx(4.0)
        assert record.items_per_pallet == pytest.approx(12.0)
        assert record.product_quantity == pytest.approx(48.0)
        assert record.vehicle_plate == "23 ABC 123"
        assert record.cheque_issue_place == "Elazığ"
        assert record.cheque_issue_date == "2026-03-15"
        assert record.cheque_due_date == "2026-03-30"
        assert record.cheque_serial_number == "CHQ-7788"
        assert record.cheque_bank_name == "Yapı Kredi"
        assert record.cheque_branch == "Elazığ Şubesi"
        assert record.cheque_account_ref == "123-456-789"
        assert record.line_items is not None
        assert len(record.line_items) == 1
        assert isinstance(record.line_items[0], InvoiceLineItem)
        assert record.line_items[0].description == "Arp Lastik"
        assert record.line_items[0].quantity == pytest.approx(3.0)
        assert record.line_items[0].unit_price == pytest.approx(25.5)
        assert record.line_items[0].line_amount == pytest.approx(76.5)

    def test_parses_compact_material_quantity_tokens_without_hardcoding(self):
        record = _normalize_record(
            self._raw(
                line_quantity=None,
                line_unit=None,
                product_quantity="18m3",
                line_items=[
                    {"description": "3AD 2m MASTAR", "quantity": None, "unit": None},
                    {"description": "1 adet 25cm Rulo", "quantity": None, "unit": None},
                    {"description": "Sap 1.50m", "quantity": "1AD", "unit": None},
                    {"description": "Çimento", "quantity": "5TRB", "unit": None},
                ],
            )
        )

        assert record.product_quantity == pytest.approx(18.0)
        assert record.line_unit == "m3"
        assert record.line_items is not None
        assert [item.quantity for item in record.line_items] == [pytest.approx(3.0), pytest.approx(1.0), pytest.approx(1.0), pytest.approx(5.0)]
        assert [item.unit for item in record.line_items] == ["AD", "adet", "AD", "TRB"]
        assert [item.description for item in record.line_items] == ["2m MASTAR", "25cm Rulo", "Sap 1.50m", "Çimento"]
        assert [unit_dictionary.canonical_unit(item.unit) for item in record.line_items] == ["adet", "adet", "adet", "torba"]


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
            tax_office="Beşiktaş",
            document_number="FIS-001",
            receipt_number="FIS-001",
            document_date="2024-03-10",
            document_time="10:15",
            currency="TRY",
            subtotal=84.75,
            vat_rate=18.0,
            vat_amount=15.25,
            total_amount=100.0,
            recipient_name="Ahmet Yılmaz",
            buyer_name="Ahmet Yılmaz",
            invoice_type="Fatura",
            iban="TR120006200000000123456789",
            bank_name="Yapı Kredi",
            line_items=[
                InvoiceLineItem(description="Ürün A", quantity=2, unit="adet", unit_price=25.0, line_amount=50.0)
            ],
            payment_method="Nakit",
            expense_category="Yemek",
            description="Market alışverişi",
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
        assert record.recipient_name == "Ahmet Yılmaz"
        assert record.iban == "TR120006200000000123456789"
        assert record.line_items is not None
        assert record.line_items[0].description == "Ürün A"
        assert record.confidence == pytest.approx(0.91)
        mock_generate.assert_called_once_with(
            model="gemini-test-extractor",
            prompt=ANY,
            system_instruction=ANY,
            response_schema=AIMultiExtractionResult,
            thinking_level="low",
            media_bytes=b"fake_image",
            mime_type="application/pdf",
        )

    def test_category_hints_are_embedded_in_prompt(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        expected = AIMultiExtractionResult(
            documents=[AIExtractionResult(company_name="Saha Kum", currency="TRY", confidence=0.77)]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate:
            records = extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-hints-1",
                source_filename="slip.jpg",
                source_type="image",
                category_hint=DocumentCategory.MALZEME,
                document_count_hint=3,
                is_return_hint=True,
            )

        assert len(records) == 1
        prompt = mock_generate.call_args.kwargs["prompt"]
        assert "Belge ailesi ipucu: malzeme" in prompt
        assert "Belge muhtemelen bir iade/iptal niteligi tasiyor." in prompt
        assert "Goruntude yaklasik 3 ayri belge bekleniyor." in prompt
        assert "El yazili hafriyat/malzeme formu" in prompt
        assert "never copy company names" in mock_generate.call_args.kwargs["system_instruction"]

    def test_strict_split_retry_prompt_requests_exact_document_count_for_checks(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        expected = AIMultiExtractionResult(
            documents=[
                AIExtractionResult(
                    company_name="Yapı Kredi",
                    recipient_name="HAMİLİNE",
                    currency="TRY",
                    confidence=0.8,
                )
            ]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate:
            extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-checks-1",
                source_filename="checks.jpg",
                source_type="image",
                category_hint=DocumentCategory.CEK,
                document_count_hint=3,
                strict_document_count=3,
                split_retry=True,
            )

        prompt = mock_generate.call_args.kwargs["prompt"]
        assert "Bu denemede tam olarak 3 ayri belge cikarilmasi gerekiyor." in prompt
        assert "Bu ikinci geciste kismi sonuc kabul edilmez" in prompt
        assert "her fiziksel cek yapragini ayri belge say" in prompt
        assert "Ayrik belgeleri ASLA birlestirme" in prompt

    def test_invoice_prompt_requests_structured_line_items_and_withholding_fields(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        expected = AIMultiExtractionResult(
            documents=[AIExtractionResult(company_name="Semsettin Yilmaz", currency="TRY", confidence=0.8)]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate:
            extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-invoice-1",
                source_filename="invoice.jpg",
                source_type="image",
                category_hint=DocumentCategory.FATURA,
            )

        prompt = mock_generate.call_args.kwargs["prompt"]
        assert "recipient_name gorunen alici" in prompt
        assert "buyer_name gorunen satin alan" in prompt
        assert "invoice_type gorunen turu yaz" in prompt
        assert "line_items varsa her satir icin description" in prompt
        assert "withholding_present" in prompt
        assert "payable_amount" in prompt
        assert "iban ve bank_name gorunuyorsa ekle" in prompt

    def test_dekont_prompt_requests_name_not_phone_for_sender(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        expected = AIMultiExtractionResult(
            documents=[AIExtractionResult(company_name="Banka", sender_name="Ahmet Yılmaz", currency="TRY", confidence=0.8)]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate:
            extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-dekont-1",
                source_filename="dekont.jpg",
                source_type="image",
                category_hint=DocumentCategory.ODEME_DEKONTU,
            )

        prompt = mock_generate.call_args.kwargs["prompt"]
        assert "recipient_name aliciyi" in prompt
        assert "sender_name alanina sadece gonderen kisi/firma adini yaz" in prompt
        assert "sender_iban gonderen hesabin ibanidir" in prompt
        assert "recipient_iban alici hesabin ibanidir" in prompt
        assert "Taraf net degilse ilgili alani null birak" in prompt

    def test_expense_prompt_requests_exact_visible_line_details(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        expected = AIMultiExtractionResult(
            documents=[AIExtractionResult(company_name="Petrol", currency="TRY", confidence=0.8)]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate:
            extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-fuel-1",
                source_filename="fuel.jpg",
                source_type="image",
                category_hint=DocumentCategory.HARCAMA_FISI,
            )

        prompt = mock_generate.call_args.kwargs["prompt"]
        assert "description alanina gorunen urun/hizmet satirlarini aynen koruyarak yaz" in prompt
        assert "75,170 LT X 62,53 | MOT V MAX E Dz10%20" in prompt

    def test_cheque_prompt_requests_bank_and_cheque_metadata(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        expected = AIMultiExtractionResult(
            documents=[
                AIExtractionResult(
                    company_name="Banka",
                    recipient_name="HAMİLİNE",
                    currency="TRY",
                    confidence=0.8,
                )
            ]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate:
            extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-cheque-1",
                source_filename="cheque.jpg",
                source_type="image",
                category_hint=DocumentCategory.CEK,
            )

        prompt = mock_generate.call_args.kwargs["prompt"]
        assert "recipient_name lehdar / alici" in prompt
        assert "recipient_name yalniz el yazisi" in prompt
        assert "sender_name veya company_name matbu" in prompt
        assert "El yazisi lehdar matbu kesideciye benziyorsa bile recipient_name'i bos birakma" in prompt
        assert "Yatay, ters veya yan donmus cek fotografini zihnen cevir" in prompt
        assert "HAMILINE veya HAMİLİNE" in prompt
        assert "cheque_issue_place, cheque_issue_date, cheque_due_date, cheque_serial_number, cheque_bank_name, cheque_branch ve cheque_account_ref" in prompt
        assert "total_amount veya payable_amount cek tutari" in prompt
        assert "description alanini sadece ayri bir ticari not varsa doldur" in prompt
        assert "lehdar/alici, kesideci/gonderen, cek tutari" in prompt

    def test_cheque_prompt_includes_ocr_hints_when_available(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        expected = AIMultiExtractionResult(
            documents=[
                AIExtractionResult(
                    company_name="Banka",
                    recipient_name="HAMİLİNE",
                    currency="TRY",
                    confidence=0.8,
                )
            ]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate, patch(
            "app.services.accounting.gemini_extractor.ocr.prepare_document",
            return_value=SimpleNamespace(ocr_bundle=object()),
        ), patch(
            "app.services.accounting.gemini_extractor.ocr.serialize_ocr_bundle",
            return_value="OCR_TEXT:\n- HAMİLİNE\n- Karakaya PVC Doğ. Ltd. Şti",
        ):
            extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-cheque-ocr-1",
                source_filename="cheque.jpg",
                source_type="image",
                category_hint=DocumentCategory.CEK,
            )

        prompt = mock_generate.call_args.kwargs["prompt"]
        assert "OCR ipucu" in prompt
        assert "HAMİLİNE" in prompt

    def test_cheque_prompt_marks_recipient_name_as_required(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        expected = AIMultiExtractionResult(
            documents=[
                AIExtractionResult(
                    company_name="Banka",
                    recipient_name="HAMİLİNE",
                    currency="TRY",
                    confidence=0.8,
                )
            ]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate:
            extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-cheque-required-1",
                source_filename="cheque.jpg",
                source_type="image",
                category_hint=DocumentCategory.CEK,
            )

        prompt = mock_generate.call_args.kwargs["prompt"]
        assert "ZORUNLU: cek belgesi icin recipient_name asla null" in prompt
        assert "Lehdar el yazisi okunaksizsa bile en yakin tahmini ver" in prompt

    def test_cheque_extract_bills_refines_missing_lehdars(self, monkeypatch):
        from app.services.accounting.gemini_extractor import _ChequeLehdarEntry, _ChequeLehdarRefinement

        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        monkeypatch.setattr(
            "app.services.accounting.gemini_extractor.settings.gemini_extractor_model",
            "gemini-test-extractor",
        )
        monkeypatch.setattr(
            "app.services.accounting.gemini_extractor.settings.gemini_lehdar_refinement_model",
            "gemini-test-flash",
        )

        primary = AIMultiExtractionResult(
            documents=[
                AIExtractionResult(
                    company_name="H.KARAKAYA INSAAT",
                    document_number="0205890",
                    recipient_name=None,
                    currency="TRY",
                    confidence=0.8,
                ),
                AIExtractionResult(
                    company_name="H.KARAKAYA INSAAT",
                    document_number="0205891",
                    recipient_name="ERIMER MERMER",
                    currency="TRY",
                    confidence=0.85,
                ),
                AIExtractionResult(
                    company_name="H.KARAKAYA INSAAT",
                    document_number="0205892",
                    recipient_name="-",
                    currency="TRY",
                    confidence=0.7,
                ),
            ]
        )
        refinement = _ChequeLehdarRefinement(
            lehdars=[
                _ChequeLehdarEntry(lehdar="HAMİLİNE"),
                _ChequeLehdarEntry(lehdar="ERIMER MERMER"),
                _ChequeLehdarEntry(lehdar="Nurteks Halı Tic."),
            ]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            side_effect=[primary, refinement],
        ) as mock_generate:
            records = extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-cheque-refine-1",
                source_filename="cheques.jpg",
                source_type="image",
                category_hint=DocumentCategory.CEK,
            )

        assert mock_generate.call_count == 2
        primary_call = mock_generate.call_args_list[0]
        refinement_call = mock_generate.call_args_list[1]
        assert primary_call.kwargs["model"] == "gemini-test-extractor"
        assert refinement_call.kwargs["model"] == "gemini-test-flash"
        assert refinement_call.kwargs["response_schema"] is _ChequeLehdarRefinement
        prompt = refinement_call.kwargs["prompt"]
        assert "lehdar" in prompt.lower()
        assert "1, 3" in prompt  # missing-index hints (1-based)
        assert records[0].recipient_name == "HAMİLİNE"
        assert records[1].recipient_name == "ERIMER MERMER"
        assert records[2].recipient_name == "Nurteks Halı Tic."

    def test_cheque_extract_bills_skips_refinement_when_all_lehdars_present(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        primary = AIMultiExtractionResult(
            documents=[
                AIExtractionResult(
                    company_name="Banka",
                    recipient_name="HAMİLİNE",
                    currency="TRY",
                    confidence=0.9,
                ),
                AIExtractionResult(
                    company_name="Banka",
                    recipient_name="Nurteks Halı",
                    currency="TRY",
                    confidence=0.9,
                ),
            ]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=primary,
        ) as mock_generate:
            records = extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-cheque-no-refine-1",
                source_filename="cheques.jpg",
                source_type="image",
                category_hint=DocumentCategory.CEK,
            )

        assert mock_generate.call_count == 1
        assert records[0].recipient_name == "HAMİLİNE"
        assert records[1].recipient_name == "Nurteks Halı"

    def test_cheque_extract_bills_keeps_originals_when_refinement_fails(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        primary = AIMultiExtractionResult(
            documents=[
                AIExtractionResult(
                    company_name="Banka",
                    recipient_name=None,
                    currency="TRY",
                    confidence=0.9,
                )
            ]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            side_effect=[primary, RuntimeError("upstream down")],
        ) as mock_generate:
            records = extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-cheque-refine-fail-1",
                source_filename="cheque.jpg",
                source_type="image",
                category_hint=DocumentCategory.CEK,
            )

        assert mock_generate.call_count == 2
        assert records[0].recipient_name is None

    def test_shipment_prompt_requests_route_and_vehicle_details(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        expected = AIMultiExtractionResult(
            documents=[AIExtractionResult(company_name="Nakliye", currency="TRY", confidence=0.8)]
        )

        with patch(
            "app.services.accounting.gemini_extractor.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate:
            extract_bills(
                b"fake_image",
                mime_type="image/jpeg",
                source_message_id="msg-shipment-1",
                source_filename="shipment.jpg",
                source_type="image",
                category_hint=DocumentCategory.MALZEME,
            )

        prompt = mock_generate.call_args.kwargs["prompt"]
        assert "shipment_origin" in prompt
        assert "shipment_destination" in prompt
        assert "vehicle_plate" in prompt
        assert "pallet_count, items_per_pallet ve product_quantity" in prompt
        assert "18m3" in prompt
        assert "3AD" in prompt
        assert "5 TRB" in prompt

    def test_multi_document_extraction(self, monkeypatch):
        monkeypatch.setattr("app.services.accounting.gemini_extractor.settings.gemini_api_key", "fake_key")
        monkeypatch.setattr(
            "app.services.accounting.gemini_extractor.settings.gemini_extractor_model",
            "gemini-test-extractor",
        )
        doc1 = AIExtractionResult(company_name="Firma A", total_amount=100.0, currency="TRY", confidence=0.9)
        doc2 = AIExtractionResult(company_name="Firma B", total_amount=200.0, currency="TRY", confidence=0.88)
        doc3 = AIExtractionResult(company_name="Firma C", total_amount=300.0, currency="TRY", confidence=0.85)
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
