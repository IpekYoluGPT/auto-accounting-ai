"""
Tests for the bill classifier service.
"""

from unittest.mock import patch

import pytest

from app.models.schemas import ClassificationResult, DocumentCategory
from app.services.accounting.bill_classifier import classify_image, classify_text
from app.services.accounting.doc_classifier import DocumentAnalysis


class TestClassifyText:
    def test_bill_text_detected(self):
        text = (
            "Fatura No: 12345\n"
            "KDV: %18\n"
            "Toplam: 1.234,56 TL\n"
            "Ödeme: Kredi Kartı\n"
            "Vergi No: 123456789"
        )
        result = classify_text(text)
        assert result.is_bill is True
        assert result.confidence >= 0.6

    def test_junk_text_ignored(self):
        text = "Merhaba! Nasılsın? 😂 İyi akşamlar herkese 👍"
        result = classify_text(text)
        assert result.is_bill is False
        assert result.confidence >= 0.7

    def test_greeting_only(self):
        text = "Selam tamam ok 👍"
        result = classify_text(text)
        assert result.is_bill is False

    def test_partial_bill_keywords(self):
        text = "Tutar: 200 TL"
        result = classify_text(text)
        assert isinstance(result, ClassificationResult)

    def test_empty_text(self):
        result = classify_text("")
        assert result.is_bill is False

    def test_invoice_english_keywords(self):
        text = "Invoice #001\nTotal: $150.00\nTax: $12.00\nPayment: Credit Card"
        result = classify_text(text)
        assert result.is_bill is True

    def test_result_is_classification_result(self):
        result = classify_text("some text")
        assert isinstance(result, ClassificationResult)
        assert 0.0 <= result.confidence <= 1.0


class TestClassifyImage:
    def test_no_api_key_raises(self, monkeypatch):
        monkeypatch.setattr("app.services.gemini_client.settings.gemini_api_key", "")
        with pytest.raises(RuntimeError, match="GEMINI_API_KEY"):
            classify_image(b"fake_image_bytes")

    def test_document_analysis_result_is_mapped(self):
        expected_analysis = DocumentAnalysis(
            is_financial_document=True,
            category=DocumentCategory.FATURA,
            is_return=False,
            document_count=1,
            quality="clear",
            needs_retry=False,
            confidence=0.92,
            reason="contains invoice data",
        )

        with patch(
            "app.services.accounting.bill_classifier.doc_classifier.analyze_document",
            return_value=expected_analysis,
        ) as mock_analyze:
            result = classify_image(b"fake_image_bytes", mime_type="image/png")

        assert result == ClassificationResult(
            is_bill=True,
            reason="contains invoice data",
            confidence=0.92,
        )
        mock_analyze.assert_called_once_with(b"fake_image_bytes", mime_type="image/png")

    def test_deprecated_ocr_bundle_is_ignored(self):
        with patch(
            "app.services.accounting.bill_classifier.doc_classifier.analyze_document",
            return_value=DocumentAnalysis(
                is_financial_document=False,
                category=DocumentCategory.BELIRSIZ,
                is_return=False,
                document_count=0,
                quality="usable",
                needs_retry=False,
                confidence=0.8,
                reason="not a document",
            ),
        ) as mock_analyze:
            result = classify_image(b"fake_image_bytes", ocr_bundle=object())

        assert result.is_bill is False
        assert result.reason == "not a document"
        mock_analyze.assert_called_once_with(b"fake_image_bytes", mime_type="image/jpeg")

    def test_generation_error_propagates(self):
        with patch(
            "app.services.accounting.bill_classifier.doc_classifier.analyze_document",
            side_effect=RuntimeError("API error"),
        ):
            with pytest.raises(RuntimeError, match="API error"):
                classify_image(b"fake_image_bytes")

    def test_sample_invoice_like_template_is_not_overridden(self):
        with patch(
            "app.services.accounting.bill_classifier.doc_classifier.analyze_document",
            return_value=DocumentAnalysis(
                is_financial_document=False,
                category=DocumentCategory.FATURA,
                is_return=False,
                document_count=1,
                quality="clear",
                needs_retry=False,
                confidence=0.95,
                reason=(
                    "The document is explicitly labeled 'ORNEK FATURA' "
                    "and appears to be a sample invoice template."
                ),
            ),
        ):
            result = classify_image(b"fake_image_bytes")

        assert result == ClassificationResult(
            is_bill=False,
            reason=(
                "The document is explicitly labeled 'ORNEK FATURA' "
                "and appears to be a sample invoice template."
            ),
            confidence=0.95,
        )

    def test_non_document_rejection_is_not_overridden(self):
        expected_analysis = DocumentAnalysis(
            is_financial_document=False,
            category=DocumentCategory.BELIRSIZ,
            is_return=False,
            document_count=0,
            quality="clear",
            needs_retry=False,
            confidence=0.98,
            reason="This is a cat photo and not a financial document.",
        )
        with patch(
            "app.services.accounting.bill_classifier.doc_classifier.analyze_document",
            return_value=expected_analysis,
        ):
            result = classify_image(b"fake_image_bytes")

        assert result == ClassificationResult(
            is_bill=False,
            reason="This is a cat photo and not a financial document.",
            confidence=0.98,
        )
