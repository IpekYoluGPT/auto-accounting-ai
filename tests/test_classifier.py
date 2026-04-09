"""
Tests for the bill classifier service.
"""

from unittest.mock import ANY, patch

import pytest

from app.models.ocr import OCRMediaMetadata, OCRParseBundle
from app.models.schemas import ClassificationResult
from app.services.accounting.bill_classifier import classify_image, classify_text


def _ocr_bundle(text: str, *, quality_score: float = 0.88) -> OCRParseBundle:
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
            width=1200,
            height=1600,
            source_hash="abc123",
        ),
    )


class TestClassifyText:
    def test_bill_text_detected(self):
        text = (
            "Fatura No: 12345\n"
            "KDV: %18\n"
            "Toplam: 1.234,56 TL\n"
            "\u00d6deme: Kredi Kart\u0131\n"
            "Vergi No: 123456789"
        )
        result = classify_text(text)
        assert result.is_bill is True
        assert result.confidence >= 0.6

    def test_junk_text_ignored(self):
        text = "Merhaba! Nas\u0131ls\u0131n? \U0001F602 \u0130yi ak\u015famlar herkese \U0001F44D"
        result = classify_text(text)
        assert result.is_bill is False
        assert result.confidence >= 0.7

    def test_greeting_only(self):
        text = "Selam tamam ok \U0001F44D"
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

    def test_structured_response_parsed_correctly(self, monkeypatch):
        monkeypatch.setattr("app.services.gemini_client.settings.gemini_api_key", "fake_key")
        monkeypatch.setattr(
            "app.services.accounting.bill_classifier.settings.gemini_classifier_model",
            "gemini-test-classifier",
        )
        expected = ClassificationResult(
            is_bill=True,
            reason="contains invoice data",
            confidence=0.92,
        )

        with patch(
            "app.services.accounting.bill_classifier.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate:
            result = classify_image(b"fake_image_bytes", mime_type="image/png")

        assert result == expected
        mock_generate.assert_called_once_with(
            model="gemini-test-classifier",
            prompt=ANY,
            response_schema=ClassificationResult,
            thinking_level="minimal",
            media_bytes=b"fake_image_bytes",
            mime_type="image/png",
        )

    def test_ocr_direct_positive_short_circuits_gemini(self):
        bundle = _ocr_bundle("ÖZTÜRK GIDA LTD. ŞTİ.\nTarih: 09.04.2026\nToplam: 1.500,00 TL\nKDV %20: 250,00 TL")

        with patch("app.services.accounting.bill_classifier.gemini_client.generate_structured_content") as mock_generate:
            result = classify_image(b"fake_image_bytes", ocr_bundle=bundle)

        assert result.is_bill is True
        assert result.reason == "ocr financial anchors"
        mock_generate.assert_not_called()

    def test_ocr_direct_negative_short_circuits_gemini(self):
        bundle = _ocr_bundle("Happy birthday to you\nSee you tomorrow at the cafe", quality_score=0.9)

        with patch("app.services.accounting.bill_classifier.gemini_client.generate_structured_content") as mock_generate:
            result = classify_image(b"fake_image_bytes", ocr_bundle=bundle)

        assert result.is_bill is False
        assert result.reason == "ocr lacks financial anchors"
        mock_generate.assert_not_called()

    def test_ocr_ambiguous_falls_back_to_gemini(self, monkeypatch):
        monkeypatch.setattr("app.services.gemini_client.settings.gemini_api_key", "fake_key")
        expected = ClassificationResult(is_bill=True, reason="Gemini fallback", confidence=0.81)
        bundle = _ocr_bundle("ABC\n123", quality_score=0.4)

        with patch(
            "app.services.accounting.bill_classifier.gemini_client.generate_structured_content",
            return_value=expected,
        ) as mock_generate:
            result = classify_image(b"fake_image_bytes", mime_type="image/png", ocr_bundle=bundle)

        assert result == expected
        assert "OCR_TEXT" in mock_generate.call_args.kwargs["prompt"]

    def test_generation_error_propagates(self, monkeypatch):
        monkeypatch.setattr("app.services.gemini_client.settings.gemini_api_key", "fake_key")

        with patch(
            "app.services.accounting.bill_classifier.gemini_client.generate_structured_content",
            side_effect=RuntimeError("API error"),
        ):
            with pytest.raises(RuntimeError, match="API error"):
                classify_image(b"fake_image_bytes")

    def test_sample_invoice_like_template_is_accepted(self, monkeypatch):
        monkeypatch.setattr("app.services.gemini_client.settings.gemini_api_key", "fake_key")
        with patch(
            "app.services.accounting.bill_classifier.gemini_client.generate_structured_content",
            return_value=ClassificationResult(
                is_bill=False,
                reason=(
                    "The document is explicitly labeled 'ORNEK FATURA' "
                    "and appears to be a sample invoice template."
                ),
                confidence=0.95,
            ),
        ):
            result = classify_image(b"fake_image_bytes")

        assert result.is_bill is True
        assert result.reason == "invoice-like template override"
        assert result.confidence >= 0.6

    def test_non_document_rejection_is_not_overridden(self, monkeypatch):
        monkeypatch.setattr("app.services.gemini_client.settings.gemini_api_key", "fake_key")
        expected = ClassificationResult(
            is_bill=False,
            reason="This is a cat photo and not a financial document.",
            confidence=0.98,
        )
        with patch(
            "app.services.accounting.bill_classifier.gemini_client.generate_structured_content",
            return_value=expected,
        ):
            result = classify_image(b"fake_image_bytes")

        assert result == expected
