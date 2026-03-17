"""
Tests for the bill classifier service.
"""

from unittest.mock import MagicMock, patch

import pytest

from app.models.schemas import ClassificationResult
from app.services.bill_classifier import classify_text, classify_image


class TestClassifyText:
    def test_bill_text_detected(self):
        text = "Fatura No: 12345\nKDV: %18\nToplam: 1.234,56 TL\nÖdeme: Kredi Kartı\nVergi No: 123456789"
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
        # has some bill keywords but fewer than threshold
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
    def test_no_api_key_defaults_to_true(self, monkeypatch):
        monkeypatch.setattr("app.services.bill_classifier.settings.gemini_api_key", "")
        result = classify_image(b"fake_image_bytes")
        assert result.is_bill is True
        assert result.confidence == 0.5

    def test_gemini_response_parsed_correctly(self, monkeypatch):
        monkeypatch.setattr("app.services.bill_classifier.settings.gemini_api_key", "fake_key")

        mock_response = MagicMock()
        mock_response.text = '{"is_bill": true, "reason": "contains invoice data", "confidence": 0.92}'

        mock_model = MagicMock()
        mock_model.generate_content.return_value = mock_response

        with patch("app.services.bill_classifier.genai.GenerativeModel", return_value=mock_model):
            with patch("app.services.bill_classifier.genai.configure"):
                result = classify_image(b"fake_image_bytes")

        assert result.is_bill is True
        assert result.confidence == 0.92
        assert result.reason == "contains invoice data"

    def test_gemini_not_bill(self, monkeypatch):
        monkeypatch.setattr("app.services.bill_classifier.settings.gemini_api_key", "fake_key")

        mock_response = MagicMock()
        mock_response.text = '{"is_bill": false, "reason": "meme image", "confidence": 0.97}'

        mock_model = MagicMock()
        mock_model.generate_content.return_value = mock_response

        with patch("app.services.bill_classifier.genai.GenerativeModel", return_value=mock_model):
            with patch("app.services.bill_classifier.genai.configure"):
                result = classify_image(b"fake_image_bytes")

        assert result.is_bill is False

    def test_gemini_json_with_code_fence(self, monkeypatch):
        monkeypatch.setattr("app.services.bill_classifier.settings.gemini_api_key", "fake_key")

        mock_response = MagicMock()
        mock_response.text = '```json\n{"is_bill": true, "reason": "receipt", "confidence": 0.85}\n```'

        mock_model = MagicMock()
        mock_model.generate_content.return_value = mock_response

        with patch("app.services.bill_classifier.genai.GenerativeModel", return_value=mock_model):
            with patch("app.services.bill_classifier.genai.configure"):
                result = classify_image(b"fake_image_bytes")

        assert result.is_bill is True
        assert result.confidence == 0.85

    def test_gemini_error_returns_default(self, monkeypatch):
        monkeypatch.setattr("app.services.bill_classifier.settings.gemini_api_key", "fake_key")

        mock_model = MagicMock()
        mock_model.generate_content.side_effect = RuntimeError("API error")

        with patch("app.services.bill_classifier.genai.GenerativeModel", return_value=mock_model):
            with patch("app.services.bill_classifier.genai.configure"):
                result = classify_image(b"fake_image_bytes")

        # On error, defaults to is_bill=True with low confidence
        assert result.is_bill is True
        assert result.confidence < 0.5
