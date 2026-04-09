"""
Tests for OCR-grounded document category classification.
"""

from unittest.mock import patch

from app.models.ocr import OCRMediaMetadata, OCRParseBundle
from app.models.schemas import DocumentCategory
from app.services.accounting.doc_classifier import classify_document_type


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
            source_hash="hash-doc",
        ),
    )


def test_ocr_direct_category_detection_skips_gemini():
    bundle = _bundle("E-Arşiv Fatura\nTarih: 09.04.2026\nToplam: 1.500,00 TL")

    with patch("app.services.accounting.doc_classifier.gemini_client.generate_structured_content") as mock_generate:
        category, is_return = classify_document_type(b"fake", ocr_bundle=bundle)

    assert category == DocumentCategory.FATURA
    assert is_return is False
    mock_generate.assert_not_called()


def test_ocr_ambiguous_category_falls_back_to_gemini(monkeypatch):
    monkeypatch.setattr("app.services.gemini_client.settings.gemini_api_key", "fake_key")
    bundle = _bundle("ABC\n123", quality_score=0.4)

    with patch(
        "app.services.accounting.doc_classifier.gemini_client.generate_structured_content",
        return_value=type("Obj", (), {"category": "cek", "is_return": False, "confidence": 0.8, "reason": "keyword"})(),
    ) as mock_generate:
        category, is_return = classify_document_type(b"fake", ocr_bundle=bundle)

    assert category == DocumentCategory.CEK
    assert is_return is False
    assert "OCR_TEXT" in mock_generate.call_args.kwargs["prompt"]
