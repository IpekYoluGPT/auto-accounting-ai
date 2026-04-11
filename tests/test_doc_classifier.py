"""
Tests for Gemini-first document category classification.
"""

from types import SimpleNamespace
from unittest.mock import ANY, patch

from app.models.schemas import DocumentCategory
from app.services.accounting.doc_classifier import DocumentAnalysis, analyze_document, classify_document_type


def test_analyze_document_parses_structured_result(monkeypatch):
    monkeypatch.setattr("app.services.gemini_client.settings.gemini_api_key", "fake_key")
    monkeypatch.setattr(
        "app.services.accounting.doc_classifier.settings.gemini_classifier_model",
        "gemini-test-classifier",
    )

    raw = SimpleNamespace(
        is_financial_document=True,
        category="cek",
        is_return=False,
        document_count=3,
        quality="usable",
        needs_retry=False,
        confidence=0.81,
        reason="three cheque leaves detected",
    )

    with patch(
        "app.services.accounting.doc_classifier.gemini_client.generate_structured_content",
        return_value=raw,
    ) as mock_generate:
        result = analyze_document(b"fake", mime_type="application/pdf")

    assert result == DocumentAnalysis(
        is_financial_document=True,
        category=DocumentCategory.CEK,
        is_return=False,
        document_count=3,
        quality="usable",
        needs_retry=False,
        confidence=0.81,
        reason="three cheque leaves detected",
    )
    mock_generate.assert_called_once_with(
        model="gemini-test-classifier",
        prompt=ANY,
        system_instruction=ANY,
        response_schema=ANY,
        thinking_level="low",
        media_bytes=b"fake",
        mime_type="application/pdf",
    )

def test_analyze_document_unknown_category_defaults_to_belirsiz(monkeypatch):
    monkeypatch.setattr("app.services.gemini_client.settings.gemini_api_key", "fake_key")
    with patch(
        "app.services.accounting.doc_classifier.gemini_client.generate_structured_content",
        return_value=SimpleNamespace(
            is_financial_document=True,
            category="something-else",
            is_return=False,
            document_count=1,
            quality="clear",
            needs_retry=False,
            confidence=0.7,
            reason="unknown type",
        ),
    ):
        result = analyze_document(b"fake")

    assert result.category == DocumentCategory.BELIRSIZ
    assert result.is_return is False

def test_analyze_document_sets_zero_count_for_non_financial(monkeypatch):
    monkeypatch.setattr("app.services.gemini_client.settings.gemini_api_key", "fake_key")
    with patch(
        "app.services.accounting.doc_classifier.gemini_client.generate_structured_content",
        return_value=SimpleNamespace(
            is_financial_document=False,
            category="belirsiz",
            is_return=False,
            document_count=4,
            quality="poor",
            needs_retry=False,
            confidence=0.64,
            reason="not a document",
        ),
    ):
        result = analyze_document(b"fake")

    assert result.document_count == 4
    assert result.is_financial_document is False

def test_classify_document_type_returns_tuple():
    with patch(
        "app.services.accounting.doc_classifier.analyze_document",
        return_value=DocumentAnalysis(
            is_financial_document=True,
            category=DocumentCategory.FATURA,
            is_return=True,
            document_count=1,
            quality="usable",
            needs_retry=False,
            confidence=0.9,
            reason="return invoice",
        ),
    ):
        category, is_return = classify_document_type(b"fake", ocr_bundle=object())

    assert category == DocumentCategory.FATURA
    assert is_return is True

def test_classify_document_type_falls_back_to_belirsiz_on_error():
    with patch(
        "app.services.accounting.doc_classifier.analyze_document",
        side_effect=RuntimeError("boom"),
    ):
        category, is_return = classify_document_type(b"fake")

    assert category == DocumentCategory.BELIRSIZ
    assert is_return is False
