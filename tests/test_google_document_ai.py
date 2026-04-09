"""
Tests for Google Document AI bundle mapping and fallback selection.
"""

from unittest.mock import patch

from app.models.ocr import OCRMediaMetadata
from app.services.providers.google_document_ai import _bundle_from_response, process_document


def _metadata() -> OCRMediaMetadata:
    return OCRMediaMetadata(
        mime_type="image/jpeg",
        original_mime_type="image/jpeg",
        byte_size=1024,
        width=1200,
        height=1600,
        source_hash="abc123",
    )


def test_bundle_from_response_maps_text_tables_and_entities():
    full_text = (
        "ÖZTÜRK GIDA LTD. ŞTİ.\n"
        "Tarih: 09.04.2026\n"
        "Genel Toplam: 1.500,00 TL\n"
    )
    payload = {
        "document": {
            "text": full_text,
            "pages": [
                {
                    "lines": [
                        {
                            "layout": {
                                "textAnchor": {"textSegments": [{"startIndex": 0, "endIndex": 21}]},
                                "confidence": 0.94,
                            }
                        },
                        {
                            "layout": {
                                "textAnchor": {"textSegments": [{"startIndex": 22, "endIndex": 40}]},
                                "confidence": 0.91,
                            }
                        },
                    ],
                    "formFields": [
                        {
                            "fieldName": {"textAnchor": {"textSegments": [{"startIndex": 22, "endIndex": 27}]}},
                            "fieldValue": {"textAnchor": {"textSegments": [{"startIndex": 29, "endIndex": 39}]}},
                            "confidence": 0.88,
                        }
                    ],
                    "tables": [
                        {
                            "bodyRows": [
                                {
                                    "cells": [
                                        {
                                            "layout": {
                                                "textAnchor": {"textSegments": [{"startIndex": 0, "endIndex": 21}]},
                                                "confidence": 0.82,
                                            }
                                        },
                                        {
                                            "layout": {
                                                "textAnchor": {"textSegments": [{"startIndex": 41, "endIndex": 66}]},
                                                "confidence": 0.81,
                                            }
                                        },
                                    ]
                                }
                            ],
                            "confidence": 0.84,
                        }
                    ],
                }
            ],
            "entities": [
                {"type": "supplier_name", "mentionText": "ÖZTÜRK GIDA LTD. ŞTİ.", "confidence": 0.96},
            ],
        }
    }

    bundle = _bundle_from_response(
        payload,
        metadata=_metadata(),
        processor_name="form_parser",
        used_fallback=False,
    )

    assert bundle.processor_used == "form_parser"
    assert bundle.text == full_text
    assert bundle.lines
    assert bundle.key_values[0].key.startswith("Tarih")
    assert bundle.entities[0].mention_text == "ÖZTÜRK GIDA LTD. ŞTİ."
    assert bundle.tables[0].cells[0].text == "ÖZTÜRK GIDA LTD. ŞTİ."
    assert bundle.quality_score > 0.0


def test_process_document_uses_enterprise_ocr_when_primary_is_weak(monkeypatch):
    monkeypatch.setattr(
        "app.services.providers.google_document_ai.settings.google_document_ai_form_processor_id",
        "form-processor",
    )
    monkeypatch.setattr(
        "app.services.providers.google_document_ai.settings.google_document_ai_ocr_processor_id",
        "ocr-processor",
    )
    monkeypatch.setattr(
        "app.services.providers.google_document_ai.settings.google_document_ai_project_id",
        "demo-project",
    )
    monkeypatch.setattr(
        "app.services.providers.google_document_ai.settings.google_service_account_json",
        '{"project_id":"demo-project","client_email":"svc@example.com","private_key":"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n","token_uri":"https://oauth2.googleapis.com/token"}',
    )

    weak_payload = {"document": {"text": "abc", "pages": []}}
    strong_payload = {
        "document": {
            "text": "ÖZTÜRK GIDA LTD. ŞTİ.\nTarih: 09.04.2026\nToplam: 1.500,00 TL",
            "pages": [],
            "entities": [{"type": "supplier_name", "mentionText": "ÖZTÜRK GIDA LTD. ŞTİ.", "confidence": 0.95}],
        }
    }

    with patch(
        "app.services.providers.google_document_ai._process_with_processor",
        side_effect=[weak_payload, strong_payload],
    ):
        bundle = process_document(b"fake-image", _metadata())

    assert bundle.processor_used == "enterprise_ocr"
    assert bundle.used_fallback_processor is True
