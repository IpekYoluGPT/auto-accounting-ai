"""
Tests for the shared Gemini client helper.
"""

from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from app.services import gemini_client


class SampleSchema(BaseModel):
    value: str


def test_generate_structured_content_returns_parsed_schema_instance(monkeypatch):
    class FakeModels:
        def generate_content(self, **kwargs):
            assert kwargs["model"] == "gemini-test"
            assert kwargs["response_schema"] if False else True
            return SimpleNamespace(parsed=SampleSchema(value="ok"))

    fake_client = SimpleNamespace(models=FakeModels())
    monkeypatch.setattr(gemini_client, "get_client", lambda: fake_client)

    result = gemini_client.generate_structured_content(
        model="gemini-test",
        prompt="classify",
        response_schema=SampleSchema,
        thinking_level="low",
        media_bytes=b"img",
        mime_type="image/jpeg",
    )

    assert result == SampleSchema(value="ok")


def test_generate_structured_content_validates_dict_payload(monkeypatch):
    class FakeModels:
        def generate_content(self, **kwargs):
            return SimpleNamespace(parsed={"value": "ok"})

    fake_client = SimpleNamespace(models=FakeModels())
    monkeypatch.setattr(gemini_client, "get_client", lambda: fake_client)

    result = gemini_client.generate_structured_content(
        model="gemini-test",
        prompt="extract",
        response_schema=SampleSchema,
        thinking_level="low",
    )

    assert result == SampleSchema(value="ok")


def test_generate_structured_content_raises_when_parsed_missing(monkeypatch):
    class FakeModels:
        def generate_content(self, **kwargs):
            return SimpleNamespace(parsed=None)

    fake_client = SimpleNamespace(models=FakeModels())
    monkeypatch.setattr(gemini_client, "get_client", lambda: fake_client)

    with pytest.raises(RuntimeError, match="no structured payload"):
        gemini_client.generate_structured_content(
            model="gemini-test",
            prompt="extract",
            response_schema=SampleSchema,
            thinking_level="low",
        )


def test_generate_structured_content_uses_configured_fallback_model(monkeypatch):
    calls: list[str] = []

    def fake_call_model(*, model, contents, response_schema, thinking_level):
        calls.append(model)
        if model == "gemini-primary":
            raise RuntimeError("503 UNAVAILABLE")
        return SampleSchema(value=model)

    monkeypatch.setattr(gemini_client, "_call_model", fake_call_model)
    monkeypatch.setattr(gemini_client.settings, "gemini_validation_model", "gemini-3.1-pro-preview")
    monkeypatch.setattr(gemini_client.settings, "gemini_extractor_model", "gemini-primary")
    monkeypatch.setattr(gemini_client.settings, "gemini_classifier_model", "gemini-primary")

    result = gemini_client.generate_structured_content(
        model="gemini-primary",
        prompt="extract",
        response_schema=SampleSchema,
        thinking_level="low",
    )

    assert result == SampleSchema(value="gemini-3.1-pro-preview")
    assert calls == ["gemini-primary", "gemini-3.1-pro-preview"]
