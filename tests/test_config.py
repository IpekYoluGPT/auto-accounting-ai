"""
Tests for application settings defaults.
"""

from app.config import Settings


def test_gemini_model_defaults():
    settings = Settings(_env_file=None)
    assert settings.gemini_classifier_model == "gemini-3-flash-preview"
    assert settings.gemini_extractor_model == "gemini-3-flash-preview"


def test_environment_overrides_are_applied(monkeypatch):
    monkeypatch.setenv("STORAGE_DIR", "/tmp/auto-accounting")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("WHATSAPP_VERIFY_TOKEN", "verify-me")

    settings = Settings(_env_file=None)

    assert settings.storage_dir == "/tmp/auto-accounting"
    assert settings.log_level == "DEBUG"
    assert settings.whatsapp_verify_token == "verify-me"
