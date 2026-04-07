"""
Tests for application settings defaults.
"""

from app.config import Settings


def test_gemini_model_defaults():
    settings = Settings(_env_file=None)
    assert settings.gemini_classifier_model == "gemini-2.5-flash"
    assert settings.gemini_extractor_model == "gemini-2.5-flash"
    assert settings.whatsapp_groups_only is True
    assert settings.periskope_api_base_url == "https://api.periskope.app/v1"
    assert settings.periskope_media_base_url == "https://api.periskope.app"


def test_environment_overrides_are_applied(monkeypatch):
    monkeypatch.setenv("STORAGE_DIR", "/tmp/auto-accounting")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("WHATSAPP_VERIFY_TOKEN", "verify-me")
    monkeypatch.setenv("WHATSAPP_GROUPS_ONLY", "false")
    monkeypatch.setenv("PERISKOPE_API_KEY", "periskope-key")
    monkeypatch.setenv("PERISKOPE_TOOL_TOKEN", "tool-key")

    settings = Settings(_env_file=None)

    assert settings.storage_dir == "/tmp/auto-accounting"
    assert settings.log_level == "DEBUG"
    assert settings.whatsapp_verify_token == "verify-me"
    assert settings.whatsapp_groups_only is False
    assert settings.periskope_api_key == "periskope-key"
    assert settings.periskope_tool_token == "tool-key"
