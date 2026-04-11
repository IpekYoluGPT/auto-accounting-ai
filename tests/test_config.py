"""
Tests for application settings defaults.
"""

from app.config import Settings


def test_gemini_model_defaults(monkeypatch):
    monkeypatch.delenv("STORAGE_DIR", raising=False)
    monkeypatch.delenv("RAILWAY_VOLUME_MOUNT_PATH", raising=False)
    settings = Settings(_env_file=None)
    assert settings.gemini_classifier_model == "gemini-2.5-pro"
    assert settings.gemini_extractor_model == "gemini-2.5-pro"
    assert settings.gemini_validation_model == "gemini-2.5-pro"
    assert settings.whatsapp_groups_only is True
    assert settings.periskope_api_base_url == "https://api.periskope.app/v1"
    assert settings.periskope_media_base_url == "https://api.periskope.app"
    assert settings.business_timezone == "Europe/Istanbul"
    assert settings.google_document_ai_location == "eu"
    assert settings.ocr_min_text_chars == 60
    assert settings.ocr_min_parse_score == 0.72
    assert settings.ocr_min_quality_score == 0.45
    assert settings.storage_dir == "./storage"
    assert settings.pending_payload_storage_limit_mb == 192


def test_environment_overrides_are_applied(monkeypatch):
    monkeypatch.setenv("STORAGE_DIR", "/tmp/auto-accounting")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("WHATSAPP_VERIFY_TOKEN", "verify-me")
    monkeypatch.setenv("WHATSAPP_GROUPS_ONLY", "false")
    monkeypatch.setenv("PERISKOPE_API_KEY", "periskope-key")
    monkeypatch.setenv("PERISKOPE_TOOL_TOKEN", "tool-key")
    monkeypatch.setenv("BUSINESS_TIMEZONE", "UTC")
    monkeypatch.setenv("GOOGLE_DOCUMENT_AI_PROJECT_ID", "ocr-project")
    monkeypatch.setenv("OCR_MIN_TEXT_CHARS", "80")
    monkeypatch.setenv("OCR_MIN_PARSE_SCORE", "0.8")
    monkeypatch.setenv("OCR_MIN_QUALITY_SCORE", "0.55")

    settings = Settings(_env_file=None)

    assert settings.storage_dir == "/tmp/auto-accounting"
    assert settings.log_level == "DEBUG"
    assert settings.whatsapp_verify_token == "verify-me"
    assert settings.whatsapp_groups_only is False
    assert settings.periskope_api_key == "periskope-key"
    assert settings.periskope_tool_token == "tool-key"
    assert settings.business_timezone == "UTC"
    assert settings.google_document_ai_project_id == "ocr-project"
    assert settings.ocr_min_text_chars == 80
    assert settings.ocr_min_parse_score == 0.8
    assert settings.ocr_min_quality_score == 0.55


def test_short_document_ai_env_aliases_are_accepted(monkeypatch):
    monkeypatch.setenv("GOOGLE_DOCUMENT_AI_FOR_PROCESSOR", "form-short")
    monkeypatch.setenv("GOOGLE_DOCUMENT_AI_OCR_PROCESSOR", "ocr-short")

    settings = Settings(_env_file=None)

    assert settings.google_document_ai_form_processor_id == "form-short"
    assert settings.google_document_ai_ocr_processor_id == "ocr-short"



def test_railway_volume_mount_path_becomes_default_storage_dir(monkeypatch):
    monkeypatch.delenv("STORAGE_DIR", raising=False)
    monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", "/data/railway-volume")

    settings = Settings(_env_file=None)

    assert settings.storage_dir == "/data/railway-volume"
