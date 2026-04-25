"""
Application configuration loaded from environment variables.
"""

import os

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_storage_dir() -> str:
    return os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip() or "./storage"


DEFAULT_GEMINI_MODEL = "gemini-3.1-pro-preview"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Server
    port: int = 8000
    business_timezone: str = "Europe/Istanbul"

    # WhatsApp Cloud API
    whatsapp_verify_token: str = "changeme"
    whatsapp_access_token: str = ""
    whatsapp_phone_number_id: str = ""
    whatsapp_groups_only: bool = True

    # Periskope
    periskope_api_key: str = ""
    periskope_phone: str = ""
    periskope_api_base_url: str = "https://api.periskope.app/v1"
    periskope_media_base_url: str = "https://api.periskope.app"
    periskope_signing_key: str = ""
    periskope_tool_token: str = ""
    periskope_allowed_chat_ids: str = ""
    periskope_max_message_age_minutes: int = 60

    # Gemini
    gemini_api_key: str = ""
    gemini_classifier_model: str = DEFAULT_GEMINI_MODEL
    gemini_extractor_model: str = DEFAULT_GEMINI_MODEL
    gemini_validation_model: str = DEFAULT_GEMINI_MODEL

    # Google Sheets
    # Base64-encoded service account JSON (from Google Cloud Console)
    google_service_account_json: str = ""
    # Current month's spreadsheet ID. Used as seed for this month; auto-creates next month.
    google_sheets_spreadsheet_id: str = ""
    # Google Drive folder ID owned by user. New monthly sheets are created here.
    # User must share this folder with the service account (Editor).
    google_drive_parent_folder_id: str = ""
    # Google account email to share every auto-created spreadsheet with (editor access).
    google_sheets_owner_email: str = ""

    # Google OAuth2 (for spreadsheet creation on behalf of user)
    # Service accounts cannot create Sheets files (403 quota/permission error).
    # OAuth2 lets the system create files as the real user.
    # Set client_id + client_secret from Google Cloud Console > Credentials > OAuth 2.0.
    # Set refresh_token after running the one-time /setup/google-auth flow.
    google_oauth_client_id: str = ""
    google_oauth_client_secret: str = ""
    google_oauth_refresh_token: str = ""

    # Google Document AI
    google_document_ai_project_id: str = ""
    google_document_ai_location: str = "eu"
    google_document_ai_form_processor_id: str = Field(
        default="",
        validation_alias=AliasChoices(
            "GOOGLE_DOCUMENT_AI_FORM_PROCESSOR_ID",
            "GOOGLE_DOCUMENT_AI_FOR_PROCESSOR",
        ),
    )
    google_document_ai_ocr_processor_id: str = Field(
        default="",
        validation_alias=AliasChoices(
            "GOOGLE_DOCUMENT_AI_OCR_PROCESSOR_ID",
            "GOOGLE_DOCUMENT_AI_OCR_PROCESSOR",
        ),
    )

    # OCR pipeline thresholds
    ocr_min_text_chars: int = 60
    ocr_min_parse_score: float = 0.72
    ocr_min_quality_score: float = 0.45

    # Manager phone number (WhatsApp format, e.g. 905XXXXXXXXX@c.us or just 905XXXXXXXXX)
    # Text messages from this number are treated as elden ödeme (cash payment) entries.
    manager_phone_number: str = ""

    # Storage
    storage_dir: str = Field(default_factory=_default_storage_dir)
    pending_payload_storage_limit_mb: int = 192
    inbound_retry_max_attempts: int = 20
    inbound_retry_max_age_hours: int = 24
    inbound_worker_poll_seconds: int = 5
    inbound_max_active_jobs: int = 2
    gemini_max_concurrency: int = 1
    storage_soft_pressure_bytes: int = 3221225472
    storage_hard_reject_bytes: int = 4026531840
    storage_emergency_stop_bytes: int = 4563402752
    storage_min_free_bytes: int = 1342177280

    # Logging
    log_level: str = "INFO"


settings = Settings()
