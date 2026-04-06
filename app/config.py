"""
Application configuration loaded from environment variables.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Server
    port: int = 8000

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

    # Gemini
    gemini_api_key: str = ""
    gemini_classifier_model: str = "gemini-2.5-flash-preview-04-17"
    gemini_extractor_model: str = "gemini-2.5-flash-preview-04-17"

    # Google Sheets
    # Base64-encoded service account JSON (from Google Cloud Console)
    google_service_account_json: str = ""
    # If set, always write to this specific spreadsheet (manual/single-sheet mode).
    # If empty, a new spreadsheet is auto-created each month and tracked in state/sheets_registry.json.
    google_sheets_spreadsheet_id: str = ""
    # Google account email to share every auto-created spreadsheet with (editor access).
    google_sheets_owner_email: str = "yilmazatakan4423@gmail.com"

    # Manager phone number (WhatsApp format, e.g. 905XXXXXXXXX@c.us or just 905XXXXXXXXX)
    # Text messages from this number are treated as elden ödeme (cash payment) entries.
    manager_phone_number: str = ""

    # Storage
    storage_dir: str = "./storage"

    # Logging
    log_level: str = "INFO"


settings = Settings()
