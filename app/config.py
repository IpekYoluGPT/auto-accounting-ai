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
    gemini_classifier_model: str = "gemini-flash-lite-latest"
    gemini_extractor_model: str = "gemini-flash-lite-latest"

    # Storage
    storage_dir: str = "./storage"

    # Logging
    log_level: str = "INFO"


settings = Settings()
