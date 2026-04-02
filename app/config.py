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

    # Gemini
    gemini_api_key: str = ""
    gemini_classifier_model: str = "gemini-3-flash-preview"
    gemini_extractor_model: str = "gemini-3-flash-preview"

    # Storage
    storage_dir: str = "./storage"

    # Logging
    log_level: str = "INFO"


settings = Settings()
