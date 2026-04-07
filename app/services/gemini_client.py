"""
Shared Gemini client helpers built on the modern google-genai SDK.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TypeVar

from google import genai
from google.genai import types
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings
from app.utils.logging import get_logger

logger = get_logger(__name__)

SchemaT = TypeVar("SchemaT", bound=BaseModel)

# Fallback model when the primary model returns 503 overload errors.
# gemini-2.0-flash is no longer available to new API key users (returns 404).
_FALLBACK_MODEL = "gemini-1.5-flash"


@lru_cache(maxsize=4)
def _build_client(api_key: str) -> genai.Client:
    """Create and memoize Gemini clients by API key."""
    return genai.Client(api_key=api_key)


def get_client() -> genai.Client:
    """Return a configured Gemini API client."""
    if not settings.gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured.")
    return _build_client(settings.gemini_api_key)


def _call_model(
    *,
    model: str,
    contents: list,
    response_schema: type[SchemaT],
    thinking_level: str,
) -> SchemaT:
    """Single attempt to call a model (no retry — caller handles retries)."""
    client = get_client()

    config_kwargs: dict = {
        "response_mime_type": "application/json",
        "response_schema": response_schema,
    }
    # Only add thinking_config for models that support it (preview models)
    if thinking_level and "preview" in model:
        config_kwargs["thinking_config"] = types.ThinkingConfig(thinking_level=thinking_level)

    response = client.models.generate_content(
        model=model,
        contents=contents,
        config=types.GenerateContentConfig(**config_kwargs),
    )

    if response.parsed is None:
        raise RuntimeError("Gemini returned no structured payload.")

    if isinstance(response.parsed, response_schema):
        return response.parsed

    return response_schema.model_validate(response.parsed)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def generate_structured_content(
    *,
    model: str,
    prompt: str,
    response_schema: type[SchemaT],
    thinking_level: str,
    media_bytes: bytes | None = None,
    mime_type: str | None = None,
) -> SchemaT:
    """Generate structured JSON and validate it against a Pydantic schema.

    Retries up to 5 times with exponential backoff.
    On 503 overload errors, automatically falls back to gemini-2.0-flash.
    """
    contents: list[str | types.Part] = [prompt]
    if media_bytes is not None:
        contents.append(
            types.Part.from_bytes(
                data=media_bytes,
                mime_type=mime_type or "application/octet-stream",
            )
        )

    try:
        return _call_model(
            model=model,
            contents=contents,
            response_schema=response_schema,
            thinking_level=thinking_level,
        )
    except Exception as exc:
        error_str = str(exc)
        # On 503 overload, try fallback model immediately before tenacity retries
        if "503" in error_str and model != _FALLBACK_MODEL:
            logger.warning(
                "Model %s returned 503 overload — trying fallback %s",
                model,
                _FALLBACK_MODEL,
            )
            try:
                return _call_model(
                    model=_FALLBACK_MODEL,
                    contents=contents,
                    response_schema=response_schema,
                    thinking_level=thinking_level,
                )
            except Exception as fallback_exc:
                logger.warning("Fallback model also failed: %s", fallback_exc)
                raise exc  # re-raise original so tenacity retries primary
        raise
