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


@lru_cache(maxsize=4)
def _build_client(api_key: str) -> genai.Client:
    """Create and memoize Gemini clients by API key."""
    return genai.Client(api_key=api_key)



def get_client() -> genai.Client:
    """Return a configured Gemini API client."""
    if not settings.gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured.")
    return _build_client(settings.gemini_api_key)



def _fallback_models(primary_model: str) -> list[str]:
    """Return configured fallback models in priority order, excluding the primary."""
    candidates = [
        settings.gemini_validation_model,
        settings.gemini_extractor_model,
        settings.gemini_classifier_model,
    ]
    unique_models: list[str] = []
    for candidate in candidates:
        model = (candidate or "").strip()
        if not model or model == primary_model or model in unique_models:
            continue
        unique_models.append(model)
    return unique_models



def _call_model(
    *,
    model: str,
    contents: list,
    response_schema: type[SchemaT],
    thinking_level: str,
    system_instruction: str | None = None,
) -> SchemaT:
    """Single attempt to call a model (no retry, caller handles retries)."""
    client = get_client()

    config_kwargs: dict = {
        "response_mime_type": "application/json",
        "response_schema": response_schema,
    }
    if system_instruction:
        config_kwargs["system_instruction"] = system_instruction
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
    system_instruction: str | None = None,
) -> SchemaT:
    """Generate structured JSON and validate it against a Pydantic schema.

    Retries up to 5 times with exponential backoff.
    On upstream overload/quota errors, automatically tries the other configured
    Gemini models before tenacity retries the original request.
    """
    contents: list[str | types.Part] = []
    if media_bytes is not None:
        contents.append(
            types.Part.from_bytes(
                data=media_bytes,
                mime_type=mime_type or "application/octet-stream",
            )
        )
    contents.append(prompt)

    try:
        call_kwargs = {
            "model": model,
            "contents": contents,
            "response_schema": response_schema,
            "thinking_level": thinking_level,
        }
        if system_instruction is not None:
            call_kwargs["system_instruction"] = system_instruction
        return _call_model(**call_kwargs)
    except Exception as exc:
        error_str = str(exc)
        if any(token in error_str for token in ("503", "429", "UNAVAILABLE", "RESOURCE_EXHAUSTED")):
            for fallback_model in _fallback_models(model):
                logger.warning(
                    "Model %s failed with upstream availability error, trying fallback %s",
                    model,
                    fallback_model,
                )
                try:
                    fallback_kwargs = {
                        "model": fallback_model,
                        "contents": contents,
                        "response_schema": response_schema,
                        "thinking_level": thinking_level,
                    }
                    if system_instruction is not None:
                        fallback_kwargs["system_instruction"] = system_instruction
                    return _call_model(**fallback_kwargs)
                except Exception as fallback_exc:
                    logger.warning("Fallback model %s also failed: %s", fallback_model, fallback_exc)
            raise exc
        raise
