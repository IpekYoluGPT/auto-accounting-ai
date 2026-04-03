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


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=20),
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
    """Generate structured JSON and validate it against a Pydantic schema."""
    client = get_client()
    contents: list[str | types.Part] = [prompt]
    if media_bytes is not None:
        contents.append(
            types.Part.from_bytes(
                data=media_bytes,
                mime_type=mime_type or "application/octet-stream",
            )
        )

    response = client.models.generate_content(
        model=model,
        contents=contents,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=response_schema,
            thinking_config=types.ThinkingConfig(thinking_level=thinking_level),
        ),
    )

    if response.parsed is None:
        raise RuntimeError("Gemini returned no structured payload.")

    if isinstance(response.parsed, response_schema):
        return response.parsed

    return response_schema.model_validate(response.parsed)
