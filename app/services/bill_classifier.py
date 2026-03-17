"""
Lightweight bill classifier.

For text messages  → rule-based keyword heuristics.
For image/document → delegates to Gemini with a classification-only prompt.
"""

from __future__ import annotations

import re

import google.generativeai as genai

from app.config import settings
from app.models.schemas import ClassificationResult
from app.utils.logging import get_logger

logger = get_logger(__name__)

# Keywords that strongly suggest a bill / invoice / receipt
_BILL_KEYWORDS = re.compile(
    r"fatura|fiş|makbuz|toplam|kdv|vergi|ödeme|tutar|fiyat|tl|₺|eur|usd|"
    r"invoice|receipt|total|tax|amount|payment",
    re.IGNORECASE,
)

# Keywords that clearly indicate junk / chat
_JUNK_KEYWORDS = re.compile(
    r"merhaba|selam|nasılsın|iyi akşamlar|günaydın|teşekkür|tamam|ok|👍|😂|❤️|🙏",
    re.IGNORECASE,
)

_CLASSIFIER_PROMPT = """You are a document classifier for an accounting system.
Your ONLY job is to decide whether the provided image is a financial document:
fatura (invoice), fiş (cash receipt), or makbuz (payment receipt).

Respond with ONLY valid JSON in this exact format:
{
  "is_bill": true,
  "reason": "one-sentence explanation in English",
  "confidence": 0.95
}

Rules:
- is_bill = true  → The image contains a real financial document with amounts, dates, and a business name.
- is_bill = false → The image is a meme, screenshot, photo, sticker, greeting, or unrelated document.
- confidence must be a float between 0 and 1.
- Do NOT include any text outside the JSON object.
"""


def classify_text(text: str) -> ClassificationResult:
    """Rule-based classification for plain-text messages."""
    bill_hits = len(_BILL_KEYWORDS.findall(text))
    junk_hits = len(_JUNK_KEYWORDS.findall(text))

    if bill_hits >= 3 and junk_hits == 0:
        return ClassificationResult(is_bill=True, reason="keyword match", confidence=0.8)
    if junk_hits > 0 and bill_hits == 0:
        return ClassificationResult(is_bill=False, reason="junk keywords", confidence=0.9)
    if bill_hits > 0 and bill_hits > junk_hits:
        return ClassificationResult(is_bill=True, reason="partial keyword match", confidence=0.6)

    return ClassificationResult(is_bill=False, reason="no bill keywords found", confidence=0.7)


def classify_image(image_bytes: bytes, mime_type: str = "image/jpeg") -> ClassificationResult:
    """Use Gemini to classify whether an image is a financial document."""
    if not settings.gemini_api_key:
        logger.warning("GEMINI_API_KEY not set; defaulting classify_image to is_bill=True")
        return ClassificationResult(is_bill=True, reason="api key missing, defaulting to true", confidence=0.5)

    genai.configure(api_key=settings.gemini_api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")

    import json

    try:
        response = model.generate_content(
            [
                _CLASSIFIER_PROMPT,
                {"mime_type": mime_type, "data": image_bytes},
            ]
        )
        raw = response.text.strip()
        # Strip optional markdown code fences
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-z]*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)
        data = json.loads(raw)
        return ClassificationResult(
            is_bill=bool(data.get("is_bill", False)),
            reason=data.get("reason"),
            confidence=float(data.get("confidence", 0.5)),
        )
    except Exception as exc:
        logger.error("Image classification failed: %s", exc)
        return ClassificationResult(is_bill=True, reason=f"classification error: {exc}", confidence=0.4)
