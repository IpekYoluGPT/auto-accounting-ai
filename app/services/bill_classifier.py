"""
Lightweight bill classifier.

For text messages -> rule-based keyword heuristics.
For image/document -> delegates to Gemini with a classification-only prompt.
"""

from __future__ import annotations

import re

from app.config import settings
from app.models.schemas import ClassificationResult
from app.services import gemini_client
from app.utils.logging import get_logger

logger = get_logger(__name__)

# Keywords that strongly suggest a bill / invoice / receipt
_BILL_KEYWORDS = re.compile(
    r"fatura|fi\u015f|makbuz|toplam|kdv|vergi|\u00f6deme|tutar|fiyat|tl|\u20ba|eur|usd|"
    r"invoice|receipt|total|tax|amount|payment",
    re.IGNORECASE,
)

# Keywords that clearly indicate junk / chat
_JUNK_KEYWORDS = re.compile(
    r"merhaba|selam|nas\u0131ls\u0131n|iyi ak\u015famlar|g\u00fcnayd\u0131n|te\u015fekk\u00fcr|tamam|ok|"
    r"\U0001F44D|\U0001F602|\u2764\ufe0f|\U0001F64F",
    re.IGNORECASE,
)

_CLASSIFIER_PROMPT = """Classify this media for bookkeeping.

Accept only real financial documents such as Turkish invoices, receipts, or payment records.
Reject memes, screenshots, greetings, stickers, casual photos, and unrelated documents.
Return the schema only."""


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
    logger.info("Classifying media with Gemini model %s", settings.gemini_classifier_model)
    return gemini_client.generate_structured_content(
        model=settings.gemini_classifier_model,
        prompt=_CLASSIFIER_PROMPT,
        response_schema=ClassificationResult,
        thinking_level="minimal",
        media_bytes=image_bytes,
        mime_type=mime_type,
    )
