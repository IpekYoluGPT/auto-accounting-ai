"""
Lightweight bill classifier.

For text messages -> rule-based keyword heuristics.
For image/document -> delegates to Gemini document analysis.
"""

from __future__ import annotations

import re

from app.models.schemas import ClassificationResult
from app.services.accounting import doc_classifier
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


# Backward-compatible wrapper retained for callers/tests still using bill_classifier.
def classify_image(
    image_bytes: bytes,
    mime_type: str = "image/jpeg",
    *,
    ocr_bundle: object | None = None,
) -> ClassificationResult:
    """Classify whether a media attachment is a financial document."""
    if ocr_bundle is not None:
        logger.debug("Ignoring deprecated ocr_bundle argument in classify_image.")

    analysis = doc_classifier.analyze_document(image_bytes, mime_type=mime_type)
    result = ClassificationResult(
        is_bill=analysis.is_financial_document,
        reason=analysis.reason,
        confidence=analysis.confidence,
    )

    return result

