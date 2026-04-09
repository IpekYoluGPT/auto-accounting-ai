"""
Lightweight bill classifier.

For text messages -> rule-based keyword heuristics.
For image/document -> delegates to Gemini with a classification-only prompt.
"""

from __future__ import annotations

import re
from typing import Optional

from app.config import settings
from app.models.ocr import OCRParseBundle
from app.models.schemas import ClassificationResult
from app.services.accounting import ocr
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

_DOCUMENT_REASON_KEYWORDS = re.compile(
    r"fatura|fi\u015f|makbuz|invoice|receipt|bill",
    re.IGNORECASE,
)

_TEMPLATE_REASON_KEYWORDS = re.compile(
    r"\u00f6rnek|sample|template|demo|demonstration",
    re.IGNORECASE,
)

_CLASSIFIER_PROMPT = """Classify this media for bookkeeping.

Accept only real financial documents such as Turkish invoices, receipts, or payment records.
Reject memes, screenshots, greetings, stickers, casual photos, and unrelated documents.
Return the schema only."""

_OCR_CLASSIFIER_PROMPT = """Classify this Turkish OCR output for bookkeeping.

You will receive OCR text, entities, key-value pairs, and tables extracted from one document image or PDF.
Prefer the OCR evidence. Do not invent document content that is not visible in the OCR.
Accept only real financial documents such as invoices, receipts, payment records, irsaliye, or cheques.
Reject unrelated screenshots, chat images, memes, and casual photos.
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


def classify_image(
    image_bytes: bytes,
    mime_type: str = "image/jpeg",
    *,
    ocr_bundle: Optional[OCRParseBundle] = None,
) -> ClassificationResult:
    """Classify whether a media attachment is a financial document."""
    if ocr_bundle is not None:
        ocr_decision = ocr.detect_bill_from_ocr(ocr_bundle)
        if ocr_decision is True:
            return ClassificationResult(is_bill=True, reason="ocr financial anchors", confidence=0.93)
        if ocr_decision is False:
            return ClassificationResult(is_bill=False, reason="ocr lacks financial anchors", confidence=0.82)

    logger.info("Classifying media with Gemini model %s", settings.gemini_classifier_model)
    prompt = _CLASSIFIER_PROMPT
    if ocr_bundle is not None:
        prompt = f"{_OCR_CLASSIFIER_PROMPT}\n\n{ocr.serialize_ocr_bundle(ocr_bundle)}"
    result = gemini_client.generate_structured_content(
        model=settings.gemini_classifier_model,
        prompt=prompt,
        response_schema=ClassificationResult,
        thinking_level="minimal",
        media_bytes=image_bytes,
        mime_type=mime_type,
    )

    if _should_accept_invoice_like_template(result):
        logger.warning("Overriding classifier rejection for invoice-like template document.")
        return ClassificationResult(
            is_bill=True,
            reason="invoice-like template override",
            confidence=max(0.6, min(result.confidence, 0.85)),
        )

    return result


def _should_accept_invoice_like_template(result: ClassificationResult) -> bool:
    if result.is_bill:
        return False

    reason = (result.reason or "").strip()
    if not reason:
        return False

    return bool(
        _DOCUMENT_REASON_KEYWORDS.search(reason)
        and _TEMPLATE_REASON_KEYWORDS.search(reason)
    )
