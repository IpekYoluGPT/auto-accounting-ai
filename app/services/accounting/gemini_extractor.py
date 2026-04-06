"""
Gemini-powered invoice / receipt field extractor.

Sends an image (or document) to Gemini and requests a strictly structured
JSON response conforming to the BillRecord schema.
"""

from __future__ import annotations

import re
from typing import Optional

from app.config import settings
from app.models.schemas import AIExtractionResult, BillRecord
from app.services import gemini_client
from app.utils.logging import get_logger

logger = get_logger(__name__)

_EXTRACTION_PROMPT = """Extract bookkeeping fields from this Turkish invoice, receipt, or payment document.

Return only the requested schema.
Use null for missing values.
Normalize dates to YYYY-MM-DD, times to HH:MM, and Turkish decimal numbers to standard decimals.
Default currency to TRY when it is not shown."""

# Turkish number format: 1.234,56 -> 1234.56
_TR_NUMBER_RE = re.compile(r"(\d{1,3}(?:\.\d{3})*),(\d{2})")


def _parse_tr_number(value: str) -> Optional[float]:
    """Convert a Turkish-formatted number string to float."""
    if value is None:
        return None
    cleaned = _TR_NUMBER_RE.sub(lambda m: m.group(0).replace(".", "").replace(",", "."), str(value))
    cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _normalize_record(raw: dict) -> BillRecord:
    """Coerce raw Gemini JSON dict into a validated BillRecord."""

    def _safe_float(v) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        return _parse_tr_number(str(v))

    def _safe_str(v) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    currency_raw = _safe_str(raw.get("currency"))
    if currency_raw:
        currency_raw = currency_raw.upper()
        if currency_raw not in ("TRY", "EUR", "USD"):
            currency_raw = "TRY"
    else:
        currency_raw = "TRY"

    doc_date = _safe_str(raw.get("document_date"))
    if doc_date:
        match = re.match(r"^(\d{1,2})[./](\d{1,2})[./](\d{4})$", doc_date)
        if match:
            doc_date = f"{match.group(3)}-{match.group(2).zfill(2)}-{match.group(1).zfill(2)}"

    doc_time = _safe_str(raw.get("document_time"))
    if doc_time:
        match = re.match(r"^(\d{1,2})[:.](\d{2})(?::\d{2})?$", doc_time)
        if match:
            doc_time = f"{match.group(1).zfill(2)}:{match.group(2)}"

    return BillRecord(
        company_name=_safe_str(raw.get("company_name")),
        tax_number=_safe_str(raw.get("tax_number")),
        tax_office=_safe_str(raw.get("tax_office")),
        document_number=_safe_str(raw.get("document_number")),
        invoice_number=_safe_str(raw.get("invoice_number")),
        receipt_number=_safe_str(raw.get("receipt_number")),
        document_date=doc_date,
        document_time=doc_time,
        currency=currency_raw,
        subtotal=_safe_float(raw.get("subtotal")),
        vat_rate=_safe_float(raw.get("vat_rate")),
        vat_amount=_safe_float(raw.get("vat_amount")),
        total_amount=_safe_float(raw.get("total_amount")),
        payment_method=_safe_str(raw.get("payment_method")),
        expense_category=_safe_str(raw.get("expense_category")),
        description=_safe_str(raw.get("description")),
        notes=_safe_str(raw.get("notes")),
        source_message_id=_safe_str(raw.get("source_message_id")),
        source_filename=_safe_str(raw.get("source_filename")),
        source_type=_safe_str(raw.get("source_type")),
        confidence=_safe_float(raw.get("confidence")),
    )


def extract_bill(
    image_bytes: bytes,
    mime_type: str = "image/jpeg",
    source_message_id: Optional[str] = None,
    source_filename: Optional[str] = None,
    source_type: Optional[str] = None,
    source_sender_id: Optional[str] = None,
    source_group_id: Optional[str] = None,
    source_chat_type: Optional[str] = None,
) -> BillRecord:
    """
    Send *image_bytes* to Gemini and return a normalised BillRecord.

    Raises RuntimeError immediately if the API key is not configured.
    Retries transient Gemini API errors up to 3 times.
    """
    if not settings.gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured.")

    logger.info(
        "Sending image (%d bytes, %s) to Gemini model %s for extraction",
        len(image_bytes),
        mime_type,
        settings.gemini_extractor_model,
    )

    extracted = gemini_client.generate_structured_content(
        model=settings.gemini_extractor_model,
        prompt=_EXTRACTION_PROMPT,
        response_schema=AIExtractionResult,
        thinking_level="low",
        media_bytes=image_bytes,
        mime_type=mime_type,
    )

    record = _normalize_record(extracted.model_dump())
    record.source_message_id = source_message_id
    record.source_filename = source_filename
    record.source_type = source_type
    record.source_sender_id = source_sender_id
    record.source_group_id = source_group_id
    record.source_chat_type = source_chat_type

    logger.info(
        "Extraction complete: company=%s, total=%s, confidence=%s",
        record.company_name,
        record.total_amount,
        record.confidence,
    )
    return record
