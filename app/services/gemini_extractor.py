"""
Gemini-powered invoice / receipt field extractor.

Sends an image (or document) to Gemini and requests a strictly structured
JSON response conforming to the BillRecord schema.
"""

from __future__ import annotations

import json
import re
from typing import Optional

import google.generativeai as genai
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings
from app.models.schemas import BillRecord
from app.utils.logging import get_logger

logger = get_logger(__name__)

_EXTRACTION_PROMPT = """You are an expert accounting data extractor for a Turkish small-business accounting system.

Analyze the provided bill / invoice / receipt image and extract every available field.
Return ONLY a single valid JSON object — no markdown, no extra text.

JSON schema (use null for missing fields):
{
  "company_name": "string or null",
  "tax_number": "string or null",
  "tax_office": "string or null",
  "document_number": "string or null",
  "invoice_number": "string or null",
  "receipt_number": "string or null",
  "document_date": "YYYY-MM-DD or null",
  "document_time": "HH:MM or null",
  "currency": "TRY | EUR | USD or null",
  "subtotal": number or null,
  "vat_rate": number or null,
  "vat_amount": number or null,
  "total_amount": number or null,
  "payment_method": "Nakit | Kredi Kartı | Banka Transferi | Diğer or null",
  "expense_category": "Yemek | Ulaşım | Konaklama | Ofis | Yazılım | Donanım | Abonelik | Kargo | Vergi | Diğer or null",
  "description": "brief description of goods/services or null",
  "notes": "any additional useful information or null",
  "confidence": 0.0 to 1.0
}

Rules:
- Normalize all dates to YYYY-MM-DD format.
- Normalize all times to HH:MM (24-hour) format.
- Turkish decimal separator is comma (,) — convert to dot (.) for numbers.
- currency must be one of: TRY, EUR, USD.  Default to TRY if not shown.
- confidence reflects how clearly the document is readable (0 = unreadable, 1 = perfect).
- Do NOT include any text outside the JSON object.
"""

# Turkish number format: 1.234,56 → 1234.56
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

    # Currency normalisation
    currency_raw = _safe_str(raw.get("currency"))
    if currency_raw:
        currency_raw = currency_raw.upper()
        if currency_raw not in ("TRY", "EUR", "USD"):
            currency_raw = "TRY"

    # Date normalisation: accept D.M.YYYY or D/M/YYYY → YYYY-MM-DD
    doc_date = _safe_str(raw.get("document_date"))
    if doc_date:
        m = re.match(r"^(\d{1,2})[./](\d{1,2})[./](\d{4})$", doc_date)
        if m:
            doc_date = f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    return BillRecord(
        company_name=_safe_str(raw.get("company_name")),
        tax_number=_safe_str(raw.get("tax_number")),
        tax_office=_safe_str(raw.get("tax_office")),
        document_number=_safe_str(raw.get("document_number")),
        invoice_number=_safe_str(raw.get("invoice_number")),
        receipt_number=_safe_str(raw.get("receipt_number")),
        document_date=doc_date,
        document_time=_safe_str(raw.get("document_time")),
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


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=20))
def _call_gemini(image_bytes: bytes, mime_type: str) -> str:
    """Internal: call Gemini and return raw text. Retried on transient errors."""
    genai.configure(api_key=settings.gemini_api_key)
    model = genai.GenerativeModel("gemini-1.5-pro")
    response = model.generate_content(
        [
            _EXTRACTION_PROMPT,
            {"mime_type": mime_type, "data": image_bytes},
        ]
    )
    return response.text.strip()


def extract_bill(
    image_bytes: bytes,
    mime_type: str = "image/jpeg",
    source_message_id: Optional[str] = None,
    source_filename: Optional[str] = None,
    source_type: Optional[str] = None,
) -> BillRecord:
    """
    Send *image_bytes* to Gemini and return a normalised BillRecord.

    Raises RuntimeError immediately if the API key is not configured.
    Retries transient Gemini API errors up to 3 times.
    """
    if not settings.gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured.")

    logger.info("Sending image (%d bytes, %s) to Gemini for extraction…", len(image_bytes), mime_type)

    raw_text = _call_gemini(image_bytes, mime_type)

    # Strip optional markdown code fences
    if raw_text.startswith("```"):
        raw_text = re.sub(r"^```[a-z]*\n?", "", raw_text)
        raw_text = re.sub(r"\n?```$", "", raw_text)

    logger.debug("Gemini raw response: %s", raw_text[:500])

    raw_dict = json.loads(raw_text)

    record = _normalize_record(raw_dict)
    record.source_message_id = source_message_id
    record.source_filename = source_filename
    record.source_type = source_type

    logger.info(
        "Extraction complete: company=%s, total=%s, confidence=%s",
        record.company_name,
        record.total_amount,
        record.confidence,
    )
    return record
