"""
Gemini-backed extraction with an OCR-first direct path.
"""

from __future__ import annotations

from typing import Optional

from app.config import settings
from app.models.ocr import OCRParseBundle
from app.models.schemas import AIMultiExtractionResult, BillRecord, DocumentCategory
from app.services import gemini_client
from app.services.accounting import ocr
from app.utils.logging import get_logger

logger = get_logger(__name__)

_EXTRACTION_PROMPT = """Extract bookkeeping fields from this Turkish invoice, receipt, or payment document.

IMPORTANT: This image may contain MORE THAN ONE document (e.g. multiple cheques,
receipts, or invoices side by side, stacked, or overlapping).
Return one entry per DISTINCT document you can identify.
If there is only one document, return a list with a single entry.

Return only the requested schema.
Use null for missing values.
Normalize dates to YYYY-MM-DD, times to HH:MM, and Turkish decimal numbers to standard decimals.
Default currency to TRY when it is not shown."""

_OCR_VALIDATION_PROMPT = """Validate and extract bookkeeping fields from this Turkish financial document.

You will receive:
- the original media
- OCR text
- OCR key-value fields
- OCR entities
- OCR tables
- a deterministic candidate record built from OCR

Rules:
- Prefer explicit OCR-grounded values.
- Do not invent numbers, tax IDs, or document IDs.
- Preserve Turkish characters exactly when visible.
- Fix OCR mistakes only when the OCR evidence and arithmetic clearly support the correction.
- Return multiple documents only if the image truly contains more than one distinct document.
- Return only the schema."""


def _parse_tr_number(value: str) -> Optional[float]:
    """Backward-compatible wrapper for tests."""
    return ocr.parse_tr_number(value)


def _normalize_record(raw: dict) -> BillRecord:
    """Coerce raw JSON dict into a validated BillRecord."""

    def _safe_float(v) -> Optional[float]:
        return ocr.parse_tr_number(v)

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

    return BillRecord(
        company_name=_safe_str(raw.get("company_name")),
        tax_number=_safe_str(raw.get("tax_number")),
        tax_office=_safe_str(raw.get("tax_office")),
        document_number=_safe_str(raw.get("document_number")),
        invoice_number=_safe_str(raw.get("invoice_number")),
        receipt_number=_safe_str(raw.get("receipt_number")),
        document_date=ocr.normalize_date(_safe_str(raw.get("document_date"))) or _safe_str(raw.get("document_date")),
        document_time=ocr.normalize_time(_safe_str(raw.get("document_time"))) or _safe_str(raw.get("document_time")),
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


def extract_bills(
    image_bytes: bytes,
    mime_type: str = "image/jpeg",
    source_message_id: Optional[str] = None,
    source_filename: Optional[str] = None,
    source_type: Optional[str] = None,
    source_sender_id: Optional[str] = None,
    source_group_id: Optional[str] = None,
    source_chat_type: Optional[str] = None,
    *,
    ocr_bundle: OCRParseBundle | None = None,
    category_hint: DocumentCategory | None = None,
) -> list[BillRecord]:
    """
    Extract one or more bookkeeping records from media.

    When a strong OCR bundle is present, returns a direct deterministic record
    without calling Gemini. Otherwise, Gemini receives the media together with
    OCR context and validates/fills the record(s).
    """
    assessment = None
    model_name = settings.gemini_extractor_model
    prompt = _EXTRACTION_PROMPT

    if ocr_bundle is not None:
        assessment = ocr.assess_extraction(ocr_bundle, category_hint=category_hint)
        if assessment.use_direct:
            logger.info(
                "Using direct OCR extraction for %s (score=%.2f quality=%.2f)",
                source_message_id or source_filename or "document",
                assessment.parse_score,
                ocr_bundle.quality_score,
            )
            direct = assessment.record.model_copy(deep=True)
            _attach_source_metadata(
                direct,
                source_message_id=source_message_id,
                source_filename=source_filename,
                source_type=source_type,
                source_sender_id=source_sender_id,
                source_group_id=source_group_id,
                source_chat_type=source_chat_type,
            )
            return [direct]

        model_name = settings.gemini_validation_model
        prompt = _build_ocr_validation_prompt(ocr_bundle, assessment, category_hint)

    if not settings.gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured.")

    logger.info(
        "Sending media (%d bytes, %s) to Gemini model %s for extraction",
        len(image_bytes),
        mime_type,
        model_name,
    )

    multi_result = gemini_client.generate_structured_content(
        model=model_name,
        prompt=prompt,
        response_schema=AIMultiExtractionResult,
        thinking_level="low",
        media_bytes=image_bytes,
        mime_type=mime_type,
    )

    raw_docs = multi_result.documents
    if not raw_docs:
        logger.warning("Gemini returned zero documents for image; returning empty list.")
        return []

    records: list[BillRecord] = []
    for idx, doc in enumerate(raw_docs):
        record = _normalize_record(doc.model_dump())
        if len(raw_docs) > 1 and source_message_id:
            record.source_message_id = f"{source_message_id}__doc{idx + 1}"
        else:
            record.source_message_id = source_message_id
        record.source_filename = source_filename
        record.source_type = source_type
        record.source_sender_id = source_sender_id
        record.source_group_id = source_group_id
        record.source_chat_type = source_chat_type
        records.append(record)

    logger.info(
        "Extraction complete: %d document(s) found. [%s]",
        len(records),
        ", ".join(f"{r.company_name or '?'}={r.total_amount}" for r in records),
    )
    return records


def extract_bill(
    image_bytes: bytes,
    mime_type: str = "image/jpeg",
    source_message_id: Optional[str] = None,
    source_filename: Optional[str] = None,
    source_type: Optional[str] = None,
    source_sender_id: Optional[str] = None,
    source_group_id: Optional[str] = None,
    source_chat_type: Optional[str] = None,
    *,
    ocr_bundle: OCRParseBundle | None = None,
    category_hint: DocumentCategory | None = None,
) -> BillRecord:
    """Backward-compatible wrapper: returns the first extracted record."""
    records = extract_bills(
        image_bytes=image_bytes,
        mime_type=mime_type,
        source_message_id=source_message_id,
        source_filename=source_filename,
        source_type=source_type,
        source_sender_id=source_sender_id,
        source_group_id=source_group_id,
        source_chat_type=source_chat_type,
        ocr_bundle=ocr_bundle,
        category_hint=category_hint,
    )
    if not records:
        raise RuntimeError("Gemini returned no documents from the image.")
    return records[0]


def _build_ocr_validation_prompt(
    ocr_bundle: OCRParseBundle,
    assessment: ocr.OCRExtractionAssessment,
    category_hint: DocumentCategory | None,
) -> str:
    category_text = category_hint.value if category_hint is not None else "unknown"
    multi_doc_text = "yes" if assessment.multi_document_suspected else "no"
    return (
        f"{_OCR_VALIDATION_PROMPT}\n\n"
        f"Category hint: {category_text}\n"
        f"Multiple documents suspected: {multi_doc_text}\n"
        f"OCR quality score: {ocr_bundle.quality_score}\n"
        f"OCR parse score: {assessment.parse_score}\n"
        f"OCR parse reasons: {', '.join(assessment.reasons) or 'none'}\n\n"
        f"Deterministic OCR candidate:\n{ocr.serialize_candidate_record(assessment.record)}\n\n"
        f"OCR bundle:\n{ocr.serialize_ocr_bundle(ocr_bundle)}"
    )


def _attach_source_metadata(
    record: BillRecord,
    *,
    source_message_id: Optional[str],
    source_filename: Optional[str],
    source_type: Optional[str],
    source_sender_id: Optional[str],
    source_group_id: Optional[str],
    source_chat_type: Optional[str],
) -> None:
    record.source_message_id = source_message_id
    record.source_filename = source_filename
    record.source_type = source_type
    record.source_sender_id = source_sender_id
    record.source_group_id = source_group_id
    record.source_chat_type = source_chat_type
