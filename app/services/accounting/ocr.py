"""
OCR pipeline helpers: media normalization, caching, serialization, and
deterministic Turkish field extraction.
"""

from __future__ import annotations

import json
import re
import threading
from hashlib import sha256
from io import BytesIO

from PIL import Image, ImageOps, UnidentifiedImageError
from pydantic import BaseModel, Field

from app.config import settings
from app.models.ocr import OCRMediaMetadata, OCRParseBundle, PreparedOCRDocument
from app.models.schemas import BillRecord, DocumentCategory
from app.services.providers import google_document_ai
from app.utils.logging import get_logger

logger = get_logger(__name__)

_MAX_IMAGE_DIMENSION = 4096
_OCR_CACHE: dict[str, OCRParseBundle] = {}
_OCR_CACHE_LOCK = threading.Lock()

_TR_NUMBER_RE = re.compile(r"(\d{1,3}(?:[.\s]\d{3})*),(\d{2})")
_AMOUNT_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+(?:[.,]\d{2})?)")
_DATE_RE = re.compile(r"\b(\d{1,2})[./-](\d{1,2})[./-](\d{4})\b")
_TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})(?::\d{2})?\b")
_TAX_NUMBER_RE = re.compile(r"\b\d{10,11}\b")
_DOC_NUMBER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("invoice_number", re.compile(r"(?:fatura(?:\s*no|\s*numarası)?|invoice(?:\s*no)?)\s*[:#-]?\s*([A-Z0-9\-\/]+)", re.IGNORECASE)),
    ("receipt_number", re.compile(r"(?:fi[şs](?:\s*no|\s*numarası)?|receipt(?:\s*no)?)\s*[:#-]?\s*([A-Z0-9\-\/]+)", re.IGNORECASE)),
    ("document_number", re.compile(r"(?:belge(?:\s*no|\s*numarası)?|referans(?:\s*no)?|no)\s*[:#-]?\s*([A-Z0-9\-\/]{3,})", re.IGNORECASE)),
)
_KDV_RATE_RE = re.compile(r"(?:kdv|vat)[^\n%]{0,20}%\s*(\d{1,2}(?:[.,]\d{1,2})?)", re.IGNORECASE)
_CURRENCY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("TRY", re.compile(r"\b(?:TRY|TL|₺)\b", re.IGNORECASE)),
    ("EUR", re.compile(r"\bEUR\b|€", re.IGNORECASE)),
    ("USD", re.compile(r"\bUSD\b|\$", re.IGNORECASE)),
)
_PAYMENT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("Nakit", re.compile(r"\bnakit\b", re.IGNORECASE)),
    ("Kredi Kartı", re.compile(r"kredi\s*kart[ıi]|visa|mastercard|pos", re.IGNORECASE)),
    ("Banka Transferi", re.compile(r"eft|fast|havale|iban|transfer|dekont", re.IGNORECASE)),
)
_CATEGORY_PATTERNS: tuple[tuple[DocumentCategory, re.Pattern[str]], ...] = (
    (DocumentCategory.IADE, re.compile(r"\biade\b|iptal|return", re.IGNORECASE)),
    (DocumentCategory.CEK, re.compile(r"\bçek\b|\bcek\b", re.IGNORECASE)),
    (DocumentCategory.ODEME_DEKONTU, re.compile(r"dekont|eft|fast|havale|iban|banka", re.IGNORECASE)),
    (DocumentCategory.MALZEME, re.compile(r"irsaliye|sevk|teslim|hafriyat|malzeme", re.IGNORECASE)),
    (DocumentCategory.FATURA, re.compile(r"e[\s-]*fatura|\bfatura\b|invoice", re.IGNORECASE)),
    (DocumentCategory.HARCAMA_FISI, re.compile(r"fi[şs]|pos|yazar\s*kasa|ökc|yakıt|market", re.IGNORECASE)),
)
_BILL_ANCHORS = re.compile(
    r"\bfatura\b|fi[şs]|makbuz|dekont|irsaliye|çek|cek|kdv|vergi|toplam|ara toplam|genel toplam|ödeme|payment|receipt|invoice",
    re.IGNORECASE,
)
_COMPANY_EXCLUDE = re.compile(
    r"vergi|tarih|saat|fatura|fi[şs]|makbuz|dekont|toplam|ara toplam|genel toplam|kdv|ödeme|payment|phone|tel|www|adres",
    re.IGNORECASE,
)
_TOTAL_KEYWORDS = ("genel toplam", "toplam", "ödenecek", "total")
_SUBTOTAL_KEYWORDS = ("ara toplam", "kdvsiz", "matrah", "mal hizmet toplam")
_VAT_KEYWORDS = ("kdv", "vat", "vergi")


class OCRExtractionAssessment(BaseModel):
    record: BillRecord
    parse_score: float = Field(default=0.0, ge=0.0, le=1.0)
    completeness_score: float = Field(default=0.0, ge=0.0, le=1.0)
    numeric_consistency_score: float = Field(default=0.0, ge=0.0, le=1.0)
    use_direct: bool = False
    requires_gemini: bool = False
    multi_document_suspected: bool = False
    reasons: list[str] = Field(default_factory=list)


def prepare_document(media_bytes: bytes, mime_type: str) -> PreparedOCRDocument:
    normalized_bytes, normalized_mime_type, width, height, warnings = _normalize_media(media_bytes, mime_type)
    source_hash = sha256(normalized_bytes).hexdigest()
    metadata = OCRMediaMetadata(
        mime_type=normalized_mime_type,
        original_mime_type=mime_type,
        byte_size=len(normalized_bytes),
        width=width,
        height=height,
        source_hash=source_hash,
    )

    bundle = _cached_bundle(source_hash, normalized_mime_type)
    if bundle is None and google_document_ai.is_configured():
        try:
            bundle = google_document_ai.process_document(normalized_bytes, metadata)
            _store_cached_bundle(source_hash, normalized_mime_type, bundle)
        except Exception as exc:
            logger.warning("Google Document AI failed for source %s: %s", source_hash[:12], exc)
            warnings = warnings + [f"OCR unavailable: {exc}"]
    elif bundle is None:
        logger.info("Google Document AI not configured; using Gemini fallback path.")

    return PreparedOCRDocument(
        media_bytes=normalized_bytes,
        mime_type=normalized_mime_type,
        metadata=metadata,
        ocr_bundle=bundle,
        warnings=warnings,
    )


def serialize_ocr_bundle(bundle: OCRParseBundle, *, max_lines: int = 40, max_tables: int = 3) -> str:
    lines: list[str] = []
    if bundle.lines:
        lines.append("OCR_TEXT:")
        lines.extend(f"- {line}" for line in bundle.lines[:max_lines])
    if bundle.key_values:
        lines.append("OCR_KEY_VALUES:")
        lines.extend(
            f"- {item.key}: {item.value or ''}".rstrip()
            for item in bundle.key_values[:max_lines]
        )
    if bundle.entities:
        lines.append("OCR_ENTITIES:")
        lines.extend(
            f"- {entity.type}: {entity.mention_text}"
            for entity in bundle.entities[:max_lines]
        )
    if bundle.tables:
        lines.append("OCR_TABLES:")
        for index, table in enumerate(bundle.tables[:max_tables], start=1):
            lines.append(f"- Table {index} (page {table.page_number}):")
            for row in _table_rows(table)[:10]:
                lines.append(f"  - {' | '.join(row)}")
    if bundle.warnings:
        lines.append("OCR_WARNINGS:")
        lines.extend(f"- {warning}" for warning in bundle.warnings)
    return "\n".join(lines).strip()


def detect_category_from_ocr(bundle: OCRParseBundle) -> tuple[DocumentCategory | None, bool]:
    text = _bundle_text(bundle)
    for category, pattern in _CATEGORY_PATTERNS:
        if pattern.search(text):
            return category, category == DocumentCategory.IADE
    return None, False


def detect_bill_from_ocr(bundle: OCRParseBundle) -> bool | None:
    text = _bundle_text(bundle)
    anchor_hits = len(_BILL_ANCHORS.findall(text))
    has_money = bool(re.search(r"(?:₺|TL|TRY|EUR|USD|\$|€)", text, re.IGNORECASE))
    has_amount = len(_AMOUNT_RE.findall(text)) >= 2
    has_date = bool(_DATE_RE.search(text))

    if anchor_hits >= 2 and (has_money or has_amount):
        return True
    if anchor_hits >= 1 and has_date and has_amount:
        return True
    if bundle.text_char_count >= 30 and bundle.quality_score >= settings.ocr_min_quality_score and anchor_hits == 0:
        return False
    return None


def assess_extraction(bundle: OCRParseBundle, category_hint: DocumentCategory | None = None) -> OCRExtractionAssessment:
    lines = _clean_lines(bundle)
    company_name = _extract_company_name(bundle, lines)
    tax_number = _extract_tax_number(lines)
    tax_office = _extract_tax_office(lines)
    document_date = _extract_date(lines)
    document_time = _extract_time(lines)
    currency = _extract_currency(lines) or "TRY"

    identifiers = _extract_document_numbers(lines)
    total_amount = _extract_amount(lines, _TOTAL_KEYWORDS)
    subtotal = _extract_amount(lines, _SUBTOTAL_KEYWORDS)
    vat_amount = _extract_amount(lines, _VAT_KEYWORDS, prefer_last=True)
    vat_rate = _extract_vat_rate(lines)

    if subtotal is None and total_amount is not None and vat_amount is not None:
        subtotal = round(max(total_amount - vat_amount, 0.0), 2)
    if vat_amount is None and total_amount is not None and subtotal is not None:
        vat_amount = round(max(total_amount - subtotal, 0.0), 2)
    if vat_rate is None and subtotal and vat_amount is not None and subtotal > 0:
        vat_rate = round((vat_amount / subtotal) * 100.0, 2)

    payment_method = _extract_payment_method(lines)
    description = _extract_description(lines, category_hint)

    document_number = identifiers.get("document_number")
    invoice_number = identifiers.get("invoice_number")
    receipt_number = identifiers.get("receipt_number")

    if category_hint == DocumentCategory.FATURA and not invoice_number:
        invoice_number = document_number
    if category_hint == DocumentCategory.HARCAMA_FISI and not receipt_number:
        receipt_number = document_number

    record = BillRecord(
        company_name=company_name,
        tax_number=tax_number,
        tax_office=tax_office,
        document_number=document_number,
        invoice_number=invoice_number,
        receipt_number=receipt_number,
        document_date=document_date,
        document_time=document_time,
        currency=currency,
        subtotal=subtotal,
        vat_rate=vat_rate,
        vat_amount=vat_amount,
        total_amount=total_amount,
        payment_method=payment_method,
        expense_category=None,
        description=description,
        notes=None,
    )

    completeness_score = _completeness_score(record, category_hint)
    numeric_consistency_score, consistency_reasons = _numeric_consistency(record)
    multi_document_suspected = suspect_multi_document(bundle)

    reasons: list[str] = []
    if record.total_amount is None:
        reasons.append("missing total amount")
    if record.document_date is None and category_hint != DocumentCategory.CEK:
        reasons.append("missing document date")
    if not (record.company_name or record.document_number):
        reasons.append("missing company name and document number")
    reasons.extend(consistency_reasons)
    if multi_document_suspected:
        reasons.append("multiple documents suspected")

    parse_score = min(
        1.0,
        (bundle.quality_score * 0.45)
        + (completeness_score * 0.35)
        + (numeric_consistency_score * 0.20),
    )

    use_direct = (
        not multi_document_suspected
        and _required_fields_present(record, category_hint)
        and bundle.quality_score >= settings.ocr_min_quality_score
        and parse_score >= settings.ocr_min_parse_score
    )
    record.confidence = round(parse_score, 4)

    return OCRExtractionAssessment(
        record=record,
        parse_score=round(parse_score, 4),
        completeness_score=round(completeness_score, 4),
        numeric_consistency_score=round(numeric_consistency_score, 4),
        use_direct=use_direct,
        requires_gemini=not use_direct,
        multi_document_suspected=multi_document_suspected,
        reasons=reasons,
    )


def suspect_multi_document(bundle: OCRParseBundle) -> bool:
    lines = _clean_lines(bundle)
    text = _bundle_text(bundle)
    dates = set(_DATE_RE.findall(text))
    identifier_lines = sum(
        1
        for line in lines
        if any(token in line.lower() for token in ("fiş no", "fis no", "fatura no", "invoice", "receipt", "belge no"))
    )
    total_lines = sum(
        1
        for line in lines
        if any(keyword in line.lower() for keyword in _TOTAL_KEYWORDS)
        and not any(keyword in line.lower() for keyword in _SUBTOTAL_KEYWORDS)
    )
    if len(dates) >= 2 and total_lines >= 2:
        return True
    if identifier_lines >= 2 and total_lines >= 2:
        return True
    if len(bundle.tables) >= 2 and total_lines >= 2:
        return True
    return False


def parse_tr_number(value: str | float | int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = _TR_NUMBER_RE.sub(lambda match: match.group(0).replace(" ", "").replace(".", "").replace(",", "."), str(value))
    cleaned = cleaned.replace(" ", "").replace(",", ".")
    try:
        return float(cleaned)
    except (TypeError, ValueError):
        return None


def normalize_date(value: str | None) -> str | None:
    if not value:
        return None
    match = _DATE_RE.search(value)
    if not match:
        return None
    day, month, year = match.groups()
    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"


def normalize_time(value: str | None) -> str | None:
    if not value:
        return None
    match = _TIME_RE.search(value)
    if not match:
        return None
    hour, minute = match.groups()
    return f"{hour.zfill(2)}:{minute}"


def _cached_bundle(source_hash: str, mime_type: str) -> OCRParseBundle | None:
    cache_key = f"{mime_type}:{source_hash}"
    with _OCR_CACHE_LOCK:
        return _OCR_CACHE.get(cache_key)


def _store_cached_bundle(source_hash: str, mime_type: str, bundle: OCRParseBundle) -> None:
    cache_key = f"{mime_type}:{source_hash}"
    with _OCR_CACHE_LOCK:
        _OCR_CACHE[cache_key] = bundle


def _normalize_media(media_bytes: bytes, mime_type: str) -> tuple[bytes, str, int | None, int | None, list[str]]:
    warnings: list[str] = []
    if mime_type == "application/pdf":
        return media_bytes, mime_type, None, None, warnings
    if not mime_type.startswith("image/"):
        warnings.append(f"Unsupported OCR MIME type {mime_type}; using original bytes.")
        return media_bytes, mime_type, None, None, warnings

    try:
        with Image.open(BytesIO(media_bytes)) as image:
            orientation = image.getexif().get(274, 1)
            normalized = ImageOps.exif_transpose(image)
            width, height = normalized.size
            resized = False
            if max(width, height) > _MAX_IMAGE_DIMENSION:
                normalized.thumbnail((_MAX_IMAGE_DIMENSION, _MAX_IMAGE_DIMENSION))
                resized = True

            output_format = "PNG" if mime_type == "image/png" else "JPEG"
            output_mime = "image/png" if output_format == "PNG" else "image/jpeg"

            if output_format == "JPEG" and normalized.mode not in ("RGB", "L"):
                normalized = normalized.convert("RGB")
            elif output_format == "PNG" and normalized.mode == "P":
                normalized = normalized.convert("RGBA")

            width, height = normalized.size
            needs_reencode = orientation not in (None, 1) or resized or output_mime != mime_type
            if not needs_reencode:
                return media_bytes, mime_type, width, height, warnings

            buffer = BytesIO()
            save_kwargs = {"format": output_format}
            if output_format == "JPEG":
                save_kwargs.update({"quality": 95, "optimize": True})
            normalized.save(buffer, **save_kwargs)
            if orientation not in (None, 1):
                warnings.append("Image was rotated using EXIF orientation for OCR.")
            if resized:
                warnings.append("Large image was downscaled for OCR stability.")
            return buffer.getvalue(), output_mime, width, height, warnings
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        logger.debug("Image normalization skipped for MIME %s: %s", mime_type, exc)
        return media_bytes, mime_type, None, None, warnings


def _table_rows(table) -> list[list[str]]:
    rows = [["" for _ in range(max(table.column_count, 1))] for _ in range(max(table.row_count, 1))]
    for cell in table.cells:
        while len(rows) <= cell.row_index:
            rows.append(["" for _ in range(max(table.column_count, 1))])
        while len(rows[cell.row_index]) <= cell.column_index:
            rows[cell.row_index].append("")
        rows[cell.row_index][cell.column_index] = cell.text
    return rows


def _bundle_text(bundle: OCRParseBundle) -> str:
    if bundle.text:
        return bundle.text
    return "\n".join(bundle.lines)


def _clean_lines(bundle: OCRParseBundle) -> list[str]:
    if bundle.lines:
        lines = bundle.lines
    else:
        lines = _bundle_text(bundle).splitlines()
    cleaned: list[str] = []
    seen: set[str] = set()
    for line in lines:
        text = " ".join(line.split()).strip()
        if not text or text in seen:
            continue
        cleaned.append(text)
        seen.add(text)
    return cleaned


def _extract_company_name(bundle: OCRParseBundle, lines: list[str]) -> str | None:
    for entity in bundle.entities:
        entity_type = entity.type.lower()
        if any(token in entity_type for token in ("supplier", "vendor", "merchant", "company", "organization", "name")):
            candidate = entity.mention_text.strip()
            if candidate and not _COMPANY_EXCLUDE.search(candidate):
                return candidate

    for line in lines[:10]:
        digit_count = sum(ch.isdigit() for ch in line)
        alpha_count = sum(ch.isalpha() for ch in line)
        if alpha_count < 4 or digit_count > max(4, alpha_count // 2):
            continue
        if _COMPANY_EXCLUDE.search(line):
            continue
        return line
    return None


def _extract_tax_number(lines: list[str]) -> str | None:
    for line in lines:
        if "vergi" in line.lower() or "vkn" in line.lower() or "tckn" in line.lower():
            match = _TAX_NUMBER_RE.search(line)
            if match:
                return match.group(0)
    return None


def _extract_tax_office(lines: list[str]) -> str | None:
    pattern = re.compile(r"vergi\s*dairesi\s*[:\-]?\s*(.+)$", re.IGNORECASE)
    for line in lines:
        match = pattern.search(line)
        if match:
            return match.group(1).strip()
    return None


def _extract_document_numbers(lines: list[str]) -> dict[str, str | None]:
    joined = "\n".join(lines)
    results: dict[str, str | None] = {
        "document_number": None,
        "invoice_number": None,
        "receipt_number": None,
    }
    for key, pattern in _DOC_NUMBER_PATTERNS:
        match = pattern.search(joined)
        if match:
            value = match.group(1).strip(" .:-")
            results[key] = value
            if results["document_number"] is None:
                results["document_number"] = value
    return results


def _extract_date(lines: list[str]) -> str | None:
    for line in lines:
        if "tarih" in line.lower():
            value = normalize_date(line)
            if value:
                return value
    for line in lines:
        value = normalize_date(line)
        if value:
            return value
    return None


def _extract_time(lines: list[str]) -> str | None:
    for line in lines:
        if "saat" in line.lower():
            value = normalize_time(line)
            if value:
                return value
    for line in lines:
        value = normalize_time(line)
        if value:
            return value
    return None


def _extract_currency(lines: list[str]) -> str | None:
    joined = "\n".join(lines)
    for code, pattern in _CURRENCY_PATTERNS:
        if pattern.search(joined):
            return code
    return None


def _extract_vat_rate(lines: list[str]) -> float | None:
    for line in lines:
        if "kdv" not in line.lower() and "vat" not in line.lower():
            continue
        match = _KDV_RATE_RE.search(line)
        if match:
            return parse_tr_number(match.group(1))
        percent_match = re.search(r"%\s*(\d{1,2}(?:[.,]\d{1,2})?)", line)
        if percent_match:
            return parse_tr_number(percent_match.group(1))
    return None


def _extract_payment_method(lines: list[str]) -> str | None:
    joined = "\n".join(lines)
    for label, pattern in _PAYMENT_PATTERNS:
        if pattern.search(joined):
            return label
    return None


def _extract_description(lines: list[str], category_hint: DocumentCategory | None) -> str | None:
    if category_hint == DocumentCategory.ODEME_DEKONTU:
        for line in lines:
            if any(keyword in line.lower() for keyword in ("açıklama", "aciklama", "alıcı", "alici", "gönderen", "gonderen")):
                return line
    for line in lines:
        if line.lower().startswith(("açıklama", "aciklama", "description")):
            return line
    return None


def _extract_amount(lines: list[str], keywords: tuple[str, ...], *, prefer_last: bool = False) -> float | None:
    candidates: list[float] = []
    for line in lines:
        lowered = line.lower()
        if not any(keyword in lowered for keyword in keywords):
            continue
        matches = _AMOUNT_RE.findall(line)
        if not matches:
            continue
        number = matches[-1] if prefer_last or len(matches) > 1 else matches[0]
        parsed = parse_tr_number(number)
        if parsed is not None:
            candidates.append(parsed)
    if not candidates:
        return None
    return candidates[-1]


def _required_fields_present(record: BillRecord, category_hint: DocumentCategory | None) -> bool:
    if record.total_amount is None:
        return False
    if category_hint not in {DocumentCategory.CEK, DocumentCategory.MALZEME} and record.document_date is None:
        return False
    if not (record.company_name or record.document_number or record.invoice_number or record.receipt_number):
        return False
    return True


def _completeness_score(record: BillRecord, category_hint: DocumentCategory | None) -> float:
    checks = [
        record.total_amount is not None,
        record.document_date is not None or category_hint in {DocumentCategory.CEK, DocumentCategory.MALZEME},
        bool(record.company_name or record.document_number or record.invoice_number or record.receipt_number),
        record.currency is not None,
    ]
    return sum(1 for check in checks if check) / len(checks)


def _numeric_consistency(record: BillRecord) -> tuple[float, list[str]]:
    reasons: list[str] = []
    if record.total_amount is None:
        return 0.4, ["missing total for arithmetic validation"]
    if record.subtotal is None and record.vat_amount is None:
        return 0.8, reasons
    if record.subtotal is None or record.vat_amount is None:
        return 0.7, ["subtotal or VAT amount is missing"]
    expected_total = round(record.subtotal + record.vat_amount, 2)
    actual_total = round(record.total_amount, 2)
    diff = abs(expected_total - actual_total)
    if diff <= 0.05:
        return 1.0, reasons
    reasons.append(f"subtotal + VAT does not match total ({expected_total} vs {actual_total})")
    if diff <= 1.0:
        return 0.55, reasons
    return 0.2, reasons


def serialize_candidate_record(record: BillRecord) -> str:
    return json.dumps(record.model_dump(exclude_none=True), ensure_ascii=False, indent=2)
