"""
Gemini-backed extraction with category-aware prompts.
"""

from __future__ import annotations

from typing import Optional

from app.config import settings
from app.models.schemas import AIMultiExtractionResult, BillRecord, DocumentCategory
from app.services import gemini_client
from app.services.accounting import ocr
from app.utils.logging import get_logger

logger = get_logger(__name__)

_EXTRACTION_SYSTEM_INSTRUCTION = """You extract structured bookkeeping data from Turkish business documents.

Rules:
- Use only information visible in the provided media.
- Preserve Turkish characters exactly when visible.
- Do not invent tax IDs, document IDs, totals, company names, or dates.
- When values are missing, return null.
- Normalize dates to YYYY-MM-DD and times to HH:MM when clearly visible.
- Convert Turkish formatted numbers to JSON numbers.
- If multiple documents are present, return one object per distinct document sorted left-to-right, then top-to-bottom.
- Do not merge separate documents into one record.
- For return documents, extract the values exactly as shown; do not flip signs unless the document itself shows a negative or return amount.
"""

_EXTRACTION_PROMPT = """Extract bookkeeping fields from this Turkish business document image or PDF.

This media may contain invoices, receipts, payment dekonts, cheques, delivery/material slips, or multiple documents in one photo.
Return the requested schema only.
Default currency to TRY when it is not shown."""

_CATEGORY_SPECIFIC_INSTRUCTIONS: dict[DocumentCategory, str] = {
    DocumentCategory.FATURA: """Belge ailesi: FATURA.
- company_name satici / duzenleyen firmadir.
- invoice_number icin once Fatura No, yoksa Belge No kullan.
- subtotal, vat_rate, vat_amount ve total_amount alanlarini yalnizca acikca gorunuyorsa doldur.
- description alanina kisa bir fatura ozeti yaz; payment_method sadece belgede acikca varsa doldur.""",
    DocumentCategory.ODEME_DEKONTU: """Belge ailesi: ODEME DEKONTU.
- company_name alanina gorunur banka, odeme kurumu veya baskin karsi taraf adini yaz.
- document_number alanina referans / islem / dekont numarasini koy.
- total_amount transfer edilen nihai tutardir.
- description alanina alici, gonderen veya islem aciklamasini kisa ve yararli bicimde koy.""",
    DocumentCategory.HARCAMA_FISI: """Belge ailesi: HARCAMA FISI.
- company_name satici isletmedir.
- receipt_number icin once Fis No, yoksa Belge No kullan.
- total_amount fisteki nihai toplamdir.
- description alanina kisa urun/hizmet ozeti yaz.""",
    DocumentCategory.CEK: """Belge ailesi: CEK.
- document_number cek seri / belge numarasidir.
- company_name duzenleyen banka veya firma adidir.
- document_date alanina vade tarihi gorunuyorsa onu yaz; aksi halde gorunur ana tarihi yaz.
- notes alanina lehdar/alici gibi onemli serbest metni koy.""",
    DocumentCategory.MALZEME: """Belge ailesi: MALZEME / IRSALIYE.
- company_name tedarikci veya belge ust bilgisindeki firmadir.
- document_number irsaliye / belge / form numarasidir.
- description alanina malzeme cinsi veya malzeme listesinin kisa ozeti yaz.
- notes alanina teslim yeri / santiye / aciklama gibi sahaya ait bilgileri yaz.
- total_amount gorunmuyorsa null birak; sirf tahmin etmek icin hesap yapma.
- expense_category alanina kisa belge turu ozeti yaz (ornegin 'Irsaliye', 'Teslim fisi', 'Veresiye senedi').""",
    DocumentCategory.IADE: """Belge ailesi: IADE / IPTAL.
- Mumkunse belgeyi esas aile mantigiyla oku ama iade niteliklerini koru.
- description alaninda kisa iade ozeti kullan.
- Gorunmeyen karsit kayit veya orijinal belge ayrintilarini uydurma.""",
    DocumentCategory.BELIRSIZ: """Belge ailesi belirsiz.
- Gorunen belge yapisina en yakin alanlari doldur.
- Emin olmadigin alanlari null birak.""",
    DocumentCategory.ELDEN_ODEME: """Belge ailesi: ELDEN ODEME.
- Bu kategori medya belgeleri icin beklenmez; yine de gorunur odeme kaydi varsa en yakin alanlari doldur.""",
}

_EXTRACTION_EXAMPLES = """Kisa ornekler:
1. El yazili hafriyat/malzeme formunda tutar gorunmuyorsa total_amount=null olabilir; description malzeme cinsi, notes teslim yeri olabilir.
2. Ayni fotografta 3 cek varsa 3 ayri document dondur ve siralamayi soldan saga yap.
3. Iade faturasinda iade oldugu acikca gorunuyorsa tutarlari goruldugu gibi cikar; normal faturaya cevirme."""


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
    category_hint: DocumentCategory | None = None,
    document_count_hint: int | None = None,
    is_return_hint: bool = False,
) -> list[BillRecord]:
    """
    Extract one or more bookkeeping records from media.
    """
    if not settings.gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured.")

    model_name = settings.gemini_extractor_model
    prompt = _build_extraction_prompt(
        category_hint=category_hint,
        document_count_hint=document_count_hint,
        is_return_hint=is_return_hint,
    )

    logger.info(
        "Sending media (%d bytes, %s) to Gemini model %s for extraction",
        len(image_bytes),
        mime_type,
        model_name,
    )

    multi_result = gemini_client.generate_structured_content(
        model=model_name,
        prompt=prompt,
        system_instruction=_EXTRACTION_SYSTEM_INSTRUCTION,
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
        category_hint=category_hint,
    )
    if not records:
        raise RuntimeError("Gemini returned no documents from the image.")
    return records[0]



def _build_extraction_prompt(
    *,
    category_hint: DocumentCategory | None,
    document_count_hint: int | None,
    is_return_hint: bool,
) -> str:
    category = category_hint or DocumentCategory.BELIRSIZ
    category_instructions = _CATEGORY_SPECIFIC_INSTRUCTIONS.get(
        category,
        _CATEGORY_SPECIFIC_INSTRUCTIONS[DocumentCategory.BELIRSIZ],
    )
    count_text = (
        f"Goruntude yaklasik {document_count_hint} ayri belge bekleniyor."
        if document_count_hint and document_count_hint > 1
        else "Tek belge de olabilir, birden fazla belge de olabilir."
    )
    return_text = (
        "Belge muhtemelen bir iade/iptal niteligi tasiyor."
        if is_return_hint
        else "Iade ipucu yok."
    )
    return (
        f"{_EXTRACTION_PROMPT}\n\n"
        f"Belge ailesi ipucu: {category.value}\n"
        f"{return_text}\n"
        f"{count_text}\n\n"
        f"{category_instructions}\n\n"
        f"{_EXTRACTION_EXAMPLES}"
    )
