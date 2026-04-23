"""
Gemini-backed extraction with category-aware prompts.
"""

from __future__ import annotations

import re
from typing import Optional

from app.config import settings
from app.models.schemas import AIMultiExtractionResult, BillRecord, DocumentCategory, InvoiceLineItem
from app.services import gemini_client
from app.services.accounting import ocr, unit_dictionary
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
- document_number, invoice_number, receipt_number, cheque_serial_number, and cheque_account_ref are identifiers, not monetary numbers. Return them as strings exactly as seen, preserve leading zeroes, and do not drop digits because of Turkish number punctuation.
- If multiple documents are present, return one object per distinct document sorted left-to-right, then top-to-bottom.
- Do not merge separate documents into one record.
- For return documents, extract the values exactly as shown; do not flip signs unless the document itself shows a negative or return amount.
- Examples and category hints are pattern guidance only; never copy company names, people names, amounts, serial numbers, or wording from prior examples.
- Do not overfit to sample templates or prior test images; infer values only from the current document.
- If an invoice has visible line items, return them as structured line_items ordered top-to-bottom.
"""

_EXTRACTION_PROMPT = """Extract bookkeeping fields from this Turkish business document image or PDF.

This media may contain invoices, receipts, payment dekonts, cheques, delivery/material slips, or multiple documents in one photo.
Return the requested schema only.
Default currency to TRY when it is not shown."""

_CATEGORY_SPECIFIC_INSTRUCTIONS: dict[DocumentCategory, str] = {
    DocumentCategory.FATURA: """Belge ailesi: FATURA.
- company_name satici / duzenleyen firmadir.
- recipient_name gorunen alici / musteri / sevk edilen taraf ise onu yaz.
- buyer_name gorunen satin alan veya faturadaki musteri adidir; gorunmuyorsa null birak.
- invoice_type gorunen turu yaz (ornegin Satis Faturasi, Iade Faturasi, E-Arsiv).
- invoice_number icin once Fatura No, yoksa Belge No kullan.
- subtotal, vat_rate, vat_amount, total_amount, withholding_present, withholding_rate, withholding_amount ve payable_amount alanlarini yalnizca acikca gorunuyorsa doldur.
- iban ve bank_name gorunuyorsa ekle.
- line_items varsa her satir icin description, line_quantity, line_unit, unit_price ve line_amount alanlarini doldur; satirlari yukaridan asagiya sirala.
- description alanina kisa bir fatura ozeti yaz; payment_method sadece belgede acikca varsa doldur.""",
    DocumentCategory.ODEME_DEKONTU: """Belge ailesi: ODEME DEKONTU.
- company_name alanina gorunur banka, odeme kurumu veya baskin karsi taraf adini yaz.
- recipient_name aliciyi / paranin gittigi kisi veya firmayi belirtir.
- document_number alanina referans / islem / dekont numarasini koy.
- total_amount transfer edilen nihai tutardir.
- sender_iban gonderen hesabin ibanidir; net degilse null birak.
- recipient_iban alici hesabin ibanidir; net degilse null birak.
- iban ve bank_name gorunuyorsa ayrica doldur.
- sender_name alanina sadece gonderen kisi/firma adini yaz; telefon, IBAN, hesap numarasi, referans veya aciklama yazma. Isim gorunmuyorsa null birak.
- Taraf net degilse ilgili alani null birak; banka/firma adiyla tahmin yapma.
- description alanina alici veya islem aciklamasini kisa ve yararli bicimde koy.""",
    DocumentCategory.HARCAMA_FISI: """Belge ailesi: HARCAMA FISI.
- company_name satici isletmedir.
- recipient_name fiste gorunen alici / musteri / teslim alan varsa onu yaz.
- receipt_number icin once Fis No, yoksa Belge No kullan.
- total_amount fisteki nihai toplamdir.
- description alanina kisa urun/hizmet ozeti yaz.""",
    DocumentCategory.CEK: """Belge ailesi: CEK.
- document_number cek seri / belge numarasidir.
- recipient_name lehdar / alici / cekin gidecegi kisi veya firmadir.
- company_name duzenleyen banka veya firma adidir.
- cheque_issue_place, cheque_issue_date, cheque_due_date, cheque_serial_number, cheque_bank_name, cheque_branch ve cheque_account_ref gorunuyorsa ayri ayri doldur.
- document_date alanina vade tarihi gorunuyorsa onu yaz; aksi halde gorunur ana tarihi yaz.
- total_amount veya payable_amount cek tutari gorunuyorsa doldur.
- notes alanina lehdar/alici gibi onemli serbest metni koy.""",
    DocumentCategory.MALZEME: """Belge ailesi: MALZEME / IRSALIYE / SEVK.
- company_name tedarikci veya belge ust bilgisindeki firmadir.
- recipient_name teslim alan / alici / sevk edilen taraf gorunuyorsa onu yaz.
- document_number irsaliye / belge / form numarasidir.
- shipment_origin, shipment_destination, vehicle_plate, pallet_count, items_per_pallet ve product_quantity gorunuyorsa doldur.
- `18m3`, `18 m3`, `3AD`, `1 adet`, `25kg`, `2 ton`, `5 TRB` gibi miktar+birim yazimlari varsa sayi ve birimi ayri alanlara dagit.
- line_items varsa her satiri ayri ayri cikar; yalnizca description degil, gorunen satir miktari ve birimini de doldur.
- product_quantity yalniz belge genelinde tek baskin toplam miktar varsa doldur; cok kalemli belgelerde satir detaylarini line_items icinde tut.
- description alanina malzeme cinsi veya malzeme listesinin kisa ozeti yaz.
- notes alanina teslim yeri / santiye / aciklama gibi sahaya ait bilgileri yaz.
- total_amount gorunmuyorsa null birak; sirf tahmin etmek icin hesap yapma.
- expense_category alanina kisa belge turu ozeti yaz (ornegin 'Irsaliye', 'Teslim fisi', 'Veresiye senedi').""",
    DocumentCategory.IADE: """Belge ailesi: IADE / IPTAL.
- Mumkunse belgeyi esas aile mantigiyla oku ama iade niteliklerini koru.
- description alaninda kisa iade ozeti kullan.
- Gorunmeyen karsit kayit veya orijinal belge ayrintilarini uydurma.
- Iade belgesinde gorunen satirlar varsa line_items olarak cikar.""",
    DocumentCategory.BELIRSIZ: """Belge ailesi belirsiz.
- Gorunen belge yapisina en yakin alanlari doldur.
- Emin olmadigin alanlari null birak.
- line_items ya da banka / cek ayrintilari gorunuyorsa, alan adlarini doldur.""",
    DocumentCategory.ELDEN_ODEME: """Belge ailesi: ELDEN ODEME.
- Bu kategori medya belgeleri icin beklenmez; yine de gorunur odeme kaydi varsa en yakin alanlari doldur.
- recipient_name, payable_amount ve description gorunuyorsa ayri ayri yaz.""",
}

_EXTRACTION_EXAMPLES = """Kisa ornekler:
1. El yazili hafriyat/malzeme formunda tutar gorunmuyorsa total_amount=null olabilir; description malzeme cinsi, notes teslim yeri olabilir.
2. Ayni fotografta 3 cek varsa 3 ayri document dondur ve siralamayi soldan saga yap.
3. Iade faturasinda iade oldugu acikca gorunuyorsa tutarlari goruldugu gibi cikar; normal faturaya cevirme."""

_QUANTITY_WITH_UNIT_TOKEN_RE = unit_dictionary.QUANTITY_WITH_UNIT_TOKEN_RE
_LINE_ITEM_LEADING_QUANTITY_RE = unit_dictionary.LINE_ITEM_LEADING_QUANTITY_RE

_MULTI_DOCUMENT_RETRY_INSTRUCTIONS = """Bu goruntude birden fazla belge var ve onceki denemede belge ayrimi eksik kaldi.
- Bu turda gorunur her ayri belgeyi ayri object olarak dondur.
- Ayrik belgeleri ASLA birlestirme.
- Belgeleri soldan saga, sonra yukaridan asagiya sirala.
- Eger ayni firmaya, ayni vergi numarasina veya ayni tutara sahip birden fazla belge varsa yine de her birini AYRI object olarak dondur.
- Tek bir object dondurup diger belgeleri yoksayma.
- Belge sayisi kesin olarak verildiyse tam o sayida object dondur; eksik veya fazla dondurme.
"""

_THOUSANDS_STYLE_IDENTIFIER_RE = re.compile(r"^\d{1,3}(?:\.\d{3})+$")


def _parse_tr_number(value: str) -> Optional[float]:
    """Backward-compatible wrapper for tests."""
    return ocr.parse_tr_number(value)



def _normalize_unit_text(value: object) -> Optional[str]:
    return unit_dictionary.display_unit(value, compact=False)


def _extract_quantity_and_unit(raw_value: object, raw_unit: object = None) -> tuple[Optional[float], Optional[str]]:
    quantity = ocr.parse_tr_number(raw_value)
    unit = _normalize_unit_text(raw_unit)
    if quantity is not None and unit:
        return quantity, unit

    value_text = str(raw_value or '').strip()
    if not value_text:
        return quantity, unit

    match = _QUANTITY_WITH_UNIT_TOKEN_RE.match(value_text)
    if match is None:
        return quantity, unit

    parsed_quantity = ocr.parse_tr_number(match.group('quantity'))
    parsed_unit = _normalize_unit_text(match.group('unit'))
    return parsed_quantity if parsed_quantity is not None else quantity, parsed_unit or unit


def _strip_line_item_quantity_prefix(description: object) -> tuple[Optional[float], Optional[str], Optional[str]]:
    description_text = str(description or '').strip()
    if not description_text:
        return None, None, None

    match = _LINE_ITEM_LEADING_QUANTITY_RE.match(description_text)
    if match is None:
        return None, None, description_text

    parsed_quantity = ocr.parse_tr_number(match.group('quantity'))
    parsed_unit = _normalize_unit_text(match.group('unit'))
    remainder = str(match.group('rest') or '').strip() or description_text
    return parsed_quantity, parsed_unit, remainder


def _normalize_line_item(item: dict, *, safe_str, safe_float) -> InvoiceLineItem:
    description = safe_str(item.get('description'))
    quantity, unit = _extract_quantity_and_unit(item.get('quantity'), item.get('unit'))
    if (quantity is None or unit is None) and description:
        parsed_quantity, parsed_unit, stripped_description = _strip_line_item_quantity_prefix(description)
        if quantity is None and parsed_quantity is not None:
            quantity = parsed_quantity
        if unit is None and parsed_unit is not None:
            unit = parsed_unit
        description = safe_str(stripped_description)

    return InvoiceLineItem(
        description=description,
        quantity=quantity if quantity is not None else safe_float(item.get('quantity')),
        unit=unit,
        unit_price=safe_float(item.get('unit_price')),
        line_amount=safe_float(item.get('line_amount')),
    )


def _safe_bool(value) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if not s:
        return None
    if s in {"true", "1", "yes", "evet", "var", "y"}:
        return True
    if s in {"false", "0", "no", "hayir", "hayır", "yok", "n"}:
        return False
    return None

def _normalize_record(raw: dict) -> BillRecord:
    """Coerce raw JSON dict into a validated BillRecord."""

    def _safe_float(v) -> Optional[float]:
        return ocr.parse_tr_number(v)

    def _safe_str(v) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    def _safe_identifier_str(v) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, bool):
            return str(v)
        if isinstance(v, int):
            return str(v)
        if isinstance(v, float):
            return str(int(v)) if v.is_integer() else str(v)

        raw_text = str(v).strip()
        if not raw_text:
            return None

        compact_whitespace = "".join(raw_text.split())
        if compact_whitespace.isdigit():
            return compact_whitespace

        if "," in compact_whitespace and "." in compact_whitespace:
            digits_only = compact_whitespace.replace(".", "").replace(",", "")
            if digits_only.isdigit():
                return digits_only

        if "," in compact_whitespace:
            digits_only = compact_whitespace.replace(",", "")
            if digits_only.isdigit():
                return digits_only

        if _THOUSANDS_STYLE_IDENTIFIER_RE.fullmatch(compact_whitespace):
            return compact_whitespace.replace(".", "")

        return raw_text

    line_items_raw = raw.get("line_items") or []
    normalized_line_items: list[InvoiceLineItem] = []
    if isinstance(line_items_raw, list):
        for item in line_items_raw:
            if not isinstance(item, dict):
                continue
            normalized_line_items.append(_normalize_line_item(item, safe_str=_safe_str, safe_float=_safe_float))

    line_quantity, line_unit = _extract_quantity_and_unit(raw.get("line_quantity"), raw.get("line_unit"))
    product_quantity, product_unit = _extract_quantity_and_unit(raw.get("product_quantity"), raw.get("line_unit"))
    if line_unit is None and product_unit is not None:
        line_unit = product_unit

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
        document_number=_safe_identifier_str(raw.get("document_number")),
        invoice_number=_safe_identifier_str(raw.get("invoice_number")),
        receipt_number=_safe_identifier_str(raw.get("receipt_number")),
        document_date=ocr.normalize_date(_safe_str(raw.get("document_date"))) or _safe_str(raw.get("document_date")),
        document_time=ocr.normalize_time(_safe_str(raw.get("document_time"))) or _safe_str(raw.get("document_time")),
        currency=currency_raw,
        subtotal=_safe_float(raw.get("subtotal")),
        vat_rate=_safe_float(raw.get("vat_rate")),
        vat_amount=_safe_float(raw.get("vat_amount")),
        total_amount=_safe_float(raw.get("total_amount")),
        sender_name=_safe_str(raw.get("sender_name")),
        recipient_name=_safe_str(raw.get("recipient_name")),
        buyer_name=_safe_str(raw.get("buyer_name")),
        invoice_type=_safe_str(raw.get("invoice_type")),
        line_quantity=line_quantity,
        line_unit=line_unit,
        unit_price=_safe_float(raw.get("unit_price")),
        line_amount=_safe_float(raw.get("line_amount")),
        withholding_present=_safe_bool(raw.get("withholding_present")),
        withholding_rate=_safe_float(raw.get("withholding_rate")),
        withholding_amount=_safe_float(raw.get("withholding_amount")),
        payable_amount=_safe_float(raw.get("payable_amount")),
        sender_iban=_safe_str(raw.get("sender_iban")),
        recipient_iban=_safe_str(raw.get("recipient_iban")),
        iban=_safe_str(raw.get("iban")),
        bank_name=_safe_str(raw.get("bank_name")),
        shipment_origin=_safe_str(raw.get("shipment_origin")),
        shipment_destination=_safe_str(raw.get("shipment_destination")),
        pallet_count=_safe_float(raw.get("pallet_count")),
        items_per_pallet=_safe_float(raw.get("items_per_pallet")),
        product_quantity=product_quantity,
        vehicle_plate=_safe_str(raw.get("vehicle_plate")),
        cheque_issue_place=_safe_str(raw.get("cheque_issue_place")),
        cheque_issue_date=ocr.normalize_date(_safe_str(raw.get("cheque_issue_date"))) or _safe_str(raw.get("cheque_issue_date")),
        cheque_due_date=ocr.normalize_date(_safe_str(raw.get("cheque_due_date"))) or _safe_str(raw.get("cheque_due_date")),
        cheque_serial_number=_safe_identifier_str(raw.get("cheque_serial_number")),
        cheque_bank_name=_safe_str(raw.get("cheque_bank_name")),
        cheque_branch=_safe_str(raw.get("cheque_branch")),
        cheque_account_ref=_safe_identifier_str(raw.get("cheque_account_ref")),
        line_items=normalized_line_items or None,
        source_message_id=_safe_str(raw.get("source_message_id")),
        source_filename=_safe_str(raw.get("source_filename")),
        source_type=_safe_str(raw.get("source_type")),
        confidence=_safe_float(raw.get("confidence")),
        payment_method=_safe_str(raw.get("payment_method")),
        expense_category=_safe_str(raw.get("expense_category")),
        description=_safe_str(raw.get("description")),
        notes=_safe_str(raw.get("notes")),
        source_sender_id=_safe_str(raw.get("source_sender_id")),
        source_sender_name=_safe_str(raw.get("source_sender_name")),
        source_group_id=_safe_str(raw.get("source_group_id")),
        source_chat_type=_safe_str(raw.get("source_chat_type")),
    )


def extract_bills(
    image_bytes: bytes,
    mime_type: str = "image/jpeg",
    source_message_id: Optional[str] = None,
    source_filename: Optional[str] = None,
    source_type: Optional[str] = None,
    source_sender_id: Optional[str] = None,
    source_sender_name: Optional[str] = None,
    source_group_id: Optional[str] = None,
    source_chat_type: Optional[str] = None,
    *,
    category_hint: DocumentCategory | None = None,
    document_count_hint: int | None = None,
    is_return_hint: bool = False,
    strict_document_count: int | None = None,
    split_retry: bool = False,
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
        strict_document_count=strict_document_count,
        split_retry=split_retry,
    )

    logger.info(
        "Sending media (%d bytes, %s) to Gemini model %s for extraction (split_retry=%s strict_document_count=%s)",
        len(image_bytes),
        mime_type,
        model_name,
        split_retry,
        strict_document_count,
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
        record.source_sender_name = source_sender_name
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
    source_sender_name: Optional[str] = None,
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
        source_sender_name=source_sender_name,
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
    strict_document_count: int | None,
    split_retry: bool,
) -> str:
    category = category_hint or DocumentCategory.BELIRSIZ
    category_instructions = _CATEGORY_SPECIFIC_INSTRUCTIONS.get(
        category,
        _CATEGORY_SPECIFIC_INSTRUCTIONS[DocumentCategory.BELIRSIZ],
    )

    if strict_document_count and strict_document_count > 1:
        count_text = f"Bu denemede tam olarak {strict_document_count} ayri belge cikarilmasi gerekiyor."
    elif document_count_hint and document_count_hint > 1:
        count_text = f"Goruntude yaklasik {document_count_hint} ayri belge bekleniyor."
    else:
        count_text = "Tek belge de olabilir, birden fazla belge de olabilir."

    return_text = (
        "Belge muhtemelen bir iade/iptal niteligi tasiyor."
        if is_return_hint
        else "Iade ipucu yok."
    )

    retry_lines: list[str] = []
    if split_retry:
        retry_lines.append(_MULTI_DOCUMENT_RETRY_INSTRUCTIONS.strip())
        if strict_document_count and strict_document_count > 1:
            retry_lines.append(
                f"Bu ikinci geciste kismi sonuc kabul edilmez; ya tam olarak {strict_document_count} ayri document dondur ya da hic document dondurma."
            )
        if category == DocumentCategory.CEK:
            retry_lines.append(
                "CEK icin: her fiziksel cek yapragini ayri belge say. Banka, vergi numarasi veya duzenleyen sirket ayni olsa bile cekleri BIRLESTIRME. Vade tarihi, seri numarasi, lehdar, el yazisi ve konuma gore ayir."
            )

    prompt_parts = [
        _EXTRACTION_PROMPT,
        f"Belge ailesi ipucu: {category.value}",
        return_text,
        count_text,
        category_instructions,
    ]
    if retry_lines:
        prompt_parts.append("\n\n".join(retry_lines))
    prompt_parts.append(_EXTRACTION_EXAMPLES)
    return "\n\n".join(prompt_parts)
