"""
Gemini-first document analysis for Turkish bookkeeping documents.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

from app.config import settings
from app.models.schemas import DocumentCategory
from app.services import gemini_client
from app.utils.logging import get_logger

logger = get_logger(__name__)


class _RawDocumentAnalysis(BaseModel):
    """Internal Gemini response schema for document intake triage."""

    is_financial_document: bool = False
    category: str = "belirsiz"
    is_return: bool = False
    document_count: int = Field(default=1, ge=0, le=10)
    quality: Literal["clear", "usable", "poor", "unusable"] = "usable"
    needs_retry: bool = False
    confidence: float = Field(default=0.7, ge=0.0, le=1.0)
    reason: Optional[str] = None

    model_config = {"extra": "ignore"}


class DocumentAnalysis(BaseModel):
    is_financial_document: bool = False
    category: DocumentCategory = DocumentCategory.BELIRSIZ
    is_return: bool = False
    document_count: int = Field(default=1, ge=0, le=10)
    quality: Literal["clear", "usable", "poor", "unusable"] = "usable"
    needs_retry: bool = False
    confidence: float = Field(default=0.7, ge=0.0, le=1.0)
    reason: Optional[str] = None

    model_config = {"extra": "ignore"}


_ANALYSIS_SYSTEM_INSTRUCTION = """Sen Turkce muhasebe evraki inceleyen dikkatli bir yapay zeka yardimcisisin.

Kurallar:
- Yalnizca goruntude veya PDF icinde acikca gorulen bilgiye dayan.
- Gorunmeyen metinleri, sayilari, belge numaralarini veya sirket adlarini uydurma.
- Telefonla cekilmis, egik, burusturulmus, golgeli veya el yazili belgeler yaygindir; yine de okunabiliyorsa retry isteme.
- needs_retry alanini sadece belge ciddi bicimde bulanik, karanlik, kesik, kapali, asiri uzak veya parca parca ise true yap.
- Bir iade belgesinde mumkunse temel belge ailesini sec ve is_return=true yap. Sadece belge ailesi gercekten anlasilamiyorsa category=iade kullan.
- Birden fazla belge varsa document_count alaninda tahmini sayiyi ver.
"""

_ANALYSIS_PROMPT = """Bu medya muhasebe surecine girecek bir Turkce is belgesi olabilir. JSON dondur.

Alanlar:
- is_financial_document: muhasebe acisindan islenecek gercek bir belge mi?
- category: asagidakilerden biri
  - fatura
  - odeme_dekontu
  - harcama_fisi
  - cek
  - malzeme
  - iade
  - belirsiz
- is_return: iade / iptal / red / ters islem mi?
- document_count: goruntudeki ayri belge sayisi
- quality: clear | usable | poor | unusable
- needs_retry: yalnizca gercekten kotu kalite veya ciddi eksiklik varsa true
- confidence: 0 ile 1 arasi
- reason: kisa gerekce

Kategori ipuclari:
- fatura: e-fatura, e-arsiv, toptan satis faturasi, kurumsal KDV'li satis belgesi
- odeme_dekontu: banka dekontu, EFT/FAST/havale, IBAN transfer PDF'i veya ekran ciktisi
- harcama_fisi: akaryakit fisi, POS fisi, market/yemek/otopark fisi
- cek: cek yapragi veya cek goruntusu
- malzeme: irsaliye, teslim fisi, hafriyat/kum/cakil formu, veresiye satis senedi, el yazili malzeme teslim belgesi
- iade: belge ailesi belli degil ama iade/iptal oldugu net belge

Ornekler:
- El yazili hafriyat formu okunuyorsa financial_document=true, category=malzeme, needs_retry=false.
- Ayni fotografta 3 cek varsa document_count=3.
- Iade faturasinda temel aile fatura ise category=fatura ve is_return=true.
"""


class _EldenOdemeRaw(BaseModel):
    """Gemini extraction schema for manager cash payment text messages."""

    total_amount: Optional[float] = None
    currency: str = "TRY"
    recipient: Optional[str] = None
    description: Optional[str] = None

    model_config = {"extra": "ignore"}


_ELDEN_ODEME_PROMPT = """Bu mesaj bir sirket yoneticisinden gelen elden/nakit odeme kaydidir.

Sunlari cikar:
- total_amount: Odeme tutari (sadece sayi, TL isareti olmadan)
- currency: Para birimi (genellikle TRY)
- recipient: Odeme yapilan kisi veya firma adi (varsa)
- description: Odemenin kisa aciklamasi

Eger mesaj bir odeme kaydi degilse total_amount'u null birak.
JSON formatinda dondur."""


def analyze_document(
    image_bytes: bytes,
    mime_type: str = "image/jpeg",
) -> DocumentAnalysis:
    logger.info(
        "Analyzing document with Gemini %s (%d bytes)",
        settings.gemini_classifier_model,
        len(image_bytes),
    )

    raw: _RawDocumentAnalysis = gemini_client.generate_structured_content(
        model=settings.gemini_classifier_model,
        prompt=_ANALYSIS_PROMPT,
        system_instruction=_ANALYSIS_SYSTEM_INSTRUCTION,
        response_schema=_RawDocumentAnalysis,
        thinking_level="low",
        media_bytes=image_bytes,
        mime_type=mime_type,
    )

    category_str = (raw.category or "belirsiz").lower().strip()
    try:
        category = DocumentCategory(category_str)
    except ValueError:
        logger.warning("Unknown category '%s' from Gemini; defaulting to belirsiz.", category_str)
        category = DocumentCategory.BELIRSIZ

    is_return = raw.is_return or category == DocumentCategory.IADE
    if raw.is_financial_document:
        document_count = max(1, raw.document_count or 1)
    else:
        document_count = max(0, raw.document_count or 0)

    result = DocumentAnalysis(
        is_financial_document=raw.is_financial_document,
        category=category,
        is_return=is_return,
        document_count=document_count,
        quality=raw.quality,
        needs_retry=(raw.needs_retry or raw.quality == "unusable"),
        confidence=raw.confidence,
        reason=raw.reason,
    )
    logger.info(
        "Document analysis: financial=%s category=%s is_return=%s count=%d quality=%s retry=%s confidence=%.2f",
        result.is_financial_document,
        result.category.value,
        result.is_return,
        result.document_count,
        result.quality,
        result.needs_retry,
        result.confidence,
    )
    return result


# Backward-compatible wrapper retained for tests/callers that still expect just category.
def classify_document_type(
    image_bytes: bytes,
    mime_type: str = "image/jpeg",
    *,
    ocr_bundle: Optional[object] = None,
) -> tuple[DocumentCategory, bool]:
    if ocr_bundle is not None:
        logger.debug("Ignoring deprecated ocr_bundle argument in classify_document_type.")

    try:
        analysis = analyze_document(image_bytes, mime_type=mime_type)
        return analysis.category, analysis.is_return
    except Exception as exc:
        logger.error("Document type classification failed: %s", exc, exc_info=True)
        return DocumentCategory.BELIRSIZ, False


def extract_elden_odeme_from_text(text: str) -> tuple[Optional[float], str, Optional[str], Optional[str]]:
    """
    Extract cash payment fields from a manager's text message.

    Returns:
        Tuple of (total_amount, currency, recipient, description).
        total_amount is None when the text is not a payment record.
    """
    logger.info("Extracting elden odeme from manager text (%d chars)", len(text))
    try:
        full_prompt = f"{_ELDEN_ODEME_PROMPT}\n\nMesaj: {text}"
        raw: _EldenOdemeRaw = gemini_client.generate_structured_content(
            model=settings.gemini_extractor_model,
            prompt=full_prompt,
            system_instruction=(
                "Yalnizca mesajda acikca yazan odeme bilgisini cikar. "
                "Tutar gorunmuyorsa total_amount null olmali."
            ),
            response_schema=_EldenOdemeRaw,
            thinking_level="minimal",
        )
        logger.info(
            "Elden odeme extraction: amount=%s currency=%s recipient=%s",
            raw.total_amount,
            raw.currency,
            raw.recipient,
        )
        return raw.total_amount, raw.currency or "TRY", raw.recipient, raw.description
    except Exception as exc:
        logger.error("Elden odeme text extraction failed: %s", exc, exc_info=True)
        return None, "TRY", None, None
