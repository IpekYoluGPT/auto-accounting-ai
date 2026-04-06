"""
6-category document type classifier.

Runs after bill_classifier confirms a message is a financial document.
Determines which accounting table the record belongs to.

Categories:
    fatura        — Resmi KDV'li fatura, e-fatura, toptan satış faturası
    odeme_dekontu — Banka dekontu, EFT, FAST transferi, havale
    harcama_fisi  — Akaryakıt fişi, market fişi, POS fişi, yemek fişi
    cek           — Banka çeki
    malzeme       — İrsaliye, teslim belgesi, veresiye satış senedi
    iade          — İade faturası, iade dekontu, iptal belgesi
    belirsiz      — Tanımlanamayan belge
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from app.config import settings
from app.models.schemas import DocumentCategory
from app.services import gemini_client
from app.utils.logging import get_logger

logger = get_logger(__name__)


class _RawDocumentTypeResult(BaseModel):
    """Internal Gemini response schema for document type classification."""

    category: str = "belirsiz"
    is_return: bool = False
    confidence: float = Field(default=0.7, ge=0.0, le=1.0)
    reason: Optional[str] = None

    model_config = {"extra": "ignore"}


_CATEGORY_PROMPT = """Bu bir Türkçe finansal belgedir. Belgenin kategorisini belirle.

Kategori seçenekleri (yalnızca birini seç):
- fatura: Resmi KDV'li fatura, e-fatura, toptan satış faturası, irsaliyeli fatura
- odeme_dekontu: Banka dekontu, EFT belgesi, FAST transferi, havale belgesi, IBAN ödeme makbuzu
- harcama_fisi: Akaryakıt pompası fişi, market/POS fişi, yemek fişi, otopark fişi
- cek: Banka çeki belgesi
- malzeme: İrsaliye, sevk irsaliyesi, malzeme teslim belgesi, kum/çakıl/hafriyat belgesi, veresiye satış senedi
- iade: İade faturası, iade dekontu, iptal belgesi, red belgesi
- belirsiz: Hiçbiri veya tanımlanamayan belge

Ayrıca: is_return alanını true yap eğer bu belge bir iade veya iptal işlemini gösteriyorsa.

JSON formatında döndür."""


# ─── Text-based elden ödeme extraction ──────────────────────────────────────


class _EldenOdemeRaw(BaseModel):
    """Gemini extraction schema for manager cash payment text messages."""

    total_amount: Optional[float] = None
    currency: str = "TRY"
    recipient: Optional[str] = None
    description: Optional[str] = None

    model_config = {"extra": "ignore"}


_ELDEN_ODEME_PROMPT = """Bu mesaj bir şirket yöneticisinden gelen elden/nakit ödeme kaydıdır.

Şunları çıkar:
- total_amount: Ödeme tutarı (sadece sayı, TL işareti olmadan)
- currency: Para birimi (genellikle TRY)
- recipient: Ödeme yapılan kişi veya firma adı (varsa)
- description: Ödemenin kısa açıklaması

Eğer mesaj bir ödeme kaydı değilse total_amount'u null bırak.
JSON formatında döndür."""


# ─── Public interface ────────────────────────────────────────────────────────


def classify_document_type(
    image_bytes: bytes,
    mime_type: str = "image/jpeg",
) -> tuple[DocumentCategory, bool]:
    """
    Classify a confirmed financial document into one of 6 categories.

    Args:
        image_bytes: Raw image or PDF bytes.
        mime_type:   MIME type of the media.

    Returns:
        Tuple of (DocumentCategory, is_return).
        Falls back to (BELIRSIZ, False) on any error.
    """
    logger.info(
        "Classifying document type with Gemini %s (%d bytes)",
        settings.gemini_classifier_model,
        len(image_bytes),
    )
    try:
        raw: _RawDocumentTypeResult = gemini_client.generate_structured_content(
            model=settings.gemini_classifier_model,
            prompt=_CATEGORY_PROMPT,
            response_schema=_RawDocumentTypeResult,
            thinking_level="minimal",
            media_bytes=image_bytes,
            mime_type=mime_type,
        )
        category_str = (raw.category or "belirsiz").lower().strip()
        try:
            category = DocumentCategory(category_str)
        except ValueError:
            logger.warning("Unknown category '%s' from Gemini; defaulting to belirsiz.", category_str)
            category = DocumentCategory.BELIRSIZ

        logger.info(
            "Document type: %s | is_return=%s | confidence=%.2f | reason=%s",
            category.value,
            raw.is_return,
            raw.confidence,
            raw.reason,
        )
        return category, raw.is_return

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
    logger.info("Extracting elden ödeme from manager text (%d chars)", len(text))
    try:
        full_prompt = f"{_ELDEN_ODEME_PROMPT}\n\nMesaj: {text}"
        raw: _EldenOdemeRaw = gemini_client.generate_structured_content(
            model=settings.gemini_extractor_model,
            prompt=full_prompt,
            response_schema=_EldenOdemeRaw,
            thinking_level="minimal",
        )
        logger.info(
            "Elden ödeme extraction: amount=%s currency=%s recipient=%s",
            raw.total_amount,
            raw.currency,
            raw.recipient,
        )
        return raw.total_amount, raw.currency or "TRY", raw.recipient, raw.description
    except Exception as exc:
        logger.error("Elden ödeme text extraction failed: %s", exc, exc_info=True)
        return None, "TRY", None, None
