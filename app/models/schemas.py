"""
Pydantic models for WhatsApp webhook payloads and internal bill records.
"""

from __future__ import annotations

from datetime import date, time
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


# ─── WhatsApp Webhook Models ──────────────────────────────────────────────────


class WhatsAppTextBody(BaseModel):
    body: str


class WhatsAppImageMedia(BaseModel):
    id: str
    mime_type: Optional[str] = None
    sha256: Optional[str] = None
    caption: Optional[str] = None


class WhatsAppDocumentMedia(BaseModel):
    id: str
    filename: Optional[str] = None
    mime_type: Optional[str] = None
    sha256: Optional[str] = None
    caption: Optional[str] = None


class WhatsAppStickerMedia(BaseModel):
    id: str
    mime_type: Optional[str] = None


class WhatsAppMessage(BaseModel):
    id: str
    from_: str = Field(alias="from")
    timestamp: str
    type: str
    text: Optional[WhatsAppTextBody] = None
    image: Optional[WhatsAppImageMedia] = None
    document: Optional[WhatsAppDocumentMedia] = None
    sticker: Optional[WhatsAppStickerMedia] = None

    model_config = {"populate_by_name": True}


class WhatsAppContact(BaseModel):
    profile: Optional[dict[str, Any]] = None
    wa_id: str


class WhatsAppValue(BaseModel):
    messaging_product: str
    metadata: Optional[dict[str, Any]] = None
    contacts: Optional[list[WhatsAppContact]] = None
    messages: Optional[list[WhatsAppMessage]] = None
    statuses: Optional[list[dict[str, Any]]] = None


class WhatsAppChange(BaseModel):
    value: WhatsAppValue
    field: str


class WhatsAppEntry(BaseModel):
    id: str
    changes: list[WhatsAppChange]


class WhatsAppWebhookPayload(BaseModel):
    object: str
    entry: list[WhatsAppEntry]


# ─── Bill / Invoice Record ────────────────────────────────────────────────────


class BillRecord(BaseModel):
    """Normalised internal representation of an extracted bill / invoice."""

    # Company info
    company_name: Optional[str] = None
    tax_number: Optional[str] = None
    tax_office: Optional[str] = None

    # Document identifiers
    document_number: Optional[str] = None
    invoice_number: Optional[str] = None
    receipt_number: Optional[str] = None

    # Date / time
    document_date: Optional[str] = None
    document_time: Optional[str] = None

    # Monetary
    currency: Optional[str] = None
    subtotal: Optional[float] = None
    vat_rate: Optional[float] = None
    vat_amount: Optional[float] = None
    total_amount: Optional[float] = None

    # Classification
    payment_method: Optional[str] = None
    expense_category: Optional[str] = None

    # Free text
    description: Optional[str] = None
    notes: Optional[str] = None

    # Source tracking
    source_message_id: Optional[str] = None
    source_filename: Optional[str] = None
    source_type: Optional[str] = None

    # AI confidence
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class AIExtractionResult(BaseModel):
    """Structured Gemini extraction output before source metadata is attached."""

    company_name: Optional[str] = None
    tax_number: Optional[str] = None
    tax_office: Optional[str] = None
    document_number: Optional[str] = None
    invoice_number: Optional[str] = None
    receipt_number: Optional[str] = None
    document_date: Optional[str] = None
    document_time: Optional[str] = None
    currency: Optional[Literal["TRY", "EUR", "USD"]] = None
    subtotal: Optional[float] = None
    vat_rate: Optional[float] = None
    vat_amount: Optional[float] = None
    total_amount: Optional[float] = None
    payment_method: Optional[
        Literal["Nakit", "Kredi Karti", "Kredi Kartı", "Banka Transferi", "Diger", "Diğer"]
    ] = None
    expense_category: Optional[
        Literal[
            "Yemek",
            "Ulasim",
            "Ulaşım",
            "Konaklama",
            "Ofis",
            "Yazilim",
            "Yazılım",
            "Donanim",
            "Donanım",
            "Abonelik",
            "Kargo",
            "Vergi",
            "Diger",
            "Diğer",
        ]
    ] = None
    description: Optional[str] = None
    notes: Optional[str] = None
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)

    model_config = {"extra": "ignore"}


# ─── Classification Result ────────────────────────────────────────────────────


class ClassificationResult(BaseModel):
    is_bill: bool
    reason: Optional[str] = None
    confidence: float = Field(ge=0.0, le=1.0)

    model_config = {"extra": "ignore"}
