"""
Pydantic models for WhatsApp webhook payloads and internal bill records.
"""

from __future__ import annotations

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
    group_id: Optional[str] = None
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


# ─── Periskope Webhook Models ────────────────────────────────────────────────


class PeriskopeMessageId(BaseModel):
    id: Optional[str] = None
    from_me: Optional[bool] = Field(default=None, alias="fromMe")
    remote: Optional[str] = None
    serialized: Optional[str] = Field(default=None, alias="_serialized")

    model_config = {"populate_by_name": True}


class PeriskopeMediaDimensions(BaseModel):
    ar: Optional[float] = None
    width: Optional[int] = None
    height: Optional[int] = None


class PeriskopeMedia(BaseModel):
    path: Optional[str] = None
    size: Optional[int] = None
    filename: Optional[str] = None
    mimetype: Optional[str] = None
    dimensions: Optional[PeriskopeMediaDimensions] = None


class PeriskopeMessage(BaseModel):
    message_id: str
    org_id: str
    org_phone: str
    chat_id: str
    message_type: str
    author: Optional[str] = None
    body: Optional[str] = None
    from_: Optional[str] = Field(default=None, alias="from")
    from_me: bool = False
    has_media: bool = False
    media: Optional[PeriskopeMedia] = None
    sender_phone: Optional[str] = None
    quoted_message_id: Optional[str] = None
    timestamp: Optional[str] = None
    unique_id: Optional[str] = None
    id: Optional[PeriskopeMessageId] = None
    is_private_note: Optional[bool] = None

    model_config = {"populate_by_name": True}


class PeriskopeWebhookEvent(BaseModel):
    event: str
    data: dict[str, Any]
    org_id: Optional[str] = None
    timestamp: Optional[str] = None


# ─── Groups API Models ────────────────────────────────────────────────────────


class GroupOnboardingRequest(BaseModel):
    subject: str = Field(min_length=1, max_length=128)
    description: Optional[str] = Field(default=None, max_length=2048)
    join_approval_mode: Literal["auto_approve", "approval_required"] = "auto_approve"


class GroupJoinRequestDecisionRequest(BaseModel):
    join_request_ids: list[str] = Field(min_length=1)


# ─── Periskope Tool Models ───────────────────────────────────────────────────


class PeriskopeCreateAccountingRecordRequest(BaseModel):
    chat_id: Optional[str] = None
    source_message_id: Optional[str] = None
    source_filename: Optional[str] = None
    source_type: Optional[str] = "periskope_tool"
    source_sender_id: Optional[str] = None
    source_group_id: Optional[str] = None
    source_chat_type: Optional[Literal["individual", "group"]] = None
    company_name: Optional[str] = None
    tax_number: Optional[str] = None
    tax_office: Optional[str] = None
    document_number: Optional[str] = None
    invoice_number: Optional[str] = None
    receipt_number: Optional[str] = None
    document_date: Optional[str] = None
    document_time: Optional[str] = None
    currency: Optional[str] = None
    subtotal: Optional[float] = None
    vat_rate: Optional[float] = None
    vat_amount: Optional[float] = None
    total_amount: Optional[float] = None
    payment_method: Optional[str] = None
    expense_category: Optional[str] = None
    description: Optional[str] = None
    notes: Optional[str] = None
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class PeriskopeSubmissionStatusRequest(BaseModel):
    source_message_id: Optional[str] = None
    chat_id: Optional[str] = None
    limit: int = Field(default=5, ge=1, le=25)


class PeriskopeAssignToHumanRequest(BaseModel):
    chat_id: str = Field(min_length=1)
    message: str = Field(min_length=1, max_length=4000)
    reply_to: Optional[str] = None


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
    source_sender_id: Optional[str] = None
    source_group_id: Optional[str] = None
    source_chat_type: Optional[Literal["individual", "group"]] = None

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
