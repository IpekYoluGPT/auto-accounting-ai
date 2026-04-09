"""
Pydantic models for OCR parsing and normalized media metadata.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class OCRTextBlock(BaseModel):
    text: str
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class OCRPage(BaseModel):
    page_number: int
    blocks: list[OCRTextBlock] = Field(default_factory=list)
    lines: list[OCRTextBlock] = Field(default_factory=list)


class OCRTableCell(BaseModel):
    row_index: int
    column_index: int
    text: str
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class OCRTable(BaseModel):
    page_number: int
    row_count: int = 0
    column_count: int = 0
    cells: list[OCRTableCell] = Field(default_factory=list)
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class OCRKeyValue(BaseModel):
    key: str
    value: Optional[str] = None
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class OCREntity(BaseModel):
    type: str
    mention_text: str
    normalized_value: Optional[str] = None
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class OCRMediaMetadata(BaseModel):
    mime_type: str
    original_mime_type: str
    byte_size: int
    width: Optional[int] = None
    height: Optional[int] = None
    source_hash: str


class OCRParseBundle(BaseModel):
    text: str = ""
    lines: list[str] = Field(default_factory=list)
    pages: list[OCRPage] = Field(default_factory=list)
    tables: list[OCRTable] = Field(default_factory=list)
    key_values: list[OCRKeyValue] = Field(default_factory=list)
    entities: list[OCREntity] = Field(default_factory=list)
    quality_score: float = Field(default=0.0, ge=0.0, le=1.0)
    readability_score: float = Field(default=0.0, ge=0.0, le=1.0)
    text_char_count: int = 0
    processor_used: str = ""
    used_fallback_processor: bool = False
    metadata: OCRMediaMetadata
    warnings: list[str] = Field(default_factory=list)


class PreparedOCRDocument(BaseModel):
    media_bytes: bytes
    mime_type: str
    metadata: OCRMediaMetadata
    ocr_bundle: OCRParseBundle | None = None
    warnings: list[str] = Field(default_factory=list)
