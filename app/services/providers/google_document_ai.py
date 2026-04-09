"""
Google Document AI OCR provider.
"""

from __future__ import annotations

import base64
import json
from functools import lru_cache
from statistics import mean
from typing import Any

import httpx
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings
from app.models.ocr import (
    OCREntity,
    OCRKeyValue,
    OCRMediaMetadata,
    OCRPage,
    OCRParseBundle,
    OCRTable,
    OCRTableCell,
    OCRTextBlock,
)
from app.utils.logging import get_logger

logger = get_logger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


def is_configured() -> bool:
    return bool(
        settings.google_service_account_json
        and settings.google_document_ai_form_processor_id
        and _project_id()
    )


@lru_cache(maxsize=1)
def _service_account_info() -> dict[str, Any]:
    raw = settings.google_service_account_json.strip()
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not configured.")
    if raw.startswith("{"):
        return json.loads(raw)
    decoded = base64.b64decode(raw).decode("utf-8")
    return json.loads(decoded)


def _project_id() -> str:
    if settings.google_document_ai_project_id.strip():
        return settings.google_document_ai_project_id.strip()
    info = _service_account_info()
    return str(info.get("project_id", "")).strip()


@lru_cache(maxsize=1)
def _credentials() -> Credentials:
    return Credentials.from_service_account_info(_service_account_info(), scopes=_SCOPES)


def _access_token() -> str:
    creds = _credentials()
    if not creds.valid:
        creds.refresh(Request())
    if not creds.token:
        raise RuntimeError("Could not obtain a Google access token for Document AI.")
    return creds.token


def _api_base_url() -> str:
    location = settings.google_document_ai_location.strip() or "eu"
    return f"https://{location}-documentai.googleapis.com/v1"


def _processor_url(processor_id: str) -> str:
    project_id = _project_id()
    location = settings.google_document_ai_location.strip() or "eu"
    return (
        f"{_api_base_url()}/projects/{project_id}/locations/{location}"
        f"/processors/{processor_id}:process"
    )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), reraise=True)
def _process_with_processor(processor_id: str, media_bytes: bytes, mime_type: str) -> dict[str, Any]:
    payload = {
        "skipHumanReview": True,
        "rawDocument": {
            "mimeType": mime_type,
            "content": base64.b64encode(media_bytes).decode("ascii"),
        },
    }
    with httpx.Client(timeout=60) as client:
        response = client.post(
            _processor_url(processor_id),
            headers={
                "Authorization": f"Bearer {_access_token()}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        response.raise_for_status()
        return response.json()


def process_document(media_bytes: bytes, metadata: OCRMediaMetadata) -> OCRParseBundle:
    if not is_configured():
        raise RuntimeError("Google Document AI is not configured.")

    primary = _bundle_from_response(
        _process_with_processor(
            settings.google_document_ai_form_processor_id,
            media_bytes,
            metadata.mime_type,
        ),
        metadata=metadata,
        processor_name="form_parser",
        used_fallback=False,
    )

    fallback_processor = settings.google_document_ai_ocr_processor_id.strip()
    if not fallback_processor or not _should_use_ocr_fallback(primary):
        return primary

    fallback = _bundle_from_response(
        _process_with_processor(fallback_processor, media_bytes, metadata.mime_type),
        metadata=metadata,
        processor_name="enterprise_ocr",
        used_fallback=True,
    )

    if _bundle_score(fallback) >= _bundle_score(primary):
        fallback.warnings = primary.warnings + fallback.warnings
        return fallback

    primary.warnings.append("Enterprise OCR fallback ran but Form Parser output scored higher.")
    return primary


def _should_use_ocr_fallback(bundle: OCRParseBundle) -> bool:
    has_structure = bool(bundle.tables or bundle.key_values or bundle.entities)
    return (
        bundle.text_char_count < settings.ocr_min_text_chars
        or bundle.quality_score < settings.ocr_min_quality_score
        or not has_structure
    )


def _bundle_score(bundle: OCRParseBundle) -> float:
    structure_bonus = 0.1 if (bundle.tables or bundle.key_values or bundle.entities) else 0.0
    return bundle.quality_score + structure_bonus


def _bundle_from_response(
    payload: dict[str, Any],
    *,
    metadata: OCRMediaMetadata,
    processor_name: str,
    used_fallback: bool,
) -> OCRParseBundle:
    document = payload.get("document", {})
    full_text = str(document.get("text", "") or "")

    pages: list[OCRPage] = []
    tables: list[OCRTable] = []
    key_values: list[OCRKeyValue] = []
    entities: list[OCREntity] = []
    confidences: list[float] = []
    all_lines: list[str] = []

    for page_index, page in enumerate(document.get("pages", []), start=1):
        page_blocks: list[OCRTextBlock] = []
        page_lines: list[OCRTextBlock] = []

        for block in page.get("blocks", []):
            text = _text_from_layout(full_text, block.get("layout"))
            if text:
                confidence = _confidence(block, block.get("layout"))
                if confidence is not None:
                    confidences.append(confidence)
                page_blocks.append(OCRTextBlock(text=text, confidence=confidence))

        for line in page.get("lines", []):
            text = _text_from_layout(full_text, line.get("layout"))
            if text:
                confidence = _confidence(line, line.get("layout"))
                if confidence is not None:
                    confidences.append(confidence)
                page_lines.append(OCRTextBlock(text=text, confidence=confidence))
                all_lines.append(text)

        form_fields = page.get("formFields", [])
        for field in form_fields:
            key_text = _text_from_layout(full_text, field.get("fieldName"))
            value_text = _text_from_layout(full_text, field.get("fieldValue"))
            if key_text or value_text:
                confidence = _confidence(field)
                if confidence is not None:
                    confidences.append(confidence)
                key_values.append(
                    OCRKeyValue(
                        key=key_text or "unknown",
                        value=value_text or None,
                        confidence=confidence,
                    )
                )

        for table in page.get("tables", []):
            mapped = _map_table(table, page_index, full_text)
            if mapped.cells:
                if mapped.confidence is not None:
                    confidences.append(mapped.confidence)
                tables.append(mapped)

        pages.append(OCRPage(page_number=page_index, blocks=page_blocks, lines=page_lines))

    for entity in document.get("entities", []):
        mention_text = str(entity.get("mentionText", "") or "").strip()
        if not mention_text:
            mention_text = _text_from_anchor(full_text, entity.get("textAnchor"))
        if not mention_text:
            continue
        confidence = _confidence(entity)
        if confidence is not None:
            confidences.append(confidence)
        normalized_value = _normalized_entity_value(entity)
        entities.append(
            OCREntity(
                type=str(entity.get("type", "unknown")),
                mention_text=mention_text,
                normalized_value=normalized_value,
                confidence=confidence,
            )
        )

    if not all_lines and full_text:
        all_lines = [line.strip() for line in full_text.splitlines() if line.strip()]

    average_confidence = mean(confidences) if confidences else 0.0
    readability_score = min(1.0, len(full_text.strip()) / 320.0)
    if all_lines:
        readability_score = min(1.0, readability_score + min(0.2, len(all_lines) / 40.0))
    quality_score = min(1.0, (average_confidence * 0.7) + (readability_score * 0.3))

    warnings: list[str] = []
    if not full_text.strip():
        warnings.append("Document AI returned no OCR text.")
    if quality_score < settings.ocr_min_quality_score:
        warnings.append("OCR quality score is below the configured threshold.")

    return OCRParseBundle(
        text=full_text,
        lines=all_lines,
        pages=pages,
        tables=tables,
        key_values=key_values,
        entities=entities,
        quality_score=round(quality_score, 4),
        readability_score=round(readability_score, 4),
        text_char_count=len(full_text.strip()),
        processor_used=processor_name,
        used_fallback_processor=used_fallback,
        metadata=metadata,
        warnings=warnings,
    )


def _map_table(table: dict[str, Any], page_number: int, full_text: str) -> OCRTable:
    cells: list[OCRTableCell] = []
    row_groups: list[list[dict[str, Any]]] = []
    row_groups.extend(table.get("headerRows", []))
    row_groups.extend(table.get("bodyRows", []))

    if row_groups:
        for row_index, row in enumerate(row_groups):
            raw_cells = row.get("cells", []) if isinstance(row, dict) else []
            for column_index, cell in enumerate(raw_cells):
                text = _text_from_layout(full_text, cell.get("layout")) or str(cell.get("text", "") or "").strip()
                if not text:
                    continue
                confidence = _confidence(cell, cell.get("layout"))
                cells.append(
                    OCRTableCell(
                        row_index=row_index,
                        column_index=column_index,
                        text=text,
                        confidence=confidence,
                    )
                )
    else:
        for cell in table.get("cells", []):
            text = _text_from_layout(full_text, cell.get("layout")) or str(cell.get("text", "") or "").strip()
            if not text:
                continue
            confidence = _confidence(cell, cell.get("layout"))
            cells.append(
                OCRTableCell(
                    row_index=int(cell.get("rowIndex", 0) or 0),
                    column_index=int(cell.get("colIndex", 0) or 0),
                    text=text,
                    confidence=confidence,
                )
            )

    row_count = max((cell.row_index for cell in cells), default=-1) + 1
    column_count = max((cell.column_index for cell in cells), default=-1) + 1
    return OCRTable(
        page_number=page_number,
        row_count=row_count,
        column_count=column_count,
        cells=cells,
        confidence=_confidence(table),
    )


def _normalized_entity_value(entity: dict[str, Any]) -> str | None:
    normalized_value = entity.get("normalizedValue")
    if not isinstance(normalized_value, dict):
        return None
    text = normalized_value.get("text")
    if text:
        return str(text)
    for value in normalized_value.values():
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _confidence(node: dict[str, Any], layout: dict[str, Any] | None = None) -> float | None:
    for candidate in (node.get("confidence"), (layout or {}).get("confidence")):
        if candidate is None:
            continue
        try:
            value = float(candidate)
        except (TypeError, ValueError):
            continue
        if 0.0 <= value <= 1.0:
            return value
    return None


def _text_from_layout(full_text: str, layout: dict[str, Any] | None) -> str:
    if not isinstance(layout, dict):
        return ""
    return _text_from_anchor(full_text, layout.get("textAnchor")).strip()


def _text_from_anchor(full_text: str, anchor: dict[str, Any] | None) -> str:
    if not isinstance(anchor, dict):
        return ""
    segments = anchor.get("textSegments") or []
    if not segments:
        return ""
    pieces: list[str] = []
    for segment in segments:
        start = int(segment.get("startIndex", 0) or 0)
        end = int(segment.get("endIndex", 0) or 0)
        if end <= start:
            continue
        pieces.append(full_text[start:end])
    return "".join(pieces)
