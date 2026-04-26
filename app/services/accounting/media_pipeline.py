"""
Media-specific intake pipeline helpers.
"""

from __future__ import annotations

import hashlib

from app.models.schemas import BillRecord, DocumentCategory
from app.services.accounting.intake_messages import (
    MSG_MEDIA_CLASSIFICATION_ERROR,
    MSG_MEDIA_EMPTY_EXTRACTION,
    MSG_MEDIA_EXTRACTION_ERROR,
    MSG_MEDIA_MULTI_DOCUMENT_RETRY,
    MSG_MEDIA_RETRY_QUALITY,
    MSG_MEDIA_TEMPORARY_UPSTREAM_ERROR,
)
from app.services.accounting.intake_types import MediaPayload, MediaProcessingResult


def process_media_payload(*, intake_module, payload: MediaPayload) -> MediaProcessingResult:
    media_sha256 = hashlib.sha256(payload.raw_bytes).hexdigest()

    prepared = intake_module.media_prep.prepare_media(payload.raw_bytes, mime_type=payload.mime_type)
    working_bytes = prepared.media_bytes
    working_mime_type = prepared.mime_type
    if prepared.warnings:
        intake_module.logger.info(
            "Media prepared for Gemini: message_id=%s warnings=%s",
            payload.message_id,
            prepared.warnings,
        )

    try:
        analysis, initial_records = intake_module.gemini_extractor.analyze_and_extract(
            working_bytes,
            working_mime_type,
            source_message_id=payload.message_id,
            source_filename=payload.filename,
            source_type=payload.source_type,
            source_sender_id=payload.route.sender_id,
            source_sender_name=payload.route.sender_name,
            source_group_id=payload.route.group_id,
            source_chat_type=payload.route.chat_type,
        )
    except Exception as exc:
        intake_module.logger.warning("Failed to analyze/extract media for message id=%s: %s", payload.message_id, exc)
        return MediaProcessingResult(
            outcome="classification_failed",
            retryable=is_temporary_media_exception(exc),
            user_message=message_for_media_exception(exc, MSG_MEDIA_CLASSIFICATION_ERROR),
            stage="classification",
        )

    if not analysis.is_financial_document:
        return MediaProcessingResult(
            outcome="warned_non_bill_media",
            user_message=intake_module.MSG_UNRELATED_IMAGE,
            stage="classification",
        )

    if analysis.needs_retry and analysis.quality == "unusable":
        return MediaProcessingResult(
            outcome="category_failed",
            user_message=MSG_MEDIA_RETRY_QUALITY,
            stage="classification",
        )

    category = analysis.category
    is_return = analysis.is_return
    expected_document_count = analysis.document_count if analysis.document_count > 1 else None

    valid_records = [r for r in initial_records if record_meets_minimum_fields(r, category)]

    def _extract_valid_records_retry(*, split_retry: bool) -> list[BillRecord]:
        extracted_records = intake_module.gemini_extractor.extract_bills(
            image_bytes=working_bytes,
            mime_type=working_mime_type,
            source_message_id=payload.message_id,
            source_filename=payload.filename,
            source_type=payload.source_type,
            source_sender_id=payload.route.sender_id,
            source_sender_name=payload.route.sender_name,
            source_group_id=payload.route.group_id,
            source_chat_type=payload.route.chat_type,
            category_hint=category,
            document_count_hint=expected_document_count,
            is_return_hint=is_return,
            strict_document_count=expected_document_count if split_retry else None,
            split_retry=split_retry,
        )
        return [record for record in extracted_records if record_meets_minimum_fields(record, category)]

    if expected_document_count and len(valid_records) != expected_document_count:
        intake_module.logger.warning(
            "Multi-document extraction count mismatch for message id=%s: expected=%d got=%d; retrying with strict split prompt.",
            payload.message_id,
            expected_document_count,
            len(valid_records),
        )
        try:
            retry_records = _extract_valid_records_retry(split_retry=True)
        except Exception as exc:
            intake_module.logger.warning("Failed strict multi-document extraction for message id=%s: %s", payload.message_id, exc)
            return MediaProcessingResult(
                outcome="extraction_failed",
                retryable=is_temporary_media_exception(exc),
                user_message=message_for_media_exception(exc, MSG_MEDIA_EXTRACTION_ERROR),
                stage="extraction",
            )

        if len(retry_records) != expected_document_count:
            intake_module.logger.warning(
                "Strict multi-document extraction still mismatched for message id=%s: expected=%d got=%d.",
                payload.message_id,
                expected_document_count,
                len(retry_records),
            )
            if category == DocumentCategory.CEK and (retry_records or valid_records):
                valid_records = retry_records or valid_records
                intake_module.logger.warning(
                    "Accepting %d usable cheque record(s) for message id=%s despite estimated count mismatch.",
                    len(valid_records),
                    payload.message_id,
                )
            else:
                return MediaProcessingResult(
                    outcome="multi_document_retry_required",
                    user_message=MSG_MEDIA_MULTI_DOCUMENT_RETRY,
                    stage="extraction",
                )
        else:
            valid_records = retry_records

    if not valid_records:
        intake_module.logger.warning("Gemini returned no usable documents for message id=%s", payload.message_id)
        return MediaProcessingResult(
            outcome="empty_extraction",
            user_message=MSG_MEDIA_RETRY_QUALITY if analysis.needs_retry else MSG_MEDIA_EMPTY_EXTRACTION,
            stage="extraction",
        )

    try:
        persisted_records: list[BillRecord] = []
        for record in valid_records:
            record.source_media_sha256 = media_sha256
            persisted = intake_module.record_store.persist_record_once(record)
            if not persisted:
                continue
            persisted_records.append(record)

        for record in persisted_records:
            intake_module.google_sheets.append_record(
                record,
                category,
                is_return=is_return,
                drive_link=None,
                pending_document_bytes=payload.raw_bytes,
                pending_document_filename=payload.filename,
                pending_document_mime_type=payload.mime_type,
                feedback_target={
                    "platform": payload.route.platform,
                    "chat_id": payload.route.chat_id,
                    "recipient_type": payload.route.recipient_type,
                    "message_id": payload.message_id,
                },
            )
    except Exception as exc:
        intake_module.logger.warning("Failed to persist media-derived records for message id=%s: %s", payload.message_id, exc)
        if is_temporary_media_exception(exc):
            return MediaProcessingResult(
                outcome="persistence_failed",
                retryable=True,
                user_message=MSG_MEDIA_TEMPORARY_UPSTREAM_ERROR,
                stage="persistence",
            )
        raise

    persisted_count = len(persisted_records)
    if persisted_count == 0:
        return MediaProcessingResult(outcome="already_exported", stage="persistence")

    return MediaProcessingResult(outcome="exported", exported_count=persisted_count, stage="persistence")


def record_meets_minimum_fields(record: BillRecord, category: DocumentCategory) -> bool:
    has_identity = bool(record.company_name or record.document_number or record.invoice_number or record.receipt_number)
    has_total = record.total_amount is not None
    has_date = bool(record.document_date)

    if category == DocumentCategory.MALZEME:
        return bool(record.description and (has_identity or record.notes))
    if category == DocumentCategory.CEK:
        return has_total and bool(record.document_number or record.company_name or record.document_date)
    if category == DocumentCategory.ODEME_DEKONTU:
        return has_total and has_date and bool(has_identity or record.description)
    if category in {DocumentCategory.FATURA, DocumentCategory.HARCAMA_FISI, DocumentCategory.BELIRSIZ, DocumentCategory.IADE}:
        return has_total and has_date and has_identity
    if category == DocumentCategory.ELDEN_ODEME:
        return has_total and bool(record.description)
    return has_total and has_identity


def message_for_media_exception(exc: Exception, default_message: str) -> str:
    if is_temporary_media_exception(exc):
        return MSG_MEDIA_TEMPORARY_UPSTREAM_ERROR
    return default_message


def is_temporary_media_exception(exc: Exception) -> bool:
    error = str(exc).lower()
    return any(
        token in error
        for token in (
            "503",
            "429",
            "unavailable",
            "resource_exhausted",
            "overload",
            "timed out",
            "timeout",
            "connection reset",
            "temporarily unavailable",
            "ssl",
            "quota",
        )
    )
