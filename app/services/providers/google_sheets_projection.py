"""
Projection and visible-sheet queue helpers for Google Sheets integration.
"""

from __future__ import annotations

import threading
from uuid import uuid4

_pending_sheet_worker_thread: threading.Thread | None = None
_pending_sheet_worker_stop_event: threading.Event | None = None
_pending_sheet_worker_lock = threading.Lock()


def queue_status(sheets) -> dict[str, int]:
    status: dict[str, int | str] = {
        "pending_sheet_appends": sheets._canonical_store.pending_projection_count(),
        "pending_projection_rows": sheets._canonical_store.pending_projection_count(),
        "pending_drive_uploads": len(sheets._load_pending_drive_uploads()),
        "pending_drive_link_backfills": len(sheets._load_pending_drive_uploads()),
        "sheet_flush_count": int(sheets._canonical_store.get_state("sheet_flush_count") or "0"),
        "sheet_write_request_count": int(sheets._canonical_store.get_state("sheet_write_request_count") or "0"),
        "last_visible_flush_at": sheets._canonical_store.last_visible_flush_at() or "",
        "last_override_sync_at": sheets._canonical_store.last_override_sync_at() or "",
    }
    try:
        from app.services.accounting import inbound_queue

        status.update(inbound_queue.queue_status())
    except Exception:
        status.setdefault("pending_inbound_jobs", 0)
        status.setdefault("retry_waiting_inbound_jobs", 0)
        status.setdefault("failed_inbound_jobs", 0)
        status.setdefault("inbound_payload_storage_bytes", 0)

    try:
        status.update(sheets.storage_guard.storage_snapshot().as_dict())
    except Exception:
        status.setdefault("disk_total_bytes", 0)
        status.setdefault("disk_used_bytes", 0)
        status.setdefault("disk_free_bytes", 0)
        status.setdefault("total_managed_storage_bytes", 0)
        status.setdefault("disk_pressure_state", "unknown")
    return status  # type: ignore[return-value]


def has_pending_visible_appends(sheets, *, message_id: str, chat_id: str | None = None, platform: str | None = None) -> bool:
    if sheets._canonical_store.feedback_pending_for_message(message_id=message_id, chat_id=chat_id, platform=platform):
        return True

    normalized_message_id = (message_id or "").strip()
    if not normalized_message_id:
        return False

    with sheets._pending_sheet_appends_lock:
        items = sheets._load_pending_sheet_appends()
    for item in items:
        if not bool(item.get("is_visible_tab")):
            continue
        if str(item.get("feedback_message_id") or "").strip() != normalized_message_id:
            continue
        if chat_id is not None and str(item.get("feedback_chat_id") or "").strip() != chat_id:
            continue
        if platform is not None and str(item.get("feedback_platform") or "").strip() != platform:
            continue
        return True
    return False


def process_pending_sheet_appends(sheets, *, max_items: int | None = None) -> int:
    sheets._migrate_legacy_pending_sheet_appends_to_canonical_store()
    pending_doc_ids = sheets._canonical_store.pending_projection_doc_ids(limit=max_items)
    if not pending_doc_ids:
        return 0

    client = sheets._get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable for projection flush.")

    rows_by_tab, hashes_by_tab = sheets._build_visible_projection_snapshot()
    try:
        with sheets._lock:
            sh = sheets._get_or_create_spreadsheet(client)
            request_count = sheets._write_visible_projection_rows(sh, rows_by_tab)
    except Exception as exc:
        if sheets._is_rate_limit_exception(exc):
            sheets.logger.warning(
                "Projection flush deferred by Google Sheets rate limiting for %d document(s): %s",
                len(pending_doc_ids),
                exc,
            )
            return 0

        with sheets._lock:
            sh = sheets._get_or_create_spreadsheet(client)
            sheets._ensure_projection_workbook_layout(sh)
            request_count = sheets._write_visible_projection_rows(sh, rows_by_tab)

    for tab_name, hashes in hashes_by_tab.items():
        sheets._canonical_store.update_last_sheet_hashes(tab_name, hashes)
    sheets._canonical_store.record_projection_flush(request_count=request_count, processed_doc_ids=pending_doc_ids)
    sheets._dispatch_projection_success_feedback(pending_doc_ids)
    sheets.logger.info(
        "Projection flush completed for %d document(s) with %d Sheets write request(s).",
        len(pending_doc_ids),
        request_count,
    )
    return len(pending_doc_ids)


def _projection_worker_loop(sheets, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            processed = process_pending_sheet_appends(sheets)
            if processed == 0 and sheets._should_sync_visible_overrides():
                client = sheets._get_client()
                if client is not None:
                    with sheets._lock:
                        sh = sheets._get_or_create_spreadsheet(client)
                        sheets._ensure_projection_workbook_layout(sh)
                        sheets._sync_visible_overrides_from_sheet(sh)
        except Exception as exc:
            sheets.logger.warning("Projection worker iteration failed: %s", exc, exc_info=True)

        if stop_event.wait(sheets._PROJECTION_WORKER_POLL_SECONDS):
            break


def start_pending_sheet_append_worker(sheets) -> None:
    if not sheets.current_pipeline_context().is_production:
        return

    global _pending_sheet_worker_thread, _pending_sheet_worker_stop_event
    with _pending_sheet_worker_lock:
        if _pending_sheet_worker_thread is not None and _pending_sheet_worker_thread.is_alive():
            return
        _pending_sheet_worker_stop_event = threading.Event()
        _pending_sheet_worker_thread = threading.Thread(
            target=_projection_worker_loop,
            args=(sheets, _pending_sheet_worker_stop_event),
            name="google-sheets-projection-worker",
            daemon=True,
        )
        _pending_sheet_worker_thread.start()


def stop_pending_sheet_append_worker(*, timeout_seconds: float = 1.0) -> None:
    global _pending_sheet_worker_thread, _pending_sheet_worker_stop_event

    with _pending_sheet_worker_lock:
        thread = _pending_sheet_worker_thread
        stop_event = _pending_sheet_worker_stop_event
        if thread is None:
            _pending_sheet_worker_stop_event = None
            return
        if stop_event is not None:
            stop_event.set()

    if thread.is_alive():
        thread.join(timeout=timeout_seconds)

    with _pending_sheet_worker_lock:
        if _pending_sheet_worker_thread is thread and not thread.is_alive():
            _pending_sheet_worker_thread = None
            _pending_sheet_worker_stop_event = None


def append_record(
    sheets,
    record,
    category,
    is_return: bool = False,
    drive_link=None,
    *,
    pending_document_bytes: bytes | None = None,
    pending_document_filename: str | None = None,
    pending_document_mime_type: str | None = None,
    feedback_target: dict[str, str] | None = None,
) -> list[dict[str, str | int]]:
    try:
        normalized_category = sheets.DocumentCategory.FATURA if category == sheets.DocumentCategory.IADE else category
        source_doc_id = str(record.source_message_id or uuid4().hex).strip()
        sheets._canonical_store.upsert_document(
            source_doc_id=source_doc_id,
            record=record,
            category=normalized_category,
            return_source_category=category if category == sheets.DocumentCategory.IADE else None,
            drive_link=drive_link,
            feedback_target=feedback_target,
            projection_reason="append_record",
        )
        if not drive_link and pending_document_bytes:
            sheets.queue_pending_document_upload(
                file_bytes=pending_document_bytes,
                filename=pending_document_filename or (record.source_filename or f"{source_doc_id}.bin"),
                mime_type=pending_document_mime_type or "application/octet-stream",
                targets=[],
                source_doc_ids=[source_doc_id],
                source_message_id=record.source_message_id or source_doc_id,
            )
        sheets.start_pending_sheet_append_worker()
        return [{"source_doc_id": source_doc_id, "category": normalized_category.value}]
    except Exception as exc:
        sheets.logger.error(
            "Google Sheets canonical append queueing failed for category=%s message_id=%s: %s",
            category,
            record.source_message_id,
            exc,
            exc_info=True,
        )
        return []
