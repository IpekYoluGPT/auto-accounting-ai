"""
Pending Drive upload queue helpers for Google Sheets integration.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

_pending_drive_worker_thread: threading.Thread | None = None
_pending_drive_worker_lock = threading.Lock()


def queue_pending_document_upload(
    sheets,
    *,
    file_bytes: bytes,
    filename: str,
    mime_type: str,
    targets: list[dict[str, str | int]],
    source_doc_ids: list[str] | None = None,
    source_message_id: str | None = None,
) -> None:
    normalized_targets = [sheets._normalize_drive_link_target(target) for target in (targets or [])]
    normalized_doc_ids = [str(source_doc_id or "").strip() for source_doc_id in (source_doc_ids or []) if str(source_doc_id or "").strip()]
    if not normalized_targets and not normalized_doc_ids:
        return

    normalized_source_message_id = sheets._root_source_message_id(source_message_id)
    sheets.storage_guard.prune_stale_transient_storage()
    if sheets.storage_guard.should_stop_payload_writes():
        sheets.logger.warning(
            "Skipping pending Drive upload queue for message id=%s because disk pressure forbids transient writes.",
            normalized_source_message_id or "?",
        )
        return

    with sheets._pending_drive_uploads_lock:
        items = sheets._load_pending_drive_uploads()
        if source_message_id:
            for existing in items:
                if (
                    str(existing.get("source_message_id") or "") == normalized_source_message_id
                    and str(existing.get("filename") or "") == filename
                    and str(existing.get("mime_type") or "") == mime_type
                ):
                    known_targets = {
                        sheets._drive_link_target_key(target)
                        for target in existing.get("targets", [])
                    }
                    for target in normalized_targets:
                        target_key = sheets._drive_link_target_key(target)
                        if target_key not in known_targets:
                            existing.setdefault("targets", []).append(target)
                            known_targets.add(target_key)
                    known_doc_ids = set(str(source_doc_id or "").strip() for source_doc_id in existing.get("source_doc_ids", []))
                    for source_doc_id in normalized_doc_ids:
                        if source_doc_id not in known_doc_ids:
                            existing.setdefault("source_doc_ids", []).append(source_doc_id)
                            known_doc_ids.add(source_doc_id)
                    sheets._save_pending_drive_uploads(items)
                    sheets.start_pending_drive_upload_worker()
                    return

    pending_id = uuid4().hex
    payload_path = sheets._pending_drive_uploads_dir() / f"{pending_id}.bin"
    payload_path.write_bytes(file_bytes)
    item = {
        "id": pending_id,
        "filename": filename,
        "mime_type": mime_type,
        "payload_path": str(payload_path),
        "targets": normalized_targets,
        "source_doc_ids": normalized_doc_ids,
        "source_message_id": normalized_source_message_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "attempts": 0,
    }

    with sheets._pending_drive_uploads_lock:
        items = sheets._load_pending_drive_uploads()
        items.append(item)
        sheets._save_pending_drive_uploads(items)

    sheets.start_pending_drive_upload_worker()


def process_pending_document_uploads(sheets, *, max_items: int | None = None) -> int:
    processed = 0

    while True:
        if max_items is not None and processed >= max_items:
            break

        with sheets._pending_drive_uploads_lock:
            items = sheets._load_pending_drive_uploads()
            if not items:
                break
            item = dict(items[0])

        payload_path = Path(str(item.get("payload_path", "")))
        if not payload_path.exists():
            with sheets._pending_drive_uploads_lock:
                items = [
                    existing for existing in sheets._load_pending_drive_uploads()
                    if str(existing.get("id") or "") != str(item.get("id") or "")
                ]
                sheets._save_pending_drive_uploads(items)
            continue

        drive_link = str(item.get("drive_link") or "").strip()
        try:
            if not drive_link:
                drive_link = sheets.upload_document(
                    payload_path.read_bytes(),
                    filename=str(item.get("filename") or payload_path.name),
                    mime_type=str(item.get("mime_type") or "application/octet-stream"),
                )
            if not drive_link:
                raise RuntimeError("Drive upload returned no link.")

            for source_doc_id in item.get("source_doc_ids", []):
                sheets._canonical_store.set_drive_link(str(source_doc_id), drive_link)
            for target in item.get("targets", []):
                sheets._write_drive_link_to_target(target, drive_link)

            with sheets._pending_drive_uploads_lock:
                items = [
                    existing for existing in sheets._load_pending_drive_uploads()
                    if str(existing.get("id") or "") != str(item.get("id") or "")
                ]
                sheets._save_pending_drive_uploads(items)
            payload_path.unlink(missing_ok=True)
            processed += 1
        except Exception as exc:
            with sheets._pending_drive_uploads_lock:
                items = sheets._load_pending_drive_uploads()
                for existing in items:
                    if str(existing.get("id") or "") != str(item.get("id") or ""):
                        continue
                    existing["attempts"] = int(existing.get("attempts", 0)) + 1
                    existing["last_error"] = str(exc)
                    if drive_link:
                        existing["drive_link"] = drive_link
                    break
                sheets._save_pending_drive_uploads(items)
            sheets.logger.warning(
                "Pending Drive upload retry failed for message id=%s: %s",
                item.get("source_message_id") or "?",
                exc,
            )
            break

    return processed


def _pending_drive_upload_worker(sheets) -> None:
    try:
        time.sleep(sheets._PENDING_DRIVE_WORKER_DELAY_SECONDS)
        process_pending_document_uploads(sheets)
    except Exception as exc:
        sheets.logger.warning("Pending Drive upload worker stopped after error: %s", exc)


def start_pending_drive_upload_worker(sheets) -> None:
    if not sheets.current_pipeline_context().is_production:
        return

    global _pending_drive_worker_thread

    with _pending_drive_worker_lock:
        if _pending_drive_worker_thread is not None and _pending_drive_worker_thread.is_alive():
            return

        _pending_drive_worker_thread = threading.Thread(
            target=_pending_drive_upload_worker,
            args=(sheets,),
            name="google-sheets-pending-drive-upload",
            daemon=True,
        )
        _pending_drive_worker_thread.start()
