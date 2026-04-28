"""SQLite-backed canonical document and projection state store."""

from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from app.config import settings
from app.models.schemas import BillRecord, DocumentCategory
from app.services.accounting.pipeline_context import namespace_storage_root

_LOCK = threading.Lock()


def _storage_root() -> Path:
    path = namespace_storage_root(settings.storage_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _state_dir() -> Path:
    path = _storage_root() / "state"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _db_path() -> Path:
    return _state_dir() / "canonical_store.sqlite3"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS documents (
            source_doc_id TEXT PRIMARY KEY,
            category TEXT NOT NULL,
            return_source_category TEXT NOT NULL DEFAULT '',
            source_message_id TEXT NOT NULL DEFAULT '',
            drive_link TEXT NOT NULL DEFAULT '',
            record_json TEXT NOT NULL,
            feedback_platform TEXT NOT NULL DEFAULT '',
            feedback_chat_id TEXT NOT NULL DEFAULT '',
            feedback_recipient_type TEXT NOT NULL DEFAULT '',
            feedback_message_id TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pending_projection_docs (
            source_doc_id TEXT PRIMARY KEY,
            reason TEXT NOT NULL DEFAULT '',
            enqueued_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS overrides (
            tab_name TEXT NOT NULL,
            source_doc_id TEXT NOT NULL,
            overrides_json TEXT NOT NULL DEFAULT '{}',
            last_sheet_hash TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            PRIMARY KEY (tab_name, source_doc_id)
        );

        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )


@dataclass(frozen=True)
class StoredDocument:
    source_doc_id: str
    category: DocumentCategory
    return_source_category: DocumentCategory | None
    source_message_id: str | None
    record: BillRecord
    drive_link: str | None
    feedback_platform: str | None
    feedback_chat_id: str | None
    feedback_recipient_type: str | None
    feedback_message_id: str | None
    created_at: str
    updated_at: str


def _row_to_document(row: sqlite3.Row) -> StoredDocument:
    return_source_category_raw = str(row["return_source_category"] or "").strip()
    return StoredDocument(
        source_doc_id=str(row["source_doc_id"]),
        category=DocumentCategory(str(row["category"])),
        return_source_category=DocumentCategory(return_source_category_raw) if return_source_category_raw else None,
        source_message_id=str(row["source_message_id"] or "").strip() or None,
        drive_link=str(row["drive_link"] or "").strip() or None,
        record=BillRecord.model_validate(json.loads(str(row["record_json"]))),
        feedback_platform=str(row["feedback_platform"] or "").strip() or None,
        feedback_chat_id=str(row["feedback_chat_id"] or "").strip() or None,
        feedback_recipient_type=str(row["feedback_recipient_type"] or "").strip() or None,
        feedback_message_id=str(row["feedback_message_id"] or "").strip() or None,
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _serialize_feedback(feedback_target: dict[str, str] | None) -> dict[str, str]:
    feedback = feedback_target or {}
    return {
        "platform": str(feedback.get("platform") or ""),
        "chat_id": str(feedback.get("chat_id") or ""),
        "recipient_type": str(feedback.get("recipient_type") or ""),
        "message_id": str(feedback.get("message_id") or ""),
    }


def upsert_document(
    *,
    source_doc_id: str,
    record: BillRecord,
    category: DocumentCategory,
    return_source_category: DocumentCategory | None = None,
    drive_link: str | None = None,
    feedback_target: dict[str, str] | None = None,
    projection_reason: str = "document_upsert",
) -> None:
    source_doc_id = source_doc_id.strip()
    if not source_doc_id:
        raise ValueError("source_doc_id is required")

    now = _now_iso()
    feedback = _serialize_feedback(feedback_target)
    record_json = json.dumps(record.model_dump(mode="json"), ensure_ascii=False, sort_keys=True)

    with _LOCK:
        conn = _connect()
        try:
            existing = conn.execute(
                "SELECT created_at, drive_link FROM documents WHERE source_doc_id = ?",
                (source_doc_id,),
            ).fetchone()
            created_at = str(existing["created_at"]) if existing is not None else now
            effective_drive_link = (drive_link or "").strip() or (str(existing["drive_link"]) if existing is not None else "")

            conn.execute(
                """
                INSERT INTO documents (
                    source_doc_id, category, return_source_category, source_message_id, drive_link,
                    record_json, feedback_platform, feedback_chat_id, feedback_recipient_type, feedback_message_id,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_doc_id) DO UPDATE SET
                    category = excluded.category,
                    return_source_category = excluded.return_source_category,
                    source_message_id = excluded.source_message_id,
                    drive_link = CASE
                        WHEN excluded.drive_link != '' THEN excluded.drive_link
                        ELSE documents.drive_link
                    END,
                    record_json = excluded.record_json,
                    feedback_platform = excluded.feedback_platform,
                    feedback_chat_id = excluded.feedback_chat_id,
                    feedback_recipient_type = excluded.feedback_recipient_type,
                    feedback_message_id = excluded.feedback_message_id,
                    updated_at = excluded.updated_at
                """,
                (
                    source_doc_id,
                    category.value,
                    return_source_category.value if return_source_category else "",
                    str(record.source_message_id or ""),
                    effective_drive_link,
                    record_json,
                    feedback["platform"],
                    feedback["chat_id"],
                    feedback["recipient_type"],
                    feedback["message_id"],
                    created_at,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO pending_projection_docs (source_doc_id, reason, enqueued_at)
                VALUES (?, ?, ?)
                ON CONFLICT(source_doc_id) DO UPDATE SET
                    reason = excluded.reason,
                    enqueued_at = excluded.enqueued_at
                """,
                (source_doc_id, projection_reason, now),
            )
            conn.commit()
        finally:
            conn.close()


def mark_projection_dirty(source_doc_ids: Iterable[str], *, reason: str = "projection_dirty") -> int:
    normalized = [str(source_doc_id or "").strip() for source_doc_id in source_doc_ids]
    normalized = [source_doc_id for source_doc_id in normalized if source_doc_id]
    if not normalized:
        return 0

    now = _now_iso()
    with _LOCK:
        conn = _connect()
        try:
            conn.executemany(
                """
                INSERT INTO pending_projection_docs (source_doc_id, reason, enqueued_at)
                VALUES (?, ?, ?)
                ON CONFLICT(source_doc_id) DO UPDATE SET
                    reason = excluded.reason,
                    enqueued_at = excluded.enqueued_at
                """,
                [(source_doc_id, reason, now) for source_doc_id in normalized],
            )
            conn.commit()
            return len(normalized)
        finally:
            conn.close()


def pending_projection_doc_ids(*, limit: int | None = None) -> list[str]:
    sql = "SELECT source_doc_id FROM pending_projection_docs ORDER BY enqueued_at ASC, source_doc_id ASC"
    params: tuple[object, ...] = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (max(int(limit), 1),)

    with _LOCK:
        conn = _connect()
        try:
            rows = conn.execute(sql, params).fetchall()
            return [str(row["source_doc_id"]) for row in rows]
        finally:
            conn.close()


def clear_pending_projection_docs(source_doc_ids: Iterable[str]) -> int:
    normalized = [str(source_doc_id or "").strip() for source_doc_id in source_doc_ids]
    normalized = [source_doc_id for source_doc_id in normalized if source_doc_id]
    if not normalized:
        return 0

    placeholders = ", ".join(["?"] * len(normalized))
    with _LOCK:
        conn = _connect()
        try:
            cursor = conn.execute(
                f"DELETE FROM pending_projection_docs WHERE source_doc_id IN ({placeholders})",
                tuple(normalized),
            )
            conn.commit()
            return int(cursor.rowcount or 0)
        finally:
            conn.close()


def pending_projection_count() -> int:
    with _LOCK:
        conn = _connect()
        try:
            row = conn.execute("SELECT COUNT(*) AS count FROM pending_projection_docs").fetchone()
            return int(row["count"] if row is not None else 0)
        finally:
            conn.close()


def list_documents() -> list[StoredDocument]:
    with _LOCK:
        conn = _connect()
        try:
            rows = conn.execute("SELECT * FROM documents").fetchall()
        finally:
            conn.close()

    documents = [_row_to_document(row) for row in rows]
    documents.sort(
        key=lambda item: (
            str(item.record.document_date or ""),
            item.created_at,
            item.source_doc_id,
        )
    )
    return documents


def set_drive_link(source_doc_id: str, drive_link: str | None) -> None:
    normalized_doc_id = str(source_doc_id or "").strip()
    if not normalized_doc_id:
        return
    normalized_link = str(drive_link or "").strip()
    now = _now_iso()

    with _LOCK:
        conn = _connect()
        try:
            conn.execute(
                "UPDATE documents SET drive_link = ?, updated_at = ? WHERE source_doc_id = ?",
                (normalized_link, now, normalized_doc_id),
            )
            conn.execute(
                """
                INSERT INTO pending_projection_docs (source_doc_id, reason, enqueued_at)
                VALUES (?, ?, ?)
                ON CONFLICT(source_doc_id) DO UPDATE SET
                    reason = excluded.reason,
                    enqueued_at = excluded.enqueued_at
                """,
                (normalized_doc_id, "drive_link_updated", now),
            )
            conn.commit()
        finally:
            conn.close()


def feedback_pending_for_message(
    *,
    message_id: str,
    chat_id: str | None = None,
    platform: str | None = None,
) -> bool:
    normalized_message_id = str(message_id or "").strip()
    if not normalized_message_id:
        return False

    clauses = ["d.feedback_message_id = ?"]
    params: list[object] = [normalized_message_id]
    if chat_id is not None:
        clauses.append("d.feedback_chat_id = ?")
        params.append(str(chat_id))
    if platform is not None:
        clauses.append("d.feedback_platform = ?")
        params.append(str(platform))

    with _LOCK:
        conn = _connect()
        try:
            row = conn.execute(
                f"""
                SELECT 1
                FROM pending_projection_docs q
                JOIN documents d ON d.source_doc_id = q.source_doc_id
                WHERE {' AND '.join(clauses)}
                LIMIT 1
                """,
                tuple(params),
            ).fetchone()
            return row is not None
        finally:
            conn.close()


def feedback_targets_for_docs(source_doc_ids: Iterable[str]) -> list[dict[str, str]]:
    normalized = [str(source_doc_id or "").strip() for source_doc_id in source_doc_ids]
    normalized = [source_doc_id for source_doc_id in normalized if source_doc_id]
    if not normalized:
        return []
    placeholders = ", ".join(["?"] * len(normalized))

    with _LOCK:
        conn = _connect()
        try:
            rows = conn.execute(
                f"""
                SELECT DISTINCT feedback_platform, feedback_chat_id, feedback_recipient_type, feedback_message_id
                FROM documents
                WHERE source_doc_id IN ({placeholders})
                """,
                tuple(normalized),
            ).fetchall()
        finally:
            conn.close()

    result: list[dict[str, str]] = []
    for row in rows:
        item = {
            "platform": str(row["feedback_platform"] or ""),
            "chat_id": str(row["feedback_chat_id"] or ""),
            "recipient_type": str(row["feedback_recipient_type"] or ""),
            "message_id": str(row["feedback_message_id"] or ""),
        }
        if item["platform"] and item["message_id"]:
            result.append(item)
    return result


def override_map_for_tab(tab_name: str) -> dict[str, dict[str, object]]:
    with _LOCK:
        conn = _connect()
        try:
            rows = conn.execute(
                "SELECT source_doc_id, overrides_json FROM overrides WHERE tab_name = ?",
                (tab_name,),
            ).fetchall()
        finally:
            conn.close()
    result: dict[str, dict[str, object]] = {}
    for row in rows:
        try:
            result[str(row["source_doc_id"])] = dict(json.loads(str(row["overrides_json"])))
        except Exception:
            continue
    return result


def last_sheet_hash_map(tab_name: str) -> dict[str, str]:
    with _LOCK:
        conn = _connect()
        try:
            rows = conn.execute(
                "SELECT source_doc_id, last_sheet_hash FROM overrides WHERE tab_name = ?",
                (tab_name,),
            ).fetchall()
        finally:
            conn.close()
    return {
        str(row["source_doc_id"]): str(row["last_sheet_hash"] or "")
        for row in rows
        if str(row["source_doc_id"] or "").strip()
    }


def upsert_override(
    *,
    tab_name: str,
    source_doc_id: str,
    overrides: dict[str, object] | None,
    last_sheet_hash: str,
) -> None:
    normalized_doc_id = str(source_doc_id or "").strip()
    if not normalized_doc_id:
        return
    now = _now_iso()
    overrides_json = json.dumps(overrides or {}, ensure_ascii=False, sort_keys=True)
    with _LOCK:
        conn = _connect()
        try:
            conn.execute(
                """
                INSERT INTO overrides (tab_name, source_doc_id, overrides_json, last_sheet_hash, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(tab_name, source_doc_id) DO UPDATE SET
                    overrides_json = excluded.overrides_json,
                    last_sheet_hash = excluded.last_sheet_hash,
                    updated_at = excluded.updated_at
                """,
                (tab_name, normalized_doc_id, overrides_json, last_sheet_hash, now),
            )
            conn.commit()
        finally:
            conn.close()


def update_last_sheet_hashes(tab_name: str, hashes: dict[str, str]) -> None:
    if not hashes:
        return
    now = _now_iso()
    with _LOCK:
        conn = _connect()
        try:
            for source_doc_id, last_sheet_hash in hashes.items():
                normalized_doc_id = str(source_doc_id or "").strip()
                if not normalized_doc_id:
                    continue
                conn.execute(
                    """
                    INSERT INTO overrides (tab_name, source_doc_id, overrides_json, last_sheet_hash, updated_at)
                    VALUES (?, ?, '{}', ?, ?)
                    ON CONFLICT(tab_name, source_doc_id) DO UPDATE SET
                        last_sheet_hash = excluded.last_sheet_hash,
                        updated_at = excluded.updated_at
                    """,
                    (tab_name, normalized_doc_id, str(last_sheet_hash or ""), now),
                )
            conn.commit()
        finally:
            conn.close()


def get_state(key: str) -> str | None:
    with _LOCK:
        conn = _connect()
        try:
            row = conn.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
            return None if row is None else str(row["value"])
        finally:
            conn.close()


def set_state(key: str, value: str) -> None:
    with _LOCK:
        conn = _connect()
        try:
            conn.execute(
                """
                INSERT INTO state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )
            conn.commit()
        finally:
            conn.close()


def patch_document_date(source_doc_id: str, new_date: str) -> bool:
    """Update document_date inside record_json for a stored document and queue re-projection.

    Returns True if the document was found and patched, False if not found.
    """
    normalized_doc_id = str(source_doc_id or "").strip()
    if not normalized_doc_id:
        return False

    now = _now_iso()
    with _LOCK:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT record_json FROM documents WHERE source_doc_id = ?",
                (normalized_doc_id,),
            ).fetchone()
            if row is None:
                return False

            record_data = json.loads(str(row["record_json"]))
            record_data["document_date"] = new_date
            new_json = json.dumps(record_data, ensure_ascii=False, sort_keys=True)

            conn.execute(
                "UPDATE documents SET record_json = ?, updated_at = ? WHERE source_doc_id = ?",
                (new_json, now, normalized_doc_id),
            )
            conn.execute(
                """
                INSERT INTO pending_projection_docs (source_doc_id, reason, enqueued_at)
                VALUES (?, 'date_patch', ?)
                ON CONFLICT(source_doc_id) DO UPDATE SET
                    reason = 'date_patch',
                    enqueued_at = excluded.enqueued_at
                """,
                (normalized_doc_id, now),
            )
            conn.commit()
            return True
        finally:
            conn.close()


def patch_document_date_by_message_id(source_message_id: str, new_date: str) -> dict[str, object]:
    """Update document_date for all documents whose source_message_id matches.

    Returns a summary dict with matched and patched counts.
    """
    normalized_msg_id = str(source_message_id or "").strip()
    if not normalized_msg_id:
        return {"source_message_id": source_message_id, "docs_found": 0, "docs_patched": 0, "doc_ids": []}

    now = _now_iso()
    with _LOCK:
        conn = _connect()
        try:
            rows = conn.execute(
                "SELECT source_doc_id, record_json FROM documents WHERE source_message_id = ?",
                (normalized_msg_id,),
            ).fetchall()

            if not rows:
                return {
                    "source_message_id": source_message_id,
                    "docs_found": 0,
                    "docs_patched": 0,
                    "doc_ids": [],
                }

            patched_ids: list[str] = []
            for row in rows:
                doc_id = str(row["source_doc_id"])
                record_data = json.loads(str(row["record_json"]))
                record_data["document_date"] = new_date
                new_json = json.dumps(record_data, ensure_ascii=False, sort_keys=True)

                conn.execute(
                    "UPDATE documents SET record_json = ?, updated_at = ? WHERE source_doc_id = ?",
                    (new_json, now, doc_id),
                )
                conn.execute(
                    """
                    INSERT INTO pending_projection_docs (source_doc_id, reason, enqueued_at)
                    VALUES (?, 'date_patch', ?)
                    ON CONFLICT(source_doc_id) DO UPDATE SET
                        reason = 'date_patch',
                        enqueued_at = excluded.enqueued_at
                    """,
                    (doc_id, now),
                )
                patched_ids.append(doc_id)

            conn.commit()
            return {
                "source_message_id": source_message_id,
                "docs_found": len(rows),
                "docs_patched": len(patched_ids),
                "doc_ids": patched_ids,
            }
        finally:
            conn.close()


def record_projection_flush(*, request_count: int, processed_doc_ids: Iterable[str]) -> None:
    processed = [str(source_doc_id or "").strip() for source_doc_id in processed_doc_ids]
    processed = [source_doc_id for source_doc_id in processed if source_doc_id]
    if processed:
        clear_pending_projection_docs(processed)

    flush_count = int(get_state("sheet_flush_count") or "0") + 1
    write_count = int(get_state("sheet_write_request_count") or "0") + max(int(request_count), 0)
    now = _now_iso()
    set_state("sheet_flush_count", str(flush_count))
    set_state("sheet_write_request_count", str(write_count))
    set_state("last_visible_flush_at", now)


def touch_override_sync() -> None:
    set_state("last_override_sync_at", _now_iso())


def last_visible_flush_at() -> str | None:
    return get_state("last_visible_flush_at")


def last_override_sync_at() -> str | None:
    return get_state("last_override_sync_at")
