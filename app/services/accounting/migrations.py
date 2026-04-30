"""Startup data migrations — idempotent, run on every boot."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.utils.logging import get_logger

logger = get_logger(__name__)

# The 13 Sevk Fişleri records Gemini misread as 2020/2023/2024 instead of 2026.
# source_doc_id == source_message_id for these single-doc records.
_SEVK_DATE_FIXES: list[tuple[str, str]] = [
    ("false_120363406782030766@g.us_3AD0FD9D008FC2C2293B", "2026-04-22"),  # 003975
    ("false_120363406782030766@g.us_3AFF8402C4723E12B52C", "2026-04-26"),  # 003982
    ("false_120363406782030766@g.us_3A1BC3D774ACD81A6E22", "2026-04-22"),  # 000312
    ("false_120363406782030766@g.us_3A84E045D5BFE2BD83DE", "2026-04-18"),  # 005733
    ("false_120363406782030766@g.us_3A5B630C541186B807F2", "2026-04-20"),  # 003972
    ("false_120363406782030766@g.us_3ADFDCA3188B3302FCC5", "2026-04-20"),  # 000304
    ("false_120363406782030766@g.us_3A67418FBC4EAF2C8A78", "2026-04-23"),  # 000313
    ("false_120363406782030766@g.us_3AF1E0F8DEC319EAF619", "2026-04-24"),  # 000317
    ("false_120363406782030766@g.us_3A387B7FB54EBCFD4B3A", "2026-04-24"),  # 003980
    ("false_120363406782030766@g.us_3A19F0572AD1297F4A71", "2026-04-24"),  # 006379
    ("false_120363406782030766@g.us_3A828EFDE88D8B9667DA", "2026-04-26"),  # 006380
    ("false_120363406782030766@g.us_3A2D2006EE2BA7296B8E", "2026-04-26"),  # 003981
    ("false_120363406782030766@g.us_3A6DBA7AF4BB8EBC6B7E", "2026-04-25"),  # 000318
]


def _patch_sqlite(db_path: str, fixes: list[tuple[str, str]]) -> int:
    """Patch document dates in a specific SQLite file. Returns number of records patched."""
    patched = 0
    try:
        conn = sqlite3.connect(db_path, timeout=10.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        try:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "documents" not in tables:
                return 0

            now = datetime.now(timezone.utc).isoformat()
            has_pending = "pending_projection_docs" in tables

            for msg_id, new_date in fixes:
                rows = conn.execute(
                    "SELECT source_doc_id, record_json FROM documents"
                    " WHERE source_message_id = ? OR source_doc_id = ?",
                    (msg_id, msg_id),
                ).fetchall()
                for row in rows:
                    doc_id = str(row[0])
                    record_data = json.loads(str(row[1]))
                    if record_data.get("document_date") == new_date:
                        continue  # already correct, skip
                    record_data["document_date"] = new_date
                    new_json = json.dumps(record_data, ensure_ascii=False, sort_keys=True)
                    conn.execute(
                        "UPDATE documents SET record_json = ?, updated_at = ? WHERE source_doc_id = ?",
                        (new_json, now, doc_id),
                    )
                    if has_pending:
                        conn.execute(
                            """INSERT INTO pending_projection_docs (source_doc_id, reason, enqueued_at)
                               VALUES (?, 'date_patch', ?)
                               ON CONFLICT(source_doc_id) DO UPDATE SET
                                   reason = 'date_patch',
                                   enqueued_at = excluded.enqueued_at""",
                            (doc_id, now),
                        )
                    patched += 1

            if patched:
                conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("Sevk date migration failed for %s: %s", db_path, exc)
    return patched


def run_sevk_date_fix(storage_dir: str) -> None:
    """Find every canonical-store SQLite on the volume and patch the 13 wrong-date records.

    Idempotent: records already holding the correct date are skipped.
    Also patches the legacy muhasebe service's database if it lives on the
    shared /data volume — that is the root cause of the nightly date reversions.
    """
    search_roots = [Path("/data"), Path("./storage"), Path(storage_dir)]
    seen: set[str] = set()
    total = 0

    for root in search_roots:
        try:
            for db_file in root.rglob("*.sqlite3"):
                key = str(db_file.resolve())
                if key in seen:
                    continue
                seen.add(key)
                n = _patch_sqlite(key, _SEVK_DATE_FIXES)
                if n:
                    logger.info("Sevk date migration: patched %d record(s) in %s", n, key)
                    total += n
        except Exception:
            continue

    if total:
        logger.info("Sevk date migration complete: %d total record(s) patched across all databases.", total)


def patch_all_sqlite(
    storage_dir: str,
    fixes: list[tuple[str, str]],
) -> list[dict]:
    """Patch arbitrary (message_id, new_date) fixes across every SQLite on the volume.

    Used by the /setup/bulk-patch-dates admin endpoint.
    Returns one result dict per database examined.
    """
    search_roots = [Path("/data"), Path("./storage"), Path(storage_dir)]
    seen: set[str] = set()
    results: list[dict] = []

    for root in search_roots:
        try:
            for db_file in root.rglob("*.sqlite3"):
                key = str(db_file.resolve())
                if key in seen:
                    continue
                seen.add(key)

                entry: dict = {"path": key, "total_documents": 0, "patched": 0, "error": None}
                try:
                    conn = sqlite3.connect(key, timeout=10.0)
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute("PRAGMA synchronous=NORMAL")
                    try:
                        tables = {
                            r[0]
                            for r in conn.execute(
                                "SELECT name FROM sqlite_master WHERE type='table'"
                            ).fetchall()
                        }
                        if "documents" not in tables:
                            entry["skipped"] = "no documents table"
                            results.append(entry)
                            continue

                        entry["total_documents"] = conn.execute(
                            "SELECT COUNT(*) FROM documents"
                        ).fetchone()[0]
                        has_pending = "pending_projection_docs" in tables
                        now = datetime.now(timezone.utc).isoformat()

                        for msg_id, new_date in fixes:
                            rows = conn.execute(
                                "SELECT source_doc_id, record_json FROM documents"
                                " WHERE source_message_id = ? OR source_doc_id = ?",
                                (msg_id, msg_id),
                            ).fetchall()
                            for row in rows:
                                doc_id = str(row[0])
                                record_data = json.loads(str(row[1]))
                                record_data["document_date"] = new_date
                                new_json = json.dumps(record_data, ensure_ascii=False, sort_keys=True)
                                conn.execute(
                                    "UPDATE documents SET record_json = ?, updated_at = ? WHERE source_doc_id = ?",
                                    (new_json, now, doc_id),
                                )
                                if has_pending:
                                    conn.execute(
                                        """INSERT INTO pending_projection_docs (source_doc_id, reason, enqueued_at)
                                           VALUES (?, 'date_patch', ?)
                                           ON CONFLICT(source_doc_id) DO UPDATE SET
                                               reason = 'date_patch',
                                               enqueued_at = excluded.enqueued_at""",
                                        (doc_id, now),
                                    )
                                entry["patched"] += 1

                        if entry["patched"]:
                            conn.commit()
                    finally:
                        conn.close()
                except Exception as exc:
                    entry["error"] = str(exc)

                results.append(entry)
        except Exception:
            continue

    return results
