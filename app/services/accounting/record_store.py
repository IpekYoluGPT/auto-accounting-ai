"""
Persistent storage helpers for exported rows and processed-message tracking.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import threading
import unicodedata
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Mapping

from app.config import settings
from app.models.schemas import BillRecord
from app.services.accounting.exporter import COLUMN_MAP, TURKISH_HEADERS, record_to_row
from app.services.accounting.pipeline_context import PipelineContext, namespace_storage_root, pipeline_context_scope
from app.utils.logging import get_logger

logger = get_logger(__name__)

_PERSIST_LOCK = threading.Lock()
_MESSAGE_CLAIM_TTL = timedelta(minutes=15)
_WARNING_THROTTLE_TTL = timedelta(minutes=3)
_WARNING_STATE_RETENTION = timedelta(days=1)

if os.name == "nt":
    import msvcrt
else:
    import fcntl


def _storage_root() -> Path:
    path = namespace_storage_root(settings.storage_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _exports_dir() -> Path:
    path = _storage_root() / "exports"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _state_dir() -> Path:
    path = _storage_root() / "state"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _registry_path() -> Path:
    return _state_dir() / "processed_message_ids.txt"


def _inflight_registry_path() -> Path:
    return _state_dir() / "inflight_message_ids.json"


def _warning_registry_path() -> Path:
    return _state_dir() / "warning_throttle.json"


def _content_fingerprint_registry_path() -> Path:
    return _state_dir() / "content_fingerprints.txt"


def _lock_path() -> Path:
    return _state_dir() / ".record_store.lock"


@contextmanager
def _interprocess_lock() -> Iterator[None]:
    lock_path = _lock_path()
    with lock_path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)

        if os.name == "nt":
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)

        try:
            yield
        finally:
            handle.flush()
            os.fsync(handle.fileno())
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _load_processed_ids_unlocked() -> set[str]:
    filepath = _registry_path()
    if not filepath.exists():
        return set()
    return {
        line.strip()
        for line in filepath.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def _load_inflight_unlocked() -> dict[str, str]:
    filepath = _inflight_registry_path()
    if not filepath.exists():
        return {}

    try:
        payload = json.loads(filepath.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("In-flight registry is invalid JSON; resetting it.")
        return {}

    if not isinstance(payload, dict):
        logger.warning("In-flight registry has unexpected shape; resetting it.")
        return {}

    return {
        str(message_id): str(claimed_at)
        for message_id, claimed_at in payload.items()
        if str(message_id).strip() and str(claimed_at).strip()
    }


def _write_inflight_unlocked(inflight: dict[str, str]) -> None:
    _inflight_registry_path().write_text(
        json.dumps(inflight, ensure_ascii=True, sort_keys=True),
        encoding="utf-8",
    )


def _load_warning_state_unlocked() -> dict[str, str]:
    filepath = _warning_registry_path()
    if not filepath.exists():
        return {}

    try:
        payload = json.loads(filepath.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("Warning-throttle registry is invalid JSON; resetting it.")
        return {}

    if not isinstance(payload, dict):
        logger.warning("Warning-throttle registry has unexpected shape; resetting it.")
        return {}

    return {
        str(bucket): str(sent_at)
        for bucket, sent_at in payload.items()
        if str(bucket).strip() and str(sent_at).strip()
    }


def _load_content_fingerprints_unlocked() -> set[str]:
    filepath = _content_fingerprint_registry_path()
    if not filepath.exists():
        return set()

    return {
        line.strip()
        for line in filepath.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def _append_content_fingerprints_unlocked(fingerprints: set[str]) -> None:
    if not fingerprints:
        return

    with _content_fingerprint_registry_path().open("a", encoding="utf-8") as handle:
        for fingerprint in sorted(fingerprints):
            handle.write(f"{fingerprint}\n")


def _write_warning_state_unlocked(warning_state: dict[str, str]) -> None:
    _warning_registry_path().write_text(
        json.dumps(warning_state, ensure_ascii=True, sort_keys=True),
        encoding="utf-8",
    )


def _append_processed_id_unlocked(message_id: str) -> None:
    with _registry_path().open("a", encoding="utf-8") as handle:
        handle.write(f"{message_id}\n")


def _purge_stale_inflight_unlocked(inflight: dict[str, str]) -> dict[str, str]:
    now = datetime.now(timezone.utc)
    fresh: dict[str, str] = {}
    stale_ids: list[str] = []

    for message_id, claimed_at_raw in inflight.items():
        try:
            claimed_at = datetime.fromisoformat(claimed_at_raw)
        except ValueError:
            stale_ids.append(message_id)
            continue

        if claimed_at.tzinfo is None:
            claimed_at = claimed_at.replace(tzinfo=timezone.utc)

        if now - claimed_at <= _MESSAGE_CLAIM_TTL:
            fresh[message_id] = claimed_at.isoformat()
        else:
            stale_ids.append(message_id)

    if stale_ids:
        logger.warning("Releasing %d stale in-flight message claims.", len(stale_ids))

    return fresh


def _purge_stale_warnings_unlocked(warning_state: dict[str, str]) -> dict[str, str]:
    now = datetime.now(timezone.utc)
    fresh: dict[str, str] = {}

    for bucket, sent_at_raw in warning_state.items():
        try:
            sent_at = datetime.fromisoformat(sent_at_raw)
        except ValueError:
            continue

        if sent_at.tzinfo is None:
            sent_at = sent_at.replace(tzinfo=timezone.utc)

        if now - sent_at <= _WARNING_STATE_RETENTION:
            fresh[bucket] = sent_at.isoformat()

    return fresh


def _compact_text(value: object | None) -> str | None:
    if value is None:
        return None

    normalized = unicodedata.normalize("NFKC", str(value)).strip()
    if not normalized:
        return None

    normalized = " ".join(normalized.split())
    compact = "".join(ch for ch in normalized.casefold() if ch.isalnum())
    return compact or None


def _free_text(value: object | None) -> str | None:
    if value is None:
        return None

    normalized = unicodedata.normalize("NFKC", str(value)).strip()
    if not normalized:
        return None

    return " ".join(normalized.casefold().split())


def _amount_token(value: float | None) -> str | None:
    if value is None:
        return None
    return f"{float(value):.2f}"


def _is_multi_document_source_message(message_id: str | None) -> bool:
    if not message_id:
        return False
    prefix, separator, suffix = message_id.rpartition("__doc")
    return bool(prefix and separator and suffix.isdigit())


def _content_fingerprints(record: BillRecord) -> set[str]:
    fingerprints: set[str] = set()

    if record.source_media_sha256 and not _is_multi_document_source_message(record.source_message_id):
        fingerprints.add(f"media:{record.source_media_sha256}")

    identifier_tokens = [
        token
        for token in (
            _compact_text(record.invoice_number),
            _compact_text(record.document_number),
            _compact_text(record.receipt_number),
        )
        if token
    ]
    party_token = _compact_text(record.tax_number) or _compact_text(record.company_name)
    amount_token = _amount_token(record.total_amount)
    currency_token = _compact_text(record.currency)
    date_token = _compact_text(record.document_date)
    time_token = _compact_text(record.document_time)
    sender_token = _compact_text(record.sender_name)
    description_token = _free_text(record.description)

    exact_payload = {
        key: value
        for key, value in {
            "company": _free_text(record.company_name),
            "tax_number": _compact_text(record.tax_number),
            "tax_office": _free_text(record.tax_office),
            "document_number": _compact_text(record.document_number),
            "invoice_number": _compact_text(record.invoice_number),
            "receipt_number": _compact_text(record.receipt_number),
            "document_date": date_token,
            "document_time": time_token,
            "currency": currency_token,
            "total_amount": amount_token,
            "payable_amount": _amount_token(record.payable_amount),
            "subtotal": _amount_token(record.subtotal),
            "vat_amount": _amount_token(record.vat_amount),
            "withholding_amount": _amount_token(record.withholding_amount),
            "sender_name": sender_token,
            "recipient_name": _compact_text(record.recipient_name),
            "buyer_name": _compact_text(record.buyer_name),
            "payment_method": _free_text(record.payment_method),
            "expense_category": _free_text(record.expense_category),
            "iban": _compact_text(record.iban),
            "bank_name": _free_text(record.bank_name),
            "description": description_token,
            "notes": _free_text(record.notes),
        }.items()
        if value is not None
    }
    semantic_signal = bool(record.source_media_sha256 or identifier_tokens or time_token or sender_token or description_token)
    if exact_payload and semantic_signal:
        payload_hash = hashlib.sha256(
            json.dumps(exact_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        fingerprints.add(f"semantic:{payload_hash}")

    for identifier in identifier_tokens:
        parts = [f"id:{identifier}"]
        if party_token:
            parts.append(f"party:{party_token}")
        if date_token:
            parts.append(f"date:{date_token}")
        if amount_token:
            parts.append(f"total:{amount_token}")
        if currency_token:
            parts.append(f"currency:{currency_token}")
        fingerprints.add("doc:" + "|".join(parts))

    if party_token and date_token and amount_token and time_token:
        fingerprints.add(
            "party-date-total-time:"
            f"{party_token}|{date_token}|{time_token}|{amount_token}|{currency_token or 'na'}"
        )

    if party_token and date_token and amount_token and sender_token:
        fingerprints.add(
            "party-date-total-sender:"
            f"{party_token}|{date_token}|{amount_token}|{currency_token or 'na'}|{sender_token}"
        )

    if party_token and date_token and amount_token and description_token and len(description_token) >= 8:
        fingerprints.add(
            "party-date-total-description:"
            f"{party_token}|{date_token}|{amount_token}|{currency_token or 'na'}|{description_token[:80]}"
        )

    return fingerprints


def is_message_processed(message_id: str | None) -> bool:
    """Return whether the message ID already completed processing."""
    if not message_id:
        return False

    with _PERSIST_LOCK:
        with _interprocess_lock():
            return message_id in _load_processed_ids_unlocked()


def claim_message_processing(message_id: str | None, *, context: PipelineContext | None = None) -> bool:
    """Claim a message ID so only one worker processes it at a time."""
    with pipeline_context_scope(context):
        if not message_id:
            return True

        with _PERSIST_LOCK:
            with _interprocess_lock():
                processed_ids = _load_processed_ids_unlocked()
                if message_id in processed_ids:
                    logger.info("Skipping already completed message id=%s", message_id)
                    return False

                inflight = _purge_stale_inflight_unlocked(_load_inflight_unlocked())
                if message_id in inflight:
                    logger.info("Skipping already claimed message id=%s", message_id)
                    return False

                inflight[message_id] = datetime.now(timezone.utc).isoformat()
                _write_inflight_unlocked(inflight)
                logger.info("Claimed message id=%s for processing", message_id)
                return True


def mark_message_handled(
    message_id: str | None,
    *,
    outcome: str,
    context: PipelineContext | None = None,
) -> None:
    """Mark a non-export flow as fully handled and suppress duplicate reprocessing."""
    with pipeline_context_scope(context):
        if not message_id:
            return

        with _PERSIST_LOCK:
            with _interprocess_lock():
                processed_ids = _load_processed_ids_unlocked()
                if message_id in processed_ids:
                    return

                inflight = _purge_stale_inflight_unlocked(_load_inflight_unlocked())
                inflight.pop(message_id, None)
                _write_inflight_unlocked(inflight)
                _append_processed_id_unlocked(message_id)
                logger.info("Marked message id=%s as handled (%s)", message_id, outcome)


def release_message_processing(message_id: str | None, *, context: PipelineContext | None = None) -> None:
    """Release an in-flight claim so the message can be retried later."""
    with pipeline_context_scope(context):
        if not message_id:
            return

        with _PERSIST_LOCK:
            with _interprocess_lock():
                inflight = _purge_stale_inflight_unlocked(_load_inflight_unlocked())
                if inflight.pop(message_id, None) is not None:
                    _write_inflight_unlocked(inflight)
                    logger.info("Released in-flight claim for message id=%s", message_id)


def should_send_warning(
    recipient: str | None,
    warning_key: str,
    *,
    context: PipelineContext | None = None,
) -> bool:
    """Throttle repetitive user warnings by recipient and warning type."""
    with pipeline_context_scope(context):
        if not recipient or not warning_key:
            return True

        bucket = f"{recipient}:{warning_key}"
        now = datetime.now(timezone.utc)

        with _PERSIST_LOCK:
            with _interprocess_lock():
                warning_state = _purge_stale_warnings_unlocked(_load_warning_state_unlocked())
                last_sent_raw = warning_state.get(bucket)
                if last_sent_raw:
                    try:
                        last_sent = datetime.fromisoformat(last_sent_raw)
                    except ValueError:
                        last_sent = None
                    else:
                        if last_sent.tzinfo is None:
                            last_sent = last_sent.replace(tzinfo=timezone.utc)

                    if last_sent is not None and now - last_sent < _WARNING_THROTTLE_TTL:
                        logger.info("Skipping throttled warning bucket=%s", bucket)
                        return False

                warning_state[bucket] = now.isoformat()
                _write_warning_state_unlocked(warning_state)
                return True


def persist_record_once(record: BillRecord, *, context: PipelineContext | None = None) -> bool:
    """
    Append the record to the daily CSV once.

    Returns False when the message was already processed successfully.
    """
    with pipeline_context_scope(context):
        with _PERSIST_LOCK:
            with _interprocess_lock():
                processed_ids = _load_processed_ids_unlocked()
                seen_fingerprints = _load_content_fingerprints_unlocked()
                message_id = record.source_message_id
                record_fingerprints = _content_fingerprints(record)
                if message_id and message_id in processed_ids:
                    logger.info("Skipping duplicate export for message id=%s", message_id)
                    inflight = _purge_stale_inflight_unlocked(_load_inflight_unlocked())
                    inflight.pop(message_id, None)
                    _write_inflight_unlocked(inflight)
                    return False

                duplicate_fingerprints = sorted(record_fingerprints & seen_fingerprints)
                if duplicate_fingerprints:
                    logger.info(
                        "Skipping duplicate content for message id=%s via fingerprint=%s",
                        message_id,
                        duplicate_fingerprints[0],
                    )
                    inflight = _purge_stale_inflight_unlocked(_load_inflight_unlocked())
                    inflight.pop(message_id, None)
                    _write_inflight_unlocked(inflight)
                    return False

                filepath = _exports_dir() / f"records_{date.today().isoformat()}.csv"
                write_header = not filepath.exists()
                with filepath.open("a", encoding="utf-8-sig", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=TURKISH_HEADERS)
                    if write_header:
                        writer.writeheader()
                    writer.writerow(record_to_row(record))

                if record_fingerprints:
                    _append_content_fingerprints_unlocked(record_fingerprints)

                if message_id:
                    inflight = _purge_stale_inflight_unlocked(_load_inflight_unlocked())
                    inflight.pop(message_id, None)
                    _write_inflight_unlocked(inflight)
                    _append_processed_id_unlocked(message_id)

                logger.info("Record appended to %s", filepath)
                return True


def find_export_rows(
    *,
    source_message_id: str | None = None,
    chat_id: str | None = None,
    limit: int = 5,
    context: PipelineContext | None = None,
) -> list[Mapping[str, str]]:
    """Return the latest exported rows matching a source message ID or chat."""
    with pipeline_context_scope(context):
        if not source_message_id and not chat_id:
            return []

        matches: list[Mapping[str, str]] = []
        group_column = COLUMN_MAP["source_group_id"]
        sender_column = COLUMN_MAP["source_sender_id"]
        message_column = COLUMN_MAP["source_message_id"]

        with _PERSIST_LOCK:
            export_files = sorted(_exports_dir().glob("records_*.csv"), reverse=True)
            for filepath in export_files:
                with filepath.open("r", encoding="utf-8-sig", newline="") as handle:
                    rows = list(csv.DictReader(handle))

                for row in reversed(rows):
                    if source_message_id and row.get(message_column) == source_message_id:
                        matches.append(dict(row))
                    elif chat_id:
                        is_group_chat = chat_id.endswith("@g.us")
                        if is_group_chat and row.get(group_column) == chat_id:
                            matches.append(dict(row))
                        elif not is_group_chat and row.get(sender_column) == chat_id:
                            matches.append(dict(row))

                    if len(matches) >= limit:
                        return matches

        return matches
