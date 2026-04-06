"""
Tests for exported-row persistence and duplicate tracking.
"""

import csv
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app.models.schemas import BillRecord
from app.services.accounting import record_store


def _record(message_id: str = "wamid-1") -> BillRecord:
    return BillRecord(
        company_name="ABC Market",
        total_amount=100.0,
        currency="TRY",
        source_message_id=message_id,
        source_filename="receipt.jpg",
        source_type="image",
        source_sender_id="905551112233",
        source_group_id="group-123",
        source_chat_type="group",
        confidence=0.91,
    )


def _read_rows(storage_dir: str) -> list[dict[str, str]]:
    export_files = list((Path(storage_dir) / "exports").glob("records_*.csv"))
    if not export_files:
        return []

    with export_files[0].open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_persist_record_once_writes_export_and_registry():
    with TemporaryDirectory() as tmpdir, patch(
        "app.services.accounting.record_store.settings.storage_dir", tmpdir
    ):
        persisted = record_store.persist_record_once(_record())

        registry_path = Path(tmpdir) / "state" / "processed_message_ids.txt"

        assert persisted is True
        assert record_store.is_message_processed("wamid-1") is True
        rows = _read_rows(tmpdir)
        assert len(rows) == 1
        assert rows[0]["Kaynak Gönderen ID"] == "905551112233"
        assert rows[0]["Kaynak Grup ID"] == "group-123"
        assert rows[0]["Sohbet Türü"] == "group"
        assert registry_path.read_text(encoding="utf-8").splitlines() == ["wamid-1"]


def test_persist_record_once_skips_duplicate_message_ids():
    with TemporaryDirectory() as tmpdir, patch(
        "app.services.accounting.record_store.settings.storage_dir", tmpdir
    ):
        assert record_store.persist_record_once(_record("wamid-1")) is True
        assert record_store.persist_record_once(_record("wamid-1")) is False

        rows = _read_rows(tmpdir)
        export_files = list((Path(tmpdir) / "exports").glob("records_*.csv"))

        assert len(rows) == 1
        assert len(export_files) == 1
        assert export_files[0].read_text(encoding="utf-8-sig").count("\n") == 2


def test_claim_message_processing_blocks_duplicate_claims_until_release():
    with TemporaryDirectory() as tmpdir, patch(
        "app.services.accounting.record_store.settings.storage_dir", tmpdir
    ):
        assert record_store.claim_message_processing("wamid-1") is True
        assert record_store.claim_message_processing("wamid-1") is False

        record_store.release_message_processing("wamid-1")

        assert record_store.claim_message_processing("wamid-1") is True


def test_mark_message_handled_prevents_future_duplicate_processing():
    with TemporaryDirectory() as tmpdir, patch(
        "app.services.accounting.record_store.settings.storage_dir", tmpdir
    ):
        assert record_store.claim_message_processing("wamid-1") is True

        record_store.mark_message_handled("wamid-1", outcome="ignored_non_bill_text")

        assert record_store.is_message_processed("wamid-1") is True
        assert record_store.claim_message_processing("wamid-1") is False


def test_should_send_warning_throttles_repeated_warning_buckets():
    with TemporaryDirectory() as tmpdir, patch(
        "app.services.accounting.record_store.settings.storage_dir", tmpdir
    ):
        assert record_store.should_send_warning("905551112233", "unrelated_text") is True
        assert record_store.should_send_warning("905551112233", "unrelated_text") is False


def test_should_send_warning_is_independent_per_warning_type():
    with TemporaryDirectory() as tmpdir, patch(
        "app.services.accounting.record_store.settings.storage_dir", tmpdir
    ):
        assert record_store.should_send_warning("905551112233", "unrelated_text") is True
        assert record_store.should_send_warning("905551112233", "unrelated_media") is True
