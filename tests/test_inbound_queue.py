from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app.services.accounting import inbound_queue, storage_guard
from app.services.accounting.intake import MSG_MEDIA_TEMPORARY_UPSTREAM_ERROR, MediaProcessingResult, MessageRoute


@contextmanager
def _patch_storage(tmpdir: str):
    inbound_queue.stop_pending_inbound_job_worker(timeout_seconds=0.1)
    with patch("app.services.accounting.inbound_queue.settings.storage_dir", tmpdir), patch(
        "app.services.accounting.storage_guard.settings.storage_dir", tmpdir
    ), patch(
        "app.services.accounting.record_store.settings.storage_dir", tmpdir
    ), patch(
        "app.services.providers.google_sheets.settings.storage_dir", tmpdir
    ), patch(
        "app.services.accounting.inbound_queue.start_pending_inbound_job_worker", lambda: None
    ):
        yield


def _route() -> MessageRoute:
    return MessageRoute(
        platform="meta_whatsapp",
        sender_id="905551112233",
        sender_name="Ahmet Yılmaz",
        chat_id="120363410789660631@g.us",
        chat_type="group",
        recipient_type="group",
        group_id="120363410789660631@g.us",
        reply_to_message_id="wamid-1",
    )


def test_enqueue_media_job_rejects_when_storage_is_hard_reject():
    with TemporaryDirectory() as tmpdir, _patch_storage(tmpdir), patch(
        "app.services.accounting.inbound_queue.storage_guard.should_reject_new_media_jobs",
        return_value=True,
    ):
        result = inbound_queue.enqueue_media_job(
            message_id="wamid-1",
            msg_type="image",
            route=_route(),
            mime_type="image/jpeg",
            filename="receipt.jpg",
            source_type="image",
            media_id="media-1",
        )

    assert result.status == "rejected_due_to_storage"
    assert result.message == inbound_queue.MSG_STORAGE_PRESSURE


def test_process_pending_inbound_jobs_success_cleans_payload_and_reacts():
    with TemporaryDirectory() as tmpdir, _patch_storage(tmpdir), patch(
        "app.services.accounting.inbound_queue.whatsapp.fetch_media",
        return_value=b"fake-image",
    ), patch(
        "app.services.accounting.inbound_queue.intake.process_media_payload",
        return_value=MediaProcessingResult(outcome="exported", exported_count=1),
    ), patch(
        "app.services.accounting.inbound_queue.intake.maybe_send_sheet_backlog_notice",
    ) as backlog_mock, patch(
        "app.services.accounting.inbound_queue.whatsapp.send_reaction_message",
    ) as reaction_mock:
        result = inbound_queue.enqueue_media_job(
            message_id="wamid-1",
            msg_type="image",
            route=_route(),
            mime_type="image/jpeg",
            filename="receipt.jpg",
            source_type="image",
            media_id="media-1",
        )

        processed = inbound_queue.process_pending_inbound_jobs(max_jobs=1)

        assert result.status == "enqueued"
        assert processed == 1
        assert inbound_queue.queue_status()["pending_inbound_jobs"] == 0
        assert list((Path(tmpdir) / "state" / "pending_inbound_jobs").glob("*.bin")) == []
        backlog_mock.assert_called_once()
        assert reaction_mock.call_args.args == ("120363410789660631@g.us", "wamid-1", "✅")


def test_process_pending_inbound_jobs_retryable_failure_sends_delay_once():
    with TemporaryDirectory() as tmpdir, _patch_storage(tmpdir), patch(
        "app.services.accounting.inbound_queue.whatsapp.fetch_media",
        return_value=b"fake-image",
    ), patch(
        "app.services.accounting.inbound_queue.intake.process_media_payload",
        return_value=MediaProcessingResult(
            outcome="extraction_failed",
            retryable=True,
            user_message=MSG_MEDIA_TEMPORARY_UPSTREAM_ERROR,
            stage="extraction",
        ),
    ), patch(
        "app.services.accounting.inbound_queue.whatsapp.send_text_message",
    ) as send_mock, patch(
        "app.services.accounting.inbound_queue.whatsapp.send_reaction_message",
    ) as reaction_mock:
        inbound_queue.enqueue_media_job(
            message_id="wamid-1",
            msg_type="image",
            route=_route(),
            mime_type="image/jpeg",
            filename="receipt.jpg",
            source_type="image",
            media_id="media-1",
        )

        assert inbound_queue.process_pending_inbound_jobs(max_jobs=1) == 1
        assert inbound_queue.queue_status()["retry_waiting_inbound_jobs"] == 1
        assert send_mock.call_count == 1
        assert send_mock.call_args.args[1] == inbound_queue.MSG_DELAY_NOTICE
        reaction_mock.assert_not_called()

        inbound_queue.retry_pending_inbound_jobs()
        assert inbound_queue.process_pending_inbound_jobs(max_jobs=1) == 1
        assert send_mock.call_count == 1


def test_process_pending_inbound_jobs_retry_exhaustion_marks_failure_and_cleans_payload():
    with TemporaryDirectory() as tmpdir, _patch_storage(tmpdir), patch(
        "app.services.accounting.inbound_queue.whatsapp.fetch_media",
        return_value=b"fake-image",
    ), patch(
        "app.services.accounting.inbound_queue.intake.process_media_payload",
        return_value=MediaProcessingResult(
            outcome="classification_failed",
            retryable=True,
            user_message=MSG_MEDIA_TEMPORARY_UPSTREAM_ERROR,
            stage="classification",
        ),
    ), patch(
        "app.services.accounting.inbound_queue.record_store.mark_message_handled",
    ) as handled_mock, patch(
        "app.services.accounting.inbound_queue.whatsapp.send_text_message",
    ) as send_mock, patch(
        "app.services.accounting.inbound_queue.whatsapp.send_reaction_message",
    ) as reaction_mock:
        inbound_queue.enqueue_media_job(
            message_id="wamid-1",
            msg_type="image",
            route=_route(),
            mime_type="image/jpeg",
            filename="receipt.jpg",
            source_type="image",
            media_id="media-1",
        )

        queue_path = Path(tmpdir) / "state" / "pending_inbound_jobs.json"
        items = json.loads(queue_path.read_text(encoding="utf-8"))
        items[0]["attempts"] = 20
        queue_path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

        assert inbound_queue.process_pending_inbound_jobs(max_jobs=1) == 1
        status = inbound_queue.queue_status()
        assert status["pending_inbound_jobs"] == 0
        assert status["retry_waiting_inbound_jobs"] == 0
        assert status["failed_inbound_jobs"] == 1
        handled_mock.assert_called_once_with("wamid-1", outcome="inbound_retry_exhausted")
        assert reaction_mock.call_args.args == ("120363410789660631@g.us", "wamid-1", "⚠️")
        assert send_mock.call_args.args[1] == inbound_queue.MSG_TERMINAL_RETRY_FAILURE


def test_storage_snapshot_uses_managed_storage_thresholds():
    with TemporaryDirectory() as tmpdir, _patch_storage(tmpdir):
        big_file = Path(tmpdir) / "state" / "pending_inbound_jobs" / "blob.bin"
        big_file.parent.mkdir(parents=True, exist_ok=True)
        big_file.write_bytes(b"x" * 1024)

        snapshot = storage_guard.storage_snapshot()

    assert snapshot.total_managed_storage_bytes >= 1024
    assert snapshot.disk_pressure_state == "normal"
