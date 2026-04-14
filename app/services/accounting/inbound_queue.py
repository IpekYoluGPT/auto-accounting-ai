"""Durable inbound queue for media messages before Gemini processing."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from app.config import settings
from app.services.accounting import intake, record_store, storage_guard
from app.services.accounting.pipeline_context import PipelineContext, current_pipeline_context, pipeline_context_scope
from app.services.providers import google_sheets, periskope, whatsapp
from app.utils.logging import get_logger

logger = get_logger(__name__)

MSG_DELAY_NOTICE = "Belge alındı, işleniyor; yoğunluk nedeniyle biraz sürebilir."
MSG_TERMINAL_RETRY_FAILURE = "Belge alındı ancak şu anda işlenemedi. Lütfen biraz sonra tekrar gönderin."
MSG_STORAGE_PRESSURE = "Sistem şu anda yoğun ve depolama sınırında. Lütfen biraz sonra tekrar gönderin."

_RETRY_DELAYS_SECONDS = (30, 60, 120, 300, 600, 900, 1800, 3600)
_JOB_LOCK = threading.Lock()
_WORKER_LOCK = threading.Lock()
_WORKER_WAKE_EVENT = threading.Event()
_WORKER_STOP_EVENT = threading.Event()
_WORKER_THREADS: list[threading.Thread] = []


@dataclass(frozen=True)
class EnqueueResult:
    status: str
    message: str | None = None

    def as_dict(self) -> dict[str, str]:
        payload = {"status": self.status}
        if self.message:
            payload["message"] = self.message
        return payload


def _state_dir() -> Path:
    path = storage_guard.managed_storage_root() / "state"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _pending_jobs_path() -> Path:
    return _state_dir() / "pending_inbound_jobs.json"


def _pending_payload_dir() -> Path:
    path = _state_dir() / "pending_inbound_jobs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _failures_path() -> Path:
    return _state_dir() / "inbound_failures.json"


def _load_pending_jobs_unlocked() -> list[dict]:
    path = _pending_jobs_path()
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Inbound queue state is invalid JSON; resetting it.")
        return []
    if not isinstance(payload, list):
        return []
    return [dict(item) for item in payload if isinstance(item, dict)]


def _save_pending_jobs_unlocked(items: list[dict]) -> None:
    _pending_jobs_path().write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_failures_unlocked() -> list[dict]:
    path = _failures_path()
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    return [dict(item) for item in payload if isinstance(item, dict)]


def _save_failures_unlocked(items: list[dict]) -> None:
    _failures_path().write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _retry_delay_seconds(attempts: int) -> int:
    index = max(attempts - 1, 0)
    if index >= len(_RETRY_DELAYS_SECONDS):
        return _RETRY_DELAYS_SECONDS[-1]
    return _RETRY_DELAYS_SECONDS[index]


def _job_age_exceeded(job: dict, *, now: datetime | None = None) -> bool:
    created_at_raw = str(job.get("created_at") or "").strip()
    if not created_at_raw:
        return False
    try:
        created_at = datetime.fromisoformat(created_at_raw)
    except ValueError:
        return False
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    current = now or _now()
    return (current - created_at) > timedelta(hours=max(int(settings.inbound_retry_max_age_hours), 1))


def _job_attempts_exceeded(job: dict, *, next_attempts: int | None = None) -> bool:
    attempts = next_attempts if next_attempts is not None else int(job.get("attempts", 0))
    return attempts > max(int(settings.inbound_retry_max_attempts), 1)


def _delete_payload(path_raw: str | None) -> None:
    raw = (path_raw or "").strip()
    if not raw:
        return
    try:
        Path(raw).unlink(missing_ok=True)
    except Exception as exc:
        logger.warning("Could not delete inbound payload %s: %s", raw, exc)


def _append_failure_unlocked(job: dict, *, failure_reason: str, terminal_message: str | None = None) -> None:
    failures = _load_failures_unlocked()
    failure_entry = {
        "job_id": str(job.get("job_id") or ""),
        "message_id": str(job.get("message_id") or ""),
        "chat_id": str(job.get("chat_id") or ""),
        "platform": str(job.get("platform") or ""),
        "status": str(job.get("status") or ""),
        "attempts": int(job.get("attempts", 0)),
        "last_error": str(job.get("last_error") or ""),
        "last_stage": str(job.get("last_stage") or ""),
        "failure_reason": failure_reason,
        "terminal_message": terminal_message or "",
        "created_at": str(job.get("created_at") or ""),
        "failed_at": _now().isoformat(),
    }
    failures.append(failure_entry)
    _save_failures_unlocked(failures)


def _normalize_jobs_unlocked(items: list[dict]) -> list[dict]:
    normalized: list[dict] = []
    current = _now()
    changed = False

    for item in items:
        job = dict(item)
        payload_path = str(job.get("payload_path") or "").strip()
        if payload_path and not Path(payload_path).exists():
            job["payload_path"] = ""
            changed = True

        if str(job.get("status") or "") == "processing":
            job["status"] = "retry_wait"
            job["next_attempt_at"] = time.time()
            changed = True

        if _job_age_exceeded(job, now=current) or _job_attempts_exceeded(job):
            _delete_payload(job.get("payload_path"))
            _append_failure_unlocked(job, failure_reason="expired", terminal_message=MSG_TERMINAL_RETRY_FAILURE)
            changed = True
            continue

        normalized.append(job)

    if changed:
        _save_pending_jobs_unlocked(normalized)
    return normalized


def bootstrap_inbound_queue(*, context: PipelineContext | None = None) -> dict[str, int]:
    with pipeline_context_scope(context):
        storage_guard.prune_stale_transient_storage()
        with _JOB_LOCK:
            jobs = _normalize_jobs_unlocked(_load_pending_jobs_unlocked())
        return {
            "pending_inbound_jobs": len([job for job in jobs if str(job.get("status") or "") in {"queued", "processing"}]),
            "retry_waiting_inbound_jobs": len([job for job in jobs if str(job.get("status") or "") == "retry_wait"]),
        }


def queue_status(*, context: PipelineContext | None = None) -> dict[str, int]:
    with pipeline_context_scope(context):
        with _JOB_LOCK:
            jobs = _normalize_jobs_unlocked(_load_pending_jobs_unlocked())
            failed = len(_load_failures_unlocked())
        return {
            "pending_inbound_jobs": len([job for job in jobs if str(job.get("status") or "") in {"queued", "processing"}]),
            "retry_waiting_inbound_jobs": len([job for job in jobs if str(job.get("status") or "") == "retry_wait"]),
            "failed_inbound_jobs": failed,
            "inbound_payload_storage_bytes": storage_guard.inbound_payload_storage_bytes(),
        }


def storage_status(*, context: PipelineContext | None = None) -> dict[str, int | str]:
    with pipeline_context_scope(context):
        return storage_guard.storage_snapshot().as_dict()


def _route_to_job_dict(route: intake.MessageRoute) -> dict[str, str]:
    return {
        "platform": route.platform,
        "sender_id": route.sender_id,
        "sender_name": route.sender_name or "",
        "chat_id": route.chat_id,
        "chat_type": route.chat_type,
        "recipient_type": route.recipient_type,
        "group_id": route.group_id or "",
        "reply_to_message_id": route.reply_to_message_id or "",
    }


def _job_to_route(job: dict) -> intake.MessageRoute:
    return intake.MessageRoute(
        platform=str(job.get("platform") or "meta_whatsapp"),
        sender_id=str(job.get("sender_id") or ""),
        sender_name=str(job.get("sender_name") or "").strip() or None,
        chat_id=str(job.get("chat_id") or ""),
        chat_type=str(job.get("chat_type") or "individual"),
        recipient_type=str(job.get("recipient_type") or "individual"),
        group_id=str(job.get("group_id") or "").strip() or None,
        reply_to_message_id=str(job.get("reply_to_message_id") or "").strip() or None,
    )


def enqueue_media_job(
    *,
    message_id: str,
    msg_type: str,
    route: intake.MessageRoute,
    mime_type: str,
    filename: str,
    source_type: str,
    media_id: str | None = None,
    media_path: str | None = None,
    attachment_url: str | None = None,
    context: PipelineContext | None = None,
) -> EnqueueResult:
    with pipeline_context_scope(context):
        storage_guard.prune_stale_transient_storage()
        if storage_guard.should_reject_new_media_jobs():
            return EnqueueResult(status="rejected_due_to_storage", message=MSG_STORAGE_PRESSURE)

        with _JOB_LOCK:
            jobs = _normalize_jobs_unlocked(_load_pending_jobs_unlocked())
            if record_store.is_message_processed(message_id):
                return EnqueueResult(status="duplicate")
            if any(str(item.get("message_id") or "") == message_id for item in jobs):
                return EnqueueResult(status="duplicate")

            job = {
                "job_id": uuid4().hex,
                "message_id": message_id,
                "msg_type": msg_type,
                "mime_type": mime_type,
                "filename": filename,
                "source_type": source_type,
                "media_id": (media_id or "").strip(),
                "media_path": (media_path or "").strip(),
                "attachment_url": (attachment_url or "").strip(),
                "payload_path": "",
                "created_at": _now().isoformat(),
                "attempts": 0,
                "next_attempt_at": time.time(),
                "last_error": "",
                "last_stage": "queued",
                "status": "queued",
                "accepted_notice_sent": True,
                "delay_notice_sent": False,
                "terminal_warning_sent": False,
                **_route_to_job_dict(route),
            }
            jobs.append(job)
            _save_pending_jobs_unlocked(jobs)

        start_pending_inbound_job_worker()
        _WORKER_WAKE_EVENT.set()
        return EnqueueResult(status="enqueued")


def _resolve_send_text(route: intake.MessageRoute, text: str) -> None:
    if route.platform == "periskope":
        periskope.send_text_message(route.chat_id, text, reply_to=route.reply_to_message_id)
        return
    whatsapp.send_text_message(
        route.chat_id,
        text,
        recipient_type=route.recipient_type,
        reply_to_message_id=route.reply_to_message_id,
    )


def _resolve_send_reaction(route: intake.MessageRoute, emoji: str) -> None:
    if not route.reply_to_message_id:
        return
    if route.platform == "periskope":
        periskope.react_to_message(route.reply_to_message_id, emoji)
        return
    whatsapp.send_reaction_message(
        route.chat_id,
        route.reply_to_message_id,
        emoji,
        recipient_type=route.recipient_type,
    )


def _fetch_job_media(job: dict) -> bytes:
    platform = str(job.get("platform") or "")
    if platform == "periskope":
        media_path = str(job.get("media_path") or "").strip()
        if not media_path:
            raise RuntimeError("Missing Periskope media path.")
        return periskope.fetch_media(media_path, message_id=str(job.get("message_id") or "") or None)

    media_id = str(job.get("media_id") or "").strip()
    if not media_id:
        raise RuntimeError("Missing WhatsApp media id.")
    return whatsapp.fetch_media(media_id)


def _store_payload_if_allowed(job: dict, payload: bytes) -> str:
    if storage_guard.should_stop_payload_writes():
        return ""
    payload_path = _pending_payload_dir() / f"{str(job.get('job_id') or uuid4().hex)}.bin"
    payload_path.write_bytes(payload)
    with _JOB_LOCK:
        items = _load_pending_jobs_unlocked()
        for item in items:
            if str(item.get("job_id") or "") != str(job.get("job_id") or ""):
                continue
            item["payload_path"] = str(payload_path)
            break
        _save_pending_jobs_unlocked(items)
    return str(payload_path)


def _load_or_fetch_payload(job: dict) -> bytes:
    payload_path = str(job.get("payload_path") or "").strip()
    if payload_path:
        path = Path(payload_path)
        if path.exists():
            return path.read_bytes()

    payload = _fetch_job_media(job)
    try:
        stored_path = _store_payload_if_allowed(job, payload)
        if stored_path:
            job["payload_path"] = stored_path
    except Exception as exc:
        logger.warning("Could not persist inbound payload for message id=%s: %s", job.get("message_id"), exc)
    return payload


def _remove_job(job_id: str) -> None:
    with _JOB_LOCK:
        items = [
            item for item in _load_pending_jobs_unlocked()
            if str(item.get("job_id") or "") != job_id
        ]
        _save_pending_jobs_unlocked(items)


def _set_job_retry(job: dict, *, error: str, stage: str) -> bool:
    next_attempts = int(job.get("attempts", 0)) + 1
    terminal = _job_attempts_exceeded(job, next_attempts=next_attempts) or _job_age_exceeded(job)

    with _JOB_LOCK:
        items = _load_pending_jobs_unlocked()
        for item in items:
            if str(item.get("job_id") or "") != str(job.get("job_id") or ""):
                continue
            item["attempts"] = next_attempts
            item["last_error"] = error
            item["last_stage"] = stage
            if terminal:
                item["status"] = "failed"
            else:
                item["status"] = "retry_wait"
                item["next_attempt_at"] = time.time() + _retry_delay_seconds(next_attempts)
            _save_pending_jobs_unlocked(items)
            break

    return terminal


def _mark_delay_notice_sent(job: dict) -> None:
    with _JOB_LOCK:
        items = _load_pending_jobs_unlocked()
        for item in items:
            if str(item.get("job_id") or "") != str(job.get("job_id") or ""):
                continue
            item["delay_notice_sent"] = True
            _save_pending_jobs_unlocked(items)
            break


def _mark_terminal_warning_sent(job: dict) -> None:
    with _JOB_LOCK:
        items = _load_pending_jobs_unlocked()
        for item in items:
            if str(item.get("job_id") or "") != str(job.get("job_id") or ""):
                continue
            item["terminal_warning_sent"] = True
            _save_pending_jobs_unlocked(items)
            break


def _finalize_failed_job(job: dict, *, failure_reason: str, terminal_message: str) -> None:
    payload_path = str(job.get("payload_path") or "")
    with _JOB_LOCK:
        items = _load_pending_jobs_unlocked()
        remaining: list[dict] = []
        target: dict | None = None
        for item in items:
            if str(item.get("job_id") or "") == str(job.get("job_id") or ""):
                target = dict(item)
                continue
            remaining.append(item)
        if target is None:
            target = dict(job)
        target["status"] = "failed"
        target["last_error"] = terminal_message
        target["last_stage"] = failure_reason
        _append_failure_unlocked(target, failure_reason=failure_reason, terminal_message=terminal_message)
        _save_pending_jobs_unlocked(remaining)
    _delete_payload(payload_path)


def _complete_successful_job(job: dict) -> None:
    _remove_job(str(job.get("job_id") or ""))
    _delete_payload(str(job.get("payload_path") or ""))


def _claim_next_ready_job() -> dict | None:
    with _JOB_LOCK:
        items = _normalize_jobs_unlocked(_load_pending_jobs_unlocked())
        now_ts = time.time()
        ready_items = [
            item for item in items
            if str(item.get("status") or "") in {"queued", "retry_wait"}
            and float(item.get("next_attempt_at") or 0.0) <= now_ts
        ]
        if not ready_items:
            return None

        ready_items.sort(
            key=lambda item: (
                float(item.get("next_attempt_at") or 0.0),
                str(item.get("created_at") or ""),
            )
        )
        selected = dict(ready_items[0])
        selected_id = str(selected.get("job_id") or "")
        for item in items:
            if str(item.get("job_id") or "") != selected_id:
                continue
            item["status"] = "processing"
            item["last_stage"] = "processing"
            item["processing_started_at"] = _now().isoformat()
            break
        _save_pending_jobs_unlocked(items)
        return selected


def _next_retry_delay_seconds() -> float | None:
    with _JOB_LOCK:
        items = _normalize_jobs_unlocked(_load_pending_jobs_unlocked())
    candidates = [
        float(item.get("next_attempt_at") or 0.0)
        for item in items
        if str(item.get("status") or "") in {"queued", "retry_wait"}
    ]
    if not candidates:
        return None
    return max(min(candidates) - time.time(), 0.0)


def _schedule_retry_or_fail(job: dict, *, route: intake.MessageRoute, error: str, stage: str) -> None:
    terminal = _set_job_retry(job, error=error, stage=stage)
    if terminal:
        record_store.mark_message_handled(str(job.get("message_id") or ""), outcome="inbound_retry_exhausted")
        intake._safe_send_reaction(route, intake.REACTION_WARNING, reason="queue terminal warning", send_reaction=_resolve_send_reaction)
        intake._safe_send_text_message(route, MSG_TERMINAL_RETRY_FAILURE, reason="queue terminal warning", send_text=_resolve_send_text)
        _mark_terminal_warning_sent(job)
        _finalize_failed_job(job, failure_reason="retry_exhausted", terminal_message=MSG_TERMINAL_RETRY_FAILURE)
        return

    if not bool(job.get("delay_notice_sent")):
        intake._safe_send_text_message(route, MSG_DELAY_NOTICE, reason="queue delay notice", send_text=_resolve_send_text)
        _mark_delay_notice_sent(job)


def _process_one_job(job: dict) -> None:
    route = _job_to_route(job)

    try:
        raw_bytes = _load_or_fetch_payload(job)
    except Exception as exc:
        logger.warning("Failed to fetch queued media for message id=%s: %s", job.get("message_id"), exc)
        _schedule_retry_or_fail(job, route=route, error=str(exc), stage="media_fetch")
        return

    try:
        result = intake.process_media_payload(
            message_id=str(job.get("message_id") or ""),
            route=route,
            raw_bytes=raw_bytes,
            mime_type=str(job.get("mime_type") or "application/octet-stream"),
            filename=str(job.get("filename") or "document.bin"),
            source_type=str(job.get("source_type") or str(job.get("msg_type") or "document")),
            attachment_url=str(job.get("attachment_url") or "") or None,
        )
    except Exception as exc:
        logger.warning("Queued media processing crashed for message id=%s: %s", job.get("message_id"), exc, exc_info=True)
        if intake.is_temporary_media_exception(exc):
            _schedule_retry_or_fail(job, route=route, error=str(exc), stage="processing")
            return
        record_store.mark_message_handled(str(job.get("message_id") or ""), outcome="fatal_processing_error")
        intake._safe_send_reaction(route, intake.REACTION_WARNING, reason="queue fatal reaction", send_reaction=_resolve_send_reaction)
        intake._safe_send_text_message(route, intake.MSG_ERROR, reason="queue fatal error", send_text=_resolve_send_text)
        _mark_terminal_warning_sent(job)
        _finalize_failed_job(job, failure_reason="fatal_processing_error", terminal_message=intake.MSG_ERROR)
        return

    if result.retryable:
        _schedule_retry_or_fail(job, route=route, error=result.user_message or result.outcome, stage=result.stage)
        return

    visible_sheet_pending = False
    if result.exported_count > 0:
        intake.maybe_send_sheet_backlog_notice(route, send_text=_resolve_send_text)
        visible_sheet_pending = google_sheets.has_pending_visible_appends(
            message_id=str(job.get("message_id") or ""),
            chat_id=route.chat_id,
            platform=route.platform,
        )

    if result.outcome in {"exported", "already_exported"}:
        record_store.mark_message_handled(str(job.get("message_id") or ""), outcome=result.outcome)
        intake._safe_send_reaction(
            route,
            intake.REACTION_SHEET_PENDING if visible_sheet_pending else intake.REACTION_SUCCESS,
            reason="queue success reaction",
            send_reaction=_resolve_send_reaction,
        )
        _complete_successful_job(job)
        return

    record_store.mark_message_handled(str(job.get("message_id") or ""), outcome=result.outcome)
    intake._safe_send_reaction(route, intake.REACTION_WARNING, reason="queue terminal reaction", send_reaction=_resolve_send_reaction)
    intake._safe_send_text_message(
        route,
        result.user_message or intake.MSG_ERROR,
        reason=f"queue terminal {result.outcome}",
        send_text=_resolve_send_text,
    )
    _mark_terminal_warning_sent(job)
    _finalize_failed_job(
        job,
        failure_reason=result.outcome,
        terminal_message=result.user_message or intake.MSG_ERROR,
    )


def process_pending_inbound_jobs(
    *,
    max_jobs: int | None = None,
    context: PipelineContext | None = None,
) -> int:
    with pipeline_context_scope(context):
        storage_guard.prune_stale_transient_storage()
        processed = 0
        limit = max_jobs if max_jobs is not None else max(int(settings.inbound_max_active_jobs), 1)

        while processed < max(limit, 1):
            job = _claim_next_ready_job()
            if job is None:
                break
            _process_one_job(job)
            processed += 1

        return processed


def retry_pending_inbound_jobs(*, context: PipelineContext | None = None) -> dict[str, int]:
    with pipeline_context_scope(context):
        with _JOB_LOCK:
            items = _normalize_jobs_unlocked(_load_pending_jobs_unlocked())
            retried = 0
            for item in items:
                if str(item.get("status") or "") != "retry_wait":
                    continue
                item["status"] = "queued"
                item["next_attempt_at"] = time.time()
                retried += 1
            _save_pending_jobs_unlocked(items)
        if retried:
            start_pending_inbound_job_worker()
            _WORKER_WAKE_EVENT.set()
        return {"retried_jobs": retried}


def reset_inbound_queue(*, context: PipelineContext | None = None) -> dict[str, int]:
    with pipeline_context_scope(context):
        with _JOB_LOCK:
            items = _load_pending_jobs_unlocked()
            failures = _load_failures_unlocked()
            for item in items:
                _delete_payload(str(item.get("payload_path") or ""))
            _save_pending_jobs_unlocked([])
            _save_failures_unlocked([])
        storage_guard.prune_stale_transient_storage()
        return {
            "cleared_jobs": len(items),
            "cleared_failures": len(failures),
        }


def _pending_inbound_worker() -> None:
    while not _WORKER_STOP_EVENT.is_set():
        try:
            processed = process_pending_inbound_jobs(max_jobs=1)
        except Exception as exc:
            logger.warning("Pending inbound queue worker error: %s", exc, exc_info=True)
            processed = 0

        if processed > 0:
            continue

        wait_seconds = _next_retry_delay_seconds()
        if wait_seconds is None:
            wait_seconds = float(max(int(settings.inbound_worker_poll_seconds), 1))
        else:
            wait_seconds = max(min(wait_seconds, float(max(int(settings.inbound_worker_poll_seconds), 1))), 1.0)

        _WORKER_WAKE_EVENT.wait(timeout=wait_seconds)
        _WORKER_WAKE_EVENT.clear()


def start_pending_inbound_job_worker() -> None:
    if not current_pipeline_context().is_production:
        return

    with _WORKER_LOCK:
        desired_workers = max(int(settings.inbound_max_active_jobs), 1)
        alive_threads = [thread for thread in _WORKER_THREADS if thread.is_alive()]
        _WORKER_THREADS[:] = alive_threads
        if len(alive_threads) >= desired_workers:
            return

        _WORKER_STOP_EVENT.clear()
        for worker_index in range(len(alive_threads), desired_workers):
            thread = threading.Thread(
                target=_pending_inbound_worker,
                name=f"inbound-media-queue-{worker_index + 1}",
                daemon=True,
            )
            _WORKER_THREADS.append(thread)
            thread.start()


def stop_pending_inbound_job_worker(*, timeout_seconds: float = 1.0) -> None:
    with _WORKER_LOCK:
        threads = list(_WORKER_THREADS)
        if not threads:
            return
        _WORKER_STOP_EVENT.set()
        _WORKER_WAKE_EVENT.set()

    for thread in threads:
        if thread.is_alive():
            thread.join(timeout=timeout_seconds)

    with _WORKER_LOCK:
        _WORKER_THREADS[:] = [thread for thread in _WORKER_THREADS if thread.is_alive()]
        if not _WORKER_THREADS:
            _WORKER_STOP_EVENT.clear()
