"""
Monthly spreadsheet readiness and rollover scheduling helpers.
"""

from __future__ import annotations

import threading

from app.services.providers import google_sheets_layout

_rollover_thread: threading.Thread | None = None
_rollover_stop_event: threading.Event | None = None
_rollover_lock = threading.Lock()


def next_month_rollover_at(sheets, now=None):
    current = now or sheets._now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=sheets._get_business_timezone())

    year = current.year
    month = current.month + 1
    if month == 13:
        month = 1
        year += 1

    return current.replace(
        year=year,
        month=month,
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )


def seconds_until_next_month_rollover(sheets, now=None) -> float:
    current = now or sheets._now()
    rollover_at = next_month_rollover_at(sheets, current)
    return max((rollover_at - current).total_seconds(), 1.0)


def ensure_current_month_spreadsheet_ready(sheets):
    client = sheets._get_client()
    if client is None:
        sheets.logger.debug("Google Sheets projection bootstrap skipped; client unavailable.")
        return None

    with sheets._lock:
        try:
            sh = sheets._get_or_create_spreadsheet(client)
            if sheets._was_recently_prepared(sh):
                sheets._audit_spreadsheet_layout(sh, repair=True, refresh_formatting=False)
            else:
                google_sheets_layout.ensure_projection_workbook_layout(sheets, sh)
            sheets.logger.info("Google Sheets projection workbook is ready for %s.", sheets._month_key())
            return sh.id
        except Exception as exc:
            sheets.logger.warning("Could not prepare current month's spreadsheet: %s", exc)
            return None


def _monthly_rollover_worker(sheets, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        wait_seconds = seconds_until_next_month_rollover(sheets)
        if stop_event.wait(wait_seconds):
            break
        ensure_current_month_spreadsheet_ready(sheets)


def start_monthly_rollover_scheduler(sheets) -> None:
    if not sheets.current_pipeline_context().is_production:
        return

    global _rollover_thread, _rollover_stop_event

    with _rollover_lock:
        if _rollover_thread is not None and _rollover_thread.is_alive():
            return

        _rollover_stop_event = threading.Event()
        _rollover_thread = threading.Thread(
            target=_monthly_rollover_worker,
            args=(sheets, _rollover_stop_event),
            name="google-sheets-monthly-rollover",
            daemon=True,
        )
        _rollover_thread.start()
        sheets.logger.info(
            "Started Google Sheets monthly rollover scheduler (timezone=%s).",
            sheets.settings.business_timezone,
        )


def stop_monthly_rollover_scheduler(sheets) -> None:
    global _rollover_thread, _rollover_stop_event

    with _rollover_lock:
        stop_event = _rollover_stop_event
        thread = _rollover_thread
        _rollover_stop_event = None
        _rollover_thread = None

    if stop_event is not None:
        stop_event.set()
    if thread is not None and thread.is_alive():
        thread.join(timeout=1.0)
