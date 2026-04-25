import shutil
from contextlib import ExitStack
from unittest.mock import Mock, call, patch

from fastapi.testclient import TestClient

from app.main import app
from app.routes import setup_sandbox
from app.services.accounting.pipeline_context import sandbox_context
from app.services.providers import google_sheets


def _lifespan_patches():
    stack = ExitStack()
    stack.enter_context(patch("app.main.google_sheets.ensure_current_month_spreadsheet_ready"))
    stack.enter_context(patch("app.main.google_sheets.process_pending_sheet_appends"))
    stack.enter_context(patch("app.main.google_sheets.process_pending_document_uploads"))
    stack.enter_context(patch("app.main.inbound_queue.bootstrap_inbound_queue"))
    stack.enter_context(patch("app.main.inbound_queue.process_pending_inbound_jobs"))
    stack.enter_context(patch("app.main.inbound_queue.start_pending_inbound_job_worker"))
    stack.enter_context(patch("app.main.inbound_queue.stop_pending_inbound_job_worker"))
    stack.enter_context(patch("app.main.google_sheets.start_pending_sheet_append_worker"))
    stack.enter_context(patch("app.main.google_sheets.start_monthly_rollover_scheduler"))
    stack.enter_context(patch("app.main.google_sheets.stop_monthly_rollover_scheduler"))
    return stack


def test_reset_sheet_requires_valid_tool_token():
    with patch(
        "app.main.google_sheets.ensure_current_month_spreadsheet_ready"
    ), patch(
        "app.main.google_sheets.process_pending_sheet_appends"
    ), patch(
        "app.main.google_sheets.process_pending_document_uploads"
    ), patch(
        "app.main.google_sheets.start_pending_sheet_append_worker"
    ), patch(
        "app.main.google_sheets.start_monthly_rollover_scheduler"
    ), patch(
        "app.main.google_sheets.stop_monthly_rollover_scheduler"
    ), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ):
        with TestClient(app) as client:
            response = client.post("/setup/reset-sheet", json={"spreadsheet_id": "sheet-123"})

    assert response.status_code == 401


def test_reset_sheet_allows_request_when_tool_token_is_unset():
    with patch(
        "app.main.google_sheets.ensure_current_month_spreadsheet_ready"
    ), patch(
        "app.main.google_sheets.process_pending_sheet_appends"
    ), patch(
        "app.main.google_sheets.process_pending_document_uploads"
    ), patch(
        "app.main.google_sheets.start_pending_sheet_append_worker"
    ), patch(
        "app.main.google_sheets.start_monthly_rollover_scheduler"
    ), patch(
        "app.main.google_sheets.stop_monthly_rollover_scheduler"
    ), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "",
    ), patch(
        "app.routes.setup.google_sheets.reset_current_month_spreadsheet_data",
        return_value=8,
    ) as reset_mock, patch(
        "app.routes.setup.google_sheets.queue_status",
        return_value={"pending_sheet_appends": 3, "pending_drive_uploads": 1},
    ), patch(
        "app.routes.setup.google_sheets.clear_current_namespace_storage",
        return_value={
            "queue_status": {"pending_sheet_appends": 3, "pending_drive_uploads": 1},
            "deleted_paths": ["/data/storage/state/canonical_store.sqlite3"],
        },
    ) as clear_mock, patch(
        "app.routes.setup.inbound_queue.stop_pending_inbound_job_worker"
    ) as stop_inbound_mock, patch(
        "app.routes.setup.inbound_queue.start_pending_inbound_job_worker"
    ) as start_inbound_mock, patch(
        "app.routes.setup_admin.google_sheets_scheduler.start_monthly_rollover_scheduler"
    ) as start_rollover_mock:
        with TestClient(app) as client:
            response = client.post("/setup/reset-sheet", json={"spreadsheet_id": "sheet-123"})

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "spreadsheet_id": "sheet-123",
        "tabs_reset": 8,
        "queue_before": {"pending_sheet_appends": 3, "pending_drive_uploads": 1},
        "queue_cleared": {"pending_sheet_appends": 3, "pending_drive_uploads": 1},
        "deleted_paths": ["/data/storage/state/canonical_store.sqlite3"],
        "workers_restarted": True,
    }
    reset_mock.assert_called_once_with(spreadsheet_id="sheet-123")
    clear_mock.assert_called_once_with()
    stop_inbound_mock.assert_any_call(timeout_seconds=5.0)
    assert start_inbound_mock.called
    start_rollover_mock.assert_called_once_with(google_sheets)


def test_reset_sheet_calls_google_sheets_with_payload_id():
    with patch(
        "app.main.google_sheets.ensure_current_month_spreadsheet_ready"
    ), patch(
        "app.main.google_sheets.process_pending_sheet_appends"
    ), patch(
        "app.main.google_sheets.process_pending_document_uploads"
    ), patch(
        "app.main.google_sheets.start_pending_sheet_append_worker"
    ), patch(
        "app.main.google_sheets.start_monthly_rollover_scheduler"
    ), patch(
        "app.main.google_sheets.stop_monthly_rollover_scheduler"
    ), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup.google_sheets.reset_current_month_spreadsheet_data",
        return_value=8,
    ) as reset_mock, patch(
        "app.routes.setup.google_sheets.queue_status",
        return_value={"pending_sheet_appends": 0, "pending_drive_uploads": 0},
    ), patch(
        "app.routes.setup.google_sheets.clear_current_namespace_storage",
        return_value={
            "queue_status": {"pending_sheet_appends": 0, "pending_drive_uploads": 0},
            "deleted_paths": [],
        },
    ) as clear_mock, patch(
        "app.routes.setup.inbound_queue.stop_pending_inbound_job_worker"
    ) as stop_inbound_mock, patch(
        "app.routes.setup.inbound_queue.start_pending_inbound_job_worker"
    ) as start_inbound_mock, patch(
        "app.routes.setup_admin.google_sheets_scheduler.start_monthly_rollover_scheduler"
    ) as start_rollover_mock:
        with TestClient(app) as client:
            response = client.post(
                "/setup/reset-sheet",
                json={"spreadsheet_id": "sheet-123"},
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "spreadsheet_id": "sheet-123",
        "tabs_reset": 8,
        "queue_before": {"pending_sheet_appends": 0, "pending_drive_uploads": 0},
        "queue_cleared": {"pending_sheet_appends": 0, "pending_drive_uploads": 0},
        "deleted_paths": [],
        "workers_restarted": True,
    }
    reset_mock.assert_called_once_with(spreadsheet_id="sheet-123")
    clear_mock.assert_called_once_with()
    stop_inbound_mock.assert_any_call(timeout_seconds=5.0)
    assert start_inbound_mock.called
    start_rollover_mock.assert_called_once_with(google_sheets)


def test_reset_sheet_clears_storage_before_resetting_workbook():
    tracker = Mock()

    def stop_inbound(*, timeout_seconds: float | None = None):
        if timeout_seconds == 5.0:
            tracker.stop_inbound(timeout_seconds=timeout_seconds)

    def queue_status():
        tracker.queue_status()
        return {"pending_sheet_appends": 2, "pending_projection_rows": 2}

    def clear_storage():
        tracker.clear_storage()
        return {
            "queue_status": {"pending_sheet_appends": 2, "pending_projection_rows": 2},
            "deleted_paths": [
                "/data/storage/state/canonical_store.sqlite3",
                "/data/storage/state/content_fingerprints.txt",
            ],
        }

    route_reset_done = {"value": False}

    def reset_workbook(*, spreadsheet_id: str | None = None):
        tracker.reset_workbook(spreadsheet_id=spreadsheet_id)
        route_reset_done["value"] = True
        return 8

    def start_inbound():
        if route_reset_done["value"]:
            tracker.start_inbound()

    def start_rollover(sheets):
        tracker.start_rollover(sheets=sheets)

    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup.google_sheets.queue_status",
        side_effect=queue_status,
    ), patch(
        "app.routes.setup.google_sheets.clear_current_namespace_storage",
        side_effect=clear_storage,
    ), patch(
        "app.routes.setup.google_sheets.reset_current_month_spreadsheet_data",
        side_effect=reset_workbook,
    ), patch(
        "app.routes.setup.inbound_queue.stop_pending_inbound_job_worker",
        side_effect=stop_inbound,
    ), patch(
        "app.routes.setup.inbound_queue.start_pending_inbound_job_worker",
        side_effect=start_inbound,
    ), patch(
        "app.routes.setup_admin.google_sheets_scheduler.start_monthly_rollover_scheduler",
        side_effect=start_rollover,
    ):
        with TestClient(app) as client:
            response = client.post(
                "/setup/reset-sheet",
                json={"spreadsheet_id": "sheet-123", "clear_storage": True},
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    assert tracker.method_calls == [
        call.stop_inbound(timeout_seconds=5.0),
        call.queue_status(),
        call.clear_storage(),
        call.reset_workbook(spreadsheet_id="sheet-123"),
        call.start_inbound(),
        call.start_rollover(sheets=google_sheets),
    ]
    assert response.json()["queue_cleared"] == {"pending_sheet_appends": 2, "pending_projection_rows": 2}
    assert response.json()["deleted_paths"] == [
        "/data/storage/state/canonical_store.sqlite3",
        "/data/storage/state/content_fingerprints.txt",
    ]
    assert response.json()["workers_restarted"] is True


def test_clear_current_namespace_storage_stops_workers_and_returns_deleted_paths(tmp_path):
    tracker = Mock()
    storage_root = tmp_path / "storage"
    state_dir = storage_root / "state"
    exports_dir = storage_root / "exports"
    canonical_store = state_dir / "canonical_store.sqlite3"
    fingerprints = state_dir / "content_fingerprints.txt"
    export = exports_dir / "records_2026-04-25.csv"
    state_dir.mkdir(parents=True)
    exports_dir.mkdir(parents=True)
    canonical_store.write_text("db", encoding="utf-8")
    fingerprints.write_text("fp", encoding="utf-8")
    export.write_text("rows", encoding="utf-8")

    real_rmtree = shutil.rmtree

    def queue_status():
        tracker.queue_status()
        return {"pending_projection_rows": 2}

    def rmtree(path):
        tracker.rmtree(path)
        real_rmtree(path)

    with patch(
        "app.services.providers.google_sheets._storage_root",
        return_value=storage_root,
    ), patch(
        "app.services.providers.google_sheets.stop_pending_sheet_append_worker",
        side_effect=lambda **_: tracker.stop_projection(),
    ), patch(
        "app.services.providers.google_sheets.stop_monthly_rollover_scheduler",
        side_effect=lambda: tracker.stop_rollover(),
    ), patch(
        "app.services.providers.google_sheets.queue_status",
        side_effect=queue_status,
    ), patch(
        "app.services.providers.google_sheets.shutil.rmtree",
        side_effect=rmtree,
    ):
        result = google_sheets.clear_current_namespace_storage()

    assert tracker.method_calls == [
        call.stop_projection(),
        call.stop_rollover(),
        call.queue_status(),
        call.rmtree(storage_root),
    ]
    assert result["queue_status"] == {"pending_projection_rows": 2}
    assert result["deleted_paths"] == sorted(
        [
            str(state_dir.absolute()),
            str(exports_dir.absolute()),
            str(canonical_store.absolute()),
            str(fingerprints.absolute()),
            str(export.absolute()),
        ]
    )
    assert storage_root.exists()


def test_reset_sheet_accepts_x_api_key_header():
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup.google_sheets.reset_current_month_spreadsheet_data",
        return_value=8,
    ) as reset_mock, patch(
        "app.routes.setup.google_sheets.queue_status",
        return_value={"pending_sheet_appends": 0, "pending_drive_uploads": 0},
    ), patch(
        "app.routes.setup.google_sheets.clear_current_namespace_storage",
        return_value={
            "queue_status": {"pending_sheet_appends": 0, "pending_drive_uploads": 0},
            "deleted_paths": [],
        },
    ) as clear_mock, patch(
        "app.routes.setup.inbound_queue.stop_pending_inbound_job_worker"
    ), patch(
        "app.routes.setup.inbound_queue.start_pending_inbound_job_worker"
    ), patch(
        "app.routes.setup_admin.google_sheets_scheduler.start_monthly_rollover_scheduler"
    ):
        with TestClient(app) as client:
            response = client.post(
                "/setup/reset-sheet",
                json={"spreadsheet_id": "sheet-123"},
                headers={"X-API-Key": "secret-token"},
            )

    assert response.status_code == 200
    assert response.json()["queue_cleared"] == {"pending_sheet_appends": 0, "pending_drive_uploads": 0}
    assert response.json()["deleted_paths"] == []
    assert response.json()["workers_restarted"] is True
    reset_mock.assert_called_once_with(spreadsheet_id="sheet-123")
    clear_mock.assert_called_once_with()


def test_reset_sheet_can_skip_storage_clear():
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup.google_sheets.reset_current_month_spreadsheet_data",
        return_value=8,
    ) as reset_mock, patch(
        "app.routes.setup.google_sheets.queue_status"
    ) as queue_mock, patch(
        "app.routes.setup.google_sheets.clear_current_namespace_storage"
    ) as clear_mock:
        with TestClient(app) as client:
            response = client.post(
                "/setup/reset-sheet",
                json={"spreadsheet_id": "sheet-123", "clear_storage": False},
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "spreadsheet_id": "sheet-123",
        "tabs_reset": 8,
    }
    reset_mock.assert_called_once_with(spreadsheet_id="sheet-123")
    queue_mock.assert_not_called()
    clear_mock.assert_not_called()


def test_repair_sheet_runs_google_sheets_audit_in_repair_mode():
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup.google_sheets.audit_current_month_spreadsheet",
        return_value={
            "spreadsheet_id": "sheet-123",
            "month_key": "2026-04",
            "findings": [{"tab_name": "Faturalar", "message": "Repaired 3 Drive link formula(s)."}],
            "queue": {"pending_sheet_appends": 0, "pending_drive_uploads": 0},
        },
    ) as audit_mock:
        with TestClient(app) as client:
            response = client.post(
                "/setup/repair-sheet",
                json={
                    "spreadsheet_id": "sheet-123",
                    "tab_name": ["Faturalar"],
                    "refresh_formatting": False,
                },
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "spreadsheet_id": "sheet-123",
        "month_key": "2026-04",
        "findings": [{"tab_name": "Faturalar", "message": "Repaired 3 Drive link formula(s)."}],
        "queue": {"pending_sheet_appends": 0, "pending_drive_uploads": 0},
        "sheet_url": "https://docs.google.com/spreadsheets/d/sheet-123/edit",
        "audited_tabs": ["Faturalar"],
    }
    audit_mock.assert_called_once_with(
        spreadsheet_id="sheet-123",
        repair=True,
        target_tabs={"Faturalar"},
        refresh_formatting=False,
    )


def test_rewrite_belge_links_forces_live_sheet_update():
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup.google_sheets.force_rewrite_drive_links",
        return_value={"Faturalar": 3},
    ) as rewrite_mock:
        with TestClient(app) as client:
            response = client.post(
                "/setup/rewrite-belge-links",
                json={
                    "spreadsheet_id": "sheet-123",
                    "tab_name": ["Faturalar"],
                },
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "spreadsheet_id": "sheet-123",
        "rewritten_tabs": {"Faturalar": 3},
        "sheet_url": "https://docs.google.com/spreadsheets/d/sheet-123/edit",
        "audited_tabs": ["Faturalar"],
    }
    rewrite_mock.assert_called_once_with(
        spreadsheet_id="sheet-123",
        target_tabs={"Faturalar"},
    )


def test_hide_hidden_tabs_rehides_technical_and_orphan_tabs():
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup.google_sheets.hide_nonvisible_tabs",
        return_value={"canonical_hidden": 5, "ignored_orphans": 2},
    ) as hide_mock:
        with TestClient(app) as client:
            response = client.post(
                "/setup/hide-hidden-tabs",
                json={"spreadsheet_id": "sheet-123"},
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "spreadsheet_id": "sheet-123",
        "hidden_tabs": {"canonical_hidden": 5, "ignored_orphans": 2},
        "sheet_url": "https://docs.google.com/spreadsheets/d/sheet-123/edit",
    }
    hide_mock.assert_called_once_with(spreadsheet_id="sheet-123")


def test_drain_queues_requires_valid_tool_token():
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ):
        with TestClient(app) as client:
            response = client.post("/setup/drain-queues", json={"max_rounds": 3})

    assert response.status_code == 401



def test_drain_queues_returns_processed_counts_and_remaining_queue():
    with patch(
        "app.main._start_google_sheets_bootstrap",
    ), patch(
        "app.main.google_sheets.start_pending_sheet_append_worker",
    ), patch(
        "app.main.google_sheets.start_pending_drive_upload_worker",
    ), patch(
        "app.main.google_sheets.start_monthly_rollover_scheduler",
    ), patch(
        "app.main.google_sheets.stop_monthly_rollover_scheduler",
    ), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup.google_sheets.queue_status",
        side_effect=[
            {"pending_sheet_appends": 4, "pending_drive_uploads": 2},
            {"pending_sheet_appends": 0, "pending_drive_uploads": 0},
        ],
    ), patch(
        "app.routes.setup.google_sheets.process_pending_sheet_appends",
        side_effect=[3, 0],
    ) as sheet_mock, patch(
        "app.routes.setup.google_sheets.process_pending_document_uploads",
        side_effect=[2, 0],
    ) as drive_mock:
        with TestClient(app) as client:
            response = client.post(
                "/setup/drain-queues",
                json={"max_rounds": 5},
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "queue_before": {"pending_sheet_appends": 4, "pending_drive_uploads": 2},
        "drain": {
            "pending_inbound_jobs_processed": 0,
            "pending_sheet_appends_processed": 3,
            "pending_drive_uploads_processed": 2,
        },
        "queue_after": {"pending_sheet_appends": 0, "pending_drive_uploads": 0},
        "rounds": 5,
    }
    assert sheet_mock.call_count == 2
    assert drive_mock.call_count == 2


def test_sandbox_ensure_returns_structured_payload():
    context = sandbox_context(session_id="alpha")
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup_sandbox._ensure_sandbox_context",
        return_value=(context, "sandbox-sheet-1", False),
    ), patch(
        "app.routes.setup.google_sheets._month_key",
        return_value="2026-04",
    ):
        with TestClient(app) as client:
            response = client.post(
                "/setup/sandbox/ensure",
                json={"session_id": "alpha"},
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "session_id": "alpha",
        "namespace": "sandbox-alpha",
        "spreadsheet_id": "sandbox-sheet-1",
        "sheet_url": "https://docs.google.com/spreadsheets/d/sandbox-sheet-1/edit",
        "month_key": "2026-04",
        "created": False,
    }


def test_ensure_sandbox_context_repairs_layout_after_prepare():
    with patch(
        "app.routes.setup_sandbox._resolve_existing_sandbox_spreadsheet_id",
        return_value="sandbox-sheet-1",
    ), patch(
        "app.routes.setup_sandbox.google_sheets.ensure_current_month_spreadsheet_ready",
        return_value="sandbox-sheet-1",
    ), patch(
        "app.routes.setup_sandbox.google_sheets.audit_current_month_spreadsheet",
        return_value={"spreadsheet_id": "sandbox-sheet-1", "findings": []},
    ) as audit_mock:
        context, spreadsheet_id, created = setup_sandbox._ensure_sandbox_context("alpha")

    assert context.disable_outbound_messages is True
    assert spreadsheet_id == "sandbox-sheet-1"
    assert created is False
    audit_mock.assert_called_once_with(
        spreadsheet_id="sandbox-sheet-1",
        repair=True,
        refresh_formatting=True,
    )


def test_sandbox_intake_rejects_invalid_base64():
    context = sandbox_context(session_id="alpha")
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup_sandbox._ensure_sandbox_context",
        return_value=(context, "sandbox-sheet-1", False),
    ):
        with TestClient(app) as client:
            response = client.post(
                "/setup/sandbox/intake",
                json={
                    "session_id": "alpha",
                    "msg_type": "image",
                    "media_base64": "***not-base64***",
                    "mime_type": "image/jpeg",
                    "filename": "test.jpg",
                },
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid media_base64 payload."


def test_sandbox_intake_processes_text_in_sandbox_context():
    context = sandbox_context(session_id="alpha")
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup_sandbox._ensure_sandbox_context",
        return_value=(context, "sandbox-sheet-1", False),
    ), patch(
        "app.routes.setup.record_store.find_export_rows",
        side_effect=[[], [{"source_message_id": "sandbox-alpha-1"}]],
    ), patch(
        "app.routes.setup.intake.process_incoming_message",
        return_value="exported",
    ) as intake_mock, patch(
        "app.routes.setup_sandbox._drain_sandbox_queues",
        return_value={"pending_sheet_appends_processed": 1, "pending_drive_uploads_processed": 1},
    ), patch(
        "app.routes.setup.google_sheets.queue_status",
        return_value={"pending_sheet_appends": 0, "pending_drive_uploads": 0},
    ):
        with TestClient(app) as client:
            response = client.post(
                "/setup/sandbox/intake",
                json={
                    "session_id": "alpha",
                    "msg_type": "text",
                    "text": "elden odeme 500 tl yakit",
                },
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["outcome"] == "exported"
    assert body["record_count"] == 1
    assert body["spreadsheet_id"] == "sandbox-sheet-1"
    assert body["queue"] == {"pending_sheet_appends": 0, "pending_drive_uploads": 0}
    assert body["drain"] == {"pending_sheet_appends_processed": 1, "pending_drive_uploads_processed": 1}
    assert intake_mock.call_args.kwargs["context"].disable_outbound_messages is True
    assert intake_mock.call_args.kwargs["route"].chat_id == "sandbox-alpha@g.us"


def test_sandbox_intake_can_target_override_spreadsheet_silently():
    context = sandbox_context(session_id="alpha", spreadsheet_id_override="live-sheet-1")
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup_sandbox._ensure_sandbox_context",
        return_value=(context, "live-sheet-1", False),
    ) as ensure_mock, patch(
        "app.routes.setup.record_store.find_export_rows",
        side_effect=[[], [{"source_message_id": "sandbox-alpha-1"}]],
    ), patch(
        "app.routes.setup.intake.process_incoming_message",
        return_value="exported",
    ) as intake_mock, patch(
        "app.routes.setup_sandbox._drain_sandbox_queues",
        return_value={"pending_sheet_appends_processed": 1, "pending_drive_uploads_processed": 0},
    ), patch(
        "app.routes.setup.google_sheets.queue_status",
        return_value={"pending_sheet_appends": 0, "pending_drive_uploads": 0},
    ):
        with TestClient(app) as client:
            response = client.post(
                "/setup/sandbox/intake",
                json={
                    "session_id": "alpha",
                    "spreadsheet_id": "live-sheet-1",
                    "msg_type": "text",
                    "text": "elden odeme 500 tl yakit",
                },
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    assert response.json()["spreadsheet_id"] == "live-sheet-1"
    ensure_mock.assert_called_once()
    assert ensure_mock.call_args.kwargs["spreadsheet_id_override"] == "live-sheet-1"
    assert intake_mock.call_args.kwargs["context"].disable_outbound_messages is True


def test_sandbox_intake_reports_rows_by_source_message_id():
    context = sandbox_context(session_id="alpha")
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup_sandbox._ensure_sandbox_context",
        return_value=(context, "sandbox-sheet-1", False),
    ), patch(
        "app.routes.setup.record_store.find_export_rows",
        side_effect=[[], [{"source_message_id": "sandbox-alpha-1"}]],
    ) as rows_mock, patch(
        "app.routes.setup.intake.process_incoming_message",
        return_value="exported",
    ), patch(
        "app.routes.setup_sandbox._drain_sandbox_queues",
        return_value={"pending_sheet_appends_processed": 1, "pending_drive_uploads_processed": 0},
    ), patch(
        "app.routes.setup.google_sheets.queue_status",
        return_value={"pending_sheet_appends": 0, "pending_drive_uploads": 0},
    ):
        with TestClient(app) as client:
            response = client.post(
                "/setup/sandbox/intake",
                json={
                    "session_id": "alpha",
                    "message_id": "sandbox-alpha-1",
                    "msg_type": "text",
                    "text": "elden odeme 500 tl yakit",
                },
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    assert response.json()["record_count"] == 1
    assert rows_mock.call_args_list[0].kwargs["source_message_id"] == "sandbox-alpha-1"
    assert rows_mock.call_args_list[1].kwargs["source_message_id"] == "sandbox-alpha-1"


def test_sandbox_audit_returns_404_for_unknown_session():
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup_sandbox._resolve_existing_sandbox_spreadsheet_id",
        return_value=None,
    ):
        with TestClient(app) as client:
            response = client.get(
                "/setup/sandbox/audit",
                params={"session_id": "missing"},
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 404


def test_sandbox_audit_passes_target_tabs_to_google_sheets():
    context = sandbox_context(session_id="alpha")
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup_sandbox._require_existing_sandbox_context",
        return_value=(context, "sandbox-sheet-1"),
    ), patch(
        "app.routes.setup.google_sheets.audit_current_month_spreadsheet",
        return_value={"spreadsheet_id": "sandbox-sheet-1", "month_key": "2026-04", "findings": [], "queue": {"pending_sheet_appends": 0, "pending_drive_uploads": 0}},
    ) as audit_mock:
        with TestClient(app) as client:
            response = client.get(
                "/setup/sandbox/audit",
                params=[("session_id", "alpha"), ("repair", "true"), ("tab_name", "🧾 Faturalar"), ("tab_name", "📊 Özet")],
                headers={"Authorization": "Bearer secret-token"},
            )

    assert response.status_code == 200
    assert response.json()["audited_tabs"] == ["📊 Özet", "🧾 Faturalar"]
    audit_mock.assert_called_once_with(
        spreadsheet_id="sandbox-sheet-1",
        repair=True,
        target_tabs={"🧾 Faturalar", "📊 Özet"},
        refresh_formatting=True,
    )
