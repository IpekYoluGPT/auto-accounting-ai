from contextlib import ExitStack
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app
from app.services.accounting.pipeline_context import sandbox_context


def _lifespan_patches():
    stack = ExitStack()
    stack.enter_context(patch("app.main.google_sheets.ensure_current_month_spreadsheet_ready"))
    stack.enter_context(patch("app.main.google_sheets.process_pending_sheet_appends"))
    stack.enter_context(patch("app.main.google_sheets.process_pending_document_uploads"))
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
        return_value={"pending_sheet_appends": 3, "pending_drive_uploads": 1},
    ) as clear_mock:
        with TestClient(app) as client:
            response = client.post("/setup/reset-sheet", json={"spreadsheet_id": "sheet-123"})

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "spreadsheet_id": "sheet-123",
        "tabs_reset": 8,
        "queue_before": {"pending_sheet_appends": 3, "pending_drive_uploads": 1},
        "queue_cleared": {"pending_sheet_appends": 3, "pending_drive_uploads": 1},
    }
    reset_mock.assert_called_once_with(spreadsheet_id="sheet-123")
    clear_mock.assert_called_once_with()


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
        return_value={"pending_sheet_appends": 0, "pending_drive_uploads": 0},
    ) as clear_mock:
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
    }
    reset_mock.assert_called_once_with(spreadsheet_id="sheet-123")
    clear_mock.assert_called_once_with()


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
        return_value={"pending_sheet_appends": 0, "pending_drive_uploads": 0},
    ) as clear_mock:
        with TestClient(app) as client:
            response = client.post(
                "/setup/reset-sheet",
                json={"spreadsheet_id": "sheet-123"},
                headers={"X-API-Key": "secret-token"},
            )

    assert response.status_code == 200
    assert response.json()["queue_cleared"] == {"pending_sheet_appends": 0, "pending_drive_uploads": 0}
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
        "app.routes.setup._ensure_sandbox_context",
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


def test_sandbox_intake_rejects_invalid_base64():
    context = sandbox_context(session_id="alpha")
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup._ensure_sandbox_context",
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
        "app.routes.setup._ensure_sandbox_context",
        return_value=(context, "sandbox-sheet-1", False),
    ), patch(
        "app.routes.setup.record_store.find_export_rows",
        side_effect=[[], [{"source_message_id": "sandbox-alpha-1"}]],
    ), patch(
        "app.routes.setup.intake.process_incoming_message",
        return_value="exported",
    ) as intake_mock, patch(
        "app.routes.setup._drain_sandbox_queues",
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


def test_sandbox_intake_reports_rows_by_source_message_id():
    context = sandbox_context(session_id="alpha")
    with _lifespan_patches(), patch(
        "app.routes.setup.settings.periskope_tool_token",
        "secret-token",
    ), patch(
        "app.routes.setup._ensure_sandbox_context",
        return_value=(context, "sandbox-sheet-1", False),
    ), patch(
        "app.routes.setup.record_store.find_export_rows",
        side_effect=[[], [{"source_message_id": "sandbox-alpha-1"}]],
    ) as rows_mock, patch(
        "app.routes.setup.intake.process_incoming_message",
        return_value="exported",
    ), patch(
        "app.routes.setup._drain_sandbox_queues",
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
        "app.routes.setup._resolve_existing_sandbox_spreadsheet_id",
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
        "app.routes.setup._require_existing_sandbox_context",
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
