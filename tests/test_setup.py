from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app


def test_reset_sheet_requires_valid_tool_token():
    with patch(
        "app.main.google_sheets.ensure_current_month_spreadsheet_ready"
    ), patch(
        "app.main.google_sheets.process_pending_document_uploads"
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
        "app.main.google_sheets.process_pending_document_uploads"
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
    ) as reset_mock:
        with TestClient(app) as client:
            response = client.post("/setup/reset-sheet", json={"spreadsheet_id": "sheet-123"})

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    reset_mock.assert_called_once_with(spreadsheet_id="sheet-123")


def test_reset_sheet_calls_google_sheets_with_payload_id():
    with patch(
        "app.main.google_sheets.ensure_current_month_spreadsheet_ready"
    ), patch(
        "app.main.google_sheets.process_pending_document_uploads"
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
    ) as reset_mock:
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
    }
    reset_mock.assert_called_once_with(spreadsheet_id="sheet-123")
