from __future__ import annotations

import base64
from pathlib import Path

import app.delivery_smoke as delivery_smoke


def _service_account_json() -> str:
    raw = b'{"client_email":"bot@example.com"}'
    return base64.b64encode(raw).decode("ascii")


def _settings(**overrides):
    values = {
        "gemini_api_key": "gemini-key",
        "periskope_api_key": "periskope-key",
        "periskope_phone": "905551112233",
        "periskope_signing_key": "signing-secret",
        "periskope_allowed_chat_ids": "120363410789660631@g.us",
        "storage_dir": "./storage",
        "google_service_account_json": _service_account_json(),
        "google_sheets_spreadsheet_id": "sheet-123",
        "google_drive_parent_folder_id": "drive-parent",
        "google_sheets_owner_email": "ops@example.com",
        "google_oauth_client_id": "",
        "google_oauth_client_secret": "",
        "google_oauth_refresh_token": "",
    }
    values.update(overrides)
    return type("FakeSettings", (), values)()


def test_validate_runtime_config_accepts_minimum_live_hot_path(tmp_path: Path):
    settings = _settings(storage_dir=str(tmp_path / "storage"))

    report = delivery_smoke.validate_runtime_config(settings, project_root=tmp_path)

    assert report.errors == []
    assert report.warnings == []


def test_validate_runtime_config_flags_release_blockers(tmp_path: Path):
    settings = _settings(
        gemini_api_key="your_gemini_api_key_here",
        periskope_allowed_chat_ids="",
        periskope_signing_key="",
        google_service_account_json="",
        google_oauth_client_id="oauth-client-id",
        storage_dir="",
    )

    report = delivery_smoke.validate_runtime_config(settings, project_root=tmp_path)

    assert "GEMINI_API_KEY is missing or still set to an example value." in report.errors
    assert "PERISKOPE_ALLOWED_CHAT_IDS is empty; the primary webhook will reject every chat." in report.errors
    assert "PERISKOPE_SIGNING_KEY is empty; webhook signature verification is effectively disabled." in report.errors
    assert "GOOGLE_SERVICE_ACCOUNT_JSON is missing or still set to an example value." in report.errors
    assert (
        "GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, and GOOGLE_OAUTH_REFRESH_TOKEN must be set together."
        in report.errors
    )
    assert "STORAGE_DIR is empty." in report.errors


def test_validate_runtime_config_warns_when_drive_backfill_is_disabled(tmp_path: Path):
    settings = _settings(
        storage_dir=str(tmp_path / "storage"),
        google_drive_parent_folder_id="",
    )

    report = delivery_smoke.validate_runtime_config(settings, project_root=tmp_path)

    assert report.errors == []
    assert report.warnings == [
        "GOOGLE_DRIVE_PARENT_FOLDER_ID is empty; Drive upload and Belge link backfill stay disabled."
    ]


def test_validate_runtime_config_warns_when_owner_email_is_missing(tmp_path: Path):
    settings = _settings(
        storage_dir=str(tmp_path / "storage"),
        google_sheets_owner_email="",
    )

    report = delivery_smoke.validate_runtime_config(settings, project_root=tmp_path)

    assert report.errors == []
    assert report.warnings == [
        "GOOGLE_SHEETS_OWNER_EMAIL is empty; auto-created spreadsheets will not be shared with the customer's company account."
    ]


def test_validate_runtime_config_flags_railway_storage_outside_volume(tmp_path: Path):
    storage_dir = tmp_path / "storage"
    settings = _settings(storage_dir=str(storage_dir))
    environment = {
        "RAILWAY_SERVICE_ID": "service-123",
        "RAILWAY_VOLUME_MOUNT_PATH": str(tmp_path / "railway-volume"),
    }

    report = delivery_smoke.validate_runtime_config(
        settings,
        project_root=tmp_path,
        environment=environment,
    )

    assert (
        f"STORAGE_DIR={storage_dir} is outside Railway volume mount path {environment['RAILWAY_VOLUME_MOUNT_PATH']}."
        in report.errors
    )


def test_build_pytest_command_targets_curated_delivery_checks():
    command = delivery_smoke.build_pytest_command("/tmp/project", python_executable="python")

    assert command[:5] == ["python", "-m", "pytest", "-q", "-s"]
    assert command[5:] == list(delivery_smoke.DELIVERY_SMOKE_TESTS)


def test_run_short_circuits_before_pytest_when_config_fails(tmp_path: Path):
    calls: list[list[str]] = []

    def _runner(command: list[str], cwd: str) -> int:
        calls.append(command)
        return 0

    exit_code = delivery_smoke.run(
        settings=_settings(storage_dir=""),
        project_root=tmp_path,
        python_executable="python",
        subprocess_runner=_runner,
    )

    assert exit_code == 1
    assert calls == []


def test_run_executes_pytest_when_config_is_clean(tmp_path: Path):
    calls: list[tuple[list[str], str]] = []

    def _runner(command: list[str], cwd: str) -> int:
        calls.append((command, cwd))
        return 0

    exit_code = delivery_smoke.run(
        settings=_settings(storage_dir=str(tmp_path / "storage")),
        project_root=tmp_path,
        python_executable="python",
        subprocess_runner=_runner,
    )

    assert exit_code == 0
    assert calls == [
        (
            delivery_smoke.build_pytest_command(tmp_path, python_executable="python"),
            str(tmp_path),
        )
    ]


def test_env_example_lists_delivery_smoke_variables():
    env_example = Path(".env.example").read_text(encoding="utf-8")

    for name in (
        "PERISKOPE_ALLOWED_CHAT_IDS",
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        "GOOGLE_SHEETS_SPREADSHEET_ID",
        "GOOGLE_DRIVE_PARENT_FOLDER_ID",
        "GOOGLE_SHEETS_OWNER_EMAIL",
        "GOOGLE_OAUTH_CLIENT_ID",
        "GOOGLE_OAUTH_CLIENT_SECRET",
        "GOOGLE_OAUTH_REFRESH_TOKEN",
    ):
        assert f"{name}=" in env_example
