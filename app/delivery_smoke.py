from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping

from app.config import settings as app_settings

DELIVERY_SMOKE_TESTS = (
    "tests/test_periskope.py::test_periskope_image_webhook_enqueues_media_without_inline_fetch",
    "tests/test_periskope.py::test_periskope_group_image_webhook_exports_and_reacts",
    "tests/test_periskope.py::test_periskope_duplicate_delivery_writes_only_one_export_row",
    "tests/test_periskope.py::test_periskope_webhook_rejects_invalid_signature",
    "tests/test_periskope.py::test_periskope_webhook_ignores_non_allowed_group_chat",
    "tests/test_exports.py::test_export_csv_returns_latest_export_file",
    "tests/test_inbound_queue.py::test_enqueue_media_job_rejects_when_storage_is_hard_reject",
)

_PLACEHOLDER_VALUES = {
    "changeme",
    "your_access_token_here",
    "your_document_ocr_processor_id",
    "your_enterprise_ocr_processor_id",
    "your_form_parser_processor_id",
    "your_gcp_project_id",
    "your_gemini_api_key_here",
    "your_phone_number_id_here",
    "your_verify_token_here",
}


@dataclass
class RuntimeConfigReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _emit(message: str) -> None:
    print(message, flush=True)


def _normalize(value: str | None) -> str:
    return (value or "").strip()


def _is_missing_or_placeholder(value: str | None) -> bool:
    normalized = _normalize(value)
    if not normalized:
        return True

    lowered = normalized.lower()
    return lowered in _PLACEHOLDER_VALUES or lowered.startswith("your_")


def _resolve_storage_dir(storage_dir: str | None, *, project_root: Path) -> Path | None:
    normalized = _normalize(storage_dir)
    if not normalized:
        return None

    storage_path = Path(normalized)
    if not storage_path.is_absolute():
        storage_path = project_root / storage_path
    return storage_path


def _validate_storage_dir(storage_dir: str | None, *, project_root: Path, errors: list[str]) -> Path | None:
    storage_path = _resolve_storage_dir(storage_dir, project_root=project_root)
    if storage_path is None:
        errors.append("STORAGE_DIR is empty.")
        return None

    parent = storage_path if storage_path.exists() else storage_path.parent
    if not parent.exists():
        errors.append(f"STORAGE_DIR parent does not exist: {parent}")
    return storage_path


def _validate_railway_storage_alignment(
    storage_path: Path | None,
    *,
    environment: Mapping[str, str],
    errors: list[str],
) -> None:
    if not _normalize(environment.get("RAILWAY_SERVICE_ID")):
        return

    volume_mount_path = _normalize(environment.get("RAILWAY_VOLUME_MOUNT_PATH"))
    if not volume_mount_path:
        errors.append("RAILWAY_VOLUME_MOUNT_PATH is empty; Railway deployment would use ephemeral storage.")
        return

    if storage_path is None:
        return

    resolved_storage_path = storage_path.resolve()
    resolved_volume_path = Path(volume_mount_path).resolve()
    if resolved_storage_path != resolved_volume_path and resolved_volume_path not in resolved_storage_path.parents:
        errors.append(
            f"STORAGE_DIR={resolved_storage_path} is outside Railway volume mount path {resolved_volume_path}."
        )


def _validate_google_service_account(value: str | None, errors: list[str]) -> None:
    if _is_missing_or_placeholder(value):
        errors.append("GOOGLE_SERVICE_ACCOUNT_JSON is missing or still set to an example value.")
        return

    try:
        decoded = base64.b64decode(_normalize(value)).decode("utf-8")
        payload = json.loads(decoded)
    except Exception:
        errors.append("GOOGLE_SERVICE_ACCOUNT_JSON is not valid base64-encoded JSON.")
        return

    if not isinstance(payload, dict) or not _normalize(payload.get("client_email")):
        errors.append("GOOGLE_SERVICE_ACCOUNT_JSON does not include a client_email field.")


def validate_runtime_config(
    runtime_settings,
    *,
    project_root: Path | str,
    environment: Mapping[str, str] | None = None,
) -> RuntimeConfigReport:
    project_root = Path(project_root)
    errors: list[str] = []
    warnings: list[str] = []
    runtime_environment = environment or os.environ

    if _is_missing_or_placeholder(getattr(runtime_settings, "gemini_api_key", "")):
        errors.append("GEMINI_API_KEY is missing or still set to an example value.")

    if _is_missing_or_placeholder(getattr(runtime_settings, "periskope_api_key", "")):
        errors.append("PERISKOPE_API_KEY is missing or still set to an example value.")

    if _is_missing_or_placeholder(getattr(runtime_settings, "periskope_phone", "")):
        errors.append("PERISKOPE_PHONE is missing or still set to an example value.")

    if _is_missing_or_placeholder(getattr(runtime_settings, "periskope_allowed_chat_ids", "")):
        errors.append("PERISKOPE_ALLOWED_CHAT_IDS is empty; the primary webhook will reject every chat.")

    if _is_missing_or_placeholder(getattr(runtime_settings, "periskope_signing_key", "")):
        errors.append("PERISKOPE_SIGNING_KEY is empty; webhook signature verification is effectively disabled.")

    storage_path = _validate_storage_dir(
        getattr(runtime_settings, "storage_dir", ""),
        project_root=project_root,
        errors=errors,
    )
    _validate_railway_storage_alignment(storage_path, environment=runtime_environment, errors=errors)
    _validate_google_service_account(getattr(runtime_settings, "google_service_account_json", ""), errors)

    oauth_values = (
        _normalize(getattr(runtime_settings, "google_oauth_client_id", "")),
        _normalize(getattr(runtime_settings, "google_oauth_client_secret", "")),
        _normalize(getattr(runtime_settings, "google_oauth_refresh_token", "")),
    )
    if any(oauth_values) and not all(oauth_values):
        errors.append(
            "GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, and GOOGLE_OAUTH_REFRESH_TOKEN must be set together."
        )

    if not _normalize(getattr(runtime_settings, "google_drive_parent_folder_id", "")):
        warnings.append("GOOGLE_DRIVE_PARENT_FOLDER_ID is empty; Drive upload and Belge link backfill stay disabled.")

    if not _normalize(getattr(runtime_settings, "google_sheets_owner_email", "")):
        warnings.append(
            "GOOGLE_SHEETS_OWNER_EMAIL is empty; auto-created spreadsheets will not be shared with the customer's company account."
        )

    return RuntimeConfigReport(errors=errors, warnings=warnings)


def build_pytest_command(project_root: Path | str, *, python_executable: str | None = None) -> list[str]:
    del project_root
    return [python_executable or sys.executable, "-m", "pytest", "-q", "-s", *DELIVERY_SMOKE_TESTS]


def _print_report(report: RuntimeConfigReport) -> None:
    for message in report.errors:
        _emit(f"[config] FAIL {message}")
    for message in report.warnings:
        _emit(f"[config] WARN {message}")
    if not report.errors and not report.warnings:
        _emit("[config] PASS Runtime configuration covers the live hot path.")


def _default_subprocess_runner(command: list[str], *, cwd: str) -> int:
    completed = subprocess.run(command, cwd=cwd, check=False)
    return completed.returncode


def run(
    *,
    settings=app_settings,
    project_root: Path | str | None = None,
    python_executable: str | None = None,
    subprocess_runner: Callable[[list[str], str], int] | None = None,
) -> int:
    project_root = Path(project_root or Path(__file__).resolve().parents[1])
    runner = subprocess_runner or (lambda command, cwd: _default_subprocess_runner(command, cwd=cwd))

    report = validate_runtime_config(settings, project_root=project_root)
    _print_report(report)
    if report.errors:
        _emit("[delivery-smoke] FAIL Fix config blockers before running release smoke tests.")
        return 1

    command = build_pytest_command(project_root, python_executable=python_executable)
    _emit(f"[tests] RUN {' '.join(command[3:])}")
    exit_code = runner(command, str(project_root))
    if exit_code == 0:
        _emit("[delivery-smoke] PASS")
        return 0

    _emit("[delivery-smoke] FAIL Selected smoke tests failed.")
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run pre-delivery smoke checks for the primary accounting flow.")
    parser.parse_args(argv)
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
