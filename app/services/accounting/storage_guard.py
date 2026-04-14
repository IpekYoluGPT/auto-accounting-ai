"""Disk pressure and transient-storage cleanup helpers."""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from app.config import settings
from app.services.accounting.pipeline_context import PipelineContext, namespace_storage_root, pipeline_context_scope
from app.utils.logging import get_logger

logger = get_logger(__name__)

_ORPHAN_PAYLOAD_MAX_AGE = timedelta(hours=24)
_FAILURE_RETENTION = timedelta(days=7)


@dataclass(frozen=True)
class StorageSnapshot:
    disk_total_bytes: int
    disk_used_bytes: int
    disk_free_bytes: int
    total_managed_storage_bytes: int
    inbound_payload_storage_bytes: int
    disk_pressure_state: str

    def as_dict(self) -> dict[str, int | str]:
        return asdict(self)


def _base_storage_path() -> Path:
    path = Path(settings.storage_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def managed_storage_root(*, context: PipelineContext | None = None) -> Path:
    root = namespace_storage_root(_base_storage_path(), context)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _state_dir(*, context: PipelineContext | None = None) -> Path:
    path = managed_storage_root(context=context) / "state"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _pending_inbound_dir(*, context: PipelineContext | None = None) -> Path:
    path = _state_dir(context=context) / "pending_inbound_jobs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_json_load(path: Path) -> object:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Ignoring malformed transient state file at %s", path)
        return []


def _normalize_path(path_raw: str) -> str:
    raw = (path_raw or "").strip()
    if not raw:
        return ""
    try:
        return str(Path(raw).resolve())
    except Exception:
        return raw


def _extract_referenced_paths(items: object, keys: Iterable[str]) -> set[str]:
    if not isinstance(items, list):
        return set()
    referenced: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in keys:
            normalized = _normalize_path(str(item.get(key) or ""))
            if normalized:
                referenced.add(normalized)
    return referenced


def _referenced_payload_paths(*, context: PipelineContext | None = None) -> set[str]:
    state_dir = _state_dir(context=context)
    referenced: set[str] = set()
    referenced |= _extract_referenced_paths(
        _safe_json_load(state_dir / "pending_inbound_jobs.json"),
        ("payload_path",),
    )
    referenced |= _extract_referenced_paths(
        _safe_json_load(state_dir / "pending_sheet_appends.json"),
        ("document_payload_path",),
    )
    referenced |= _extract_referenced_paths(
        _safe_json_load(state_dir / "pending_drive_uploads.json"),
        ("payload_path",),
    )
    return referenced


def _path_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return path.stat().st_size
        except FileNotFoundError:
            return 0

    total = 0
    for child in path.rglob("*"):
        try:
            if child.is_file():
                total += child.stat().st_size
        except FileNotFoundError:
            continue
    return total


def total_managed_storage_bytes(*, context: PipelineContext | None = None) -> int:
    with pipeline_context_scope(context):
        return _path_size(managed_storage_root())


def inbound_payload_storage_bytes(*, context: PipelineContext | None = None) -> int:
    with pipeline_context_scope(context):
        return _path_size(_pending_inbound_dir())


def classify_disk_pressure(*, managed_storage_bytes: int, disk_free_bytes: int) -> str:
    if managed_storage_bytes >= int(settings.storage_emergency_stop_bytes):
        return "emergency_stop"
    if managed_storage_bytes >= int(settings.storage_hard_reject_bytes) or disk_free_bytes < int(settings.storage_min_free_bytes):
        return "hard_reject"
    if managed_storage_bytes >= int(settings.storage_soft_pressure_bytes):
        return "soft_pressure"
    return "normal"


def storage_snapshot(*, context: PipelineContext | None = None) -> StorageSnapshot:
    with pipeline_context_scope(context):
        base_path = _base_storage_path()
        usage = shutil.disk_usage(base_path)
        managed_bytes = total_managed_storage_bytes()
        return StorageSnapshot(
            disk_total_bytes=int(usage.total),
            disk_used_bytes=int(usage.used),
            disk_free_bytes=int(usage.free),
            total_managed_storage_bytes=managed_bytes,
            inbound_payload_storage_bytes=inbound_payload_storage_bytes(),
            disk_pressure_state=classify_disk_pressure(
                managed_storage_bytes=managed_bytes,
                disk_free_bytes=int(usage.free),
            ),
        )


def should_reject_new_media_jobs(*, context: PipelineContext | None = None) -> bool:
    state = storage_snapshot(context=context).disk_pressure_state
    return state in {"hard_reject", "emergency_stop"}


def should_stop_payload_writes(*, context: PipelineContext | None = None) -> bool:
    state = storage_snapshot(context=context).disk_pressure_state
    return state in {"hard_reject", "emergency_stop"}


def prune_stale_transient_storage(*, context: PipelineContext | None = None) -> dict[str, int]:
    with pipeline_context_scope(context):
        state_dir = _state_dir()
        referenced = _referenced_payload_paths()
        cutoff = time.time() - _ORPHAN_PAYLOAD_MAX_AGE.total_seconds()
        removed_payloads = 0

        for directory_name in ("pending_inbound_jobs", "pending_sheet_appends", "pending_drive_uploads"):
            directory = state_dir / directory_name
            if not directory.exists():
                continue
            for path in directory.glob("*.bin"):
                normalized = _normalize_path(str(path))
                if normalized in referenced:
                    continue
                try:
                    stat = path.stat()
                except FileNotFoundError:
                    continue
                if stat.st_mtime > cutoff:
                    continue
                path.unlink(missing_ok=True)
                removed_payloads += 1

        failures_path = state_dir / "inbound_failures.json"
        pruned_failures = 0
        raw_failures = _safe_json_load(failures_path)
        if isinstance(raw_failures, list):
            keep_failures: list[dict] = []
            cutoff_dt = datetime.now(timezone.utc) - _FAILURE_RETENTION
            for item in raw_failures:
                if not isinstance(item, dict):
                    pruned_failures += 1
                    continue
                failed_at_raw = str(item.get("failed_at") or item.get("created_at") or "").strip()
                try:
                    failed_at = datetime.fromisoformat(failed_at_raw)
                except ValueError:
                    pruned_failures += 1
                    continue
                if failed_at.tzinfo is None:
                    failed_at = failed_at.replace(tzinfo=timezone.utc)
                if failed_at < cutoff_dt:
                    pruned_failures += 1
                    continue
                keep_failures.append(item)

            if pruned_failures:
                failures_path.write_text(
                    json.dumps(keep_failures, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

        if removed_payloads or pruned_failures:
            logger.info(
                "Pruned transient storage in namespace=%s payloads=%d failures=%d",
                PipelineContext().normalized_namespace if context is None else context.normalized_namespace,
                removed_payloads,
                pruned_failures,
            )

        return {
            "removed_orphan_payloads": removed_payloads,
            "pruned_failure_entries": pruned_failures,
        }
