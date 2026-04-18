"""
Lifespan and worker coordination helpers.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from app.config import DEFAULT_GEMINI_MODEL, settings
from app.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class WorkerCoordinator:
    start_workers: Callable[[], None]
    stop_workers: Callable[[], None]


def build_lifespan(
    *,
    worker_coordinator: WorkerCoordinator,
    start_bootstrap: Callable[[], object],
):
    @asynccontextmanager
    async def lifespan(app):
        del app
        logger.info("Auto Accounting AI started.")
        railway_volume_mount_path = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
        if os.getenv("RAILWAY_SERVICE_ID"):
            storage_path = Path(settings.storage_dir).resolve()
            if not railway_volume_mount_path:
                logger.warning(
                    "Railway volume mount path is not configured; storage at %s will be ephemeral.",
                    storage_path,
                )
            else:
                volume_path = Path(railway_volume_mount_path).resolve()
                if storage_path != volume_path and volume_path not in storage_path.parents:
                    logger.warning(
                        "STORAGE_DIR=%s is outside Railway volume mount path %s; queue persistence will not survive redeploys.",
                        storage_path,
                        volume_path,
                    )
                else:
                    logger.info("Using Railway volume-backed storage at %s.", storage_path)
        configured_models = {
            "classifier": settings.gemini_classifier_model,
            "extractor": settings.gemini_extractor_model,
            "validation": settings.gemini_validation_model,
        }
        non_default_models = {
            name: model for name, model in configured_models.items() if model != DEFAULT_GEMINI_MODEL
        }
        if non_default_models:
            logger.warning(
                "Gemini model override detected; expected %s but got %s",
                DEFAULT_GEMINI_MODEL,
                non_default_models,
            )
        if not settings.periskope_signing_key:
            logger.warning("PERISKOPE_SIGNING_KEY is not configured; webhook signature verification will be skipped.")
        worker_coordinator.start_workers()
        start_bootstrap()
        try:
            yield
        finally:
            worker_coordinator.stop_workers()

    return lifespan
