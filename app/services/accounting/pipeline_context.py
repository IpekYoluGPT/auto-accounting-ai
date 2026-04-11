"""
Runtime context helpers for routing pipeline state into production or sandbox namespaces.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterator

_DEFAULT_NAMESPACE = "production"
_NAMESPACE_RE = re.compile(r"[^a-zA-Z0-9._-]+")


@dataclass(frozen=True)
class PipelineContext:
    namespace: str = _DEFAULT_NAMESPACE
    session_id: str | None = None
    spreadsheet_id_override: str | None = None
    disable_outbound_messages: bool = False

    @property
    def normalized_namespace(self) -> str:
        namespace = _sanitize_namespace(self.namespace)
        return namespace or _DEFAULT_NAMESPACE

    @property
    def is_production(self) -> bool:
        return self.normalized_namespace == _DEFAULT_NAMESPACE


_CURRENT_PIPELINE_CONTEXT: ContextVar[PipelineContext] = ContextVar(
    "current_pipeline_context",
    default=PipelineContext(),
)


def _sanitize_namespace(namespace: str | None) -> str:
    if not namespace:
        return _DEFAULT_NAMESPACE
    collapsed = _NAMESPACE_RE.sub("-", str(namespace).strip()).strip("-._")
    return collapsed or _DEFAULT_NAMESPACE


def production_context() -> PipelineContext:
    return PipelineContext()


def sandbox_context(*, session_id: str, spreadsheet_id_override: str | None = None) -> PipelineContext:
    session = _sanitize_namespace(session_id)
    return PipelineContext(
        namespace=f"sandbox-{session}",
        session_id=session,
        spreadsheet_id_override=(spreadsheet_id_override or "").strip() or None,
        disable_outbound_messages=True,
    )


def resolve_pipeline_context(context: PipelineContext | None = None) -> PipelineContext:
    if context is None:
        return _CURRENT_PIPELINE_CONTEXT.get()
    namespace = _sanitize_namespace(context.namespace)
    return PipelineContext(
        namespace=namespace,
        session_id=(context.session_id or "").strip() or None,
        spreadsheet_id_override=(context.spreadsheet_id_override or "").strip() or None,
        disable_outbound_messages=bool(context.disable_outbound_messages),
    )


@contextmanager
def pipeline_context_scope(context: PipelineContext | None = None) -> Iterator[PipelineContext]:
    resolved = resolve_pipeline_context(context)
    token = _CURRENT_PIPELINE_CONTEXT.set(resolved)
    try:
        yield resolved
    finally:
        _CURRENT_PIPELINE_CONTEXT.reset(token)


def current_pipeline_context() -> PipelineContext:
    return resolve_pipeline_context()


def namespace_storage_root(base_dir: str | Path, context: PipelineContext | None = None) -> Path:
    root = Path(base_dir)
    resolved = resolve_pipeline_context(context)
    if resolved.is_production:
        return root
    return root / "sandboxes" / resolved.normalized_namespace
