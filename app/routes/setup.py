"""Stable aggregator for the `/setup` route surface."""

from __future__ import annotations

from fastapi import APIRouter

from . import setup_admin, setup_oauth, setup_sandbox

router = APIRouter(prefix="/setup")
router.include_router(setup_oauth.router)
router.include_router(setup_admin.router)
router.include_router(setup_sandbox.router)

# Re-export the prior module surface for compatibility with existing imports.
settings = setup_admin.settings
google_sheets = setup_admin.google_sheets
inbound_queue = setup_admin.inbound_queue
intake = setup_sandbox.intake
record_store = setup_sandbox.record_store
sandbox_context = setup_sandbox.sandbox_context
pipeline_context_scope = setup_sandbox.pipeline_context_scope

ResetSheetRequest = setup_admin.ResetSheetRequest
RepairSheetRequest = setup_admin.RepairSheetRequest
RewriteBelgeLinksRequest = setup_admin.RewriteBelgeLinksRequest
HideHiddenTabsRequest = setup_admin.HideHiddenTabsRequest
DrainQueuesRequest = setup_admin.DrainQueuesRequest
EnsureSandboxRequest = setup_sandbox.EnsureSandboxRequest
SandboxSessionRequest = setup_sandbox.SandboxSessionRequest
SandboxIntakeRequest = setup_sandbox.SandboxIntakeRequest
SandboxDriftRequest = setup_sandbox.SandboxDriftRequest

_verify_admin_token = setup_admin._verify_admin_token
_sandbox_sheet_url = setup_admin._sandbox_sheet_url
_oauth_flows = setup_oauth._oauth_flows
_SCOPES = setup_oauth._SCOPES
_get_redirect_uri = setup_oauth._get_redirect_uri
_build_flow = setup_oauth._build_flow
_normalize_sandbox_session_id = setup_sandbox._normalize_sandbox_session_id
_resolve_existing_sandbox_spreadsheet_id = setup_sandbox._resolve_existing_sandbox_spreadsheet_id
_require_existing_sandbox_context = setup_sandbox._require_existing_sandbox_context
_ensure_sandbox_context = setup_sandbox._ensure_sandbox_context
_drain_sandbox_queues = setup_sandbox._drain_sandbox_queues

google_auth_start = setup_oauth.google_auth_start
google_auth_callback = setup_oauth.google_auth_callback
reset_sheet = setup_admin.reset_sheet
repair_sheet = setup_admin.repair_sheet
rewrite_belge_links = setup_admin.rewrite_belge_links
hide_hidden_tabs = setup_admin.hide_hidden_tabs
drain_queues = setup_admin.drain_queues
retry_inbound = setup_admin.retry_inbound
reset_inbound_queue = setup_admin.reset_inbound_queue
storage_status = setup_admin.storage_status
ensure_sandbox = setup_sandbox.ensure_sandbox
sandbox_intake = setup_sandbox.sandbox_intake
sandbox_audit = setup_sandbox.sandbox_audit
sandbox_drift = setup_sandbox.sandbox_drift
sandbox_reset = setup_sandbox.sandbox_reset

__all__ = [
    "router",
    "settings",
    "google_sheets",
    "inbound_queue",
    "intake",
    "record_store",
    "sandbox_context",
    "pipeline_context_scope",
    "ResetSheetRequest",
    "RepairSheetRequest",
    "RewriteBelgeLinksRequest",
    "HideHiddenTabsRequest",
    "DrainQueuesRequest",
    "EnsureSandboxRequest",
    "SandboxSessionRequest",
    "SandboxIntakeRequest",
    "SandboxDriftRequest",
    "_verify_admin_token",
    "_sandbox_sheet_url",
    "_oauth_flows",
    "_SCOPES",
    "_get_redirect_uri",
    "_build_flow",
    "_normalize_sandbox_session_id",
    "_resolve_existing_sandbox_spreadsheet_id",
    "_require_existing_sandbox_context",
    "_ensure_sandbox_context",
    "_drain_sandbox_queues",
    "google_auth_start",
    "google_auth_callback",
    "reset_sheet",
    "repair_sheet",
    "rewrite_belge_links",
    "hide_hidden_tabs",
    "drain_queues",
    "retry_inbound",
    "reset_inbound_queue",
    "storage_status",
    "ensure_sandbox",
    "sandbox_intake",
    "sandbox_audit",
    "sandbox_drift",
    "sandbox_reset",
]
