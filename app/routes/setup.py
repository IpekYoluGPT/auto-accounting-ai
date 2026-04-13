"""
One-time Google OAuth2 setup flow.

The user visits /setup/google-auth once, authorises the app, and receives
a refresh token to paste into Railway env vars (GOOGLE_OAUTH_REFRESH_TOKEN).

This refresh token lets the system create Google Sheets files on behalf of the
user's real Google account — something service accounts cannot do.
"""

from __future__ import annotations

import base64
import binascii
from uuid import uuid4
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from app.config import settings
from app.services.accounting import intake, record_store
from app.services.accounting.pipeline_context import sandbox_context, pipeline_context_scope
from app.services.providers import google_sheets
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/setup", tags=["setup"])

# In-memory store: keeps the Flow object (with code_verifier) between redirect and callback
_oauth_flows: dict[str, object] = {}

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class ResetSheetRequest(BaseModel):
    spreadsheet_id: str | None = None
    clear_storage: bool = True


class RepairSheetRequest(BaseModel):
    spreadsheet_id: str | None = None
    refresh_formatting: bool = False
    tab_name: list[str] | None = None


class RewriteBelgeLinksRequest(BaseModel):
    spreadsheet_id: str | None = None
    tab_name: list[str] | None = None


class EnsureSandboxRequest(BaseModel):
    session_id: str | None = None


class DrainQueuesRequest(BaseModel):
    max_rounds: int = 10


class SandboxSessionRequest(BaseModel):
    session_id: str


class SandboxIntakeRequest(BaseModel):
    session_id: str
    message_id: str | None = None
    msg_type: Literal["text", "image", "document"]
    sender_id: str | None = None
    sender_name: str | None = None
    chat_id: str | None = None
    chat_type: Literal["individual", "group"] = "group"
    group_id: str | None = None
    text: str | None = None
    media_base64: str | None = None
    mime_type: str | None = None
    filename: str | None = None
    source_type: str | None = None
    attachment_url: str | None = None


class SandboxDriftRequest(BaseModel):
    session_id: str
    action: Literal[
        "reorder_rows",
        "delete_summary_tab",
        "rename_data_tab",
        "corrupt_total_row",
        "corrupt_header_row",
        "clear_hidden_row_ids",
    ]
    tab_name: str | None = None
    replacement_name: str | None = None
    row_count: int = 5


def _verify_admin_token(request: Request) -> None:
    expected = settings.periskope_tool_token.strip()
    if not expected:
        return

    auth_header = request.headers.get("authorization", "")
    api_key_header = request.headers.get("x-api-key", "")
    if auth_header == f"Bearer {expected}" or api_key_header == expected:
        return

    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid tool token.")


def _normalize_sandbox_session_id(raw_session_id: str | None) -> str:
    session_id = (raw_session_id or uuid4().hex[:12]).strip()
    if not session_id:
        session_id = uuid4().hex[:12]
    return session_id


def _sandbox_sheet_url(spreadsheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"


def _resolve_existing_sandbox_spreadsheet_id(session_id: str) -> str | None:
    context = sandbox_context(session_id=session_id)
    with pipeline_context_scope(context):
        return google_sheets._registered_spreadsheet_id_for_month(google_sheets._month_key())


def _require_existing_sandbox_context(session_id: str):
    normalized_session_id = _normalize_sandbox_session_id(session_id)
    spreadsheet_id = _resolve_existing_sandbox_spreadsheet_id(normalized_session_id)
    if not spreadsheet_id:
        raise HTTPException(status_code=404, detail="Sandbox session not found.")
    return sandbox_context(session_id=normalized_session_id), spreadsheet_id


def _ensure_sandbox_context(session_id: str | None):
    normalized_session_id = _normalize_sandbox_session_id(session_id)
    context = sandbox_context(session_id=normalized_session_id)
    existing_spreadsheet_id = _resolve_existing_sandbox_spreadsheet_id(normalized_session_id)
    with pipeline_context_scope(context):
        spreadsheet_id = google_sheets.ensure_current_month_spreadsheet_ready()
    if not spreadsheet_id:
        raise HTTPException(status_code=500, detail="Sandbox spreadsheet could not be prepared.")
    return context, spreadsheet_id, existing_spreadsheet_id is None


def _drain_sandbox_queues(context) -> dict[str, int]:
    total_sheet_appends = 0
    total_drive_uploads = 0
    with pipeline_context_scope(context):
        for _ in range(10):
            processed_sheet = google_sheets.process_pending_sheet_appends()
            processed_drive = google_sheets.process_pending_document_uploads()
            total_sheet_appends += processed_sheet
            total_drive_uploads += processed_drive
            if processed_sheet == 0 and processed_drive == 0:
                break
    return {
        "pending_sheet_appends_processed": total_sheet_appends,
        "pending_drive_uploads_processed": total_drive_uploads,
    }


def _get_redirect_uri(request: Request) -> str:
    """Build the OAuth callback URL from the incoming request."""
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.url.netloc)
    return f"{scheme}://{host}/setup/google-auth/callback"


def _build_flow(redirect_uri: str):
    """Build a google_auth_oauthlib Flow from config settings."""
    from google_auth_oauthlib.flow import Flow

    client_config = {
        "web": {
            "client_id": settings.google_oauth_client_id,
            "client_secret": settings.google_oauth_client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }
    flow = Flow.from_client_config(
        client_config,
        scopes=_SCOPES,
        redirect_uri=redirect_uri,
    )
    return flow


@router.get("/google-auth")
async def google_auth_start(request: Request):
    """Redirect the user to Google's consent screen."""
    if not settings.google_oauth_client_id or not settings.google_oauth_client_secret:
        raise HTTPException(
            status_code=500,
            detail=(
                "GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET must be set "
                "in Railway environment variables before running the OAuth setup."
            ),
        )

    redirect_uri = _get_redirect_uri(request)
    flow = _build_flow(redirect_uri)

    authorization_url, state = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
    )

    # Store the ENTIRE flow object — it contains the code_verifier needed for token exchange
    _oauth_flows[state] = flow
    logger.info("OAuth flow started — redirecting to Google consent screen.")

    return RedirectResponse(url=authorization_url)


@router.get("/google-auth/callback")
async def google_auth_callback(
    request: Request,
    code: str = Query(...),
    state: str = Query(...),
):
    """Receive the OAuth callback, exchange code for tokens, display refresh token."""

    # Retrieve the stored flow (with code_verifier intact)
    flow = _oauth_flows.pop(state, None)
    if flow is None:
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired OAuth state. Please restart the flow at /setup/google-auth",
        )

    try:
        flow.fetch_token(code=code)
    except Exception as exc:
        logger.error("OAuth token exchange failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Token exchange failed: {exc}",
        )

    credentials = flow.credentials
    refresh_token = credentials.refresh_token

    if not refresh_token:
        return HTMLResponse(
            content="""
            <html>
            <head><title>OAuth Setup - Error</title></head>
            <body style="font-family: system-ui; max-width: 600px; margin: 50px auto; padding: 20px;">
                <h1 style="color: #d32f2f;">Refresh Token Alinamadi</h1>
                <p>Google bir refresh token dondurmedi. Bu genellikle su durumlarda olur:</p>
                <ul>
                    <li>Daha once yetki vermistiniz (Google tekrar refresh token vermez)</li>
                    <li>Google Cloud projeniz "Testing" modunda</li>
                </ul>
                <p><strong>Cozum:</strong></p>
                <ol>
                    <li><a href="https://myaccount.google.com/permissions" target="_blank">Google Hesap Izinleri</a> sayfasina gidin</li>
                    <li>Bu uygulamanin erisimini kaldirin</li>
                    <li><a href="/setup/google-auth">Tekrar deneyin</a></li>
                </ol>
            </body>
            </html>
            """,
            status_code=200,
        )

    logger.info("OAuth setup completed successfully — refresh token obtained.")

    return HTMLResponse(
        content=f"""
        <html>
        <head><title>OAuth Setup - Basarili</title></head>
        <body style="font-family: system-ui; max-width: 700px; margin: 50px auto; padding: 20px;">
            <h1 style="color: #2e7d32;">OAuth Kurulumu Basarili!</h1>
            <p>Asagidaki refresh token'i Railway ortam degiskenlerine ekleyin:</p>

            <div style="background: #f5f5f5; padding: 15px; border-radius: 8px; margin: 20px 0;">
                <p style="margin: 0 0 8px 0; font-weight: bold;">Degisken Adi:</p>
                <code style="background: #e0e0e0; padding: 4px 8px; border-radius: 4px;">GOOGLE_OAUTH_REFRESH_TOKEN</code>
            </div>

            <div style="background: #f5f5f5; padding: 15px; border-radius: 8px; margin: 20px 0;">
                <p style="margin: 0 0 8px 0; font-weight: bold;">Deger:</p>
                <textarea readonly
                    onclick="this.select(); document.execCommand('copy');"
                    style="width: 100%; height: 80px; font-family: monospace; font-size: 12px; padding: 8px; border: 1px solid #ccc; border-radius: 4px; resize: vertical;"
                >{refresh_token}</textarea>
                <p style="font-size: 12px; color: #666; margin: 4px 0 0 0;">Tikla = otomatik secilir. Kopyala (Ctrl+C).</p>
            </div>

            <h2>Yapilacaklar:</h2>
            <ol>
                <li>Railway Dashboard'a gidin</li>
                <li>Projenizi secin > Variables</li>
                <li><code>GOOGLE_OAUTH_REFRESH_TOKEN</code> ekleyin ve yukaridaki degeri yapisttirin</li>
                <li>Deploy'u bekleyin (otomatik)</li>
                <li>Artik her ay otomatik spreadsheet olusturulacak!</li>
            </ol>

            <div style="background: #fff3e0; padding: 15px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #ff9800;">
                <p style="margin: 0; font-weight: bold;">Onemli Notlar:</p>
                <ul style="margin: 8px 0 0 0;">
                    <li>Bu islem sadece bir kez yapilmali</li>
                    <li>Refresh token sureleri dolmazsa tekrar yapmaniz gerekmez</li>
                    <li>Google Cloud Console'da uygulama "Testing" modundaysa token 7 gunde biter. "In production" veya "Internal" yapmaniz onerilir.</li>
                </ul>
            </div>
        </body>
        </html>
        """,
        status_code=200,
    )


@router.post("/reset-sheet")
async def reset_sheet(request: Request, payload: ResetSheetRequest) -> dict[str, object]:
    """Authenticated helper to clear test rows from the target spreadsheet."""
    _verify_admin_token(request)

    try:
        queue_before = google_sheets.queue_status() if payload.clear_storage else None
        reset_count = google_sheets.reset_current_month_spreadsheet_data(
            spreadsheet_id=payload.spreadsheet_id,
        )
        queue_cleared = google_sheets.clear_current_namespace_storage() if payload.clear_storage else None

        response = {
            "status": "ok",
            "spreadsheet_id": payload.spreadsheet_id or settings.google_sheets_spreadsheet_id,
            "tabs_reset": reset_count,
        }
        if payload.clear_storage:
            response["queue_before"] = queue_before
            response["queue_cleared"] = queue_cleared
        return response
    except Exception as exc:
        logger.error("Sheet reset failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/repair-sheet")
async def repair_sheet(request: Request, payload: RepairSheetRequest) -> dict[str, object]:
    """Authenticated helper to repair the live spreadsheet layout and formulas."""
    _verify_admin_token(request)

    try:
        target_tabs = {tab for tab in (payload.tab_name or []) if tab}
        report = google_sheets.audit_current_month_spreadsheet(
            spreadsheet_id=payload.spreadsheet_id,
            repair=True,
            target_tabs=target_tabs or None,
            refresh_formatting=payload.refresh_formatting,
        )
        response = {
            "status": "ok",
            **report,
            "sheet_url": _sandbox_sheet_url(report["spreadsheet_id"]),
        }
        if target_tabs:
            response["audited_tabs"] = sorted(target_tabs)
        return response
    except Exception as exc:
        logger.error("Sheet repair failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/rewrite-belge-links")
async def rewrite_belge_links(request: Request, payload: RewriteBelgeLinksRequest) -> dict[str, object]:
    """Authenticated helper to force-rewrite visible Belge formulas on the live spreadsheet."""
    _verify_admin_token(request)

    try:
        target_tabs = {tab for tab in (payload.tab_name or []) if tab}
        rewritten = google_sheets.force_rewrite_drive_links(
            spreadsheet_id=payload.spreadsheet_id,
            target_tabs=target_tabs or None,
        )
        spreadsheet_id = payload.spreadsheet_id or settings.google_sheets_spreadsheet_id
        response = {
            "status": "ok",
            "spreadsheet_id": spreadsheet_id,
            "rewritten_tabs": rewritten,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
        }
        if target_tabs:
            response["audited_tabs"] = sorted(target_tabs)
        return response
    except Exception as exc:
        logger.error("Belge link rewrite failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/drain-queues")
async def drain_queues(request: Request, payload: DrainQueuesRequest) -> dict[str, object]:
    """Authenticated helper to drain production queue workers on demand."""
    _verify_admin_token(request)

    try:
        queue_before = google_sheets.queue_status()
        processed_sheet = 0
        processed_drive = 0
        rounds = max(1, min(int(payload.max_rounds or 10), 20))

        for _ in range(rounds):
            sheet_count = google_sheets.process_pending_sheet_appends()
            drive_count = google_sheets.process_pending_document_uploads()
            processed_sheet += sheet_count
            processed_drive += drive_count
            if sheet_count == 0 and drive_count == 0:
                break

        queue_after = google_sheets.queue_status()
        return {
            "status": "ok",
            "queue_before": queue_before,
            "drain": {
                "pending_sheet_appends_processed": processed_sheet,
                "pending_drive_uploads_processed": processed_drive,
            },
            "queue_after": queue_after,
            "rounds": rounds,
        }
    except Exception as exc:
        logger.error("Queue drain failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sandbox/ensure")
async def ensure_sandbox(request: Request, payload: EnsureSandboxRequest) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        context, spreadsheet_id, created = _ensure_sandbox_context(payload.session_id)
        return {
            "status": "ok",
            "session_id": context.session_id,
            "namespace": context.normalized_namespace,
            "spreadsheet_id": spreadsheet_id,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
            "month_key": google_sheets._month_key(),
            "created": created,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Sandbox ensure failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sandbox/intake")
async def sandbox_intake(request: Request, payload: SandboxIntakeRequest) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        context, spreadsheet_id, _ = _ensure_sandbox_context(payload.session_id)
        session_id = context.session_id or _normalize_sandbox_session_id(payload.session_id)
        chat_type = payload.chat_type
        default_chat_id = f"sandbox-{session_id}@g.us" if chat_type == "group" else f"sandbox-{session_id}@c.us"
        chat_id = (payload.chat_id or default_chat_id).strip()
        sender_id = (payload.sender_id or "sandbox-user@c.us").strip()
        message_id = (payload.message_id or f"sandbox-{session_id}-{uuid4().hex[:12]}").strip()
        source_type = (payload.source_type or ("sandbox_text" if payload.msg_type == "text" else "sandbox_media")).strip()

        media_bytes: bytes | None = None
        if payload.msg_type == "text":
            if not (payload.text or "").strip():
                raise HTTPException(status_code=422, detail="text is required when msg_type=text")
        else:
            if not payload.media_base64 or not payload.mime_type or not payload.filename:
                raise HTTPException(status_code=422, detail="media_base64, mime_type, and filename are required for media intake")
            try:
                media_bytes = base64.b64decode(payload.media_base64, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise HTTPException(status_code=400, detail="Invalid media_base64 payload.") from exc

        route = intake.MessageRoute(
            platform="periskope",
            sender_id=sender_id,
            sender_name=payload.sender_name,
            chat_id=chat_id,
            chat_type=chat_type,
            recipient_type="sandbox",
            group_id=payload.group_id if chat_type == "group" else None,
        )

        with pipeline_context_scope(context):
            before_rows = len(record_store.find_export_rows(source_message_id=message_id, limit=1000, context=context))

        outcome = intake.process_incoming_message(
            message_id=message_id,
            msg_type=payload.msg_type,
            route=route,
            text=payload.text,
            fetch_media=(lambda: media_bytes) if media_bytes is not None else None,
            mime_type=payload.mime_type,
            filename=payload.filename,
            source_type=source_type,
            attachment_url=payload.attachment_url,
            send_text=lambda route, text: None,
            send_reaction=lambda route, emoji: None,
            context=context,
        )

        drain = _drain_sandbox_queues(context)
        with pipeline_context_scope(context):
            after_rows = record_store.find_export_rows(source_message_id=message_id, limit=1000, context=context)
            queue = google_sheets.queue_status()

        return {
            "status": "ok",
            "session_id": context.session_id,
            "spreadsheet_id": spreadsheet_id,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
            "message_id": message_id,
            "outcome": outcome,
            "record_count": max(len(after_rows) - before_rows, 0),
            "recent_rows": after_rows[:5],
            "queue": queue,
            "drain": drain,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Sandbox intake failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sandbox/audit")
async def sandbox_audit(
    request: Request,
    session_id: str = Query(...),
    repair: bool = Query(default=True),
    tab_name: list[str] | None = Query(default=None),
) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        context, spreadsheet_id = _require_existing_sandbox_context(session_id)
        target_tabs = {tab for tab in (tab_name or []) if tab}
        with pipeline_context_scope(context):
            report = google_sheets.audit_current_month_spreadsheet(
                spreadsheet_id=spreadsheet_id,
                repair=repair,
                target_tabs=target_tabs or None,
                refresh_formatting=repair,
            )
        return {
            "status": "ok",
            "session_id": context.session_id,
            **report,
            "audited_tabs": sorted(target_tabs) if target_tabs else None,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Sandbox audit failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sandbox/drift")
async def sandbox_drift(request: Request, payload: SandboxDriftRequest) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        context, spreadsheet_id = _require_existing_sandbox_context(payload.session_id)
        with pipeline_context_scope(context):
            details = google_sheets.apply_test_drift(
                action=payload.action,
                spreadsheet_id=spreadsheet_id,
                tab_name=payload.tab_name,
                replacement_name=payload.replacement_name,
                row_count=payload.row_count,
            )
            recommended_audit_tabs = google_sheets.recommended_audit_tabs_for_test_drift(
                action=payload.action,
                tab_name=payload.tab_name,
            )
        return {
            "status": "ok",
            "session_id": context.session_id,
            **details,
            "recommended_audit_tabs": recommended_audit_tabs,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Sandbox drift failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sandbox/reset")
async def sandbox_reset(request: Request, payload: SandboxSessionRequest) -> dict[str, object]:
    _verify_admin_token(request)

    try:
        context, spreadsheet_id = _require_existing_sandbox_context(payload.session_id)
        with pipeline_context_scope(context):
            queue_before = google_sheets.queue_status()
            tabs_reset = google_sheets.reset_current_month_spreadsheet_data(spreadsheet_id=spreadsheet_id)
            cleared = google_sheets.clear_current_namespace_storage()

        reseed_context = sandbox_context(session_id=context.session_id or payload.session_id, spreadsheet_id_override=spreadsheet_id)
        with pipeline_context_scope(reseed_context):
            google_sheets.ensure_current_month_spreadsheet_ready()

        return {
            "status": "ok",
            "session_id": context.session_id,
            "spreadsheet_id": spreadsheet_id,
            "sheet_url": _sandbox_sheet_url(spreadsheet_id),
            "tabs_reset": tabs_reset,
            "queue_before": queue_before,
            "queue_cleared": cleared,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Sandbox reset failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
