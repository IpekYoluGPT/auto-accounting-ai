"""
One-time Google OAuth2 setup flow.

The user visits /setup/google-auth once, authorises the app, and receives
a refresh token to paste into Railway env vars (GOOGLE_OAUTH_REFRESH_TOKEN).

This refresh token lets the system create Google Sheets files on behalf of the
user's real Google account — something service accounts cannot do.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from app.config import settings
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


def _verify_admin_token(request: Request) -> None:
    expected = settings.periskope_tool_token.strip()
    if not expected:
        return

    auth_header = request.headers.get("authorization", "")
    api_key_header = request.headers.get("x-api-key", "")
    if auth_header == f"Bearer {expected}" or api_key_header == expected:
        return

    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid tool token.")


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
        reset_count = google_sheets.reset_current_month_spreadsheet_data(
            spreadsheet_id=payload.spreadsheet_id,
        )
        return {
            "status": "ok",
            "spreadsheet_id": payload.spreadsheet_id or settings.google_sheets_spreadsheet_id,
            "tabs_reset": reset_count,
        }
    except Exception as exc:
        logger.error("Sheet reset failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
