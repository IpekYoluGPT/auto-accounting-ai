"""
Google Sheets integration for the accounting pipeline.

Monthly sheet management:
  1. sheets_registry.json tracks month → spreadsheet_id
  2. If current month is in registry → use that sheet
  3. If GOOGLE_SHEETS_SPREADSHEET_ID is set → seed this month's entry
  4. If neither → auto-create in GOOGLE_DRIVE_PARENT_FOLDER_ID

Tab names use emoji for visual clarity.
Backwards-compatible: plain-name tabs are renamed to emoji versions on first access.
"""

from __future__ import annotations

import base64
import hashlib
import json
import shutil
import ssl
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.config import settings
from app.models.schemas import BillRecord, DocumentCategory
from app.services.accounting.pipeline_context import PipelineContext, current_pipeline_context, namespace_storage_root, pipeline_context_scope
from app.utils.logging import get_logger

logger = get_logger(__name__)

# ─── Tab definitions ──────────────────────────────────────────────────────────

# Tab name (with emoji) → (header row, header background colour as RGB 0–1)
_TABS: dict[str, tuple[list[str], dict]] = {
    "📊 Özet": (
        [],  # summary tab — content set by _setup_summary_tab
        {"red": 0.13, "green": 0.13, "blue": 0.13},
    ),
    "🧾 Faturalar": (
        ["#", "Tarih", "Saat", "Firma Adı", "Vergi No", "Vergi Dairesi",
         "Fatura No", "KDVsiz Tutar", "KDV %", "KDV Tutarı", "GENEL TOPLAM",
         "Ödeme Yöntemi", "Gider Kategorisi", "Açıklama", "Notlar", "📎 Belge"],
        {"red": 0.16, "green": 0.38, "blue": 0.74},
    ),
    "💳 Dekontlar": (
        ["#", "Tarih", "Saat", "Banka / Firma", "Referans No",
         "Gönderen", "Alıcı / Açıklama", "TUTAR", "Para Birimi", "Notlar", "📎 Belge"],
        {"red": 0.13, "green": 0.55, "blue": 0.13},
    ),
    "⛽ Harcama Fişleri": (
        ["#", "Tarih", "Saat", "Firma", "Fiş No", "Vergi No",
         "KDVsiz", "KDV %", "KDV", "TOPLAM", "Ödeme", "Kategori",
         "Açıklama", "Plaka", "📎 Belge"],
        {"red": 0.90, "green": 0.49, "blue": 0.13},
    ),
    "📝 Çekler": (
        ["#", "Çek / Belge No", "Düzenleyen Firma", "Vergi No",
         "Lehdar (Alıcı)", "Vade Tarihi", "TUTAR", "Para Birimi",
         "Açıklama", "📎 Belge"],
        {"red": 0.76, "green": 0.09, "blue": 0.09},
    ),
    "💵 Elden Ödemeler": (
        ["#", "Tarih", "Saat", "Alıcı / Açıklama", "TUTAR", "Para Birimi", "Kaydeden", "📎 Belge"],
        {"red": 0.46, "green": 0.11, "blue": 0.64},
    ),
    "🏗️ Malzeme": (
        ["#", "Tarih", "Firma", "İrsaliye / Belge No", "Malzeme Cinsi",
         "Miktar", "Birim", "Teslim Yeri", "Plaka", "Tutar",
         "Açıklama", "📎 Belge"],
        {"red": 0.47, "green": 0.27, "blue": 0.08},
    ),
    "↩️ İadeler": (
        ["#", "Tarih", "Belge Türü", "Firma", "Belge No",
         "TUTAR", "Para Birimi", "Açıklama", "📎 Belge"],
        {"red": 0.44, "green": 0.44, "blue": 0.44},
    ),
}

# Category → tab name (with emoji)
_CATEGORY_TAB: dict[DocumentCategory, str] = {
    DocumentCategory.FATURA: "🧾 Faturalar",
    DocumentCategory.ODEME_DEKONTU: "💳 Dekontlar",
    DocumentCategory.HARCAMA_FISI: "⛽ Harcama Fişleri",
    DocumentCategory.CEK: "📝 Çekler",
    DocumentCategory.ELDEN_ODEME: "💵 Elden Ödemeler",
    DocumentCategory.MALZEME: "🏗️ Malzeme",
    DocumentCategory.IADE: "↩️ İadeler",
    DocumentCategory.BELIRSIZ: "🧾 Faturalar",
}

# Plain name → emoji name (for backwards-compat renaming)
_PLAIN_TO_EMOJI: dict[str, str] = {
    "Özet": "📊 Özet",
    "Faturalar": "🧾 Faturalar",
    "Dekontlar": "💳 Dekontlar",
    "Harcama Fişleri": "⛽ Harcama Fişleri",
    "Çekler": "📝 Çekler",
    "Elden Ödemeler": "💵 Elden Ödemeler",
    "Malzeme": "🏗️ Malzeme",
    "İadeler": "↩️ İadeler",
}

_TAB_TOTAL_COLUMNS: dict[str, str] = {
    "🧾 Faturalar": "K",
    "💳 Dekontlar": "H",
    "⛽ Harcama Fişleri": "J",
    "📝 Çekler": "G",
    "💵 Elden Ödemeler": "E",
    "🏗️ Malzeme": "J",
    "↩️ İadeler": "F",
}

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ─── Column width map (pixels) ────────────────────────────────────────────────

_COL_WIDTHS: dict[str, int] = {
    "#": 38,
    "Tarih": 90,
    "Saat": 58,
    "Firma Adı": 210,
    "Firma": 210,
    "Banka / Firma": 210,
    "Düzenleyen Firma": 210,
    "Vergi No": 105,
    "Vergi Dairesi": 120,
    "Fatura No": 100,
    "İrsaliye / Belge No": 130,
    "Çek / Belge No": 120,
    "Referans No": 120,
    "Belge No": 100,
    "Belge Türü": 120,
    "KDVsiz Tutar": 110,
    "KDVsiz": 90,
    "KDV %": 58,
    "KDV Oranı": 70,
    "KDV Tutarı": 105,
    "KDV": 80,
    "GENEL TOPLAM": 125,
    "TOPLAM": 110,
    "TUTAR": 110,
    "Tutar": 110,
    "Para Birimi": 78,
    "Ödeme Yöntemi": 115,
    "Ödeme": 100,
    "Gider Kategorisi": 130,
    "Kategori": 110,
    "Açıklama": 260,
    "Alıcı / Açıklama": 240,
    "Notlar": 200,
    "Malzeme Cinsi": 220,
    "Teslim Yeri": 180,
    "Plaka": 75,
    "Miktar": 68,
    "Birim": 58,
    "Gönderen": 130,
    "Lehdar (Alıcı)": 150,
    "Vade Tarihi": 90,
    "Kaydeden": 130,
    "📎 Belge": 48,
}

# Columns that should wrap text (long free-text fields)
_WRAP_COLUMNS = {
    "Açıklama", "Alıcı / Açıklama", "Notlar",
    "Malzeme Cinsi", "Teslim Yeri",
}

# Columns that hold monetary amounts (get number formatting)
_AMOUNT_COLUMNS = {
    "KDVsiz Tutar", "KDVsiz", "KDV Tutarı", "KDV",
    "GENEL TOPLAM", "TOPLAM", "TUTAR", "Tutar",
}

_lock = threading.Lock()
_drive_upload_lock = threading.Lock()
_pending_drive_uploads_lock = threading.Lock()
_pending_sheet_appends_lock = threading.Lock()
_gspread_client = None  # lazy-initialised
_drive_service = None   # lazy-initialised (service account)
_sheets_service = None  # lazy-initialised (Sheets API v4, service account)
_creds = None           # service account credentials
_drive_folder_cache: dict[str, str] = {}  # drive folder name → folder_id

# OAuth2 user credentials — used ONLY for creating new files (spreadsheets).
# Service accounts cannot create Workspace files (403 quota/permission error).
_oauth_creds = None
_oauth_drive_service = None
_oauth_sheets_service = None
_rollover_thread: threading.Thread | None = None
_rollover_stop_event: threading.Event | None = None
_rollover_lock = threading.Lock()
_pending_drive_worker_thread: threading.Thread | None = None
_pending_drive_worker_lock = threading.Lock()
_pending_sheet_worker_thread: threading.Thread | None = None
_pending_sheet_worker_lock = threading.Lock()
_recently_prepared_spreadsheets: dict[str, float] = {}
_RECENT_PREPARED_TTL_SECONDS = 180.0
_PENDING_DRIVE_WORKER_DELAY_SECONDS = 15.0
_PENDING_SHEET_WORKER_RETRY_DELAY_SECONDS = 30.0
_PENDING_SHEET_BATCH_SIZE = 25


def _get_business_timezone():
    timezone_name = settings.business_timezone.strip() or "Europe/Istanbul"
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        logger.warning(
            "Unknown BUSINESS_TIMEZONE '%s'; falling back to UTC for monthly rollover.",
            timezone_name,
        )
        return timezone.utc


def _now() -> datetime:
    return datetime.now(_get_business_timezone())


def _storage_root() -> Path:
    path = namespace_storage_root(settings.storage_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _sandbox_name_prefix() -> str:
    context = current_pipeline_context()
    if context.is_production:
        return ""
    session = (context.session_id or context.normalized_namespace).strip()
    return f"[SANDBOX {session}] "


def _month_sheet_title() -> str:
    return f"{_sandbox_name_prefix()}Muhasebe — {_month_label()}"


def _next_month_rollover_at(now: datetime | None = None) -> datetime:
    current = now or _now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=_get_business_timezone())

    year = current.year
    month = current.month + 1
    if month == 13:
        month = 1
        year += 1

    return current.replace(
        year=year,
        month=month,
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )


def _seconds_until_next_month_rollover(now: datetime | None = None) -> float:
    current = now or _now()
    rollover_at = _next_month_rollover_at(current)
    return max((rollover_at - current).total_seconds(), 1.0)


# ─── Client initialisation ────────────────────────────────────────────────────


def _get_client():
    global _gspread_client, _creds
    if _gspread_client is not None:
        return _gspread_client
    if not settings.google_service_account_json:
        logger.debug("GOOGLE_SERVICE_ACCOUNT_JSON not set; Google Sheets disabled.")
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        raw_json = base64.b64decode(settings.google_service_account_json).decode("utf-8")
        creds_dict = json.loads(raw_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=_SCOPES)
        _creds = creds  # store for Drive service reuse
        _gspread_client = gspread.authorize(creds)
        logger.info(
            "Google Sheets client initialised (service account: %s)",
            creds_dict.get("client_email", "?"),
        )
        return _gspread_client
    except Exception as exc:
        logger.error("Failed to initialise Google Sheets client: %s", exc, exc_info=True)
        return None


def _get_drive_service(*, force_refresh: bool = False):
    """Return a Google Drive API v3 service, sharing credentials with gspread."""
    global _drive_service
    if force_refresh:
        _drive_service = None
    if _drive_service is not None:
        return _drive_service
    _get_client()  # ensure _creds is populated
    if _creds is None:
        return None
    try:
        from googleapiclient.discovery import build
        _drive_service = build("drive", "v3", credentials=_creds, cache_discovery=False)
        logger.debug("Google Drive service initialised.")
        return _drive_service
    except Exception as exc:
        logger.error("Failed to initialise Drive service: %s", exc, exc_info=True)
        return None


def _get_sheets_service():
    """Return a Google Sheets API v4 service.

    Used to CREATE spreadsheets — the Sheets API creates native Workspace files
    which do not require Drive storage quota (unlike the Drive Files API).
    """
    global _sheets_service
    if _sheets_service is not None:
        return _sheets_service
    _get_client()  # ensure _creds is populated
    if _creds is None:
        return None
    try:
        from googleapiclient.discovery import build
        _sheets_service = build("sheets", "v4", credentials=_creds, cache_discovery=False)
        logger.debug("Google Sheets API service initialised.")
        return _sheets_service
    except Exception as exc:
        logger.error("Failed to initialise Sheets API service: %s", exc, exc_info=True)
        return None


def _get_oauth_creds():
    """Build OAuth2 user credentials from the stored refresh token.

    These credentials are used ONLY for creating new files (spreadsheets, folders)
    because service accounts cannot create Google Workspace files.
    """
    global _oauth_creds
    if _oauth_creds is not None:
        return _oauth_creds

    if (
        not settings.google_oauth_client_id
        or not settings.google_oauth_client_secret
        or not settings.google_oauth_refresh_token
    ):
        return None

    try:
        from google.oauth2.credentials import Credentials

        _oauth_creds = Credentials(
            token=None,
            refresh_token=settings.google_oauth_refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=settings.google_oauth_client_id,
            client_secret=settings.google_oauth_client_secret,
            scopes=_SCOPES,
        )
        logger.info("OAuth2 user credentials initialised for file creation.")
        return _oauth_creds
    except Exception as exc:
        logger.error("Failed to build OAuth2 credentials: %s", exc, exc_info=True)
        return None


def _get_oauth_drive_service(*, force_refresh: bool = False):
    """Return a Google Drive API v3 service using OAuth2 user credentials."""
    global _oauth_drive_service
    if force_refresh:
        _oauth_drive_service = None
    if _oauth_drive_service is not None:
        return _oauth_drive_service
    creds = _get_oauth_creds()
    if creds is None:
        return None
    try:
        from googleapiclient.discovery import build
        _oauth_drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
        logger.debug("OAuth Drive service initialised.")
        return _oauth_drive_service
    except Exception as exc:
        logger.error("Failed to initialise OAuth Drive service: %s", exc, exc_info=True)
        return None


def _get_oauth_sheets_service():
    """Return a Google Sheets API v4 service using OAuth2 user credentials."""
    global _oauth_sheets_service
    if _oauth_sheets_service is not None:
        return _oauth_sheets_service
    creds = _get_oauth_creds()
    if creds is None:
        return None
    try:
        from googleapiclient.discovery import build
        _oauth_sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        logger.debug("OAuth Sheets API service initialised.")
        return _oauth_sheets_service
    except Exception as exc:
        logger.error("Failed to initialise OAuth Sheets service: %s", exc, exc_info=True)
        return None


def _retry_on_rate_limit(fn, *, max_retries: int = 5, base_delay: float = 5.0):
    """Execute *fn()* with exponential backoff on Google API 429 rate-limit errors.

    Retries up to *max_retries* times with delays of 5s, 10s, 20s, 40s, 80s.
    Any non-429 exception is re-raised immediately.
    """
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as exc:
            exc_str = str(exc)
            is_rate_limit = "429" in exc_str or "Quota exceeded" in exc_str or "RATE_LIMIT" in exc_str.upper()
            if not is_rate_limit or attempt >= max_retries:
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning(
                "Rate limited (429) on attempt %d/%d; retrying in %.0fs …",
                attempt + 1, max_retries + 1, delay,
            )
            time.sleep(delay)
    # Should not reach here, but just in case
    return fn()


def _is_transient_drive_error(exc: Exception) -> bool:
    if isinstance(exc, (ssl.SSLError, BrokenPipeError, TimeoutError, ConnectionError)):
        return True

    error_text = str(exc).lower()
    return any(
        token in error_text
        for token in (
            "broken pipe",
            "record layer failure",
            "ssl",
            "tls",
            "connection aborted",
            "connection reset",
            "server disconnected",
            "timed out",
            "timeout",
            "temporary failure",
        )
    )


def _retry_on_transient_drive_error(fn, *, max_retries: int = 3, base_delay: float = 2.0):
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as exc:
            if not _is_transient_drive_error(exc) or attempt >= max_retries:
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning(
                "Transient Drive error on attempt %d/%d; retrying in %.0fs … (%s)",
                attempt + 1,
                max_retries + 1,
                delay,
                exc,
            )
            _get_drive_service(force_refresh=True)
            _get_oauth_drive_service(force_refresh=True)
            time.sleep(delay)
    return fn()


def _mark_recently_prepared(spreadsheet) -> None:
    sheet_id = getattr(spreadsheet, "id", None)
    if not sheet_id:
        return
    _recently_prepared_spreadsheets[str(sheet_id)] = time.monotonic()


def _was_recently_prepared(spreadsheet) -> bool:
    sheet_id = getattr(spreadsheet, "id", None)
    if not sheet_id:
        return False
    prepared_at = _recently_prepared_spreadsheets.get(str(sheet_id))
    if prepared_at is None:
        return False
    if (time.monotonic() - prepared_at) > _RECENT_PREPARED_TTL_SECONDS:
        _recently_prepared_spreadsheets.pop(str(sheet_id), None)
        return False
    return True


def _get_service_account_email() -> Optional[str]:
    """Extract the service account email from the stored JSON credentials."""
    if not settings.google_service_account_json:
        return None
    try:
        raw_json = base64.b64decode(settings.google_service_account_json).decode("utf-8")
        creds_dict = json.loads(raw_json)
        return creds_dict.get("client_email")
    except Exception:
        return None


def _share_with_service_account(file_id: str, drive_service) -> None:
    """Share a file (created via OAuth) with the service account so gspread can access it."""
    sa_email = _get_service_account_email()
    if not sa_email:
        logger.warning("Cannot share file with service account — no SA email found.")
        return
    try:
        drive_service.permissions().create(
            fileId=file_id,
            body={
                "type": "user",
                "role": "writer",
                "emailAddress": sa_email,
            },
            fields="id",
            sendNotificationEmail=False,
            supportsAllDrives=True,
        ).execute()
        logger.info("Shared file %s with service account %s", file_id, sa_email)
    except Exception as exc:
        logger.warning("Could not share file %s with service account: %s", file_id, exc)


def _month_drive_folder_name() -> str:
    return f"{_sandbox_name_prefix()}Fişler — {_month_label()}"


def _get_or_create_month_drive_folder() -> Optional[str]:
    """
    Return (creating if needed) a monthly subfolder inside GOOGLE_DRIVE_PARENT_FOLDER_ID.
    E.g. "Fişler — Nisan 2026" inside the user's Muhasebe folder.

    Prefers OAuth drive service (user credentials) for folder creation,
    falls back to service account drive service.
    """
    if not settings.google_drive_parent_folder_id:
        return settings.google_drive_parent_folder_id or None

    folder_name = _month_drive_folder_name()
    if folder_name in _drive_folder_cache:
        return _drive_folder_cache[folder_name]

    # Prefer OAuth, fall back to service account
    oauth_drive = _get_oauth_drive_service()
    sa_drive = _get_drive_service()
    # Use OAuth for searching too (it can see user's files); fall back to SA
    search_drive = oauth_drive or sa_drive
    create_drive = oauth_drive or sa_drive

    if search_drive is None:
        return None

    folder_name = _month_drive_folder_name()
    parent = settings.google_drive_parent_folder_id

    try:
        # Search for existing subfolder
        q = (
            f"name='{folder_name}' and "
            f"'{parent}' in parents and "
            "mimeType='application/vnd.google-apps.folder' and "
            "trashed=false"
        )
        results = search_drive.files().list(
            q=q, fields="files(id)", pageSize=1,
            supportsAllDrives=True, includeItemsFromAllDrives=True,
        ).execute()
        files = results.get("files", [])
        if files:
            folder_id = files[0]["id"]
            _drive_folder_cache[folder_name] = folder_id
            return folder_id

        if create_drive is None:
            return parent

        # Create subfolder
        meta = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent],
        }
        folder = create_drive.files().create(body=meta, fields="id", supportsAllDrives=True).execute()
        folder_id = folder["id"]
        _drive_folder_cache[folder_name] = folder_id
        logger.info("Created Drive subfolder '%s' (id=%s)", folder_name, folder_id)

        # If created via OAuth, share with service account so SA can also access it
        if create_drive is oauth_drive and oauth_drive is not None:
            _share_with_service_account(folder_id, oauth_drive)

        return folder_id

    except Exception as exc:
        logger.warning("Could not get/create Drive folder: %s", exc)
        return parent  # fallback to root folder


def upload_document(
    file_bytes: bytes,
    filename: str,
    mime_type: str,
) -> Optional[str]:
    """
    Upload a document (image or PDF) to Google Drive.

    Returns the web-view link (shareable URL) or None on failure.
    The file is placed in a monthly subfolder inside GOOGLE_DRIVE_PARENT_FOLDER_ID.
    """
    if not settings.google_drive_parent_folder_id:
        logger.debug("GOOGLE_DRIVE_PARENT_FOLDER_ID not set; skipping Drive upload.")
        return None

    # googleapiclient/httplib2 Drive service instances are not thread-safe.
    # Serialising uploads avoids concurrent SSL reads that can crash the worker
    # under bursty multi-image intake.
    with _drive_upload_lock:
        use_oauth_drive = _get_oauth_drive_service() is not None
        drive_getter = _get_oauth_drive_service if use_oauth_drive else _get_drive_service
        if drive_getter() is None:
            return None

        try:
            import io
            try:
                from googleapiclient.http import MediaIoBaseUpload
            except ImportError:
                # Keep mocked Drive uploads testable in environments that do not
                # install the optional HTTP helper module.
                class MediaIoBaseUpload:  # type: ignore[no-redef]
                    def __init__(self, fd, mimetype: str, resumable: bool):
                        self.fd = fd
                        self.mimetype = mimetype
                        self.resumable = resumable

            folder_id = _get_or_create_month_drive_folder()
            file_meta: dict = {"name": filename}
            if folder_id:
                file_meta["parents"] = [folder_id]

            def _upload_once():
                drive = drive_getter()
                if drive is None:
                    raise RuntimeError("Drive service unavailable during upload.")
                media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mime_type, resumable=False)
                return drive.files().create(
                    body=file_meta,
                    media_body=media,
                    fields="id,webViewLink",
                    supportsAllDrives=True,
                ).execute()

            uploaded = _retry_on_transient_drive_error(_upload_once)

            link = uploaded.get("webViewLink", "")
            logger.info("Uploaded '%s' to Drive folder '%s' → %s", filename, _month_drive_folder_name(), link)
            return link

        except Exception as exc:
            logger.error("Drive upload failed for '%s': %s", filename, exc, exc_info=True)
            return None


# ─── Registry helpers ─────────────────────────────────────────────────────────


def _registry_path() -> Path:
    path = _storage_root() / "state" / "sheets_registry.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _pending_drive_uploads_state_path() -> Path:
    path = _storage_root() / "state" / "pending_drive_uploads.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _pending_drive_uploads_dir() -> Path:
    path = _storage_root() / "state" / "pending_drive_uploads"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _pending_sheet_appends_state_path() -> Path:
    path = _storage_root() / "state" / "pending_sheet_appends.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _pending_sheet_appends_dir() -> Path:
    path = _storage_root() / "state" / "pending_sheet_appends"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _load_registry() -> dict[str, str]:
    path = _registry_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_registry(registry: dict[str, str]) -> None:
    _registry_path().write_text(
        json.dumps(registry, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _month_key() -> str:
    return _now().strftime("%Y-%m")


def _month_label() -> str:
    months_tr = {
        1: "Ocak", 2: "Şubat", 3: "Mart", 4: "Nisan",
        5: "Mayıs", 6: "Haziran", 7: "Temmuz", 8: "Ağustos",
        9: "Eylül", 10: "Ekim", 11: "Kasım", 12: "Aralık",
    }
    now = _now()
    return f"{months_tr[now.month]} {now.year}"


# ─── Spreadsheet setup ────────────────────────────────────────────────────────


def _col_letter(idx: int) -> str:
    """Convert 0-based column index to spreadsheet letter (A, B, …, Z, AA…)."""
    result = ""
    idx += 1
    while idx:
        idx, rem = divmod(idx - 1, 26)
        result = chr(65 + rem) + result
    return result


def _formula_arg_separator() -> str:
    return ";"


def _build_tab_total_formula(tab_name: str) -> str | None:
    total_col = _TAB_TOTAL_COLUMNS.get(tab_name)
    if not total_col:
        return None
    sep = _formula_arg_separator()
    return f"=IFERROR(SUM({total_col}3:{total_col}){sep}0)"


def _build_summary_formula(tab_name: str) -> str:
    total_col = _TAB_TOTAL_COLUMNS[tab_name]
    sep = _formula_arg_separator()
    return f"=IFERROR('{tab_name}'!{total_col}2{sep}0)"


def _total_row_values(tab_name: str) -> list[str]:
    headers, _ = _TABS[tab_name]
    values = [""] * len(headers)
    values[0] = "TOPLAM"
    total_formula = _build_tab_total_formula(tab_name)
    if total_formula:
        total_col_idx = ord(_TAB_TOTAL_COLUMNS[tab_name]) - ord("A")
        values[total_col_idx] = total_formula
    return values


def _looks_like_total_row(first_cell: str | None) -> bool:
    return (first_cell or "").strip().upper() == "TOPLAM"


def _setup_worksheet(ws, tab_name: str, *, lightweight: bool = False) -> None:
    """Format a data worksheet: freeze row 1, bold + coloured headers,
    column widths, text-wrap on long fields, number format on amounts."""
    headers, color = _TABS[tab_name]
    if not headers:
        return

    col_count = len(headers)
    last_col = _col_letter(col_count - 1)
    header_range = f"A1:{last_col}1"
    total_range = f"A2:{last_col}2"
    data_range = f"A3:{last_col}1000"

    # Write headers
    ws.update([headers], "A1", value_input_option="RAW")
    ws.update([_total_row_values(tab_name)], "A2", value_input_option="USER_ENTERED")
    ws.freeze(rows=2)

    if lightweight:
        logger.debug("Worksheet '%s' bootstrapped in lightweight mode.", tab_name)
        return

    # Bold white text on coloured background
    ws.format(header_range, {
        "backgroundColor": color,
        "textFormat": {
            "bold": True,
            "fontSize": 10,
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
        },
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
        "wrapStrategy": "CLIP",
    })

    # Data rows: light alternating background, middle-align, clip by default
    ws.format(data_range, {
        "verticalAlignment": "MIDDLE",
        "wrapStrategy": "CLIP",
        "textFormat": {"fontSize": 10},
    })

    ws.format(total_range, {
        "backgroundColor": {"red": 0.96, "green": 0.96, "blue": 0.96},
        "textFormat": {"bold": True, "fontSize": 10},
        "verticalAlignment": "MIDDLE",
    })
    # Build batch requests for column widths, wrap, and number format
    requests = []
    sheet_id = ws.id

    for i, header in enumerate(headers):
        # Column width
        width = _COL_WIDTHS.get(header, 120)
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": i,
                    "endIndex": i + 1,
                },
                "properties": {"pixelSize": width},
                "fields": "pixelSize",
            }
        })

        # Text wrap for long free-text columns (rows 2+)
        if header in _WRAP_COLUMNS:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 2,
                        "startColumnIndex": i,
                        "endColumnIndex": i + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "wrapStrategy": "WRAP",
                        }
                    },
                    "fields": "userEnteredFormat.wrapStrategy",
                }
            })

        # Number format for amount columns
        if header in _AMOUNT_COLUMNS:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 2,
                        "startColumnIndex": i,
                        "endColumnIndex": i + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "#,##0.00",
                            },
                            "horizontalAlignment": "RIGHT",
                        }
                    },
                    "fields": "userEnteredFormat(numberFormat,horizontalAlignment)",
                }
            })

    requests.append({
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
                "startIndex": len(headers),
                "endIndex": len(headers) + 1,
            },
            "properties": {
                "pixelSize": 1,
                "hiddenByUser": True,
            },
            "fields": "pixelSize,hiddenByUser",
        }
    })

    # Row height for header
    requests.append({
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "ROWS",
                "startIndex": 0,
                "endIndex": 1,
            },
            "properties": {"pixelSize": 32},
            "fields": "pixelSize",
        }
    })

    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as exc:
        logger.warning("Column formatting batch update failed for '%s': %s", tab_name, exc)

    logger.debug("Worksheet '%s' formatted (%d columns).", tab_name, col_count)


def _setup_summary_tab(ws, month_label: str, *, lightweight: bool = False) -> None:
    """Populate the 📊 Özet tab with title, labels, and cross-sheet SUM formulas."""
    header_color = _TABS["📊 Özet"][1]

    if not lightweight:
        try:
            ws.clear()
        except Exception:
            pass

    ws.update([["📊 ÖZET — " + month_label, ""]], "A1", value_input_option="RAW")
    ws.update([
        ["🧾 Faturalar Toplamı (TL)",       _build_summary_formula("🧾 Faturalar")],
        ["💳 Ödeme Dekontları (TL)",         _build_summary_formula("💳 Dekontlar")],
        ["⛽ Harcama Fişleri (TL)",          _build_summary_formula("⛽ Harcama Fişleri")],
        ["📝 Çekler (TL)",                   _build_summary_formula("📝 Çekler")],
        ["💵 Elden Ödemeler (TL)",           _build_summary_formula("💵 Elden Ödemeler")],
        ["🏗️ Malzeme (TL)",                 _build_summary_formula("🏗️ Malzeme")],
        ["↩️ İadeler (TL)",                  _build_summary_formula("↩️ İadeler")],
        ["", ""],
        ["💰 GENEL TOPLAM (TL)",             "=SUM(B2:B8)"],
    ], "A2", value_input_option="USER_ENTERED")
    ws.freeze(rows=1)

    if lightweight:
        logger.debug("Summary tab bootstrapped in lightweight mode for %s.", month_label)
        return

    ws.format("A1:B1", {
        "backgroundColor": header_color,
        "textFormat": {
            "bold": True,
            "fontSize": 13,
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
        },
        "horizontalAlignment": "CENTER",
    })
    ws.merge_cells("A1:B1")

    # Style label column
    ws.format("A2:A8", {"textFormat": {"fontSize": 11}})
    ws.format("B2:B8", {
        "textFormat": {"fontSize": 11, "bold": True},
        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
    })
    # Style total row
    ws.format("A10:B10", {
        "textFormat": {"bold": True, "fontSize": 12},
        "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95},
        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
    })

    # Column widths
    try:
        ws.spreadsheet.batch_update({
            "requests": [
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": 1,
                        },
                        "properties": {"pixelSize": 280},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "COLUMNS",
                            "startIndex": 1,
                            "endIndex": 2,
                        },
                        "properties": {"pixelSize": 160},
                        "fields": "pixelSize",
                    }
                },
            ]
        })
    except Exception:
        pass  # column width is cosmetic, ignore errors

    logger.debug("📊 Özet tab populated for %s.", month_label)


def _ensure_tab_total_row(ws, tab_name: str) -> None:
    headers, _ = _TABS[tab_name]
    last_col = _col_letter(len(headers) - 1)

    # Row 2 is reserved for the canonical TOPLAM row. Rewriting it in place is
    # safer than inserting a new row after manual edits, because insertion can
    # shift real data rows and intermittently fail against live Sheets.
    ws.update([_total_row_values(tab_name)], "A2", value_input_option="USER_ENTERED")
    ws.format(f"A2:{last_col}2", {
        "backgroundColor": {"red": 0.96, "green": 0.96, "blue": 0.96},
        "textFormat": {"bold": True, "fontSize": 10},
        "verticalAlignment": "MIDDLE",
    })

    total_col = _TAB_TOTAL_COLUMNS.get(tab_name)
    if total_col:
        ws.format(f"{total_col}2:{total_col}2", {
            "textFormat": {"bold": True, "fontSize": 10},
            "horizontalAlignment": "RIGHT",
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
        })
    ws.freeze(rows=2)


def _ensure_worksheet_dimensions(ws, tab_name: str) -> None:
    headers, _ = _TABS[tab_name]
    target_cols = len(headers) + 2
    target_rows = max(getattr(ws, "row_count", 0) or 0, 1000)

    try:
        if getattr(ws, "col_count", 0) < target_cols or getattr(ws, "row_count", 0) < target_rows:
            ws.resize(rows=target_rows, cols=target_cols)
    except Exception as exc:
        logger.warning("Could not resize worksheet '%s': %s", tab_name, exc)


def _repair_drive_link_formulas(ws, tab_name: str) -> None:
    drive_col = _drive_column_letter(tab_name)
    try:
        values = ws.get(
            f"{drive_col}3:{drive_col}",
            value_render_option="FORMULA",
        )
    except Exception:
        return

    repaired = 0
    separator = _formula_arg_separator()
    for row_number, row in enumerate(values, start=3):
        formula = row[0] if row else ""
        if not formula or not formula.startswith('=HYPERLINK("'):
            continue
        if f'"{separator}"' in formula:
            continue
        if '","' not in formula:
            continue

        fixed_formula = formula.replace('","', f'"{separator}"', 1)
        _retry_on_rate_limit(
            lambda fixed_formula=fixed_formula, row_number=row_number: ws.update(
                [[fixed_formula]],
                f"{drive_col}{row_number}",
                value_input_option="USER_ENTERED",
            )
        )
        repaired += 1

    if repaired:
        logger.info("Repaired %d Drive link formula(s) on '%s'.", repaired, tab_name)


def _worksheet_has_visible_data(ws, tab_name: str) -> bool:
    headers, _ = _TABS[tab_name]
    last_col = _internal_row_id_column_letter(tab_name)
    try:
        rows = ws.get(f"A3:{last_col}")
    except Exception:
        return False

    visible_cols = len(headers)
    for row in rows:
        if any(str(cell or "").strip() for cell in row[:visible_cols]):
            return True
    return False


def _archive_drifted_tab(sh, ws, tab_name: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    base_title = f"{tab_name} MANUAL_DRIFT {timestamp}"
    existing_titles = {worksheet.title for worksheet in sh.worksheets()}
    archived_title = base_title[:100]
    counter = 1
    while archived_title in existing_titles:
        suffix = f" {counter}"
        archived_title = f"{base_title[:max(1, 100 - len(suffix))]}{suffix}"
        counter += 1
    ws.update_title(archived_title)
    return archived_title


def _backfill_internal_row_ids(ws, tab_name: str) -> int:
    headers, _ = _TABS[tab_name]
    hidden_col = _internal_row_id_column_letter(tab_name)
    last_col = hidden_col
    try:
        rows = ws.get(f"A3:{last_col}")
    except Exception:
        return 0

    repaired = 0
    visible_cols = len(headers)
    for row_number, row in enumerate(rows, start=3):
        visible_values = row[:visible_cols]
        hidden_value = str(row[visible_cols]).strip() if len(row) > visible_cols else ""
        if not any(str(cell or "").strip() for cell in visible_values):
            continue
        if hidden_value:
            continue
        _retry_on_rate_limit(
            lambda row_number=row_number: ws.update(
                [[uuid4().hex]],
                f"{hidden_col}{row_number}",
                value_input_option="RAW",
            )
        )
        repaired += 1
    return repaired


def _tab_headers_match(ws, tab_name: str) -> bool:
    expected_headers, _ = _TABS[tab_name]
    actual_headers = ws.row_values(1)[: len(expected_headers)]
    return actual_headers == expected_headers


def _tab_total_row_is_valid(ws, tab_name: str) -> bool:
    headers, _ = _TABS[tab_name]
    last_col = _col_letter(len(headers) - 1)
    try:
        rows = ws.get(f"A2:{last_col}2", value_render_option="FORMULA")
    except Exception:
        return False

    row = rows[0] if rows else []
    if not row or not _looks_like_total_row(row[0] if row else ""):
        return False

    total_formula = _build_tab_total_formula(tab_name)
    if not total_formula:
        return True

    total_col_idx = ord(_TAB_TOTAL_COLUMNS[tab_name]) - ord("A")
    actual_formula = str(row[total_col_idx]).strip() if len(row) > total_col_idx else ""
    return actual_formula == total_formula


def _summary_tab_is_valid(ws) -> bool:
    try:
        rows = ws.get("A1:B10", value_render_option="FORMULA")
    except Exception:
        return False

    title = str(rows[0][0]).strip() if rows and rows[0] else ""
    if not title.startswith("📊 ÖZET — "):
        return False

    expected_formulas = [_build_summary_formula(tab_name) for tab_name in list(_TABS.keys())[1:]]
    expected_total = "=SUM(B2:B8)"
    for index, formula in enumerate(expected_formulas, start=1):
        row = rows[index] if len(rows) > index else []
        actual_formula = str(row[1]).strip() if len(row) > 1 else ""
        if actual_formula != formula:
            return False

    total_row = rows[9] if len(rows) > 9 else []
    actual_total_formula = str(total_row[1]).strip() if len(total_row) > 1 else ""
    return actual_total_formula == expected_total


def _audit_summary_tab(sh, findings: list[dict[str, object]], *, repair: bool) -> None:
    import gspread

    try:
        ws = sh.worksheet("📊 Özet")
    except gspread.WorksheetNotFound:
        findings.append({
            "tab_name": "📊 Özet",
            "code": "missing_tab",
            "severity": "error",
            "repaired": False,
            "message": "Summary tab is missing.",
        })
        if repair:
            ws = _ensure_tab_exists(sh, "📊 Özet")
            _setup_summary_tab(ws, _month_label(), lightweight=True)
            findings[-1]["repaired"] = True
        return

    if _summary_tab_is_valid(ws):
        return

    finding = {
        "tab_name": "📊 Özet",
        "code": "invalid_summary",
        "severity": "error",
        "repaired": False,
        "message": "Summary formulas or title are invalid.",
    }
    findings.append(finding)
    if repair:
        _setup_summary_tab(ws, _month_label(), lightweight=True)
        finding["repaired"] = True


def _audit_data_tab(sh, tab_name: str, findings: list[dict[str, object]], *, repair: bool) -> None:
    import gspread

    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        findings.append({
            "tab_name": tab_name,
            "code": "missing_tab",
            "severity": "error",
            "repaired": False,
            "message": "Data tab is missing.",
        })
        if repair:
            _ensure_tab_exists(sh, tab_name)
            findings[-1]["repaired"] = True
        return

    if not _tab_headers_match(ws, tab_name):
        finding = {
            "tab_name": tab_name,
            "code": "header_drift",
            "severity": "error",
            "repaired": False,
            "message": "Header row does not match the canonical layout.",
        }
        findings.append(finding)
        if repair:
            if _worksheet_has_visible_data(ws, tab_name):
                finding["archived_to"] = _archive_drifted_tab(sh, ws, tab_name)
                ws = _ensure_tab_exists(sh, tab_name)
            else:
                _setup_worksheet(ws, tab_name, lightweight=True)
            finding["repaired"] = True

    if not _tab_total_row_is_valid(ws, tab_name):
        finding = {
            "tab_name": tab_name,
            "code": "invalid_total_row",
            "severity": "error",
            "repaired": False,
            "message": "Total row is missing or corrupted.",
        }
        findings.append(finding)
        if repair:
            _ensure_tab_total_row(ws, tab_name)
            finding["repaired"] = True

    repaired_formulas_before = len(findings)
    _repair_drive_link_formulas(ws, tab_name)
    if len(findings) == repaired_formulas_before:
        pass

    repaired_row_ids = _backfill_internal_row_ids(ws, tab_name) if repair else 0
    if repaired_row_ids:
        findings.append({
            "tab_name": tab_name,
            "code": "missing_row_ids",
            "severity": "warning",
            "repaired": True,
            "message": f"Backfilled {repaired_row_ids} hidden row id value(s).",
            "count": repaired_row_ids,
        })
    elif not repair:
        headers, _ = _TABS[tab_name]
        hidden_col = _internal_row_id_column_letter(tab_name)
        try:
            rows = ws.get(f"A3:{hidden_col}")
        except Exception:
            rows = []
        missing_count = 0
        visible_cols = len(headers)
        for row in rows:
            visible_values = row[:visible_cols]
            hidden_value = str(row[visible_cols]).strip() if len(row) > visible_cols else ""
            if any(str(cell or "").strip() for cell in visible_values) and not hidden_value:
                missing_count += 1
        if missing_count:
            findings.append({
                "tab_name": tab_name,
                "code": "missing_row_ids",
                "severity": "warning",
                "repaired": False,
                "message": f"{missing_count} row(s) are missing hidden row ids.",
                "count": missing_count,
            })


def _audit_spreadsheet_layout(sh, *, repair: bool = False, target_tabs: set[str] | None = None) -> list[dict[str, object]]:
    findings: list[dict[str, object]] = []
    canonical_titles = set(_TABS.keys())
    titles_to_check = [tab_name for tab_name in _TABS if target_tabs is None or tab_name in target_tabs]

    existing_titles = {worksheet.title for worksheet in sh.worksheets()}
    for orphan_title in sorted(existing_titles - canonical_titles):
        findings.append({
            "tab_name": orphan_title,
            "code": "orphan_tab",
            "severity": "warning",
            "repaired": False,
            "message": "Found a non-canonical worksheet title.",
        })

    for tab_name in titles_to_check:
        if tab_name == "📊 Özet":
            _audit_summary_tab(sh, findings, repair=repair)
            continue
        _audit_data_tab(sh, tab_name, findings, repair=repair)

    return findings


def queue_status() -> dict[str, int]:
    return {
        "pending_sheet_appends": len(_load_pending_sheet_appends()),
        "pending_drive_uploads": len(_load_pending_drive_uploads()),
    }


def audit_current_month_spreadsheet(*, spreadsheet_id: Optional[str] = None, repair: bool = False) -> dict[str, object]:
    client = _get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable.")

    with _lock:
        sh = client.open_by_key(spreadsheet_id) if spreadsheet_id else _get_or_create_spreadsheet(client)
        findings = _audit_spreadsheet_layout(sh, repair=repair)
        return {
            "spreadsheet_id": sh.id,
            "month_key": _month_key(),
            "findings": findings,
            "queue": queue_status(),
        }


def apply_test_drift(
    *,
    action: str,
    spreadsheet_id: Optional[str] = None,
    tab_name: str | None = None,
    replacement_name: str | None = None,
    row_count: int = 5,
) -> dict[str, object]:
    client = _get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable.")

    with _lock:
        sh = client.open_by_key(spreadsheet_id) if spreadsheet_id else _get_or_create_spreadsheet(client)
        target_tab = tab_name or "🧾 Faturalar"

        if action == "delete_summary_tab":
            ws = sh.worksheet("📊 Özet")
            sh.del_worksheet(ws)
            return {"spreadsheet_id": sh.id, "action": action, "applied": True, "tab_name": "📊 Özet"}

        if action == "rename_data_tab":
            ws = sh.worksheet(target_tab)
            new_name = (replacement_name or f"{target_tab} RENAMED").strip()[:100]
            ws.update_title(new_name)
            return {"spreadsheet_id": sh.id, "action": action, "applied": True, "tab_name": target_tab, "replacement_name": new_name}

        if action == "corrupt_total_row":
            ws = sh.worksheet(target_tab)
            total_col = _TAB_TOTAL_COLUMNS[target_tab]
            ws.update([["BROKEN"]], "A2", value_input_option="RAW")
            ws.update([[""]], f"{total_col}2", value_input_option="RAW")
            return {"spreadsheet_id": sh.id, "action": action, "applied": True, "tab_name": target_tab}

        if action == "corrupt_header_row":
            ws = sh.worksheet(target_tab)
            headers = list(_TABS[target_tab][0])
            headers[0] = "BROKEN"
            ws.update([headers], "A1", value_input_option="RAW")
            return {"spreadsheet_id": sh.id, "action": action, "applied": True, "tab_name": target_tab}

        if action == "clear_hidden_row_ids":
            ws = sh.worksheet(target_tab)
            hidden_col = _internal_row_id_column_letter(target_tab)
            end_row = max(3, row_count + 2)
            _retry_on_rate_limit(lambda: ws.batch_clear([f"{hidden_col}3:{hidden_col}{end_row}"]))
            return {"spreadsheet_id": sh.id, "action": action, "applied": True, "tab_name": target_tab, "row_count": row_count}

        if action == "reorder_rows":
            ws = sh.worksheet(target_tab)
            last_col = _internal_row_id_column_letter(target_tab)
            end_row = max(3, row_count + 2)
            rows = ws.get(f"A3:{last_col}{end_row}")
            if len(rows) < 2:
                return {"spreadsheet_id": sh.id, "action": action, "applied": False, "tab_name": target_tab, "row_count": len(rows)}
            reordered = list(reversed(rows))
            ws.update(reordered, "A3", value_input_option="USER_ENTERED")
            return {"spreadsheet_id": sh.id, "action": action, "applied": True, "tab_name": target_tab, "row_count": len(reordered)}

        raise ValueError(f"Unsupported drift action: {action}")


def clear_current_namespace_storage() -> dict[str, int]:
    state_counts = queue_status()
    storage_root = _storage_root()
    if storage_root.exists():
        shutil.rmtree(storage_root)
    storage_root.mkdir(parents=True, exist_ok=True)
    return state_counts


def _repair_monthly_spreadsheet_layout(sh) -> None:
    _audit_spreadsheet_layout(sh, repair=True)
    _mark_recently_prepared(sh)


def _create_and_setup_spreadsheet(client, title: str) -> str:
    """Create a new spreadsheet with all tabs and return its ID.

    Prefers OAuth user credentials for the create call — service accounts
    cannot create Google Workspace files (403 quota / permission error).
    Falls back to service account Sheets API as a last resort.
    """
    logger.info("Creating new spreadsheet: '%s'", title)

    # Prefer OAuth, fall back to service account
    oauth_sheets = _get_oauth_sheets_service()
    sa_sheets = _get_sheets_service()
    sheets_svc = oauth_sheets or sa_sheets

    if sheets_svc is None:
        raise RuntimeError("No Sheets API service available — cannot create spreadsheet.")

    using_oauth = sheets_svc is oauth_sheets

    result = sheets_svc.spreadsheets().create(
        body={
            "properties": {
                "title": title,
                "locale": "tr_TR",
                "timeZone": settings.business_timezone,
            }
        },
        fields="spreadsheetId",
    ).execute()
    sheet_id = result["spreadsheetId"]
    logger.info(
        "Spreadsheet '%s' created via %s (id=%s)",
        title, "OAuth" if using_oauth else "service account", sheet_id,
    )

    # If created via OAuth, share with service account so gspread can open it
    if using_oauth:
        oauth_drive = _get_oauth_drive_service()
        if oauth_drive:
            _share_with_service_account(sheet_id, oauth_drive)

    # Open via gspread (service account) for all subsequent tab operations
    sh = client.open_by_key(sheet_id)

    # Move to monthly subfolder via Drive API
    if settings.google_drive_parent_folder_id:
        folder_id = _get_or_create_month_drive_folder() or settings.google_drive_parent_folder_id
        # Use OAuth drive for move if available (owner can move their own files)
        move_drive = _get_oauth_drive_service() or _get_drive_service()
        if move_drive:
            try:
                file_info = move_drive.files().get(
                    fileId=sheet_id, fields="parents", supportsAllDrives=True
                ).execute()
                current_parents = ",".join(file_info.get("parents", []))
                move_drive.files().update(
                    fileId=sheet_id,
                    addParents=folder_id,
                    removeParents=current_parents,
                    fields="id,parents",
                    supportsAllDrives=True,
                ).execute()
                logger.info("Moved spreadsheet '%s' to Drive folder %s", title, folder_id)
            except Exception as exc:
                logger.warning("Could not move spreadsheet to folder: %s", exc)

    # Rename default Sheet1 → 📊 Özet (formulas written AFTER data tabs exist)
    default_ws = sh.sheet1
    default_ws.update_title("📊 Özet")

    # Create data tabs FIRST so Özet formulas can reference them
    for tab_name in list(_TABS.keys())[1:]:
        headers, _ = _TABS[tab_name]
        ws = sh.add_worksheet(
            title=tab_name,
            rows=1000,
            cols=len(headers) + 2,
        )
        _setup_worksheet(ws, tab_name)

    # NOW write Özet formulas (all referenced tabs exist)
    _setup_summary_tab(default_ws, _month_label())

    # Share with owner
    if settings.google_sheets_owner_email:
        try:
            sh.share(
                settings.google_sheets_owner_email,
                perm_type="user",
                role="writer",
                notify=False,
            )
            logger.info("Spreadsheet shared with %s", settings.google_sheets_owner_email)
        except Exception as exc:
            logger.warning("Could not share spreadsheet: %s", exc)

    logger.info("Spreadsheet '%s' created with ID %s", title, sheet_id)
    return sheet_id


# ─── Tab resolution ───────────────────────────────────────────────────────────


def _monthly_tab_name(base_name: str) -> str:
    """Return month-prefixed tab name, e.g. 'Nisan 2026 — 🧾 Faturalar'."""
    return f"{_month_label()} — {base_name}"


def _ensure_tab_exists(sh, tab_name: str, base_name: str | None = None):
    """
    Return the worksheet for tab_name, creating it if missing.

    base_name: the canonical tab name from _TABS used for formatting/headers
               (e.g. '🧾 Faturalar' when tab_name is 'Nisan 2026 — 🧾 Faturalar').
               If None, tab_name itself is used as the lookup key.

    Also handles backwards-compat: if a plain-name version exists (no emoji),
    it is automatically renamed to the emoji version (only for non-monthly tabs).
    """
    import gspread

    # 1. Try exact name
    try:
        return sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        pass

    # 2. Try plain (no-emoji) version and rename — only for non-monthly tabs
    if base_name is None:
        plain = tab_name.split(" ", 1)[-1] if " " in tab_name else tab_name
        if plain != tab_name:
            try:
                ws = sh.worksheet(plain)
                ws.update_title(tab_name)
                logger.info("Renamed tab '%s' → '%s'", plain, tab_name)
                return ws
            except gspread.WorksheetNotFound:
                pass

    # 3. Create new tab
    logger.info("Tab '%s' not found; creating it.", tab_name)
    lookup = base_name or tab_name
    headers, _ = _TABS.get(lookup, ([], {}))
    ws = sh.add_worksheet(
        title=tab_name,
        rows=1000,
        cols=max(len(headers) + 2, 10),
    )

    if lookup == "📊 Özet":
        _setup_summary_tab(ws, _month_label())
    elif lookup in _TABS:
        _setup_worksheet(ws, lookup)

    return ws


# ─── Spreadsheet resolution ───────────────────────────────────────────────────


def _find_existing_spreadsheet_in_drive(title: str) -> Optional[str]:
    """Search Drive for an existing spreadsheet with the given title.

    Prevents duplicate creation when a previous attempt succeeded on file creation
    but failed before the registry was saved.
    """
    if not settings.google_drive_parent_folder_id:
        return None

    oauth_drive = _get_oauth_drive_service()
    sa_drive = _get_drive_service()
    drive = oauth_drive or sa_drive
    if drive is None:
        return None

    try:
        # Search in the monthly subfolder first, then in the parent folder
        for parent_id in [
            _drive_folder_cache.get(_month_drive_folder_name()),
            settings.google_drive_parent_folder_id,
        ]:
            if not parent_id:
                continue
            q = (
                f"name='{title}' and "
                f"'{parent_id}' in parents and "
                "mimeType='application/vnd.google-apps.spreadsheet' and "
                "trashed=false"
            )
            results = drive.files().list(
                q=q, fields="files(id)", pageSize=1,
                supportsAllDrives=True, includeItemsFromAllDrives=True,
            ).execute()
            files = results.get("files", [])
            if files:
                return files[0]["id"]
    except Exception as exc:
        logger.warning("Drive search for existing spreadsheet failed: %s", exc)

    return None


def _try_create_spreadsheet_in_drive(title: str) -> Optional[str]:
    """
    Attempt to create a Google Sheets file via Drive API.

    Prefers OAuth user credentials (service accounts cannot create Workspace files).
    Falls back to service account credentials as a last resort.

    Returns the spreadsheet ID on success, None on failure.
    """
    if not settings.google_drive_parent_folder_id:
        return None

    # Prefer OAuth credentials — service accounts get 403 on file creation
    oauth_drive = _get_oauth_drive_service()
    sa_drive = _get_drive_service()
    drive = oauth_drive or sa_drive

    if drive is None:
        return None

    folder_id = _get_or_create_month_drive_folder() or settings.google_drive_parent_folder_id

    file_metadata = {
        "name": title,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [folder_id],
    }
    try:
        file = drive.files().create(
            body=file_metadata,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        sheet_id = file["id"]
        logger.info(
            "Auto-created spreadsheet '%s' in folder %s (id=%s) via %s",
            title, folder_id, sheet_id,
            "OAuth" if drive is oauth_drive else "service account",
        )

        # If created via OAuth, share with service account so gspread can access it
        if drive is oauth_drive and oauth_drive is not None:
            _share_with_service_account(sheet_id, oauth_drive)

        return sheet_id
    except Exception as exc:
        logger.error(
            "Drive API spreadsheet creation failed for folder %s: %s",
            folder_id, exc, exc_info=True,
        )
        return None


def _get_or_create_spreadsheet(client):
    """
    Return the gspread Spreadsheet to use.

    Priority:
      1. sheets_registry.json entry for this month.
      2. GOOGLE_SHEETS_SPREADSHEET_ID env var (configured spreadsheet).
      3. Auto-create via OAuth/Drive API in the monthly subfolder.
    """
    registry = _load_registry()
    month_key = _month_key()
    override_spreadsheet_id = (current_pipeline_context().spreadsheet_id_override or "").strip()

    if override_spreadsheet_id:
        try:
            sh = client.open_by_key(override_spreadsheet_id)
            registry[month_key] = override_spreadsheet_id
            _save_registry(registry)
            logger.info("Using context spreadsheet override for %s.", month_key)
            return sh
        except Exception as exc:
            logger.warning("Context spreadsheet override inaccessible (%s); will retry.", exc)

    # 1. Registry hit for this month
    if month_key in registry:
        try:
            sh = client.open_by_key(registry[month_key])
            logger.debug("Using registered spreadsheet for %s.", month_key)
            return sh
        except Exception as exc:
            logger.warning("Registered spreadsheet inaccessible (%s); will retry.", exc)
            registry.pop(month_key, None)

    # Ignore legacy single-spreadsheet mode so each month gets its own sheet.
    registry.pop("permanent", None)

    # 2. Configured spreadsheet from env
    if current_pipeline_context().is_production and settings.google_sheets_spreadsheet_id:
        try:
            sh = client.open_by_key(settings.google_sheets_spreadsheet_id)
            registry[month_key] = settings.google_sheets_spreadsheet_id
            _save_registry(registry)
            logger.info("Using env spreadsheet for %s.", month_key)
            return sh
        except Exception as exc:
            logger.error("Cannot open GOOGLE_SHEETS_SPREADSHEET_ID: %s", exc)

    # 3. Search Drive for an existing spreadsheet with the expected title
    #    (guards against duplicates from previous half-failed creations)
    title = _month_sheet_title()
    sheet_id = _find_existing_spreadsheet_in_drive(title)

    if sheet_id:
        logger.info("Found existing spreadsheet '%s' in Drive (id=%s); reusing it.", title, sheet_id)
        registry[month_key] = sheet_id
        _save_registry(registry)
        try:
            return client.open_by_key(sheet_id)
        except Exception as exc:
            logger.warning("Found spreadsheet %s but cannot open it: %s", sheet_id, exc)
            # Fall through to auto-create

    # 4. Auto-create (prefers OAuth user creds, falls back to service account)
    logger.info("No spreadsheet configured; attempting auto-create: '%s'", title)
    sheet_id = _try_create_spreadsheet_in_drive(title)

    if sheet_id:
        # Share with owner email so user can see it in their Drive
        # (Only needed when created via service account; OAuth creates as user)
        if settings.google_sheets_owner_email:
            try:
                tmp = client.open_by_key(sheet_id)
                tmp.share(settings.google_sheets_owner_email, perm_type="user", role="writer", notify=False)
                logger.info("Shared new spreadsheet with %s", settings.google_sheets_owner_email)
            except Exception as exc:
                logger.warning("Could not share spreadsheet: %s", exc)
        registry[month_key] = sheet_id
        _save_registry(registry)
        sh = client.open_by_key(sheet_id)
        _bootstrap_spreadsheet_tabs(sh)
        return sh

    raise RuntimeError(
        "Cannot create or open a spreadsheet. Options:\n"
        "  A) Set GOOGLE_SHEETS_SPREADSHEET_ID in Railway env vars, OR\n"
        "  B) Run /setup/google-auth to enable OAuth auto-creation, OR\n"
        "  C) Set GOOGLE_DRIVE_PARENT_FOLDER_ID with a shared folder.\n"
        f"Service account: {_get_service_account_email() or 'not configured'}"
    )


def _bootstrap_spreadsheet_tabs(sh) -> None:
    """Set up standard tabs on a freshly created spreadsheet.

    IMPORTANT: Data tabs are created FIRST, then Özet formulas are written.
    If Özet formulas are written before the referenced tabs exist, they show #ERROR!.
    """
    try:
        # 1. Rename default Sheet1 → 📊 Özet (but don't write formulas yet)
        ozet_ws = sh.sheet1
        ozet_ws.update_title("📊 Özet")

        # 2. Create ALL data tabs first (so formula references will work)
        for tab_name in list(_TABS.keys())[1:]:
            headers, _ = _TABS[tab_name]
            new_ws = sh.add_worksheet(title=tab_name, rows=1000, cols=len(headers) + 2)
            _setup_worksheet(new_ws, tab_name, lightweight=True)

        # 3. NOW write Özet formulas (all referenced tabs exist)
        _setup_summary_tab(ozet_ws, _month_label(), lightweight=True)
        _mark_recently_prepared(sh)

        logger.info("Bootstrapped tabs on new spreadsheet.")
    except Exception as exc:
        logger.warning("Could not bootstrap tabs: %s", exc)


# ─── Row builders ─────────────────────────────────────────────────────────────


def _safe(v):
    if v is None:
        return ""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return v
    return str(v)


def _drive_column_letter(tab_name: str) -> str:
    headers, _ = _TABS[tab_name]
    return _col_letter(len(headers) - 1)


def _internal_row_id_column_letter(tab_name: str) -> str:
    headers, _ = _TABS[tab_name]
    return _col_letter(len(headers))


def _drive_cell(drive_link: Optional[str]) -> str:
    """Return a HYPERLINK formula if we have a link, otherwise empty string."""
    if drive_link:
        sep = _formula_arg_separator()
        return f'=HYPERLINK("{drive_link}"{sep}"📄 Görüntüle")'
    return ""


def _return_source_label(category: DocumentCategory | None) -> str:
    labels = {
        DocumentCategory.FATURA: "Fatura",
        DocumentCategory.ODEME_DEKONTU: "Ödeme Dekontu",
        DocumentCategory.HARCAMA_FISI: "Harcama Fişi",
        DocumentCategory.CEK: "Çek",
        DocumentCategory.ELDEN_ODEME: "Elden Ödeme",
        DocumentCategory.MALZEME: "Malzeme / İrsaliye",
        DocumentCategory.IADE: "İade",
        DocumentCategory.BELIRSIZ: "Belirsiz",
    }
    return labels.get(category or DocumentCategory.BELIRSIZ, "Belirsiz")


def _sender_display_name(record: BillRecord) -> str:
    for candidate in (record.sender_name, record.source_sender_name):
        if candidate is None:
            continue
        normalized = str(candidate).strip()
        if not normalized:
            continue
        if normalized.endswith("@c.us") or normalized.endswith("@g.us"):
            continue
        compact = normalized.replace(" ", "").replace("+", "").replace("-", "").replace("(", "").replace(")", "")
        if compact.isdigit():
            continue
        return normalized
    return ""


def _build_row(
    record: BillRecord,
    category: DocumentCategory,
    seq: int,
    drive_link: Optional[str] = None,
    *,
    return_source_category: DocumentCategory | None = None,
) -> list:
    r = record

    if category == DocumentCategory.FATURA:
        return [
            seq,
            _safe(r.document_date), _safe(r.document_time),
            _safe(r.company_name), _safe(r.tax_number), _safe(r.tax_office),
            _safe(r.invoice_number or r.document_number),
            _safe(r.subtotal), _safe(r.vat_rate), _safe(r.vat_amount),
            _safe(r.total_amount),
            _safe(r.payment_method), _safe(r.expense_category),
            _safe(r.description), _safe(r.notes),
            _drive_cell(drive_link),
        ]

    if category == DocumentCategory.ODEME_DEKONTU:
        return [
            seq,
            _safe(r.document_date), _safe(r.document_time),
            _safe(r.company_name),
            _safe(r.document_number or r.invoice_number),
            _sender_display_name(r),
            _safe(r.description),
            _safe(r.total_amount),
            _safe(r.currency or "TRY"),
            _safe(r.notes),
            _drive_cell(drive_link),
        ]

    if category == DocumentCategory.HARCAMA_FISI:
        import re
        plaka = ""
        if r.notes:
            m = re.search(r"[Pp]laka[:\s]+([A-Z0-9]+)", r.notes or "")
            if m:
                plaka = m.group(1)
        return [
            seq,
            _safe(r.document_date), _safe(r.document_time),
            _safe(r.company_name),
            _safe(r.receipt_number or r.document_number),
            _safe(r.tax_number),
            _safe(r.subtotal), _safe(r.vat_rate), _safe(r.vat_amount),
            _safe(r.total_amount),
            _safe(r.payment_method), _safe(r.expense_category),
            _safe(r.description),
            plaka,
            _drive_cell(drive_link),
        ]

    if category == DocumentCategory.CEK:
        return [
            seq,
            _safe(r.document_number or r.receipt_number),
            _safe(r.company_name), _safe(r.tax_number),
            _safe(r.notes),
            _safe(r.document_date),
            _safe(r.total_amount),
            _safe(r.currency or "TRY"),
            _safe(r.description),
            _drive_cell(drive_link),
        ]

    if category == DocumentCategory.ELDEN_ODEME:
        return [
            seq,
            _safe(r.document_date), _safe(r.document_time),
            _safe(r.description),
            _safe(r.total_amount),
            _safe(r.currency or "TRY"),
            _safe(r.source_sender_id),
            _drive_cell(drive_link),
        ]

    if category == DocumentCategory.MALZEME:
        return [
            seq,
            _safe(r.document_date),
            _safe(r.company_name),
            _safe(r.document_number or r.receipt_number),
            _safe(r.description),
            "", "",
            _safe(r.notes),
            "",
            _safe(r.total_amount),
            _safe(r.expense_category),
            _drive_cell(drive_link),
        ]

    if category == DocumentCategory.IADE:
        return [
            seq,
            _safe(r.document_date),
            _return_source_label(return_source_category),
            _safe(r.company_name),
            _safe(r.document_number or r.invoice_number),
            _safe(r.total_amount),
            _safe(r.currency or "TRY"),
            _safe(r.description),
            _drive_cell(drive_link),
        ]

    # BELIRSIZ → Faturalar fallback
    return [
        seq,
        _safe(r.document_date), _safe(r.document_time),
        _safe(r.company_name), _safe(r.tax_number), _safe(r.tax_office),
        _safe(r.invoice_number or r.document_number),
        _safe(r.subtotal), _safe(r.vat_rate), _safe(r.vat_amount),
        _safe(r.total_amount),
        _safe(r.payment_method), _safe(r.expense_category),
        _safe(r.description), _safe(r.notes),
        _drive_cell(drive_link),
    ]


def reset_current_month_spreadsheet_data(*, spreadsheet_id: Optional[str] = None) -> int:
    """Clear current-month data rows while preserving headers, formulas, and formatting."""
    client = _get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable.")

    with _lock:
        sh = client.open_by_key(spreadsheet_id) if spreadsheet_id else _get_or_create_spreadsheet(client)
        touched_tabs = 0

        for tab_name in _TABS:
            ws = _ensure_tab_exists(sh, tab_name)
            touched_tabs += 1
            if tab_name == "📊 Özet":
                continue

            clear_range = f"A3:{_internal_row_id_column_letter(tab_name)}{max(int(getattr(ws, 'row_count', 1000) or 1000), 1000)}"
            _retry_on_rate_limit(lambda ws=ws, clear_range=clear_range: ws.batch_clear([clear_range]))

        _mark_recently_prepared(sh)
        logger.info(
            "Cleared current-month spreadsheet data rows for %s (sheet=%s).",
            _month_key(),
            sh.id,
        )
        return touched_tabs


def _next_seq(ws) -> int:
    try:
        vals = ws.col_values(1)[2:]
        numeric_values: list[int] = []
        for value in vals:
            raw = (value or "").strip()
            if not raw:
                continue
            try:
                numeric_values.append(int(float(raw)))
            except ValueError:
                continue
        return (max(numeric_values) + 1) if numeric_values else 1
    except Exception:
        return 1


def _build_drive_link_target(
    *,
    spreadsheet_id: str,
    tab_name: str,
    row_number: int,
    row_id: str | None = None,
) -> dict[str, str | int]:
    target: dict[str, str | int] = {
        "spreadsheet_id": str(spreadsheet_id),
        "tab_name": tab_name,
        "row_number": int(row_number),
    }
    normalized_row_id = (row_id or "").strip()
    if normalized_row_id:
        target["row_id"] = normalized_row_id
    return target


def _load_pending_drive_uploads() -> list[dict]:
    path = _pending_drive_uploads_state_path()
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return raw if isinstance(raw, list) else []


def _save_pending_drive_uploads(items: list[dict]) -> None:
    _pending_drive_uploads_state_path().write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _normalize_drive_link_target(target: dict[str, str | int]) -> dict[str, str | int]:
    normalized: dict[str, str | int] = {
        "spreadsheet_id": str(target["spreadsheet_id"]),
        "tab_name": str(target["tab_name"]),
        "row_number": int(target["row_number"]),
    }
    row_id = str(target.get("row_id") or "").strip()
    if row_id:
        normalized["row_id"] = row_id
    return normalized


def _drive_link_target_key(target: dict[str, str | int]) -> tuple[str, str, str, str | int]:
    spreadsheet_id = str(target.get("spreadsheet_id") or "")
    tab_name = str(target.get("tab_name") or "")
    row_id = str(target.get("row_id") or "").strip()
    if row_id:
        return (spreadsheet_id, tab_name, "row_id", row_id)
    return (spreadsheet_id, tab_name, "row_number", int(target.get("row_number") or 0))


def _resolve_drive_link_target_row_number(ws, target: dict[str, str | int]) -> int:
    tab_name = str(target["tab_name"])
    row_id = str(target.get("row_id") or "").strip()
    if row_id:
        resolved = _existing_row_numbers_by_pending_id(ws, tab_name, {row_id}).get(row_id)
        if resolved is None:
            raise RuntimeError(
                f"Pending Drive backfill row id '{row_id}' was not found in tab '{tab_name}'."
            )
        return resolved
    return int(target["row_number"])


def _write_drive_link_to_target(target: dict[str, str | int], drive_link: str) -> None:
    client = _get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable for pending Drive link backfill.")

    spreadsheet_id = str(target["spreadsheet_id"])
    tab_name = str(target["tab_name"])

    sh = client.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab_name)
    row_number = _resolve_drive_link_target_row_number(ws, target)
    cell_a1 = f"{_drive_column_letter(tab_name)}{row_number}"

    _retry_on_rate_limit(
        lambda: ws.update([[_drive_cell(drive_link)]], cell_a1, value_input_option="USER_ENTERED")
    )


def queue_pending_document_upload(
    *,
    file_bytes: bytes,
    filename: str,
    mime_type: str,
    targets: list[dict[str, str | int]],
    source_message_id: str | None = None,
) -> None:
    if not targets:
        return

    normalized_targets = [_normalize_drive_link_target(target) for target in targets]

    with _pending_drive_uploads_lock:
        items = _load_pending_drive_uploads()
        if source_message_id:
            for existing in items:
                if (
                    str(existing.get("source_message_id") or "") == source_message_id
                    and str(existing.get("filename") or "") == filename
                    and str(existing.get("mime_type") or "") == mime_type
                ):
                    known_targets = {
                        _drive_link_target_key(target)
                        for target in existing.get("targets", [])
                    }
                    for target in normalized_targets:
                        target_key = _drive_link_target_key(target)
                        if target_key not in known_targets:
                            existing.setdefault("targets", []).append(target)
                            known_targets.add(target_key)
                    _save_pending_drive_uploads(items)
                    logger.warning(
                        "Merged pending Drive upload for message id=%s; target count is now %d.",
                        source_message_id,
                        len(existing.get("targets", [])),
                    )
                    start_pending_drive_upload_worker()
                    return

    pending_id = uuid4().hex
    payload_path = _pending_drive_uploads_dir() / f"{pending_id}.bin"
    payload_path.write_bytes(file_bytes)

    item = {
        "id": pending_id,
        "filename": filename,
        "mime_type": mime_type,
        "payload_path": str(payload_path),
        "targets": normalized_targets,
        "source_message_id": source_message_id or "",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "attempts": 0,
    }

    with _pending_drive_uploads_lock:
        items = _load_pending_drive_uploads()
        items.append(item)
        _save_pending_drive_uploads(items)

    logger.warning(
        "Queued pending Drive upload for message id=%s with %d target cell(s).",
        source_message_id,
        len(normalized_targets),
    )
    start_pending_drive_upload_worker()


def process_pending_document_uploads(*, max_items: int | None = None) -> int:
    processed = 0

    while True:
        if max_items is not None and processed >= max_items:
            break

        with _pending_drive_uploads_lock:
            items = _load_pending_drive_uploads()
            if not items:
                break
            item = dict(items[0])

        payload_path = Path(str(item.get("payload_path", "")))
        if not payload_path.exists():
            logger.warning(
                "Dropping pending Drive upload id=%s because payload is missing: %s",
                item.get("id"),
                payload_path,
            )
            with _pending_drive_uploads_lock:
                items = [
                    existing for existing in _load_pending_drive_uploads()
                    if str(existing.get("id") or "") != str(item.get("id") or "")
                ]
                _save_pending_drive_uploads(items)
            continue

        drive_link = str(item.get("drive_link") or "").strip()
        try:
            if not drive_link:
                drive_link = upload_document(
                    payload_path.read_bytes(),
                    filename=str(item.get("filename") or payload_path.name),
                    mime_type=str(item.get("mime_type") or "application/octet-stream"),
                )
            if not drive_link:
                raise RuntimeError("Drive upload returned no link.")

            for target in item.get("targets", []):
                _write_drive_link_to_target(target, drive_link)

            with _pending_drive_uploads_lock:
                items = [
                    existing for existing in _load_pending_drive_uploads()
                    if str(existing.get("id") or "") != str(item.get("id") or "")
                ]
                _save_pending_drive_uploads(items)
            payload_path.unlink(missing_ok=True)
            processed += 1
            logger.info(
                "Backfilled pending Drive link for message id=%s into %d sheet cell(s).",
                item.get("source_message_id") or "?",
                len(item.get("targets", [])),
            )
        except Exception as exc:
            with _pending_drive_uploads_lock:
                items = _load_pending_drive_uploads()
                for existing in items:
                    if str(existing.get("id") or "") != str(item.get("id") or ""):
                        continue
                    existing["attempts"] = int(existing.get("attempts", 0)) + 1
                    existing["last_error"] = str(exc)
                    if drive_link:
                        existing["drive_link"] = drive_link
                    break
                _save_pending_drive_uploads(items)
            logger.warning(
                "Pending Drive upload retry failed for message id=%s: %s",
                item.get("source_message_id") or "?",
                exc,
            )
            break

    return processed


def _pending_drive_upload_worker() -> None:
    try:
        time.sleep(_PENDING_DRIVE_WORKER_DELAY_SECONDS)
        process_pending_document_uploads()
    except Exception as exc:
        logger.warning("Pending Drive upload worker stopped after error: %s", exc)


def start_pending_drive_upload_worker() -> None:
    if not current_pipeline_context().is_production:
        return

    global _pending_drive_worker_thread

    with _pending_drive_worker_lock:
        if _pending_drive_worker_thread is not None and _pending_drive_worker_thread.is_alive():
            return

        _pending_drive_worker_thread = threading.Thread(
            target=_pending_drive_upload_worker,
            name="google-sheets-pending-drive-upload",
            daemon=True,
        )
        _pending_drive_worker_thread.start()


def _load_pending_sheet_appends() -> list[dict]:
    path = _pending_sheet_appends_state_path()
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return raw if isinstance(raw, list) else []


def _save_pending_sheet_appends(items: list[dict]) -> None:
    _pending_sheet_appends_state_path().write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _has_pending_sheet_appends() -> bool:
    with _pending_sheet_appends_lock:
        return bool(_load_pending_sheet_appends())


def _registered_spreadsheet_id_for_month(target_month_key: str) -> Optional[str]:
    context = current_pipeline_context()
    if context.spreadsheet_id_override:
        return context.spreadsheet_id_override

    registry = _load_registry()
    spreadsheet_id = registry.get(target_month_key)
    if spreadsheet_id:
        return spreadsheet_id
    if context.is_production and target_month_key == _month_key() and settings.google_sheets_spreadsheet_id:
        return settings.google_sheets_spreadsheet_id
    return None


def _resolve_pending_sheet_spreadsheet_id(*, client=None, month_key: Optional[str] = None) -> Optional[str]:
    target_month_key = month_key or _month_key()
    spreadsheet_id = _registered_spreadsheet_id_for_month(target_month_key)
    if spreadsheet_id:
        return spreadsheet_id

    if target_month_key != _month_key():
        return None

    sheet_client = client or _get_client()
    if sheet_client is None:
        return None

    try:
        with _lock:
            sh = _get_or_create_spreadsheet(sheet_client)
        return sh.id
    except Exception as exc:
        logger.warning("Could not resolve spreadsheet for month %s: %s", target_month_key, exc)
        return None


def _pending_payload_storage_limit_bytes() -> int:
    limit_mb = max(int(settings.pending_payload_storage_limit_mb), 0)
    return limit_mb * 1024 * 1024


def _pending_payload_storage_usage_bytes() -> int:
    total = 0
    seen_paths: set[str] = set()
    for directory in (_pending_sheet_appends_dir(), _pending_drive_uploads_dir()):
        for path in directory.glob("*.bin"):
            try:
                resolved = str(path.resolve())
            except Exception:
                resolved = str(path)
            if resolved in seen_paths or not path.exists():
                continue
            seen_paths.add(resolved)
            total += path.stat().st_size
    return total


def _shared_pending_sheet_payload_path(
    *,
    source_message_id: str | None,
    filename: str | None,
    mime_type: str | None,
) -> Path:
    key = "|".join([
        (source_message_id or "").strip(),
        (filename or "").strip(),
        (mime_type or "").strip(),
    ])
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return _pending_sheet_appends_dir() / f"{digest}.bin"


def _get_or_create_shared_pending_sheet_payload(
    *,
    source_message_id: str | None,
    filename: str | None,
    mime_type: str | None,
    payload_bytes: bytes,
) -> str:
    payload_path = _shared_pending_sheet_payload_path(
        source_message_id=source_message_id,
        filename=filename,
        mime_type=mime_type,
    )
    if payload_path.exists():
        return str(payload_path)

    payload_size = len(payload_bytes)
    storage_limit = _pending_payload_storage_limit_bytes()
    current_usage = _pending_payload_storage_usage_bytes()
    if storage_limit and (current_usage + payload_size) > storage_limit:
        logger.error(
            "Skipping pending sheet payload for message id=%s because payload queue budget would exceed %d MB (current=%d bytes, incoming=%d bytes).",
            source_message_id or "?",
            settings.pending_payload_storage_limit_mb,
            current_usage,
            payload_size,
        )
        return ""

    payload_path.write_bytes(payload_bytes)
    return str(payload_path)


def _cleanup_pending_sheet_payload_if_unused(payload_path_raw: str) -> None:
    if not payload_path_raw:
        return

    payload_path = Path(payload_path_raw)
    with _pending_sheet_appends_lock:
        still_referenced = any(
            str(item.get("document_payload_path") or "") == payload_path_raw
            for item in _load_pending_sheet_appends()
        )
    if not still_referenced:
        payload_path.unlink(missing_ok=True)


def _queue_pending_sheet_append_item(
    *,
    record: BillRecord,
    category: DocumentCategory,
    tab_name: str,
    drive_link: Optional[str],
    return_source_category: DocumentCategory | None = None,
    document_payload: bytes | None = None,
    document_filename: str | None = None,
    document_mime_type: str | None = None,
) -> dict:
    payload_path = ""
    if document_payload:
        payload_path = _get_or_create_shared_pending_sheet_payload(
            source_message_id=record.source_message_id,
            filename=document_filename,
            mime_type=document_mime_type,
            payload_bytes=document_payload,
        )

    month_key = _month_key()
    return {
        "id": uuid4().hex,
        "spreadsheet_id": _registered_spreadsheet_id_for_month(month_key) or "",
        "month_key": month_key,
        "tab_name": tab_name,
        "category": category.value,
        "return_source_category": return_source_category.value if return_source_category else "",
        "record": record.model_dump(mode="json"),
        "drive_link": drive_link or "",
        "document_payload_path": payload_path,
        "document_filename": document_filename or "",
        "document_mime_type": document_mime_type or "",
        "source_message_id": record.source_message_id or "",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "attempts": 0,
        "next_attempt_at": time.time(),
    }


def _pending_sheet_retry_delay_seconds(attempts: int) -> float:
    exponent = max(attempts - 1, 0)
    return min(_PENDING_SHEET_WORKER_RETRY_DELAY_SECONDS * (2 ** exponent), 300.0)


def _pending_sheet_item_is_ready(item: dict) -> bool:
    return float(item.get("next_attempt_at") or 0.0) <= time.time()


def _select_pending_sheet_batch(*, batch_size: int) -> list[dict]:
    with _pending_sheet_appends_lock:
        items = _load_pending_sheet_appends()

    if not items:
        return []

    first_ready = next((item for item in items if _pending_sheet_item_is_ready(item)), None)
    if first_ready is None:
        return []

    batch_key = (
        str(first_ready.get("spreadsheet_id") or ""),
        str(first_ready.get("month_key") or ""),
        str(first_ready.get("tab_name") or ""),
    )
    batch: list[dict] = []
    for item in items:
        item_key = (
            str(item.get("spreadsheet_id") or ""),
            str(item.get("month_key") or ""),
            str(item.get("tab_name") or ""),
        )
        if item_key != batch_key or not _pending_sheet_item_is_ready(item):
            continue
        batch.append(dict(item))
        if len(batch) >= batch_size:
            break
    return batch


def _remove_pending_sheet_appends(item_ids: set[str]) -> None:
    if not item_ids:
        return

    with _pending_sheet_appends_lock:
        items = [
            item for item in _load_pending_sheet_appends()
            if str(item.get("id") or "") not in item_ids
        ]
        _save_pending_sheet_appends(items)


def _mark_pending_sheet_batch_failure(batch: list[dict], exc: Exception) -> None:
    failed_ids = {str(item.get("id") or "") for item in batch}
    now = time.time()
    with _pending_sheet_appends_lock:
        items = _load_pending_sheet_appends()
        for item in items:
            item_id = str(item.get("id") or "")
            if item_id not in failed_ids:
                continue
            attempts = int(item.get("attempts", 0)) + 1
            item["attempts"] = attempts
            item["last_error"] = str(exc)
            item["next_attempt_at"] = now + _pending_sheet_retry_delay_seconds(attempts)
        _save_pending_sheet_appends(items)


def _next_pending_sheet_retry_delay() -> float | None:
    with _pending_sheet_appends_lock:
        items = _load_pending_sheet_appends()

    if not items:
        return None

    next_attempt_at = min(float(item.get("next_attempt_at") or 0.0) for item in items)
    return max(next_attempt_at - time.time(), 0.0)


def _existing_row_numbers_by_pending_id(ws, tab_name: str, pending_ids: set[str]) -> dict[str, int]:
    if not pending_ids:
        return {}

    column_letter = _internal_row_id_column_letter(tab_name)
    try:
        values = ws.get(f"{column_letter}3:{column_letter}")
    except Exception:
        return {}

    rows: dict[str, int] = {}
    for row_number, row in enumerate(values, start=3):
        cell = str(row[0]).strip() if row else ""
        if cell in pending_ids:
            rows[cell] = row_number
    return rows


def process_pending_sheet_appends(*, max_items: int | None = None) -> int:
    processed = 0

    while True:
        if max_items is not None and processed >= max_items:
            break

        remaining_limit = _PENDING_SHEET_BATCH_SIZE
        if max_items is not None:
            remaining_limit = max(1, min(_PENDING_SHEET_BATCH_SIZE, max_items - processed))

        batch = _select_pending_sheet_batch(batch_size=remaining_limit)
        if not batch:
            break

        try:
            client = _get_client()
            if client is None:
                raise RuntimeError("Google Sheets client unavailable for queued appends.")

            spreadsheet_id = str(batch[0].get("spreadsheet_id") or "")
            month_key = str(batch[0].get("month_key") or "") or None
            if not spreadsheet_id:
                spreadsheet_id = _resolve_pending_sheet_spreadsheet_id(
                    client=client,
                    month_key=month_key,
                ) or ""
            if not spreadsheet_id:
                raise RuntimeError("No spreadsheet available for queued append batch.")

            tab_name = str(batch[0].get("tab_name") or "")
            row_numbers: dict[str, int] = {}

            with _lock:
                sh = client.open_by_key(spreadsheet_id)
                _audit_spreadsheet_layout(sh, repair=True, target_tabs={tab_name, "📊 Özet"})
                ws = _ensure_tab_exists(sh, tab_name)
                existing_row_numbers = _existing_row_numbers_by_pending_id(
                    ws,
                    tab_name,
                    {str(item.get("id") or "") for item in batch},
                )
                row_numbers.update(existing_row_numbers)

                new_items = [
                    item for item in batch
                    if str(item.get("id") or "") not in existing_row_numbers
                ]
                if new_items:
                    start_seq = _next_seq(ws)
                    start_row_number = len(ws.col_values(1)) + 1
                    rows: list[list] = []

                    for index, item in enumerate(new_items):
                        record = BillRecord.model_validate(item.get("record") or {})
                        category = DocumentCategory(str(item.get("category") or DocumentCategory.BELIRSIZ.value))
                        return_source_raw = str(item.get("return_source_category") or "").strip()
                        return_source_category = DocumentCategory(return_source_raw) if return_source_raw else None
                        rows.append(
                            _build_row(
                                record,
                                category,
                                start_seq + index,
                                drive_link=str(item.get("drive_link") or "") or None,
                                return_source_category=return_source_category,
                            ) + [str(item.get("id") or "")]
                        )
                        row_numbers[str(item.get("id") or "")] = start_row_number + index

                    _retry_on_rate_limit(
                        lambda: ws.append_rows(rows, value_input_option="USER_ENTERED")
                    )

            payloads_to_cleanup: set[str] = set()
            for item in batch:
                item_id = str(item.get("id") or "")
                row_number = row_numbers.get(item_id)
                if row_number is None:
                    raise RuntimeError(f"Queued sheet append {item_id} has no resolved row number.")

                payload_path_raw = str(item.get("document_payload_path") or "")
                if str(item.get("drive_link") or "").strip():
                    if payload_path_raw:
                        payloads_to_cleanup.add(payload_path_raw)
                    continue

                if not payload_path_raw:
                    continue

                payload_path = Path(payload_path_raw)
                if not payload_path.exists():
                    raise RuntimeError(f"Queued sheet append payload is missing: {payload_path}")

                queue_pending_document_upload(
                    file_bytes=payload_path.read_bytes(),
                    filename=str(item.get("document_filename") or payload_path.name),
                    mime_type=str(item.get("document_mime_type") or "application/octet-stream"),
                    targets=[
                        _build_drive_link_target(
                            spreadsheet_id=spreadsheet_id,
                            tab_name=tab_name,
                            row_number=row_number,
                            row_id=item_id,
                        )
                    ],
                    source_message_id=str(item.get("source_message_id") or "") or None,
                )
                payloads_to_cleanup.add(payload_path_raw)

            _remove_pending_sheet_appends({str(item.get("id") or "") for item in batch})
            for payload_path_raw in payloads_to_cleanup:
                _cleanup_pending_sheet_payload_if_unused(payload_path_raw)
            processed += len(batch)
            logger.info(
                "Processed %d queued sheet append(s) into %s/%s.",
                len(batch),
                spreadsheet_id,
                tab_name,
            )
        except Exception as exc:
            _mark_pending_sheet_batch_failure(batch, exc)
            logger.warning("Pending sheet append retry failed: %s", exc)
            break

    return processed


def _pending_sheet_append_worker() -> None:
    while True:
        try:
            processed = process_pending_sheet_appends()
        except Exception as exc:
            logger.warning("Pending sheet append worker stopped after error: %s", exc)
            processed = 0

        if processed > 0:
            continue

        wait_seconds = _next_pending_sheet_retry_delay()
        if wait_seconds is None:
            break
        time.sleep(max(wait_seconds, 1.0))


def start_pending_sheet_append_worker() -> None:
    if not current_pipeline_context().is_production:
        return

    global _pending_sheet_worker_thread

    with _pending_sheet_worker_lock:
        if _pending_sheet_worker_thread is not None and _pending_sheet_worker_thread.is_alive():
            return

        _pending_sheet_worker_thread = threading.Thread(
            target=_pending_sheet_append_worker,
            name="google-sheets-pending-append",
            daemon=True,
        )
        _pending_sheet_worker_thread.start()


# ─── Public interface ─────────────────────────────────────────────────────────


def append_record(
    record: BillRecord,
    category: DocumentCategory,
    is_return: bool = False,
    drive_link: Optional[str] = None,
    *,
    pending_document_bytes: bytes | None = None,
    pending_document_filename: str | None = None,
    pending_document_mime_type: str | None = None,
) -> list[dict[str, str | int]]:
    """
    Queue *record* for append into the correct Google Sheets tab for *category*.

    drive_link: Google Drive web-view URL for the original document.
                Shown as a clickable "📄 Görüntüle" link in the 📎 Belge column.
    If is_return is True, also queues a row in ↩️ İadeler.
    All errors are caught so CSV persistence is never disrupted.
    """
    client = _get_client()
    if client is None:
        return []

    try:
        tab_name = _CATEGORY_TAB.get(category, "🧾 Faturalar")
        items = [
            _queue_pending_sheet_append_item(
                record=record,
                category=category,
                tab_name=tab_name,
                drive_link=drive_link,
                document_payload=None if drive_link else pending_document_bytes,
                document_filename=None if drive_link else pending_document_filename,
                document_mime_type=None if drive_link else pending_document_mime_type,
            )
        ]
        if is_return and tab_name != "↩️ İadeler":
            items.append(
                _queue_pending_sheet_append_item(
                    record=record,
                    category=DocumentCategory.IADE,
                    tab_name="↩️ İadeler",
                    drive_link=drive_link,
                    return_source_category=category,
                    document_payload=None if drive_link else pending_document_bytes,
                    document_filename=None if drive_link else pending_document_filename,
                    document_mime_type=None if drive_link else pending_document_mime_type,
                )
            )

        with _pending_sheet_appends_lock:
            pending_items = _load_pending_sheet_appends()
            pending_items.extend(items)
            _save_pending_sheet_appends(pending_items)

        logger.info(
            "Queued %d Google Sheets append(s) for category=%s message_id=%s.",
            len(items),
            category,
            record.source_message_id,
        )
        start_pending_sheet_append_worker()
        return []
    except Exception as exc:
        logger.error(
            "Google Sheets append queueing failed for category=%s message_id=%s: %s",
            category,
            record.source_message_id,
            exc,
            exc_info=True,
        )
        return []


def ensure_current_month_spreadsheet_ready() -> str | None:
    """
    Proactively prepare the current month's spreadsheet.

    This keeps the monthly rollover independent from the first invoice arriving
    after the month changes, while preserving all prior month sheets in Drive.
    """
    client = _get_client()
    if client is None:
        logger.debug("Google Sheets monthly rollover check skipped; client unavailable.")
        return None

    with _lock:
        try:
            sh = _get_or_create_spreadsheet(client)
            if _was_recently_prepared(sh):
                logger.info("Spreadsheet %s was prepared recently; skipping immediate repair.", sh.id)
            else:
                _repair_monthly_spreadsheet_layout(sh)
            logger.info("Google Sheets monthly spreadsheet is ready for %s.", _month_key())
            return sh.id
        except Exception as exc:
            logger.warning("Could not prepare current month's spreadsheet: %s", exc)
            return None


def _monthly_rollover_worker(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        wait_seconds = _seconds_until_next_month_rollover()
        if stop_event.wait(wait_seconds):
            break
        ensure_current_month_spreadsheet_ready()


def start_monthly_rollover_scheduler() -> None:
    if not current_pipeline_context().is_production:
        return

    global _rollover_thread, _rollover_stop_event

    with _rollover_lock:
        if _rollover_thread is not None and _rollover_thread.is_alive():
            return

        _rollover_stop_event = threading.Event()
        _rollover_thread = threading.Thread(
            target=_monthly_rollover_worker,
            args=(_rollover_stop_event,),
            name="google-sheets-monthly-rollover",
            daemon=True,
        )
        _rollover_thread.start()
        logger.info(
            "Started Google Sheets monthly rollover scheduler (timezone=%s).",
            settings.business_timezone,
        )


def stop_monthly_rollover_scheduler() -> None:
    global _rollover_thread, _rollover_stop_event

    with _rollover_lock:
        stop_event = _rollover_stop_event
        thread = _rollover_thread
        _rollover_stop_event = None
        _rollover_thread = None

    if stop_event is not None:
        stop_event.set()
    if thread is not None and thread.is_alive():
        thread.join(timeout=1.0)


def ensure_summary_tab_exists(spreadsheet_id: Optional[str] = None) -> None:
    """
    Utility: ensure the 📊 Özet tab exists on the current month's sheet.
    Called on startup or on demand.
    """
    client = _get_client()
    if client is None:
        return
    try:
        sh = client.open_by_key(spreadsheet_id) if spreadsheet_id else _get_or_create_spreadsheet(client)
        _repair_monthly_spreadsheet_layout(sh)
        logger.info("📊 Özet tab ensured.")
    except Exception as exc:
        logger.warning("Could not ensure Özet tab: %s", exc)
