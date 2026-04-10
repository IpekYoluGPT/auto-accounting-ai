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
import json
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
         "Ödeme Yöntemi", "Gider Kategorisi", "Açıklama", "Notlar", "İşleme", "📎 Belge"],
        {"red": 0.16, "green": 0.38, "blue": 0.74},
    ),
    "💳 Dekontlar": (
        ["#", "Tarih", "Saat", "Banka / Firma", "Referans No",
         "Gönderen", "Alıcı / Açıklama", "TUTAR", "Para Birimi", "Notlar", "İşleme", "📎 Belge"],
        {"red": 0.13, "green": 0.55, "blue": 0.13},
    ),
    "⛽ Harcama Fişleri": (
        ["#", "Tarih", "Saat", "Firma", "Fiş No", "Vergi No",
         "KDVsiz", "KDV %", "KDV", "TOPLAM", "Ödeme", "Kategori",
         "Açıklama", "Plaka", "İşleme", "📎 Belge"],
        {"red": 0.90, "green": 0.49, "blue": 0.13},
    ),
    "📝 Çekler": (
        ["#", "Çek / Belge No", "Düzenleyen Firma", "Vergi No",
         "Lehdar (Alıcı)", "Vade Tarihi", "TUTAR", "Para Birimi",
         "Açıklama", "İşleme", "📎 Belge"],
        {"red": 0.76, "green": 0.09, "blue": 0.09},
    ),
    "💵 Elden Ödemeler": (
        ["#", "Tarih", "Saat", "Alıcı / Açıklama", "TUTAR", "Para Birimi", "Kaydeden", "İşleme", "📎 Belge"],
        {"red": 0.46, "green": 0.11, "blue": 0.64},
    ),
    "🏗️ Malzeme": (
        ["#", "Tarih", "Firma", "İrsaliye / Belge No", "Malzeme Cinsi",
         "Miktar", "Birim", "Teslim Yeri", "Plaka", "Tutar",
         "Açıklama", "İşleme", "📎 Belge"],
        {"red": 0.47, "green": 0.27, "blue": 0.08},
    ),
    "↩️ İadeler": (
        ["#", "Tarih", "Belge Türü", "Firma", "Belge No",
         "TUTAR", "Para Birimi", "Açıklama", "İşleme", "📎 Belge"],
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
    "İşleme": 90,
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
_gspread_client = None  # lazy-initialised
_drive_service = None   # lazy-initialised (service account)
_sheets_service = None  # lazy-initialised (Sheets API v4, service account)
_creds = None           # service account credentials
_drive_folder_cache: dict[str, str] = {}  # month_label → folder_id

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
_recently_prepared_spreadsheets: dict[str, float] = {}
_RECENT_PREPARED_TTL_SECONDS = 180.0
_PENDING_DRIVE_WORKER_DELAY_SECONDS = 15.0


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
    return f"Fişler — {_month_label()}"


def _get_or_create_month_drive_folder() -> Optional[str]:
    """
    Return (creating if needed) a monthly subfolder inside GOOGLE_DRIVE_PARENT_FOLDER_ID.
    E.g. "Fişler — Nisan 2026" inside the user's Muhasebe folder.

    Prefers OAuth drive service (user credentials) for folder creation,
    falls back to service account drive service.
    """
    if not settings.google_drive_parent_folder_id:
        return settings.google_drive_parent_folder_id or None

    label = _month_label()
    if label in _drive_folder_cache:
        return _drive_folder_cache[label]

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
            _drive_folder_cache[label] = folder_id
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
        _drive_folder_cache[label] = folder_id
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
    path = Path(settings.storage_dir) / "state" / "sheets_registry.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _pending_drive_uploads_state_path() -> Path:
    path = Path(settings.storage_dir) / "state" / "pending_drive_uploads.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _pending_drive_uploads_dir() -> Path:
    path = Path(settings.storage_dir) / "state" / "pending_drive_uploads"
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
    row_two = ws.row_values(2)
    if row_two:
        first_cell = row_two[0] if row_two else ""
    else:
        first_cell = ""

    if row_two and not _looks_like_total_row(first_cell):
        ws.insert_row([""] * len(headers), index=2, value_input_option="RAW")

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


def _repair_monthly_spreadsheet_layout(sh) -> None:
    for tab_name in list(_TABS.keys())[1:]:
        ws = _ensure_tab_exists(sh, tab_name)
        _ensure_tab_total_row(ws, tab_name)
        _repair_drive_link_formulas(ws, tab_name)

    summary_ws = _ensure_tab_exists(sh, "📊 Özet")
    _setup_summary_tab(summary_ws, _month_label())
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
            _drive_folder_cache.get(_month_label()),
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
    if settings.google_sheets_spreadsheet_id:
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
    title = f"Muhasebe — {_month_label()}"
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


def _safe(v) -> str:
    if v is None:
        return ""
    return str(v)


def _drive_column_letter(tab_name: str) -> str:
    headers, _ = _TABS[tab_name]
    return _col_letter(len(headers) - 1)


def _drive_cell(drive_link: Optional[str]) -> str:
    """Return a HYPERLINK formula if we have a link, otherwise empty string."""
    if drive_link:
        sep = _formula_arg_separator()
        return f'=HYPERLINK("{drive_link}"{sep}"📄 Görüntüle")'
    return ""


def _build_row(record: BillRecord, category: DocumentCategory, seq: int, drive_link: Optional[str] = None) -> list:
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
            _safe(r.description), _safe(r.notes), _safe(r.processing_method),
            _drive_cell(drive_link),
        ]

    if category == DocumentCategory.ODEME_DEKONTU:
        return [
            seq,
            _safe(r.document_date), _safe(r.document_time),
            _safe(r.company_name),
            _safe(r.document_number or r.invoice_number),
            _safe(r.source_sender_id),
            _safe(r.description),
            _safe(r.total_amount),
            _safe(r.currency or "TRY"),
            _safe(r.notes), _safe(r.processing_method),
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
            plaka, _safe(r.processing_method),
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
            _safe(r.description), _safe(r.processing_method),
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
            _safe(r.processing_method),
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
            _safe(r.expense_category), _safe(r.processing_method),
            _drive_cell(drive_link),
        ]

    if category == DocumentCategory.IADE:
        return [
            seq,
            _safe(r.document_date),
            _safe(r.expense_category),
            _safe(r.company_name),
            _safe(r.document_number or r.invoice_number),
            _safe(r.total_amount),
            _safe(r.currency or "TRY"),
            _safe(r.description), _safe(r.processing_method),
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
        _safe(r.description), _safe(r.notes), _safe(r.processing_method),
        _drive_cell(drive_link),
    ]


def reset_current_month_spreadsheet_data() -> int:
    """Clear current-month sheet data while preserving headers, totals, and formatting."""
    client = _get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable.")

    with _lock:
        sh = _get_or_create_spreadsheet(client)

        for tab_name in _TABS:
            ws = _ensure_tab_exists(sh, tab_name)
            ws.clear()
            if tab_name == "📊 Özet":
                _setup_summary_tab(ws, _month_label())
            else:
                _setup_worksheet(ws, tab_name)

        _mark_recently_prepared(sh)
        logger.info("Reset current-month spreadsheet data for %s (sheet=%s).", _month_key(), sh.id)
        return len(_TABS)


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


def _build_drive_link_target(*, spreadsheet_id: str, tab_name: str, row_number: int) -> dict[str, str | int]:
    return {
        "spreadsheet_id": str(spreadsheet_id),
        "tab_name": tab_name,
        "row_number": int(row_number),
    }


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


def _write_drive_link_to_target(target: dict[str, str | int], drive_link: str) -> None:
    client = _get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable for pending Drive link backfill.")

    spreadsheet_id = str(target["spreadsheet_id"])
    tab_name = str(target["tab_name"])
    row_number = int(target["row_number"])
    cell_a1 = f"{_drive_column_letter(tab_name)}{row_number}"

    sh = client.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab_name)
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

    pending_id = uuid4().hex
    payload_path = _pending_drive_uploads_dir() / f"{pending_id}.bin"
    payload_path.write_bytes(file_bytes)

    item = {
        "id": pending_id,
        "filename": filename,
        "mime_type": mime_type,
        "payload_path": str(payload_path),
        "targets": targets,
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
        len(targets),
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
            item = items.pop(0)
            _save_pending_drive_uploads(items)

        payload_path = Path(str(item.get("payload_path", "")))
        if not payload_path.exists():
            logger.warning(
                "Dropping pending Drive upload id=%s because payload is missing: %s",
                item.get("id"),
                payload_path,
            )
            continue

        try:
            drive_link = upload_document(
                payload_path.read_bytes(),
                filename=str(item.get("filename") or payload_path.name),
                mime_type=str(item.get("mime_type") or "application/octet-stream"),
            )
            if not drive_link:
                raise RuntimeError("Drive upload returned no link.")

            for target in item.get("targets", []):
                _write_drive_link_to_target(target, drive_link)

            payload_path.unlink(missing_ok=True)
            processed += 1
            logger.info(
                "Backfilled pending Drive link for message id=%s into %d sheet cell(s).",
                item.get("source_message_id") or "?",
                len(item.get("targets", [])),
            )
        except Exception as exc:
            item["attempts"] = int(item.get("attempts", 0)) + 1
            item["last_error"] = str(exc)
            with _pending_drive_uploads_lock:
                items = _load_pending_drive_uploads()
                items.append(item)
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


# ─── Public interface ─────────────────────────────────────────────────────────


def append_record(
    record: BillRecord,
    category: DocumentCategory,
    is_return: bool = False,
    drive_link: Optional[str] = None,
) -> list[dict[str, str | int]]:
    """
    Append *record* to the correct Google Sheets tab for *category*.

    drive_link: Google Drive web-view URL for the original document.
                Shown as a clickable "📄 Görüntüle" link in the 📎 Belge column.
    If is_return is True, also logs a row in ↩️ İadeler.
    All errors are caught so CSV persistence is never disrupted.
    """
    client = _get_client()
    if client is None:
        return []

    with _lock:
        try:
            sh = _get_or_create_spreadsheet(client)
            appended_targets: list[dict[str, str | int]] = []

            # Each spreadsheet is already monthly ("Muhasebe — Nisan 2026"),
            # so tabs use base names directly (e.g. "🧾 Faturalar").
            tab_name = _CATEGORY_TAB.get(category, "🧾 Faturalar")
            ws = _ensure_tab_exists(sh, tab_name)
            seq = _next_seq(ws)
            row_number = len(ws.col_values(1)) + 1
            row = _build_row(record, category, seq, drive_link=drive_link)
            _retry_on_rate_limit(
                lambda: ws.append_row(row, value_input_option="USER_ENTERED")
            )
            appended_targets.append(
                _build_drive_link_target(
                    spreadsheet_id=sh.id,
                    tab_name=tab_name,
                    row_number=row_number,
                )
            )
            logger.info("Appended row #%d to '%s'.", seq, tab_name)

            # Also log to ↩️ İadeler if this is a return document
            if is_return and tab_name != "↩️ İadeler":
                iade_ws = _ensure_tab_exists(sh, "↩️ İadeler")
                iade_seq = _next_seq(iade_ws)
                iade_row_number = len(iade_ws.col_values(1)) + 1
                iade_row = _build_row(record, DocumentCategory.IADE, iade_seq, drive_link=drive_link)
                _retry_on_rate_limit(
                    lambda: iade_ws.append_row(iade_row, value_input_option="USER_ENTERED")
                )
                appended_targets.append(
                    _build_drive_link_target(
                        spreadsheet_id=sh.id,
                        tab_name="↩️ İadeler",
                        row_number=iade_row_number,
                    )
                )
                logger.info("Also logged iade row #%d.", iade_seq)

            return appended_targets

        except Exception as exc:
            logger.error(
                "Google Sheets append failed for category=%s message_id=%s: %s",
                category,
                record.source_message_id,
                exc,
                exc_info=True,
            )
            return []


def ensure_current_month_spreadsheet_ready() -> None:
    """
    Proactively prepare the current month's spreadsheet.

    This keeps the monthly rollover independent from the first invoice arriving
    after the month changes, while preserving all prior month sheets in Drive.
    """
    client = _get_client()
    if client is None:
        logger.debug("Google Sheets monthly rollover check skipped; client unavailable.")
        return

    with _lock:
        try:
            sh = _get_or_create_spreadsheet(client)
            if _was_recently_prepared(sh):
                logger.info("Spreadsheet %s was prepared recently; skipping immediate repair.", sh.id)
            else:
                _repair_monthly_spreadsheet_layout(sh)
            logger.info("Google Sheets monthly spreadsheet is ready for %s.", _month_key())
        except Exception as exc:
            logger.warning("Could not prepare current month's spreadsheet: %s", exc)


def _monthly_rollover_worker(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        wait_seconds = _seconds_until_next_month_rollover()
        if stop_event.wait(wait_seconds):
            break
        ensure_current_month_spreadsheet_ready()


def start_monthly_rollover_scheduler() -> None:
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
        sh = _get_or_create_spreadsheet(client)
        _repair_monthly_spreadsheet_layout(sh)
        logger.info("📊 Özet tab ensured.")
    except Exception as exc:
        logger.warning("Could not ensure Özet tab: %s", exc)
