"""
Google Sheets integration for the accounting pipeline.

Responsibilities:
- Authenticate with a service account (base64-encoded JSON in env).
- Resolve the correct spreadsheet for the current month.
  * If GOOGLE_SHEETS_SPREADSHEET_ID is set → always use that sheet (manual mode).
  * Otherwise → auto-create a new spreadsheet each month and track IDs in
    storage/state/sheets_registry.json.
- Set up tabs with headers, colours, and frozen rows on first use.
- Append one row to the correct tab based on DocumentCategory.

All errors are caught and logged; the caller (intake.py) continues with
CSV-only persistence if Sheets is unavailable.
"""

from __future__ import annotations

import base64
import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from app.config import settings
from app.models.schemas import BillRecord, DocumentCategory
from app.utils.logging import get_logger

logger = get_logger(__name__)

# ─── Tab definitions ──────────────────────────────────────────────────────────

# Tab name → (header row, RGBA header background as 0-1 floats)
_TABS: dict[str, tuple[list[str], dict]] = {
    "Özet": (
        [],  # Headers are set via formulas, see _setup_summary_tab
        {"red": 0.13, "green": 0.13, "blue": 0.13},
    ),
    "Faturalar": (
        ["#", "Tarih", "Saat", "Firma Adı", "Vergi No", "Vergi Dairesi",
         "Fatura No", "KDVsiz Tutar", "KDV %", "KDV Tutarı", "GENEL TOPLAM",
         "Ödeme Yöntemi", "Gider Kategorisi", "Açıklama", "Notlar", "Mesaj ID"],
        {"red": 0.16, "green": 0.38, "blue": 0.74},
    ),
    "Dekontlar": (
        ["#", "Tarih", "Saat", "Banka / Firma", "Referans No",
         "Gönderen", "Alıcı / Açıklama", "TUTAR", "Para Birimi", "Notlar", "Mesaj ID"],
        {"red": 0.13, "green": 0.55, "blue": 0.13},
    ),
    "Harcama Fişleri": (
        ["#", "Tarih", "Saat", "Firma", "Fiş No", "Vergi No",
         "KDVsiz", "KDV %", "KDV", "TOPLAM", "Ödeme", "Kategori",
         "Açıklama", "Plaka", "Mesaj ID"],
        {"red": 0.90, "green": 0.49, "blue": 0.13},
    ),
    "Çekler": (
        ["#", "Çek / Belge No", "Düzenleyen Firma", "Vergi No",
         "Lehdar (Alıcı)", "Vade Tarihi", "TUTAR", "Para Birimi",
         "Açıklama", "Mesaj ID"],
        {"red": 0.76, "green": 0.09, "blue": 0.09},
    ),
    "Elden Ödemeler": (
        ["#", "Tarih", "Saat", "Alıcı / Açıklama", "TUTAR", "Para Birimi", "Kaydeden"],
        {"red": 0.46, "green": 0.11, "blue": 0.64},
    ),
    "Malzeme": (
        ["#", "Tarih", "Firma", "İrsaliye / Belge No", "Malzeme Cinsi",
         "Miktar", "Birim", "Teslim Yeri", "Plaka", "Tutar",
         "Açıklama", "Mesaj ID"],
        {"red": 0.47, "green": 0.27, "blue": 0.08},
    ),
    "İadeler": (
        ["#", "Tarih", "Belge Türü", "Firma", "Belge No",
         "TUTAR", "Para Birimi", "Açıklama", "Mesaj ID"],
        {"red": 0.44, "green": 0.44, "blue": 0.44},
    ),
}

# Category → tab name
_CATEGORY_TAB: dict[DocumentCategory, str] = {
    DocumentCategory.FATURA: "Faturalar",
    DocumentCategory.ODEME_DEKONTU: "Dekontlar",
    DocumentCategory.HARCAMA_FISI: "Harcama Fişleri",
    DocumentCategory.CEK: "Çekler",
    DocumentCategory.ELDEN_ODEME: "Elden Ödemeler",
    DocumentCategory.MALZEME: "Malzeme",
    DocumentCategory.IADE: "İadeler",
    DocumentCategory.BELIRSIZ: "Faturalar",  # fallback
}

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_lock = threading.Lock()
_gspread_client = None  # lazy-initialised


# ─── Client initialisation ────────────────────────────────────────────────────


def _get_client():
    """Return an authenticated gspread client, or None if not configured."""
    global _gspread_client
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
        _gspread_client = gspread.authorize(creds)
        logger.info("Google Sheets client initialised (service account: %s)", creds_dict.get("client_email", "?"))
        return _gspread_client
    except Exception as exc:
        logger.error("Failed to initialise Google Sheets client: %s", exc, exc_info=True)
        return None


# ─── Registry helpers (monthly sheet tracking) ───────────────────────────────


def _registry_path() -> Path:
    path = Path(settings.storage_dir) / "state" / "sheets_registry.json"
    path.parent.mkdir(parents=True, exist_ok=True)
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
    return datetime.now().strftime("%Y-%m")


def _month_label() -> str:
    """E.g. 'Nisan 2026'"""
    months_tr = {
        1: "Ocak", 2: "Şubat", 3: "Mart", 4: "Nisan",
        5: "Mayıs", 6: "Haziran", 7: "Temmuz", 8: "Ağustos",
        9: "Eylül", 10: "Ekim", 11: "Kasım", 12: "Aralık",
    }
    now = datetime.now()
    return f"{months_tr[now.month]} {now.year}"


# ─── Spreadsheet setup ────────────────────────────────────────────────────────


def _setup_worksheet(ws, tab_name: str) -> None:
    """Format a worksheet: freeze row 1, bold + coloured headers."""
    import gspread

    headers, color = _TABS[tab_name]
    if not headers:
        return  # Özet tab handled separately

    col_count = len(headers)
    last_col_letter = chr(ord("A") + col_count - 1)
    header_range = f"A1:{last_col_letter}1"

    # Write headers
    ws.update([headers], "A1", value_input_option="RAW")

    # Bold white text on coloured background, centred
    ws.format(header_range, {
        "backgroundColor": color,
        "textFormat": {
            "bold": True,
            "fontSize": 10,
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
        },
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
    })

    # Freeze header row
    ws.freeze(rows=1)

    logger.debug("Worksheet '%s' formatted with %d columns.", tab_name, col_count)


def _setup_summary_tab(ws, month_label: str) -> None:
    """Populate the Özet (summary) tab with labels and cross-sheet formulas."""
    header_color = _TABS["Özet"][1]

    # Title row
    ws.update([["ÖZET — " + month_label]], "A1", value_input_option="RAW")
    ws.format("A1:B1", {
        "backgroundColor": header_color,
        "textFormat": {
            "bold": True,
            "fontSize": 12,
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
        },
        "horizontalAlignment": "CENTER",
    })
    ws.freeze(rows=1)

    # Summary rows: label in A, formula in B
    summary_rows = [
        ["Faturalar Toplamı (TL)", "=IFERROR(SUM(Faturalar!K2:K),0)"],
        ["Ödeme Dekontları (TL)", "=IFERROR(SUM(Dekontlar!H2:H),0)"],
        ["Harcama Fişleri (TL)", "=IFERROR(SUM('Harcama Fişleri'!J2:J),0)"],
        ["Çekler (TL)", "=IFERROR(SUM(Çekler!G2:G),0)"],
        ["Elden Ödemeler (TL)", "=IFERROR(SUM('Elden Ödemeler'!E2:E),0)"],
        ["Malzeme (TL)", "=IFERROR(SUM(Malzeme!J2:J),0)"],
        ["İadeler (TL)", "=IFERROR(SUM(İadeler!F2:F),0)"],
        [],
        ["GENEL TOPLAM (TL)", "=SUM(B2:B8)"],
    ]
    ws.update(summary_rows, "A2", value_input_option="USER_ENTERED")

    # Bold the total row
    ws.format("A10:B10", {"textFormat": {"bold": True, "fontSize": 11}})

    logger.debug("Özet tab populated for %s.", month_label)


def _create_and_setup_spreadsheet(client, title: str) -> str:
    """Create a new spreadsheet with all tabs and return its ID."""
    import gspread

    logger.info("Creating new spreadsheet: '%s'", title)
    sh = client.create(title)
    sheet_id = sh.id

    # Rename the default Sheet1 to Özet
    default_ws = sh.sheet1
    default_ws.update_title("Özet")
    _setup_summary_tab(default_ws, _month_label())

    # Create the remaining tabs
    for tab_name in list(_TABS.keys())[1:]:  # skip Özet (already created)
        ws = sh.add_worksheet(title=tab_name, rows=1000, cols=len(_TABS[tab_name][0]) + 2)
        _setup_worksheet(ws, tab_name)

    # Share with the owner so they can see it in their Google Drive
    if settings.google_sheets_owner_email:
        try:
            sh.share(
                settings.google_sheets_owner_email,
                perm_type="user",
                role="writer",
                notify=False,
            )
            logger.info(
                "Spreadsheet shared with %s", settings.google_sheets_owner_email
            )
        except Exception as exc:
            logger.warning("Could not share spreadsheet with owner: %s", exc)

    logger.info("Spreadsheet '%s' created with ID %s", title, sheet_id)
    return sheet_id


def _get_or_create_spreadsheet(client):
    """
    Return the gspread Spreadsheet object for the current context.

    Priority:
      1. GOOGLE_SHEETS_SPREADSHEET_ID env var (manual/fixed mode).
      2. Monthly auto-create (tracked in sheets_registry.json).
    """
    # --- Manual mode ---
    if settings.google_sheets_spreadsheet_id:
        try:
            return client.open_by_key(settings.google_sheets_spreadsheet_id)
        except Exception as exc:
            logger.error("Cannot open spreadsheet %s: %s", settings.google_sheets_spreadsheet_id, exc)
            raise

    # --- Monthly auto mode ---
    key = _month_key()
    registry = _load_registry()

    if key in registry:
        try:
            sh = client.open_by_key(registry[key])
            logger.debug("Reusing existing spreadsheet for %s: %s", key, registry[key])
            return sh
        except Exception as exc:
            logger.warning("Saved spreadsheet for %s no longer accessible (%s); recreating.", key, exc)

    # Create new
    title = f"Muhasebe — {_month_label()}"
    sheet_id = _create_and_setup_spreadsheet(client, title)
    registry[key] = sheet_id
    _save_registry(registry)
    return client.open_by_key(sheet_id)


def _ensure_tab_exists(sh, tab_name: str):
    """Return the worksheet for tab_name, creating it if missing."""
    import gspread

    try:
        return sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        logger.info("Tab '%s' not found; creating it.", tab_name)
        headers, _ = _TABS.get(tab_name, ([], {}))
        ws = sh.add_worksheet(title=tab_name, rows=1000, cols=max(len(headers) + 2, 10))
        _setup_worksheet(ws, tab_name)
        return ws


def _next_seq(ws) -> int:
    """Return the next sequential row number (1-based, header excluded)."""
    try:
        vals = ws.col_values(1)  # col A: ["#", "1", "2", ...]
        return max(len(vals), 1)  # len includes header row → this equals next seq num
    except Exception:
        return 1


# ─── Row builders ─────────────────────────────────────────────────────────────


def _safe(v) -> str:
    """Convert a value to string, returning empty string for None."""
    if v is None:
        return ""
    return str(v)


def _build_row(record: BillRecord, category: DocumentCategory, seq: int) -> list:
    """Map BillRecord fields to the correct column order for the given category tab."""

    r = record  # shorthand

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
            _safe(r.source_message_id),
        ]

    if category == DocumentCategory.ODEME_DEKONTU:
        return [
            seq,
            _safe(r.document_date), _safe(r.document_time),
            _safe(r.company_name),                            # Banka / Firma
            _safe(r.document_number or r.invoice_number),    # Referans No
            _safe(r.source_sender_id),                       # Gönderen
            _safe(r.description),                            # Alıcı / Açıklama
            _safe(r.total_amount),
            _safe(r.currency or "TRY"),
            _safe(r.notes),
            _safe(r.source_message_id),
        ]

    if category == DocumentCategory.HARCAMA_FISI:
        # Try to pull plate from notes (e.g. "Plaka: 23ABD075")
        plaka = ""
        if r.notes:
            import re
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
            _safe(r.source_message_id),
        ]

    if category == DocumentCategory.CEK:
        return [
            seq,
            _safe(r.document_number or r.receipt_number),   # Çek / Belge No
            _safe(r.company_name),
            _safe(r.tax_number),
            _safe(r.notes),                                  # Lehdar bilgisi notes içinde
            _safe(r.document_date),                          # Vade tarihi
            _safe(r.total_amount),
            _safe(r.currency or "TRY"),
            _safe(r.description),
            _safe(r.source_message_id),
        ]

    if category == DocumentCategory.ELDEN_ODEME:
        return [
            seq,
            _safe(r.document_date), _safe(r.document_time),
            _safe(r.description),                            # Alıcı / Açıklama
            _safe(r.total_amount),
            _safe(r.currency or "TRY"),
            _safe(r.source_sender_id),                       # Kaydeden (manager numarası)
        ]

    if category == DocumentCategory.MALZEME:
        return [
            seq,
            _safe(r.document_date),
            _safe(r.company_name),
            _safe(r.document_number or r.receipt_number),   # İrsaliye No
            _safe(r.description),                            # Malzeme Cinsi
            "",                                              # Miktar (not in BillRecord → from description)
            "",                                              # Birim
            _safe(r.notes),                                  # Teslim Yeri / Plaka
            "",                                              # Plaka ayrı alan
            _safe(r.total_amount),
            _safe(r.expense_category),
            _safe(r.source_message_id),
        ]

    if category == DocumentCategory.IADE:
        return [
            seq,
            _safe(r.document_date),
            _safe(r.expense_category),                       # Belge Türü
            _safe(r.company_name),
            _safe(r.document_number or r.invoice_number),
            _safe(r.total_amount),
            _safe(r.currency or "TRY"),
            _safe(r.description),
            _safe(r.source_message_id),
        ]

    # BELIRSIZ → write to Faturalar as fallback
    return [
        seq,
        _safe(r.document_date), _safe(r.document_time),
        _safe(r.company_name), _safe(r.tax_number), _safe(r.tax_office),
        _safe(r.invoice_number or r.document_number),
        _safe(r.subtotal), _safe(r.vat_rate), _safe(r.vat_amount),
        _safe(r.total_amount),
        _safe(r.payment_method), _safe(r.expense_category),
        _safe(r.description), _safe(r.notes),
        _safe(r.source_message_id),
    ]


# ─── Public interface ─────────────────────────────────────────────────────────


def append_record(record: BillRecord, category: DocumentCategory, is_return: bool = False) -> None:
    """
    Append *record* to the correct Google Sheets tab for *category*.

    If is_return is True, also appends a row to the İadeler tab.

    All errors are caught and logged; this function never raises so that
    the CSV backup flow in intake.py is unaffected.
    """
    client = _get_client()
    if client is None:
        return  # Sheets not configured

    with _lock:
        try:
            sh = _get_or_create_spreadsheet(client)

            # Determine primary tab
            tab_name = _CATEGORY_TAB.get(category, "Faturalar")
            ws = _ensure_tab_exists(sh, tab_name)
            seq = _next_seq(ws)
            row = _build_row(record, category, seq)
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Appended row #%d to '%s' tab.", seq, tab_name)

            # If this is a return document, also log it to İadeler
            if is_return and tab_name != "İadeler":
                iade_ws = _ensure_tab_exists(sh, "İadeler")
                iade_seq = _next_seq(iade_ws)
                iade_row = _build_row(record, DocumentCategory.IADE, iade_seq)
                iade_ws.append_row(iade_row, value_input_option="USER_ENTERED")
                logger.info("Also appended iade row #%d for is_return=True.", iade_seq)

        except Exception as exc:
            logger.error(
                "Google Sheets append failed for category=%s message_id=%s: %s",
                category,
                record.source_message_id,
                exc,
                exc_info=True,
            )
