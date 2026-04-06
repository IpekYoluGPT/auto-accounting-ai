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
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

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
         "Ödeme Yöntemi", "Gider Kategorisi", "Açıklama", "Notlar", "Mesaj ID"],
        {"red": 0.16, "green": 0.38, "blue": 0.74},
    ),
    "💳 Dekontlar": (
        ["#", "Tarih", "Saat", "Banka / Firma", "Referans No",
         "Gönderen", "Alıcı / Açıklama", "TUTAR", "Para Birimi", "Notlar", "Mesaj ID"],
        {"red": 0.13, "green": 0.55, "blue": 0.13},
    ),
    "⛽ Harcama Fişleri": (
        ["#", "Tarih", "Saat", "Firma", "Fiş No", "Vergi No",
         "KDVsiz", "KDV %", "KDV", "TOPLAM", "Ödeme", "Kategori",
         "Açıklama", "Plaka", "Mesaj ID"],
        {"red": 0.90, "green": 0.49, "blue": 0.13},
    ),
    "📝 Çekler": (
        ["#", "Çek / Belge No", "Düzenleyen Firma", "Vergi No",
         "Lehdar (Alıcı)", "Vade Tarihi", "TUTAR", "Para Birimi",
         "Açıklama", "Mesaj ID"],
        {"red": 0.76, "green": 0.09, "blue": 0.09},
    ),
    "💵 Elden Ödemeler": (
        ["#", "Tarih", "Saat", "Alıcı / Açıklama", "TUTAR", "Para Birimi", "Kaydeden"],
        {"red": 0.46, "green": 0.11, "blue": 0.64},
    ),
    "🏗️ Malzeme": (
        ["#", "Tarih", "Firma", "İrsaliye / Belge No", "Malzeme Cinsi",
         "Miktar", "Birim", "Teslim Yeri", "Plaka", "Tutar",
         "Açıklama", "Mesaj ID"],
        {"red": 0.47, "green": 0.27, "blue": 0.08},
    ),
    "↩️ İadeler": (
        ["#", "Tarih", "Belge Türü", "Firma", "Belge No",
         "TUTAR", "Para Birimi", "Açıklama", "Mesaj ID"],
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
    "Mesaj ID": 48,
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
_gspread_client = None  # lazy-initialised


# ─── Client initialisation ────────────────────────────────────────────────────


def _get_client():
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
        logger.info(
            "Google Sheets client initialised (service account: %s)",
            creds_dict.get("client_email", "?"),
        )
        return _gspread_client
    except Exception as exc:
        logger.error("Failed to initialise Google Sheets client: %s", exc, exc_info=True)
        return None


# ─── Registry helpers ─────────────────────────────────────────────────────────


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
    months_tr = {
        1: "Ocak", 2: "Şubat", 3: "Mart", 4: "Nisan",
        5: "Mayıs", 6: "Haziran", 7: "Temmuz", 8: "Ağustos",
        9: "Eylül", 10: "Ekim", 11: "Kasım", 12: "Aralık",
    }
    now = datetime.now()
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


def _setup_worksheet(ws, tab_name: str) -> None:
    """Format a data worksheet: freeze row 1, bold + coloured headers,
    column widths, text-wrap on long fields, number format on amounts."""
    headers, color = _TABS[tab_name]
    if not headers:
        return

    col_count = len(headers)
    last_col = _col_letter(col_count - 1)
    header_range = f"A1:{last_col}1"
    data_range = f"A2:{last_col}1000"

    # Write headers
    ws.update([headers], "A1", value_input_option="RAW")

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

    ws.freeze(rows=1)

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
                        "startRowIndex": 1,
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
                        "startRowIndex": 1,
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


def _setup_summary_tab(ws, month_label: str) -> None:
    """Populate the 📊 Özet tab with title, labels, and cross-sheet SUM formulas."""
    header_color = _TABS["📊 Özet"][1]

    ws.update([["📊 ÖZET — " + month_label, ""]], "A1", value_input_option="RAW")
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
    ws.freeze(rows=1)

    summary_rows = [
        ["🧾 Faturalar Toplamı (TL)",       "=IFERROR(SUM('🧾 Faturalar'!K2:K),0)"],
        ["💳 Ödeme Dekontları (TL)",         "=IFERROR(SUM('💳 Dekontlar'!H2:H),0)"],
        ["⛽ Harcama Fişleri (TL)",          "=IFERROR(SUM('⛽ Harcama Fişleri'!J2:J),0)"],
        ["📝 Çekler (TL)",                   "=IFERROR(SUM('📝 Çekler'!G2:G),0)"],
        ["💵 Elden Ödemeler (TL)",           "=IFERROR(SUM('💵 Elden Ödemeler'!E2:E),0)"],
        ["🏗️ Malzeme (TL)",                 "=IFERROR(SUM('🏗️ Malzeme'!J2:J),0)"],
        ["↩️ İadeler (TL)",                  "=IFERROR(SUM('↩️ İadeler'!F2:F),0)"],
        ["", ""],
        ["💰 GENEL TOPLAM (TL)",             "=SUM(B2:B8)"],
    ]
    ws.update(summary_rows, "A2", value_input_option="USER_ENTERED")

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


def _create_and_setup_spreadsheet(client, title: str) -> str:
    """Create a new spreadsheet with all tabs and return its ID."""
    logger.info("Creating new spreadsheet: '%s'", title)

    create_kwargs: dict = {}
    if settings.google_drive_parent_folder_id:
        create_kwargs["folder_id"] = settings.google_drive_parent_folder_id

    sh = client.create(title, **create_kwargs)
    sheet_id = sh.id

    # Rename default Sheet1 → 📊 Özet
    default_ws = sh.sheet1
    default_ws.update_title("📊 Özet")
    _setup_summary_tab(default_ws, _month_label())

    # Create remaining tabs in display order
    for tab_name in list(_TABS.keys())[1:]:
        headers, _ = _TABS[tab_name]
        ws = sh.add_worksheet(
            title=tab_name,
            rows=1000,
            cols=len(headers) + 2,
        )
        _setup_worksheet(ws, tab_name)

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


def _ensure_tab_exists(sh, tab_name: str):
    """
    Return the worksheet for tab_name, creating it if missing.

    Also handles backwards-compat: if a plain-name version exists (no emoji),
    it is automatically renamed to the emoji version.
    """
    import gspread

    # 1. Try exact name (emoji version)
    try:
        return sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        pass

    # 2. Try plain (no-emoji) version and rename if found
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
    headers, _ = _TABS.get(tab_name, ([], {}))
    ws = sh.add_worksheet(
        title=tab_name,
        rows=1000,
        cols=max(len(headers) + 2, 10),
    )

    if tab_name == "📊 Özet":
        _setup_summary_tab(ws, _month_label())
    else:
        _setup_worksheet(ws, tab_name)

    return ws


# ─── Monthly spreadsheet resolution ──────────────────────────────────────────


def _get_or_create_spreadsheet(client):
    """
    Return the gspread Spreadsheet for the current month.

    Priority:
      1. sheets_registry.json entry for this month (already tracked).
      2. GOOGLE_SHEETS_SPREADSHEET_ID env var (seed for this month, saves to registry).
      3. Auto-create in GOOGLE_DRIVE_PARENT_FOLDER_ID.
    """
    key = _month_key()
    registry = _load_registry()

    # 1. Registry hit
    if key in registry:
        try:
            sh = client.open_by_key(registry[key])
            logger.debug("Using registered spreadsheet for %s: %s", key, registry[key])
            return sh
        except Exception as exc:
            logger.warning(
                "Registered spreadsheet for %s inaccessible (%s); will recreate.", key, exc
            )
            registry.pop(key, None)

    # 2. Fixed ID from env — seed this month
    if settings.google_sheets_spreadsheet_id:
        try:
            sh = client.open_by_key(settings.google_sheets_spreadsheet_id)
            registry[key] = settings.google_sheets_spreadsheet_id
            _save_registry(registry)
            logger.info("Seeded registry for %s with env spreadsheet ID.", key)
            return sh
        except Exception as exc:
            logger.error(
                "Cannot open GOOGLE_SHEETS_SPREADSHEET_ID=%s: %s",
                settings.google_sheets_spreadsheet_id,
                exc,
            )

    # 3. Auto-create
    title = f"Muhasebe — {_month_label()}"
    sheet_id = _create_and_setup_spreadsheet(client, title)
    registry[key] = sheet_id
    _save_registry(registry)
    return client.open_by_key(sheet_id)


# ─── Row builders ─────────────────────────────────────────────────────────────


def _safe(v) -> str:
    if v is None:
        return ""
    return str(v)


def _build_row(record: BillRecord, category: DocumentCategory, seq: int) -> list:
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
            _safe(r.source_message_id),
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
            _safe(r.notes),
            _safe(r.source_message_id),
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
            _safe(r.source_message_id),
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
            _safe(r.source_message_id),
        ]

    if category == DocumentCategory.ELDEN_ODEME:
        return [
            seq,
            _safe(r.document_date), _safe(r.document_time),
            _safe(r.description),
            _safe(r.total_amount),
            _safe(r.currency or "TRY"),
            _safe(r.source_sender_id),
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
            _safe(r.source_message_id),
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
            _safe(r.description),
            _safe(r.source_message_id),
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
        _safe(r.source_message_id),
    ]


def _next_seq(ws) -> int:
    try:
        vals = ws.col_values(1)
        return max(len(vals), 1)
    except Exception:
        return 1


# ─── Public interface ─────────────────────────────────────────────────────────


def append_record(
    record: BillRecord,
    category: DocumentCategory,
    is_return: bool = False,
) -> None:
    """
    Append *record* to the correct Google Sheets tab for *category*.

    If is_return is True, also logs a row in ↩️ İadeler.
    All errors are caught so CSV persistence is never disrupted.
    """
    client = _get_client()
    if client is None:
        return

    with _lock:
        try:
            sh = _get_or_create_spreadsheet(client)

            # Primary tab
            tab_name = _CATEGORY_TAB.get(category, "🧾 Faturalar")
            ws = _ensure_tab_exists(sh, tab_name)
            seq = _next_seq(ws)
            row = _build_row(record, category, seq)
            ws.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Appended row #%d to '%s'.", seq, tab_name)

            # Also log to ↩️ İadeler if this is a return document
            if is_return and tab_name != "↩️ İadeler":
                iade_ws = _ensure_tab_exists(sh, "↩️ İadeler")
                iade_seq = _next_seq(iade_ws)
                iade_row = _build_row(record, DocumentCategory.IADE, iade_seq)
                iade_ws.append_row(iade_row, value_input_option="USER_ENTERED")
                logger.info("Also logged iade row #%d.", iade_seq)

        except Exception as exc:
            logger.error(
                "Google Sheets append failed for category=%s message_id=%s: %s",
                category,
                record.source_message_id,
                exc,
                exc_info=True,
            )


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
        _ensure_tab_exists(sh, "📊 Özet")
        logger.info("📊 Özet tab ensured.")
    except Exception as exc:
        logger.warning("Could not ensure Özet tab: %s", exc)
