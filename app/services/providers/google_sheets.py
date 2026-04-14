"""
Google Sheets integration for the accounting pipeline.

Monthly sheet management:
  1. sheets_registry.json tracks month → spreadsheet_id
  2. If current month is in registry → use that sheet
  3. If GOOGLE_SHEETS_SPREADSHEET_ID is set → seed this month's entry
  4. If neither → auto-create in GOOGLE_DRIVE_PARENT_FOLDER_ID

The customer-facing workbook uses four business tabs plus hidden technical tabs.
Backwards-compatible: legacy emoji/plain tab names are normalized to the current layout on first access.
"""

from __future__ import annotations

import base64
import hashlib
import json
import shutil
import ssl
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.config import settings
from app.models.schemas import BillRecord, DocumentCategory
from app.services.accounting import ledger, storage_guard
from app.services.accounting.pipeline_context import PipelineContext, current_pipeline_context, namespace_storage_root, pipeline_context_scope
from app.utils.logging import get_logger

logger = get_logger(__name__)

_SPREADSHEET_LOCALE_CACHE: dict[str, str] = {}

# ─── Tab definitions ──────────────────────────────────────────────────────────

_HIDDEN_ROW_ID_HEADER = "__row_id"
_VISIBLE_DRIVE_LINK_HEADER = "Belge"
_HIDDEN_DRIVE_LINK_HEADER = "__Belge Link"
_HIDDEN_PARTY_KEY_HEADER = "__party_key"
_HIDDEN_SOURCE_DOC_ID_HEADER = "__source_doc_id"
_HIDDEN_TAX_NUMBER_HEADER = "__tax_number"
_HIDDEN_RECORD_KIND_HEADER = "__record_kind"
_HIDDEN_SETTLED_AMOUNT_HEADER = "__settled_amount"
_HIDDEN_ALLOCATION_ID_HEADER = "__allocation_id"
_HIDDEN_PAYMENT_DOC_ID_HEADER = "__payment_doc_id"
_HIDDEN_DEBT_ROW_ID_HEADER = "__debt_row_id"


@dataclass(frozen=True)
class SheetSpec:
    visible_headers: tuple[str, ...]
    hidden_headers: tuple[str, ...]
    color: dict[str, float]
    total_header: str | None = None
    hidden_tab: bool = False
    summary_tab: bool = False

    @property
    def headers(self) -> list[str]:
        return list(self.visible_headers + self.hidden_headers)


_TAB_SPECS: dict[str, SheetSpec] = {
    "📊 Özet": SheetSpec(
        visible_headers=(),
        hidden_headers=(),
        color={"red": 0.13, "green": 0.13, "blue": 0.13},
        hidden_tab=True,
        summary_tab=True,
    ),
    "Masraf Kayıtları": SheetSpec(
        visible_headers=(
            "Tarih",
            "Kategori",
            "Alıcı / Tedarikçi",
            "Açıklama",
            "Belge No / Referans",
            "Bakiye (TL)",
            "Ödenen (TL)",
            "Kalan Borç (TL)",
            _VISIBLE_DRIVE_LINK_HEADER,
        ),
        hidden_headers=(
            _HIDDEN_ROW_ID_HEADER,
            _HIDDEN_PARTY_KEY_HEADER,
            _HIDDEN_SOURCE_DOC_ID_HEADER,
            _HIDDEN_TAX_NUMBER_HEADER,
            _HIDDEN_RECORD_KIND_HEADER,
            _HIDDEN_SETTLED_AMOUNT_HEADER,
        ),
        color={"red": 0.90, "green": 0.49, "blue": 0.13},
        total_header="Kalan Borç (TL)",
    ),
    "Banka Ödemeleri": SheetSpec(
        visible_headers=(
            "Alıcı / Tedarikçi",
            "Açıklama",
            "Referans No",
            "Gönderen",
            "Ödeme Tutarı (TL)",
            "Ödeme Tarihi",
            "Kalan Bakiye (TL)",
            "Durum",
            _VISIBLE_DRIVE_LINK_HEADER,
        ),
        hidden_headers=(
            _HIDDEN_ROW_ID_HEADER,
            _HIDDEN_PARTY_KEY_HEADER,
            _HIDDEN_SOURCE_DOC_ID_HEADER,
            _HIDDEN_PAYMENT_DOC_ID_HEADER,
            _HIDDEN_DEBT_ROW_ID_HEADER,
            _HIDDEN_ALLOCATION_ID_HEADER,
            _HIDDEN_TAX_NUMBER_HEADER,
            _HIDDEN_RECORD_KIND_HEADER,
        ),
        color={"red": 0.13, "green": 0.55, "blue": 0.13},
        total_header="Ödeme Tutarı (TL)",
    ),
    "Faturalar": SheetSpec(
        visible_headers=(
            "Fatura No",
            "Fatura Tarihi",
            "Fatura Tipi",
            "Satıcı (Düzenleyen)",
            "Satıcı VKN/TCKN",
            "Alıcı",
            "Açıklama / Hizmet",
            "Miktar",
            "Birim Fiyat (TL)",
            "Mal/Hizmet Tutarı (TL)",
            "KDV %",
            "KDV Tutarı (TL)",
            "Tevkifat Var mı?",
            "Tevkifat Tutarı (TL)",
            "Ödenecek Tutar (TL)",
            "Para Birimi",
            "Ek Detay",
            _VISIBLE_DRIVE_LINK_HEADER,
        ),
        hidden_headers=(
            _HIDDEN_ROW_ID_HEADER,
            _HIDDEN_PARTY_KEY_HEADER,
            _HIDDEN_SOURCE_DOC_ID_HEADER,
            _HIDDEN_TAX_NUMBER_HEADER,
            _HIDDEN_RECORD_KIND_HEADER,
        ),
        color={"red": 0.16, "green": 0.38, "blue": 0.74},
        total_header="Ödenecek Tutar (TL)",
    ),
    "Sevk Fişleri": SheetSpec(
        visible_headers=(
            "Fiş / Belge No",
            "Tarih",
            "Satıcı",
            "Alıcı",
            "Ürün Cinsi",
            "Ürün Miktarı",
            "Sevk Yeri",
            "Açıklama",
            _VISIBLE_DRIVE_LINK_HEADER,
        ),
        hidden_headers=(
            _HIDDEN_ROW_ID_HEADER,
            _HIDDEN_PARTY_KEY_HEADER,
            _HIDDEN_SOURCE_DOC_ID_HEADER,
            _HIDDEN_TAX_NUMBER_HEADER,
            _HIDDEN_RECORD_KIND_HEADER,
        ),
        color={"red": 0.47, "green": 0.27, "blue": 0.08},
    ),
    "__Raw Belgeler": SheetSpec(
        visible_headers=(
            "Belge ID",
            "Kategori",
            "İade Mi",
            "Firma",
            "Vergi No",
            "Belge No",
            "Fatura No",
            "Fiş No",
            "Tarih",
            "Saat",
            "Toplam",
            "Para Birimi",
            "Gönderen",
            "Alıcı",
            "Açıklama",
            "Notlar",
            "IBAN",
            "Banka",
            "Kaynak Mesaj ID",
        ),
        hidden_headers=(_HIDDEN_DRIVE_LINK_HEADER, _HIDDEN_ROW_ID_HEADER),
        color={"red": 0.30, "green": 0.30, "blue": 0.30},
        hidden_tab=True,
    ),
    "__Fatura Kalemleri": SheetSpec(
        visible_headers=(
            "Belge ID",
            "Kalem No",
            "Açıklama",
            "Miktar",
            "Birim",
            "Birim Fiyat",
            "Tutar",
        ),
        hidden_headers=(_HIDDEN_ROW_ID_HEADER,),
        color={"red": 0.26, "green": 0.26, "blue": 0.54},
        hidden_tab=True,
    ),
    "__Çek_Dekont_Detay": SheetSpec(
        visible_headers=(
            "Belge ID",
            "Kategori",
            "Karşı Taraf",
            "Gönderen",
            "Alıcı",
            "Referans",
            "IBAN",
            "Banka",
            "Çek Seri No",
            "Çek Banka",
            "Çek Şube",
            "Çek Hesap Ref",
            "Keşide Yeri",
            "Keşide Tarihi",
            "Vade Tarihi",
            "Açıklama",
        ),
        hidden_headers=(_HIDDEN_DRIVE_LINK_HEADER, _HIDDEN_ROW_ID_HEADER),
        color={"red": 0.58, "green": 0.14, "blue": 0.14},
        hidden_tab=True,
    ),
    "__Cari_Kartlar": SheetSpec(
        visible_headers=(
            "Party Key",
            "Görünen Ad",
            "Vergi No",
            "Aliaslar",
        ),
        hidden_headers=(_HIDDEN_ROW_ID_HEADER,),
        color={"red": 0.31, "green": 0.31, "blue": 0.31},
        hidden_tab=True,
    ),
    "__Ödeme_Dağıtımları": SheetSpec(
        visible_headers=(
            "Allocation ID",
            "Party Key",
            "Borç Row ID",
            "Ödeme Belge ID",
            "Ödeme Tarihi",
            "Borç Tarihi",
            "Borç Tutarı",
            "Ayrılan Tutar",
            "Kalan",
            "Durum",
        ),
        hidden_headers=(_HIDDEN_ROW_ID_HEADER,),
        color={"red": 0.18, "green": 0.18, "blue": 0.18},
        hidden_tab=True,
    ),
}

_TABS: dict[str, tuple[list[str], dict[str, float]]] = {
    tab_name: (spec.headers, spec.color) for tab_name, spec in _TAB_SPECS.items()
}
_VISIBLE_TABS = [tab_name for tab_name, spec in _TAB_SPECS.items() if not spec.hidden_tab and not spec.summary_tab]
_HIDDEN_TABS = {tab_name for tab_name, spec in _TAB_SPECS.items() if spec.hidden_tab}

_CATEGORY_VISIBLE_TAB: dict[DocumentCategory, str] = {
    DocumentCategory.FATURA: "Faturalar",
    DocumentCategory.ODEME_DEKONTU: "Banka Ödemeleri",
    DocumentCategory.HARCAMA_FISI: "Masraf Kayıtları",
    DocumentCategory.CEK: "Banka Ödemeleri",
    DocumentCategory.ELDEN_ODEME: "Masraf Kayıtları",
    DocumentCategory.MALZEME: "Sevk Fişleri",
    DocumentCategory.IADE: "Faturalar",
    DocumentCategory.BELIRSIZ: "Faturalar",
}

_TAB_ALIASES: dict[str, str] = {
    "Özet": "📊 Özet",
    "📊 Özet": "📊 Özet",
    "Masraf Kayitlari": "Masraf Kayıtları",
    "Masraf Kayıtları": "Masraf Kayıtları",
    "⛽ Harcama Fişleri": "Masraf Kayıtları",
    "💵 Elden Ödemeler": "Masraf Kayıtları",
    "Banka Odemeleri": "Banka Ödemeleri",
    "Banka Ödemeleri": "Banka Ödemeleri",
    "💳 Dekontlar": "Banka Ödemeleri",
    "📝 Çekler": "Banka Ödemeleri",
    "Faturalar": "Faturalar",
    "🧾 Faturalar": "Faturalar",
    "Sevk Fisleri": "Sevk Fişleri",
    "Sevk Fişleri": "Sevk Fişleri",
    "🏗️ Malzeme": "Sevk Fişleri",
    "↩️ İadeler": "Faturalar",
    "İadeler": "Faturalar",
}

_SUMMARY_ROWS: list[tuple[str, str]] = [
    ("Masraf Kalan Borç (TL)", "Masraf Kayıtları"),
    ("Banka Ödemeleri Toplamı (TL)", "Banka Ödemeleri"),
    ("Faturalar Ödenecek Toplam (TL)", "Faturalar"),
]

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ─── Column width map (pixels) ────────────────────────────────────────────────

_COL_WIDTHS: dict[str, int] = {
    "Tarih": 82,
    "Saat": 56,
    "Kategori": 100,
    "Alıcı / Tedarikçi": 150,
    "Açıklama": 180,
    "Belge No / Referans": 116,
    "Belge": 64,
    "Bakiye (TL)": 90,
    "Ödenen (TL)": 90,
    "Kalan Borç (TL)": 94,
    "Borç Tarihi": 78,
    "Kişi Toplam Borcu (TL)": 96,
    "Referans No": 98,
    "Gönderen": 140,
    "Ödeme Tutarı (TL)": 92,
    "Ödeme Tarihi": 78,
    "Kalan Bakiye (TL)": 94,
    "Durum": 84,
    "Fatura No": 78,
    "Fatura Tarihi": 78,
    "Fatura Tipi": 82,
    "Satıcı (Düzenleyen)": 120,
    "Satıcı VKN/TCKN": 92,
    "Alıcı": 108,
    "Açıklama / Hizmet": 132,
    "Miktar": 58,
    "Birim Fiyat (TL)": 74,
    "Mal/Hizmet Tutarı (TL)": 86,
    "KDV %": 52,
    "KDV Tutarı (TL)": 74,
    "Tevkifat Var mı?": 72,
    "Tevkifat Tutarı (TL)": 82,
    "Ödenecek Tutar (TL)": 82,
    "IBAN": 112,
    "Banka": 84,
    "Para Birimi": 72,
    "Ek Detay": 170,
    "Fiş No": 90,
    "Fiş / Belge No": 96,
    "Ürün Cinsi": 140,
    "Palet Sayısı": 72,
    "Adet/Palet": 72,
    "Ürün Miktarı": 82,
    "Plaka": 74,
    "Satıcı": 120,
    "Çıkış Yeri": 108,
    "Sevk Yeri": 108,
    "Belge ID": 160,
    "Firma": 210,
    "Vergi No": 130,
    "Belge No": 120,
    "Toplam": 110,
    "Notlar": 220,
    "Birim": 58,
    "Tutar": 110,
    "Kalem No": 72,
    "Karşı Taraf": 200,
    "Referans": 140,
    "Çek Seri No": 140,
    "Çek Banka": 180,
    "Çek Şube": 140,
    "Çek Hesap Ref": 150,
    "Keşide Yeri": 140,
    "Keşide Tarihi": 95,
    "Vade Tarihi": 90,
    "Party Key": 160,
    "Görünen Ad": 220,
    "Aliaslar": 220,
    "Allocation ID": 160,
    "Borç Row ID": 160,
    "Ödeme Belge ID": 160,
    "Borç Tutarı": 115,
    "Ayrılan Tutar": 115,
    "Kalan": 110,
}

_TAB_COLUMN_WIDTHS: dict[str, dict[str, int]] = {
    "Masraf Kayıtları": {
        "Tarih": 76,
        "Kategori": 92,
        "Alıcı / Tedarikçi": 132,
        "Açıklama": 168,
        "Belge No / Referans": 104,
        "Bakiye (TL)": 86,
        "Ödenen (TL)": 86,
        "Kalan Borç (TL)": 90,
        "Belge": 64,
    },
    "Banka Ödemeleri": {
        "Alıcı / Tedarikçi": 132,
        "Açıklama": 154,
        "Referans No": 92,
        "Gönderen": 126,
        "Ödeme Tutarı (TL)": 88,
        "Ödeme Tarihi": 74,
        "Kalan Bakiye (TL)": 90,
        "Durum": 80,
        "Belge": 64,
    },
    "Faturalar": {
        "Fatura No": 72,
        "Fatura Tarihi": 74,
        "Fatura Tipi": 76,
        "Satıcı (Düzenleyen)": 108,
        "Satıcı VKN/TCKN": 86,
        "Alıcı": 92,
        "Açıklama / Hizmet": 116,
        "Miktar": 54,
        "Birim Fiyat (TL)": 70,
        "Mal/Hizmet Tutarı (TL)": 82,
        "KDV %": 50,
        "KDV Tutarı (TL)": 70,
        "Tevkifat Var mı?": 66,
        "Tevkifat Tutarı (TL)": 76,
        "Ödenecek Tutar (TL)": 76,
        "Para Birimi": 68,
        "Ek Detay": 146,
        "Belge": 62,
    },
    "Sevk Fişleri": {
        "Fiş / Belge No": 88,
        "Tarih": 74,
        "Satıcı": 118,
        "Alıcı": 118,
        "Ürün Cinsi": 150,
        "Ürün Miktarı": 80,
        "Sevk Yeri": 110,
        "Açıklama": 168,
        "Belge": 64,
    },
}


def _column_width(tab_name: str, header: str) -> int:
    return _TAB_COLUMN_WIDTHS.get(tab_name, {}).get(header, _COL_WIDTHS.get(header, 120))

_BRAND_HEADER_COLOR = {"red": 0.12, "green": 0.22, "blue": 0.38}
_ROW_BAND_COLOR = {"red": 0.91, "green": 0.94, "blue": 0.98}
_STATUS_GREEN = {"red": 0.78, "green": 0.93, "blue": 0.80}
_STATUS_YELLOW = {"red": 1.0, "green": 0.93, "blue": 0.67}
_STATUS_ORANGE = {"red": 1.0, "green": 0.88, "blue": 0.74}
_STATUS_RED = {"red": 0.97, "green": 0.78, "blue": 0.80}


# Columns that should wrap text (long free-text fields)
_WRAP_COLUMNS = {
    "Açıklama", "Açıklama / Hizmet", "Ek Detay", "Notlar", "Ürün Cinsi",
    "Çıkış Yeri", "Sevk Yeri", "Aliaslar",
}

# Columns that hold monetary amounts (get number formatting)
_AMOUNT_COLUMNS = {
    "Bakiye (TL)",
    "Ödenen (TL)",
    "Kalan Borç (TL)",
    "Kişi Toplam Borcu (TL)",
    "Ödeme Tutarı (TL)",
    "Kalan Bakiye (TL)",
    "Birim Fiyat (TL)",
    "Mal/Hizmet Tutarı (TL)",
    "KDV Tutarı (TL)",
    "Tevkifat Tutarı (TL)",
    "Ödenecek Tutar (TL)",
    "Toplam",
    "Tutar",
    "Borç Tutarı",
    "Ayrılan Tutar",
    "Kalan",
}


def _add_conditional_format_rule(
    requests: list[dict],
    *,
    sheet_id: int,
    start_row: int,
    end_row: int,
    start_col: int,
    end_col: int,
    condition_type: str,
    values: list[str],
    background: dict[str, float] | None = None,
    foreground: dict[str, float] | None = None,
    bold: bool | None = None,
    index: int = 0,
) -> None:
    format_payload: dict[str, object] = {}
    if background:
        format_payload["backgroundColor"] = background
    if foreground or bold is not None:
        text_format: dict[str, object] = {}
        if foreground:
            text_format["foregroundColor"] = foreground
        if bold is not None:
            text_format["bold"] = bold
        format_payload["textFormat"] = text_format

    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{
                    "sheetId": sheet_id,
                    "startRowIndex": start_row,
                    "endRowIndex": end_row,
                    "startColumnIndex": start_col,
                    "endColumnIndex": end_col,
                }],
                "booleanRule": {
                    "condition": {
                        "type": condition_type,
                        "values": [{"userEnteredValue": value} for value in values],
                    },
                    "format": format_payload,
                },
            },
            "index": index,
        }
    })


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
_LEGACY_IADE_TITLES = {"↩️ İadeler", "İadeler"}
_LEGACY_IADE_PREFIX = "↩️ İadeler LEGACY"
_MANUAL_DRIFT_MARKER = " MANUAL_DRIFT "
_LEGACY_VISIBLE_HEADER_VARIANTS: dict[str, tuple[tuple[str, ...], ...]] = {
    "Banka Ödemeleri": ((
        "Alıcı / Tedarikçi",
        "Açıklama",
        "Borç Tarihi",
        "Kişi Toplam Borcu (TL)",
        "Ödeme Tutarı (TL)",
        "Ödeme Tarihi",
        "Kalan Bakiye (TL)",
        "Durum",
        _VISIBLE_DRIVE_LINK_HEADER,
    ),),
    "Faturalar": ((
        "Fatura No",
        "Fatura Tarihi",
        "Fatura Tipi",
        "Satıcı (Düzenleyen)",
        "Satıcı VKN/TCKN",
        "Alıcı",
        "Açıklama / Hizmet",
        "Miktar",
        "Birim Fiyat (TL)",
        "Mal/Hizmet Tutarı (TL)",
        "KDV %",
        "KDV Tutarı (TL)",
        "Tevkifat Var mı?",
        "Tevkifat Tutarı (TL)",
        "Ödenecek Tutar (TL)",
        "IBAN",
        "Banka",
        _VISIBLE_DRIVE_LINK_HEADER,
    ),),
    "Sevk Fişleri": ((
        "Fiş No",
        "Tarih",
        "Alıcı",
        "Ürün Cinsi",
        "Palet Sayısı",
        "Adet/Palet",
        "Ürün Miktarı",
        "Plaka",
        "Satıcı",
        "Çıkış Yeri",
        "Sevk Yeri",
        _VISIBLE_DRIVE_LINK_HEADER,
    ),),
}


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


def _open_spreadsheet_by_key(client, spreadsheet_id: str):
    return _retry_on_rate_limit(lambda: client.open_by_key(spreadsheet_id))


def _get_worksheet(sh, title: str):
    import gspread

    last_error: Exception | None = None
    for candidate in _tab_title_candidates(title):
        try:
            return _retry_on_rate_limit(lambda candidate=candidate: sh.worksheet(candidate))
        except gspread.WorksheetNotFound as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    return _retry_on_rate_limit(lambda: sh.worksheet(title))


def _list_worksheets(sh):
    return _retry_on_rate_limit(lambda: sh.worksheets())


def _get_range_values(ws, range_name: str, **kwargs):
    return _retry_on_rate_limit(lambda: ws.get(range_name, **kwargs))


def _row_values(ws, row_number: int):
    return _retry_on_rate_limit(lambda: ws.row_values(row_number))


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


def _drive_column_index(tab_name: str) -> int | None:
    return _header_index(tab_name, _VISIBLE_DRIVE_LINK_HEADER) or _header_index(tab_name, _HIDDEN_DRIVE_LINK_HEADER)


def _clear_drive_link_cell_number_format(ws, tab_name: str, row_number: int) -> None:
    col_index = _drive_column_index(tab_name)
    if col_index is None:
        return

    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": row_number - 1,
                        "endRowIndex": row_number,
                        "startColumnIndex": col_index,
                        "endColumnIndex": col_index + 1,
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            }]
        })
    except Exception as exc:
        logger.debug("Could not clear Drive link number format on %s row %s: %s", tab_name, row_number, exc)


def _spreadsheet_locale(*, spreadsheet=None, spreadsheet_id: str | None = None) -> str:
    resolved_id = str(spreadsheet_id or getattr(spreadsheet, "id", "") or "").strip()
    if resolved_id and resolved_id in _SPREADSHEET_LOCALE_CACHE:
        return _SPREADSHEET_LOCALE_CACHE[resolved_id]

    locale_value = str(getattr(spreadsheet, "locale", "") or "").strip()
    if not locale_value and spreadsheet is not None and hasattr(spreadsheet, "fetch_sheet_metadata"):
        try:
            metadata = spreadsheet.fetch_sheet_metadata(params={"fields": "properties.locale"})
            properties = metadata.get("properties", {}) if isinstance(metadata, dict) else {}
            locale_value = str(properties.get("locale") or "").strip()
        except Exception:
            locale_value = ""

    if not locale_value and resolved_id:
        try:
            client = _get_client()
            if client is not None:
                remote_sheet = client.open_by_key(resolved_id)
                locale_value = str(getattr(remote_sheet, "locale", "") or "").strip()
                if not locale_value and hasattr(remote_sheet, "fetch_sheet_metadata"):
                    metadata = remote_sheet.fetch_sheet_metadata(params={"fields": "properties.locale"})
                    properties = metadata.get("properties", {}) if isinstance(metadata, dict) else {}
                    locale_value = str(properties.get("locale") or "").strip()
        except Exception:
            locale_value = ""

    if not locale_value:
        target_month_key = _month_key()
        registered_id = _registered_spreadsheet_id_for_month(target_month_key)
        registered_id = str(registered_id or "").strip()
        if registered_id and registered_id != resolved_id:
            return _spreadsheet_locale(spreadsheet_id=registered_id)

    if resolved_id and locale_value:
        _SPREADSHEET_LOCALE_CACHE[resolved_id] = locale_value
    return locale_value


def _formula_arg_separator(*, spreadsheet=None, spreadsheet_id: str | None = None) -> str:
    locale_value = _spreadsheet_locale(spreadsheet=spreadsheet, spreadsheet_id=spreadsheet_id).lower()
    if locale_value.startswith("en"):
        return ","
    return ";"


def _canonical_tab_name(tab_name: str) -> str:
    raw = str(tab_name or '').strip()
    if not raw:
        return raw
    if ' — ' in raw:
        prefix, suffix = raw.split(' — ', 1)
        canonical_suffix = _canonical_tab_name(suffix)
        if canonical_suffix != suffix:
            return f'{prefix} — {canonical_suffix}'
    return _TAB_ALIASES.get(raw, raw)


def _tab_title_candidates(tab_name: str) -> list[str]:
    raw = str(tab_name or '').strip()
    if not raw:
        return []

    if ' — ' in raw:
        prefix, suffix = raw.split(' — ', 1)
        candidates = [f'{prefix} — {candidate}' for candidate in _tab_title_candidates(suffix)]
        if raw not in candidates:
            candidates.append(raw)
    else:
        canonical = _canonical_tab_name(raw)
        candidates = [canonical]
        for alias, resolved in _TAB_ALIASES.items():
            if resolved == canonical and alias not in candidates:
                candidates.append(alias)
        if raw not in candidates:
            candidates.append(raw)

    deduped: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def _tab_spec(tab_name: str) -> SheetSpec:
    return _TAB_SPECS[_canonical_tab_name(tab_name)]


def _headers(tab_name: str) -> list[str]:
    return _tab_spec(tab_name).headers


def _visible_headers(tab_name: str) -> list[str]:
    return list(_tab_spec(tab_name).visible_headers)


def _visible_header_count(tab_name: str) -> int:
    return len(_tab_spec(tab_name).visible_headers)


def _hidden_headers(tab_name: str) -> list[str]:
    return list(_tab_spec(tab_name).hidden_headers)


def _header_index(tab_name: str, header_name: str) -> int | None:
    try:
        return _headers(tab_name).index(header_name)
    except ValueError:
        return None


def _header_letter(tab_name: str, header_name: str) -> str | None:
    idx = _header_index(tab_name, header_name)
    if idx is None:
        return None
    return _col_letter(idx)


def _tab_total_column_letter(tab_name: str) -> str | None:
    total_header = _tab_spec(tab_name).total_header
    if not total_header:
        return None
    return _header_letter(tab_name, total_header)


def _build_tab_total_formula(tab_name: str, *, spreadsheet=None, spreadsheet_id: str | None = None) -> str | None:
    total_col = _tab_total_column_letter(tab_name)
    if not total_col:
        return None
    sep = _formula_arg_separator(spreadsheet=spreadsheet, spreadsheet_id=spreadsheet_id)
    return f"=IFERROR(SUM({total_col}3:{total_col}){sep}0)"


def _build_summary_formula(tab_name: str, *, spreadsheet=None, spreadsheet_id: str | None = None) -> str:
    canonical_tab_name = _canonical_tab_name(tab_name)
    total_col = _tab_total_column_letter(canonical_tab_name)
    if not total_col:
        raise KeyError(f"No total column configured for {canonical_tab_name}")
    sep = _formula_arg_separator(spreadsheet=spreadsheet, spreadsheet_id=spreadsheet_id)
    return f"=IFERROR('{canonical_tab_name}'!{total_col}2{sep}0)"


def _total_row_values(tab_name: str, *, spreadsheet=None, spreadsheet_id: str | None = None) -> list[str]:
    headers = _headers(tab_name)
    values = [""] * len(headers)
    if headers:
        values[0] = "TOPLAM"
    total_formula = _build_tab_total_formula(tab_name, spreadsheet=spreadsheet, spreadsheet_id=spreadsheet_id)
    total_col = _tab_total_column_letter(tab_name)
    if total_formula and total_col:
        total_col_idx = _header_index(tab_name, _tab_spec(tab_name).total_header or "")
        if total_col_idx is not None:
            values[total_col_idx] = total_formula
    return values


def _looks_like_total_row(first_cell: str | None) -> bool:
    return (first_cell or "").strip().upper() == "TOPLAM"


def _summary_rows() -> list[tuple[str, str]]:
    return list(_SUMMARY_ROWS)


def _visibility_requests(ws, tab_name: str) -> list[dict]:
    headers = _headers(tab_name)
    requests: list[dict] = []
    visible_count = _visible_header_count(tab_name)

    for index, header in enumerate(headers):
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "COLUMNS",
                    "startIndex": index,
                    "endIndex": index + 1,
                },
                "properties": {
                    "pixelSize": _column_width(tab_name, header),
                    "hiddenByUser": index >= visible_count,
                },
                "fields": "pixelSize,hiddenByUser",
            }
        })

    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": ws.id, "hidden": _tab_spec(tab_name).hidden_tab},
            "fields": "hidden",
        }
    })
    return requests


def _apply_lightweight_layout(ws, tab_name: str) -> None:
    requests = _visibility_requests(ws, tab_name)
    if not requests:
        return
    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as exc:
        logger.warning("Lightweight layout batch update failed for '%s': %s", ws.title, exc)


def _setup_worksheet(ws, tab_name: str, *, lightweight: bool = False) -> None:
    """Format a worksheet with visible business columns plus hidden technical columns."""
    headers = _headers(tab_name)
    color = _tab_spec(tab_name).color
    header_color = color
    if not _tab_spec(tab_name).hidden_tab and tab_name != "📊 Özet":
        header_color = _BRAND_HEADER_COLOR
    if not headers:
        return

    col_count = len(headers)
    last_col = _col_letter(col_count - 1)
    visible_last_col = _col_letter(max(_visible_header_count(tab_name) - 1, 0)) if _visible_header_count(tab_name) else "A"
    header_range = f"A1:{last_col}1"
    visible_header_range = f"A1:{visible_last_col}1"
    total_range = f"A2:{last_col}2"
    data_range = f"A3:{last_col}1000"
    visible_data_range = f"A3:{visible_last_col}1000"

    ws.update([headers], "A1", value_input_option="RAW")
    ws.update([_total_row_values(tab_name, spreadsheet=ws.spreadsheet)], "A2", value_input_option="USER_ENTERED")
    ws.freeze(rows=2)

    if lightweight:
        _apply_lightweight_layout(ws, tab_name)
        logger.debug("Worksheet '%s' bootstrapped in lightweight mode.", tab_name)
        return

    ws.format(visible_header_range, {
        "backgroundColor": header_color,
        "textFormat": {
            "bold": True,
            "fontSize": 9,
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
        },
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
        "wrapStrategy": "WRAP",
    })
    if _visible_header_count(tab_name) < len(headers):
        hidden_start = _col_letter(_visible_header_count(tab_name))
        ws.format(f"{hidden_start}1:{last_col}1", {
            "textFormat": {"foregroundColor": {"red": 0.6, "green": 0.6, "blue": 0.6}, "fontSize": 9},
            "backgroundColor": {"red": 0.96, "green": 0.96, "blue": 0.96},
        })

    ws.format(data_range, {
        "verticalAlignment": "MIDDLE",
        "wrapStrategy": "CLIP",
        "textFormat": {"fontSize": 9},
    })
    if not _tab_spec(tab_name).hidden_tab and visible_last_col:
        ws.format(visible_data_range, {
            "backgroundColor": _ROW_BAND_COLOR,
        })
    ws.format(total_range, {
        "backgroundColor": {"red": 0.96, "green": 0.96, "blue": 0.96},
        "textFormat": {"bold": True, "fontSize": 9},
        "verticalAlignment": "MIDDLE",
    })

    requests = _visibility_requests(ws, tab_name)
    requests.append({
        "updateDimensionProperties": {
            "range": {
                "sheetId": ws.id,
                "dimension": "ROWS",
                "startIndex": 0,
                "endIndex": 1,
            },
            "properties": {"pixelSize": 40 if tab_name == "Faturalar" else 34},
            "fields": "pixelSize",
        }
    })

    for i, header in enumerate(headers):
        if header in _WRAP_COLUMNS:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 2,
                        "startColumnIndex": i,
                        "endColumnIndex": i + 1,
                    },
                    "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                    "fields": "userEnteredFormat.wrapStrategy",
                }
            })
        if header in _AMOUNT_COLUMNS:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 2,
                        "startColumnIndex": i,
                        "endColumnIndex": i + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
                            "horizontalAlignment": "RIGHT",
                        }
                    },
                    "fields": "userEnteredFormat(numberFormat,horizontalAlignment)",
                }
            })


    if not _tab_spec(tab_name).hidden_tab:
        rule_index = 0
        if tab_name == "Masraf Kayıtları":
            balance_idx = _header_index(tab_name, "Kalan Borç (TL)")
            if balance_idx is not None:
                _add_conditional_format_rule(
                    requests,
                    sheet_id=ws.id,
                    start_row=2,
                    end_row=1000,
                    start_col=balance_idx,
                    end_col=balance_idx + 1,
                    condition_type="NUMBER_LESS",
                    values=["0"],
                    background=_STATUS_RED,
                    index=rule_index,
                )
                rule_index += 1
                _add_conditional_format_rule(
                    requests,
                    sheet_id=ws.id,
                    start_row=2,
                    end_row=1000,
                    start_col=balance_idx,
                    end_col=balance_idx + 1,
                    condition_type="NUMBER_EQ",
                    values=["0"],
                    background=_STATUS_GREEN,
                    index=rule_index,
                )
                rule_index += 1
                _add_conditional_format_rule(
                    requests,
                    sheet_id=ws.id,
                    start_row=2,
                    end_row=1000,
                    start_col=balance_idx,
                    end_col=balance_idx + 1,
                    condition_type="NUMBER_GREATER",
                    values=["0"],
                    background=_STATUS_YELLOW,
                    index=rule_index,
                )
                rule_index += 1
        if tab_name == "Banka Ödemeleri":
            remaining_idx = _header_index(tab_name, "Kalan Bakiye (TL)")
            if remaining_idx is not None:
                _add_conditional_format_rule(
                    requests,
                    sheet_id=ws.id,
                    start_row=2,
                    end_row=1000,
                    start_col=remaining_idx,
                    end_col=remaining_idx + 1,
                    condition_type="NUMBER_LESS",
                    values=["0"],
                    background=_STATUS_RED,
                    index=rule_index,
                )
                rule_index += 1
                _add_conditional_format_rule(
                    requests,
                    sheet_id=ws.id,
                    start_row=2,
                    end_row=1000,
                    start_col=remaining_idx,
                    end_col=remaining_idx + 1,
                    condition_type="NUMBER_EQ",
                    values=["0"],
                    background=_STATUS_GREEN,
                    index=rule_index,
                )
                rule_index += 1
                _add_conditional_format_rule(
                    requests,
                    sheet_id=ws.id,
                    start_row=2,
                    end_row=1000,
                    start_col=remaining_idx,
                    end_col=remaining_idx + 1,
                    condition_type="NUMBER_GREATER",
                    values=["0"],
                    background=_STATUS_YELLOW,
                    index=rule_index,
                )
                rule_index += 1
            status_idx = _header_index(tab_name, "Durum")
            if status_idx is not None:
                status_rules = [
                    ("TEXT_CONTAINS", "Kapandı", _STATUS_GREEN),
                    ("TEXT_CONTAINS", "Borç Yok", _STATUS_GREEN),
                    ("TEXT_CONTAINS", "Kısmi", _STATUS_YELLOW),
                    ("TEXT_CONTAINS", "Açık", _STATUS_YELLOW),
                    ("TEXT_CONTAINS", "Fazla Ödeme", _STATUS_ORANGE),
                    ("TEXT_CONTAINS", "Eşleşmedi", _STATUS_RED),
                    ("TEXT_CONTAINS", "ÖDENDİ", _STATUS_GREEN),
                    ("TEXT_CONTAINS", "KALAN VAR", _STATUS_RED),
                ]
                for condition, value, color in status_rules:
                    _add_conditional_format_rule(
                        requests,
                        sheet_id=ws.id,
                        start_row=2,
                        end_row=1000,
                        start_col=status_idx,
                        end_col=status_idx + 1,
                        condition_type=condition,
                        values=[value],
                        background=color,
                        index=rule_index,
                    )
                    rule_index += 1
            payment_idx = _header_index(tab_name, "Ödeme Tutarı (TL)")
            if payment_idx is not None:
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": 2,
                            "endRowIndex": 1000,
                            "startColumnIndex": payment_idx,
                            "endColumnIndex": payment_idx + 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {"foregroundColor": {"red": 0.12, "green": 0.32, "blue": 0.78}},
                            }
                        },
                        "fields": "userEnteredFormat.textFormat.foregroundColor",
                    }
                })

    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as exc:
        logger.warning("Column formatting batch update failed for '%s': %s", tab_name, exc)

    logger.debug("Worksheet '%s' formatted (%d columns).", tab_name, col_count)


def _setup_summary_tab(ws, month_label: str, *, lightweight: bool = False) -> None:
    """Populate the hidden summary tab with current high-level totals."""
    header_color = _tab_spec("📊 Özet").color
    summary_rows = _summary_rows()
    total_end_row = len(summary_rows) + 1
    blank_row = total_end_row + 1
    total_row = blank_row + 1
    total_formula = f"=SUM(B2:B{total_end_row})"

    if not lightweight:
        try:
            ws.clear()
        except Exception:
            pass

    values = [["📊 ÖZET — " + month_label, ""]]
    values.extend([[label, _build_summary_formula(tab_name, spreadsheet=ws.spreadsheet)] for label, tab_name in summary_rows])
    values.append(["", ""])
    values.append(["💰 GENEL TOPLAM (TL)", total_formula])
    ws.update(values, "A1", value_input_option="USER_ENTERED")
    ws.freeze(rows=1)

    if lightweight:
        _apply_lightweight_layout(ws, "📊 Özet")
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
    ws.format(f"A2:A{total_end_row}", {"textFormat": {"fontSize": 11}})
    ws.format(f"B2:B{total_end_row}", {
        "textFormat": {"fontSize": 11, "bold": True},
        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
    })
    ws.format(f"A{total_row}:B{total_row}", {
        "textFormat": {"bold": True, "fontSize": 12},
        "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95},
        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
    })
    try:
        ws.spreadsheet.batch_update({
            "requests": [
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                        "properties": {"pixelSize": 280},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
                        "properties": {"pixelSize": 160},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": ws.id, "hidden": True},
                        "fields": "hidden",
                    }
                },
            ]
        })
    except Exception:
        pass

    logger.debug("📊 Özet tab populated for %s.", month_label)

def _ensure_tab_total_row(ws, tab_name: str) -> None:
    headers = _headers(tab_name)
    last_col = _col_letter(len(headers) - 1)

    # Row 2 is reserved for the canonical TOPLAM row. Rewriting it in place is
    # safer than inserting a new row after manual edits, because insertion can
    # shift real data rows and intermittently fail against live Sheets.
    ws.update([_total_row_values(tab_name, spreadsheet=ws.spreadsheet)], "A2", value_input_option="USER_ENTERED")
    ws.format(f"A2:{last_col}2", {
        "backgroundColor": {"red": 0.96, "green": 0.96, "blue": 0.96},
        "textFormat": {"bold": True, "fontSize": 10},
        "verticalAlignment": "MIDDLE",
    })

    total_col = _tab_total_column_letter(tab_name)
    if total_col:
        ws.format(f"{total_col}2:{total_col}2", {
            "textFormat": {"bold": True, "fontSize": 10},
            "horizontalAlignment": "RIGHT",
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
        })
    ws.freeze(rows=2)


def _ensure_worksheet_dimensions(ws, tab_name: str) -> None:
    headers = _headers(tab_name)
    target_cols = len(headers) + 2
    target_rows = max(getattr(ws, "row_count", 0) or 0, 1000)

    try:
        if getattr(ws, "col_count", 0) < target_cols or getattr(ws, "row_count", 0) < target_rows:
            ws.resize(rows=target_rows, cols=target_cols)
    except Exception as exc:
        logger.warning("Could not resize worksheet '%s': %s", tab_name, exc)


def _repair_drive_link_formulas(ws, tab_name: str) -> None:
    drive_col = _drive_column_letter(tab_name)
    drive_range = f"{drive_col}3:{drive_col}"
    try:
        formula_values = ws.get(
            drive_range,
            value_render_option="FORMULA",
        )
        rendered_values = ws.get(
            drive_range,
            value_render_option="UNFORMATTED_VALUE",
        )
    except Exception:
        return

    repaired = 0
    separator = _formula_arg_separator(spreadsheet=ws.spreadsheet)
    current_separator = '","' if separator == ',' else '";"'
    alternate_separator = '";"' if separator == ',' else '","'
    for row_number, row in enumerate(formula_values, start=3):
        raw_value = str((row[0] if row else "") or "")
        normalized_value = raw_value.lstrip()
        if normalized_value.startswith("'=HYPERLINK(\""):
            normalized_value = normalized_value[1:]
        if not normalized_value.startswith('=HYPERLINK("'):
            continue

        desired_formula = normalized_value
        if alternate_separator in desired_formula:
            desired_formula = desired_formula.replace(alternate_separator, current_separator, 1)

        rendered_row = rendered_values[row_number - 3] if row_number - 3 < len(rendered_values) else []
        rendered_value = str((rendered_row[0] if rendered_row else "") or "").lstrip()
        if rendered_value.startswith("'=HYPERLINK(\""):
            rendered_value = rendered_value[1:]

        needs_rewrite = desired_formula != raw_value or rendered_value.startswith('=HYPERLINK("')
        if not needs_rewrite:
            continue

        _clear_drive_link_cell_number_format(ws, tab_name, row_number)
        _retry_on_rate_limit(
            lambda desired_formula=desired_formula, row_number=row_number: ws.update(
                [[desired_formula]],
                f"{drive_col}{row_number}",
                value_input_option="USER_ENTERED",
            )
        )
        repaired += 1

    if repaired:
        logger.info("Repaired %d Drive link formula(s) on '%s'.", repaired, tab_name)


def _worksheet_has_visible_data(ws, tab_name: str) -> bool:
    last_col = _internal_row_id_column_letter(tab_name)
    try:
        rows = _get_range_values(ws, f"A3:{last_col}")
    except Exception:
        return False

    visible_cols = _visible_header_count(tab_name)
    for row in rows:
        if any(str(cell or "").strip() for cell in row[:visible_cols]):
            return True
    return False


def _set_worksheet_hidden(ws, *, hidden: bool = True) -> None:
    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "updateSheetProperties": {
                    "properties": {"sheetId": ws.id, "hidden": hidden},
                    "fields": "hidden",
                }
            }]
        })
    except Exception as exc:
        logger.warning("Could not update hidden state for worksheet '%s': %s", getattr(ws, 'title', '?'), exc)


def _archive_drifted_tab(sh, ws, tab_name: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    base_title = f"{tab_name}{_MANUAL_DRIFT_MARKER}{timestamp}"
    existing_titles = {worksheet.title for worksheet in _list_worksheets(sh)}
    archived_title = base_title[:100]
    counter = 1
    while archived_title in existing_titles:
        suffix = f" {counter}"
        archived_title = f"{base_title[:max(1, 100 - len(suffix))]}{suffix}"
        counter += 1
    ws.update_title(archived_title)
    _set_worksheet_hidden(ws, hidden=True)
    return archived_title


def _archive_legacy_iade_tabs(sh) -> list[str]:
    archived_titles: list[str] = []
    existing_titles = {worksheet.title for worksheet in _list_worksheets(sh)}
    for ws in _list_worksheets(sh):
        title = ws.title
        if title not in _LEGACY_IADE_TITLES:
            continue
        base_title = _LEGACY_IADE_PREFIX
        archived_title = base_title[:100]
        counter = 1
        while archived_title in existing_titles:
            suffix = f" {counter}"
            archived_title = f"{base_title[:max(1, 100 - len(suffix))]}{suffix}"
            counter += 1
        ws.update_title(archived_title)
        _set_worksheet_hidden(ws, hidden=True)
        existing_titles.add(archived_title)
        existing_titles.discard(title)
        archived_titles.append(archived_title)
    return archived_titles


def _is_ignored_orphan_title(title: str) -> bool:
    return title.startswith(_LEGACY_IADE_PREFIX) or _MANUAL_DRIFT_MARKER in title


def _backfill_internal_row_ids(ws, tab_name: str) -> int:
    hidden_col = _internal_row_id_column_letter(tab_name)
    last_col = hidden_col
    try:
        rows = _get_range_values(ws, f"A3:{last_col}")
    except Exception:
        return 0

    repaired = 0
    visible_cols = _visible_header_count(tab_name)
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


def _trimmed_row(values: list[object]) -> list[object]:
    trimmed = list(values)
    while trimmed and not str(trimmed[-1] or "").strip():
        trimmed.pop()
    return trimmed


def _legacy_header_variants(tab_name: str) -> list[list[str]]:
    canonical_tab_name = _canonical_tab_name(tab_name)
    hidden_headers = _hidden_headers(canonical_tab_name)
    return [
        list(visible_headers) + hidden_headers
        for visible_headers in _LEGACY_VISIBLE_HEADER_VARIANTS.get(canonical_tab_name, ())
    ]


def _legacy_header_match(tab_name: str, actual_headers: list[object]) -> list[str] | None:
    canonical_tab_name = _canonical_tab_name(tab_name)
    hidden_headers = _hidden_headers(canonical_tab_name)
    for visible_headers in _LEGACY_VISIBLE_HEADER_VARIANTS.get(canonical_tab_name, ()):
        visible_list = list(visible_headers)
        full_headers = visible_list + hidden_headers
        if actual_headers == full_headers or actual_headers == visible_list:
            return full_headers
    return None


def _tab_headers_match(ws, tab_name: str) -> bool:
    expected_headers = _headers(tab_name)
    actual_headers = _trimmed_row(_row_values(ws, 1))
    return actual_headers == expected_headers


def _tab_headers_can_migrate_in_place(ws, tab_name: str) -> bool:
    actual_headers = _trimmed_row(_row_values(ws, 1))
    return _legacy_header_match(tab_name, actual_headers) is not None


def _tab_total_row_is_valid(ws, tab_name: str) -> bool:
    headers = _headers(tab_name)
    last_col = _col_letter(len(headers) - 1)
    try:
        rows = _get_range_values(ws, f"A2:{last_col}2", value_render_option="FORMULA")
    except Exception:
        return False

    row = rows[0] if rows else []
    if not row or not _looks_like_total_row(row[0] if row else ""):
        return False

    total_formula = _build_tab_total_formula(tab_name, spreadsheet=ws.spreadsheet)
    if not total_formula:
        return True

    total_header = _tab_spec(tab_name).total_header or ""
    total_col_idx = _header_index(tab_name, total_header)
    if total_col_idx is None:
        return True
    actual_formula = str(row[total_col_idx]).strip() if len(row) > total_col_idx else ""
    return actual_formula == total_formula


def _summary_tab_is_valid(ws) -> bool:
    summary_rows = _summary_rows()
    total_end_row = len(summary_rows) + 1
    total_row_index = len(summary_rows) + 2
    expected_total = f"=SUM(B2:B{total_end_row})"

    try:
        rows = _get_range_values(ws, f"A1:B{total_row_index + 1}", value_render_option="FORMULA")
    except Exception:
        return False

    title = str(rows[0][0]).strip() if rows and rows[0] else ""
    if not title.startswith("📊 ÖZET — "):
        return False

    expected_formulas = [_build_summary_formula(tab_name, spreadsheet=ws.spreadsheet) for _, tab_name in summary_rows]
    for index, formula in enumerate(expected_formulas, start=1):
        row = rows[index] if len(rows) > index else []
        actual_formula = str(row[1]).strip() if len(row) > 1 else ""
        if actual_formula != formula:
            return False

    total_row = rows[total_row_index] if len(rows) > total_row_index else []
    actual_total_formula = str(total_row[1]).strip() if len(total_row) > 1 else ""
    return actual_total_formula == expected_total


def _audit_summary_tab(sh, findings: list[dict[str, object]], *, repair: bool, refresh_formatting: bool = False) -> None:
    import gspread

    try:
        ws = _get_worksheet(sh, "📊 Özet")
    except gspread.WorksheetNotFound:
        findings.append({
            "tab_name": "📊 Özet",
            "code": "missing_tab",
            "severity": "error",
            "repaired": False,
            "message": "Summary tab is missing.",
        })
        if repair:
            _ensure_tab_exists(sh, "📊 Özet", lightweight=True)
            findings[-1]["repaired"] = True
        return

    if _summary_tab_is_valid(ws):
        if repair and refresh_formatting:
            _setup_summary_tab(ws, _month_label())
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


def _row_dict_from_headers(row: list[object], headers: list[str]) -> dict[str, object]:
    padded = list(row) + [""] * max(0, len(headers) - len(row))
    return {headers[index]: padded[index] if index < len(padded) else "" for index in range(len(headers))}


def _worksheet_rows_for_headers(
    ws,
    headers: list[str],
    *,
    visible_count: int,
    value_render_option: str | None = None,
) -> list[list[object]]:
    last_col = _col_letter(len(headers) - 1)
    try:
        rows = _get_range_values(ws, f"A3:{last_col}", value_render_option=value_render_option)
    except Exception:
        return []

    result: list[list[object]] = []
    for row in rows:
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        if not any(_text_value(cell) for cell in padded[:visible_count]):
            continue
        result.append(padded[: len(headers)])
    return result


def _doc_row_map(sh, tab_name: str) -> dict[str, dict[str, object]]:
    ws = _ensure_tab_exists(sh, tab_name, lightweight=True)
    rows = _worksheet_rows_as_dicts(ws, tab_name)
    result: dict[str, dict[str, object]] = {}
    for row in rows:
        doc_id = _coalesce_text(row.get("Belge ID"))
        if doc_id and doc_id not in result:
            result[doc_id] = row
    return result


def _legacy_row_hidden_values(tab_name: str, row_map: dict[str, object]) -> list[object]:
    return [row_map.get(header, "") for header in _hidden_headers(tab_name)]


def _remap_legacy_visible_row(
    tab_name: str,
    row: list[object],
    *,
    legacy_headers: list[str],
    raw_by_doc_id: dict[str, dict[str, object]],
    payment_detail_by_doc_id: dict[str, dict[str, object]],
) -> list[object]:
    row_map = _row_dict_from_headers(row, legacy_headers)
    source_doc_id = _coalesce_text(row_map.get(_HIDDEN_SOURCE_DOC_ID_HEADER))
    raw_row = raw_by_doc_id.get(source_doc_id, {})
    detail_row = payment_detail_by_doc_id.get(source_doc_id, {})
    drive_value = row_map.get(_VISIBLE_DRIVE_LINK_HEADER, "")

    if tab_name == "Banka Ödemeleri":
        visible_values = [
            row_map.get("Alıcı / Tedarikçi", ""),
            row_map.get("Açıklama", ""),
            _coalesce_text(detail_row.get("Referans"), raw_row.get("Fatura No"), raw_row.get("Belge No"), raw_row.get("Fiş No")),
            _coalesce_text(detail_row.get("Gönderen"), raw_row.get("Gönderen"), raw_row.get("Firma")),
            row_map.get("Ödeme Tutarı (TL)", ""),
            row_map.get("Ödeme Tarihi", ""),
            row_map.get("Kalan Bakiye (TL)", ""),
            row_map.get("Durum", ""),
            drive_value,
        ]
        return visible_values + _legacy_row_hidden_values(tab_name, row_map)

    if tab_name == "Faturalar":
        visible_values = [
            row_map.get("Fatura No", ""),
            row_map.get("Fatura Tarihi", ""),
            row_map.get("Fatura Tipi", ""),
            row_map.get("Satıcı (Düzenleyen)", ""),
            row_map.get("Satıcı VKN/TCKN", ""),
            row_map.get("Alıcı", ""),
            row_map.get("Açıklama / Hizmet", ""),
            row_map.get("Miktar", ""),
            row_map.get("Birim Fiyat (TL)", ""),
            row_map.get("Mal/Hizmet Tutarı (TL)", ""),
            row_map.get("KDV %", ""),
            row_map.get("KDV Tutarı (TL)", ""),
            row_map.get("Tevkifat Var mı?", ""),
            row_map.get("Tevkifat Tutarı (TL)", ""),
            row_map.get("Ödenecek Tutar (TL)", ""),
            _coalesce_text(raw_row.get("Para Birimi"), "TRY"),
            _join_labeled_parts([
                ("Banka", _coalesce_text(raw_row.get("Banka"), row_map.get("Banka"))),
                ("IBAN", _coalesce_text(raw_row.get("IBAN"), row_map.get("IBAN"))),
                ("Not", raw_row.get("Notlar")),
            ]),
            drive_value,
        ]
        return visible_values + _legacy_row_hidden_values(tab_name, row_map)

    if tab_name == "Sevk Fişleri":
        visible_values = [
            _coalesce_text(row_map.get("Fiş No"), raw_row.get("Fiş No"), raw_row.get("Belge No")),
            row_map.get("Tarih", ""),
            _coalesce_text(row_map.get("Satıcı"), raw_row.get("Firma")),
            row_map.get("Alıcı", ""),
            row_map.get("Ürün Cinsi", ""),
            row_map.get("Ürün Miktarı", ""),
            row_map.get("Sevk Yeri", ""),
            _join_labeled_parts([
                ("Çıkış", row_map.get("Çıkış Yeri", "")),
                ("Plaka", row_map.get("Plaka", "")),
                ("Palet", row_map.get("Palet Sayısı", "")),
                ("Adet/Palet", row_map.get("Adet/Palet", "")),
                ("Not", raw_row.get("Notlar")),
            ]),
            drive_value,
        ]
        return visible_values + _legacy_row_hidden_values(tab_name, row_map)

    return list(row)


def _remapped_legacy_visible_rows(sh, ws, tab_name: str) -> list[list[object]] | None:
    actual_headers = _trimmed_row(_row_values(ws, 1))
    legacy_headers = _legacy_header_match(tab_name, actual_headers)
    if legacy_headers is None:
        return None

    legacy_visible_count = len(legacy_headers) - len(_hidden_headers(tab_name))
    legacy_rows = _worksheet_rows_for_headers(
        ws,
        legacy_headers,
        visible_count=legacy_visible_count,
        value_render_option="FORMULA",
    )
    raw_by_doc_id = _doc_row_map(sh, "__Raw Belgeler")
    payment_detail_by_doc_id = _doc_row_map(sh, "__Çek_Dekont_Detay")
    return [
        _remap_legacy_visible_row(
            tab_name,
            row,
            legacy_headers=legacy_headers,
            raw_by_doc_id=raw_by_doc_id,
            payment_detail_by_doc_id=payment_detail_by_doc_id,
        )
        for row in legacy_rows
    ]


def _apply_remapped_visible_rows(ws, tab_name: str, remapped_rows: list[list[object]]) -> None:
    target_cols = len(_headers(tab_name)) + 2
    target_rows = max(int(getattr(ws, "row_count", 1000) or 1000), 1000)
    try:
        ws.resize(rows=target_rows, cols=target_cols)
    except Exception as exc:
        logger.warning("Could not resize worksheet '%s' during visible schema migration: %s", tab_name, exc)

    _setup_worksheet(ws, tab_name, lightweight=True)
    last_col = _col_letter(len(_headers(tab_name)) - 1)
    try:
        _retry_on_rate_limit(lambda: ws.batch_clear([f"A3:{last_col}{target_rows}"]))
    except Exception as exc:
        logger.warning("Could not clear worksheet '%s' during visible schema migration: %s", tab_name, exc)
    if remapped_rows:
        ws.update(remapped_rows, "A3", value_input_option="USER_ENTERED")
        drive_index = _header_index(tab_name, _VISIBLE_DRIVE_LINK_HEADER)
        if drive_index is not None:
            row_formulas = []
            for row_number, row in enumerate(remapped_rows, start=3):
                if drive_index >= len(row):
                    continue
                url = _extract_drive_link_from_cell_value(row[drive_index])
                if not url:
                    continue
                row_formulas.append((row_number, _drive_cell(url, spreadsheet=ws.spreadsheet)))
            if row_formulas:
                _rewrite_drive_cells(ws, tab_name, row_formulas)
    _ensure_tab_total_row(ws, tab_name)



def _latest_archived_drift_worksheet(sh, tab_name: str):
    prefix = f"{tab_name}{_MANUAL_DRIFT_MARKER}"
    candidates = [ws for ws in _list_worksheets(sh) if str(getattr(ws, "title", "")).startswith(prefix)]
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: str(getattr(candidate, "title", "")))



def _rewrite_legacy_visible_schema_in_place(sh, ws, tab_name: str) -> bool:
    remapped_rows = _remapped_legacy_visible_rows(sh, ws, tab_name)
    if remapped_rows is None:
        return False
    _apply_remapped_visible_rows(ws, tab_name, remapped_rows)
    return True



def _restore_archived_drifted_visible_schema(sh, ws, tab_name: str) -> bool:
    archived_ws = _latest_archived_drift_worksheet(sh, tab_name)
    if archived_ws is None:
        return False
    remapped_rows = _remapped_legacy_visible_rows(sh, archived_ws, tab_name)
    if remapped_rows is None:
        return False
    _apply_remapped_visible_rows(ws, tab_name, remapped_rows)
    return True


def _audit_data_tab(sh, tab_name: str, findings: list[dict[str, object]], *, repair: bool, refresh_formatting: bool = False) -> None:
    import gspread

    try:
        ws = _get_worksheet(sh, tab_name)
    except gspread.WorksheetNotFound:
        findings.append({
            "tab_name": tab_name,
            "code": "missing_tab",
            "severity": "error",
            "repaired": False,
            "message": "Data tab is missing.",
        })
        if repair:
            _ensure_tab_exists(sh, tab_name, lightweight=True)
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
            if _tab_headers_can_migrate_in_place(ws, tab_name):
                migrated = _rewrite_legacy_visible_schema_in_place(sh, ws, tab_name)
                if not migrated:
                    _setup_worksheet(ws, tab_name, lightweight=True)
            elif _worksheet_has_visible_data(ws, tab_name):
                finding["archived_to"] = _archive_drifted_tab(sh, ws, tab_name)
                ws = _ensure_tab_exists(sh, tab_name, lightweight=True)
            else:
                _setup_worksheet(ws, tab_name, lightweight=True)
            finding["repaired"] = True

    if repair and not _worksheet_has_visible_data(ws, tab_name):
        if _restore_archived_drifted_visible_schema(sh, ws, tab_name):
            findings.append({
                "tab_name": tab_name,
                "code": "archived_drift_restored",
                "severity": "info",
                "repaired": True,
                "message": "Recovered visible rows from the latest archived drift worksheet.",
            })

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
        hidden_col = _internal_row_id_column_letter(tab_name)
        try:
            rows = _get_range_values(ws, f"A3:{hidden_col}")
        except Exception:
            rows = []
        missing_count = 0
        visible_cols = _visible_header_count(tab_name)
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

    if repair:
        _set_worksheet_hidden(ws, hidden=_tab_spec(tab_name).hidden_tab)

    if repair and refresh_formatting and not _tab_spec(tab_name).hidden_tab:
        _setup_worksheet(ws, tab_name)


def _audit_spreadsheet_layout(sh, *, repair: bool = False, target_tabs: set[str] | None = None, refresh_formatting: bool = False) -> list[dict[str, object]]:
    findings: list[dict[str, object]] = []
    if repair:
        archived_legacy_tabs = _archive_legacy_iade_tabs(sh)
        for archived_title in archived_legacy_tabs:
            findings.append({
                "tab_name": archived_title,
                "code": "legacy_iade_archived",
                "severity": "info",
                "repaired": True,
                "message": "Archived the legacy İadeler worksheet outside the active layout.",
            })

    canonical_titles = set(_TABS.keys())
    titles_to_check = [tab_name for tab_name in _TABS if target_tabs is None or tab_name in target_tabs]

    worksheets = _list_worksheets(sh)
    existing_titles = {worksheet.title for worksheet in worksheets}
    worksheet_by_title = {worksheet.title: worksheet for worksheet in worksheets}
    for orphan_title in sorted(existing_titles - canonical_titles):
        if _is_ignored_orphan_title(orphan_title):
            if repair:
                ws = worksheet_by_title.get(orphan_title)
                if ws is not None:
                    _set_worksheet_hidden(ws, hidden=True)
            continue
        findings.append({
            "tab_name": orphan_title,
            "code": "orphan_tab",
            "severity": "warning",
            "repaired": False,
            "message": "Found a non-canonical worksheet title.",
        })

    for tab_name in titles_to_check:
        if tab_name == "📊 Özet":
            _audit_summary_tab(sh, findings, repair=repair, refresh_formatting=refresh_formatting)
            continue
        _audit_data_tab(sh, tab_name, findings, repair=repair, refresh_formatting=refresh_formatting)

    return findings

def queue_status() -> dict[str, int]:
    status: dict[str, int | str] = {
        "pending_sheet_appends": len(_load_pending_sheet_appends()),
        "pending_drive_uploads": len(_load_pending_drive_uploads()),
    }
    try:
        from app.services.accounting import inbound_queue

        status.update(inbound_queue.queue_status())
    except Exception:
        status.setdefault("pending_inbound_jobs", 0)
        status.setdefault("retry_waiting_inbound_jobs", 0)
        status.setdefault("failed_inbound_jobs", 0)
        status.setdefault("inbound_payload_storage_bytes", 0)

    try:
        status.update(storage_guard.storage_snapshot().as_dict())
    except Exception:
        status.setdefault("disk_total_bytes", 0)
        status.setdefault("disk_used_bytes", 0)
        status.setdefault("disk_free_bytes", 0)
        status.setdefault("total_managed_storage_bytes", 0)
        status.setdefault("disk_pressure_state", "unknown")
    return status  # type: ignore[return-value]


def _extract_drive_link_from_cell_value(value: object) -> str | None:
    raw = str(value or '').strip()
    if raw.startswith("'"):
        raw = raw[1:].lstrip()
    if raw.startswith('http://') or raw.startswith('https://'):
        return raw

    prefix = '=HYPERLINK("'
    if not raw.startswith(prefix):
        return None

    remainder = raw[len(prefix):]
    url, quote, tail = remainder.partition('"')
    if not quote or not url:
        return None
    if not tail.startswith(',') and not tail.startswith(';'):
        return None
    return url


def _raw_document_drive_link_map(sh) -> dict[str, str]:
    ws = _ensure_tab_exists(sh, '__Raw Belgeler', lightweight=True)
    rows = _worksheet_rows_as_dicts(ws, '__Raw Belgeler', value_render_option='FORMULA')
    links: dict[str, str] = {}
    for row in rows:
        source_doc_id = _coalesce_text(row.get('Belge ID'))
        if not source_doc_id:
            continue
        drive_link = _extract_drive_link_from_cell_value(row.get(_HIDDEN_DRIVE_LINK_HEADER))
        if drive_link:
            links[source_doc_id] = drive_link
    return links


def _iter_visible_row_maps(ws, tab_name: str, *, value_render_option: str | None = None) -> list[tuple[int, dict[str, object]]]:
    last_col = _internal_row_id_column_letter(tab_name)
    try:
        rows = _get_range_values(ws, f"A3:{last_col}", value_render_option=value_render_option)
    except Exception:
        return []

    headers = _headers(tab_name)
    visible_count = _visible_header_count(tab_name)
    mapped_rows: list[tuple[int, dict[str, object]]] = []
    for row_number, row in enumerate(rows, start=3):
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        if not any(_text_value(cell) for cell in padded[:visible_count]):
            continue
        mapped_rows.append((row_number, {headers[idx]: padded[idx] if idx < len(padded) else "" for idx in range(len(headers))}))
    return mapped_rows


def _rewrite_drive_cells(ws, tab_name: str, row_formulas: list[tuple[int, str]]) -> int:
    repaired = 0
    drive_col = _drive_column_letter(tab_name)
    for row_number, desired_formula in row_formulas:
        _clear_drive_link_cell_number_format(ws, tab_name, row_number)
        _retry_on_rate_limit(
            lambda desired_formula=desired_formula, row_number=row_number: ws.update(
                [[desired_formula]],
                f"{drive_col}{row_number}",
                value_input_option='USER_ENTERED',
            )
        )
        repaired += 1
    return repaired


def force_rewrite_drive_links(*, spreadsheet_id: Optional[str] = None, target_tabs: set[str] | None = None) -> dict[str, int]:
    client = _get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable.")

    with _lock:
        sh = _open_spreadsheet_by_key(client, spreadsheet_id) if spreadsheet_id else _get_or_create_spreadsheet(client)
        resolved_tabs = []
        if target_tabs:
            for tab_name in target_tabs:
                canonical = _canonical_tab_name(tab_name)
                if canonical not in resolved_tabs:
                    resolved_tabs.append(canonical)
        else:
            for tab_name in _TABS:
                if _header_index(tab_name, _VISIBLE_DRIVE_LINK_HEADER) is None:
                    continue
                if _tab_spec(tab_name).hidden_tab:
                    continue
                resolved_tabs.append(tab_name)

        raw_drive_links = _raw_document_drive_link_map(sh)
        repaired_by_tab: dict[str, int] = {}
        for tab_name in resolved_tabs:
            if _header_index(tab_name, _VISIBLE_DRIVE_LINK_HEADER) is None:
                repaired_by_tab[tab_name] = 0
                continue

            ws = _ensure_tab_exists(sh, tab_name, lightweight=True)
            row_formulas: list[tuple[int, str]] = []
            for row_number, row_map in _iter_visible_row_maps(ws, tab_name, value_render_option='FORMULA'):
                url = _extract_drive_link_from_cell_value(row_map.get(_VISIBLE_DRIVE_LINK_HEADER))
                if not url:
                    source_doc_id = _coalesce_text(row_map.get(_HIDDEN_SOURCE_DOC_ID_HEADER))
                    url = raw_drive_links.get(source_doc_id, '')
                if not url:
                    continue
                row_formulas.append((row_number, _drive_cell(url, spreadsheet=ws.spreadsheet)))

            repaired = _rewrite_drive_cells(ws, tab_name, row_formulas)
            repaired_by_tab[tab_name] = repaired
            if repaired:
                logger.info("Force-rewrote %d Drive link cell(s) on '%s'.", repaired, tab_name)

        return repaired_by_tab


def hide_nonvisible_tabs(*, spreadsheet_id: Optional[str] = None) -> dict[str, int]:
    client = _get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable.")

    with _lock:
        sh = _open_spreadsheet_by_key(client, spreadsheet_id) if spreadsheet_id else _get_or_create_spreadsheet(client)
        hidden_counts = {"canonical_hidden": 0, "ignored_orphans": 0}
        for ws in _list_worksheets(sh):
            title = getattr(ws, "title", "")
            if title in _TABS and _tab_spec(title).hidden_tab:
                _set_worksheet_hidden(ws, hidden=True)
                hidden_counts["canonical_hidden"] += 1
                continue
            if _is_ignored_orphan_title(title):
                _set_worksheet_hidden(ws, hidden=True)
                hidden_counts["ignored_orphans"] += 1
        return hidden_counts


def audit_current_month_spreadsheet(
    *,
    spreadsheet_id: Optional[str] = None,
    repair: bool = False,
    target_tabs: set[str] | None = None,
    refresh_formatting: bool = False,
) -> dict[str, object]:
    client = _get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable.")

    with _lock:
        sh = _open_spreadsheet_by_key(client, spreadsheet_id) if spreadsheet_id else _get_or_create_spreadsheet(client)
        findings = _audit_spreadsheet_layout(sh, repair=repair, target_tabs=target_tabs, refresh_formatting=refresh_formatting)
        return {
            "spreadsheet_id": sh.id,
            "month_key": _month_key(),
            "findings": findings,
            "queue": queue_status(),
        }


def recommended_audit_tabs_for_test_drift(*, action: str, tab_name: str | None = None) -> list[str]:
    target_tab = tab_name or "Faturalar"
    if action == "delete_summary_tab":
        return ["📊 Özet"]
    if action == "rename_data_tab":
        return [target_tab, "📊 Özet"]
    if action == "corrupt_total_row":
        return [target_tab, "📊 Özet"]
    if action in {"corrupt_header_row", "clear_hidden_row_ids", "reorder_rows"}:
        return [target_tab]
    return [target_tab, "📊 Özet"]


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
        sh = _open_spreadsheet_by_key(client, spreadsheet_id) if spreadsheet_id else _get_or_create_spreadsheet(client)
        target_tab = tab_name or "Faturalar"

        if action == "delete_summary_tab":
            ws = _get_worksheet(sh, "📊 Özet")
            sh.del_worksheet(ws)
            return {"spreadsheet_id": sh.id, "action": action, "applied": True, "tab_name": "📊 Özet"}

        if action == "rename_data_tab":
            ws = _get_worksheet(sh, target_tab)
            new_name = (replacement_name or f"{target_tab} RENAMED").strip()[:100]
            ws.update_title(new_name)
            return {"spreadsheet_id": sh.id, "action": action, "applied": True, "tab_name": target_tab, "replacement_name": new_name}

        if action == "corrupt_total_row":
            ws = _get_worksheet(sh, target_tab)
            total_col = _tab_total_column_letter(target_tab)
            if not total_col:
                raise RuntimeError(f"No total column configured for {target_tab}")
            ws.update([["BROKEN"]], "A2", value_input_option="RAW")
            ws.update([[""]], f"{total_col}2", value_input_option="RAW")
            return {"spreadsheet_id": sh.id, "action": action, "applied": True, "tab_name": target_tab}

        if action == "corrupt_header_row":
            ws = _get_worksheet(sh, target_tab)
            headers = list(_headers(target_tab))
            headers[0] = "BROKEN"
            ws.update([headers], "A1", value_input_option="RAW")
            return {"spreadsheet_id": sh.id, "action": action, "applied": True, "tab_name": target_tab}

        if action == "clear_hidden_row_ids":
            ws = _get_worksheet(sh, target_tab)
            hidden_col = _internal_row_id_column_letter(target_tab)
            end_row = max(3, row_count + 2)
            _retry_on_rate_limit(lambda: ws.batch_clear([f"{hidden_col}3:{hidden_col}{end_row}"]))
            return {"spreadsheet_id": sh.id, "action": action, "applied": True, "tab_name": target_tab, "row_count": row_count}

        if action == "reorder_rows":
            ws = _get_worksheet(sh, target_tab)
            last_col = _internal_row_id_column_letter(target_tab)
            end_row = max(3, row_count + 2)
            rows = _get_range_values(ws, f"A3:{last_col}{end_row}")
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
    _audit_spreadsheet_layout(sh, repair=True, refresh_formatting=True)
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
    for tab_name in [name for name in _TABS if name != "📊 Özet"]:
        headers = _headers(tab_name)
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
    """Return month-prefixed tab name using the canonical base title."""
    return f"{_month_label()} — {_canonical_tab_name(base_name)}"


def _ensure_tab_exists(sh, tab_name: str, base_name: str | None = None, *, lightweight: bool = False):
    """Return the canonical worksheet for *tab_name*, creating or renaming it if needed."""
    import gspread

    canonical_title = _canonical_tab_name(tab_name)
    lookup = _canonical_tab_name(base_name or canonical_title)

    try:
        ws = _get_worksheet(sh, canonical_title)
        if ws.title != canonical_title and not _is_ignored_orphan_title(ws.title):
            try:
                ws.update_title(canonical_title)
                logger.info("Renamed tab '%s' → '%s'", ws.title, canonical_title)
            except Exception as exc:
                logger.warning("Could not rename tab '%s' to '%s': %s", ws.title, canonical_title, exc)
        return ws
    except gspread.WorksheetNotFound:
        pass

    logger.info("Tab '%s' not found; creating it.", canonical_title)
    headers = _headers(lookup) if lookup in _TABS else []
    ws = sh.add_worksheet(
        title=canonical_title,
        rows=1000,
        cols=max(len(headers) + 2, 10),
    )

    if lookup == "📊 Özet":
        _setup_summary_tab(ws, _month_label(), lightweight=lightweight)
    elif lookup in _TABS:
        _setup_worksheet(ws, lookup, lightweight=lightweight)

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
        for tab_name in [name for name in _TABS if name != "📊 Özet"]:
            headers = _headers(tab_name)
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
    return (
        _header_letter(tab_name, _VISIBLE_DRIVE_LINK_HEADER)
        or _header_letter(tab_name, _HIDDEN_DRIVE_LINK_HEADER)
        or _col_letter(len(_headers(tab_name)) - 1)
    )


def _internal_row_id_column_letter(tab_name: str) -> str:
    return _header_letter(tab_name, _HIDDEN_ROW_ID_HEADER) or _col_letter(len(_headers(tab_name)))


def _drive_cell(drive_link: Optional[str], *, spreadsheet=None, spreadsheet_id: str | None = None) -> str:
    """Return a HYPERLINK formula if we have a link, otherwise empty string."""
    if drive_link:
        sep = _formula_arg_separator(spreadsheet=spreadsheet, spreadsheet_id=spreadsheet_id)
        return f'=HYPERLINK("{drive_link}"{sep}"Görüntüle")'
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


def _text_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)
    return str(value).strip()


def _coalesce_text(*values: object) -> str:
    for value in values:
        text = _text_value(value)
        if text:
            return text
    return ""


def _join_labeled_parts(parts: list[tuple[str, object]]) -> str:
    rendered: list[str] = []
    for label, value in parts:
        text = _text_value(value)
        if text:
            rendered.append(f"{label}: {text}")
    return " | ".join(rendered)


def _display_quantity(value: object, unit: object | None = None) -> str:
    quantity_text = _text_value(value)
    if not quantity_text:
        return ""
    unit_text = _text_value(unit)
    if unit_text:
        return f"{quantity_text} {unit_text}"
    return quantity_text


def _display_sender_or_company(record: BillRecord) -> str:
    return _sender_display_name(record) or _coalesce_text(record.company_name)


def _document_reference(record: BillRecord) -> str:
    return str(record.invoice_number or record.document_number or record.receipt_number or record.cheque_serial_number or "")


def _invoice_extra_detail(record: BillRecord) -> str:
    line_item_count = _line_item_count(record)
    return _join_labeled_parts([
        ("Kalem", line_item_count if line_item_count > 1 else None),
        ("Banka", record.bank_name),
        ("IBAN", record.iban),
        ("Not", record.notes),
    ])


def _shipment_extra_detail(record: BillRecord) -> str:
    return _join_labeled_parts([
        ("Çıkış", record.shipment_origin),
        ("Plaka", record.vehicle_plate),
        ("Palet", record.pallet_count),
        ("Adet/Palet", record.items_per_pallet),
        ("Not", record.notes),
    ])


def _document_source_id(record: BillRecord, row_id: str) -> str:
    return str(row_id)


def _primary_amount(record: BillRecord) -> float | None:
    for value in (record.payable_amount, record.total_amount, record.subtotal, record.line_amount):
        if value is not None:
            return float(value)
    return None


def _is_return_record(record: BillRecord, category: DocumentCategory, return_source_category: DocumentCategory | None = None) -> bool:
    if category == DocumentCategory.IADE or return_source_category is not None:
        return True
    haystack = " ".join(
        str(part or "")
        for part in (record.invoice_type, record.description, record.notes, record.document_number, record.invoice_number)
    ).casefold()
    return "iade" in haystack or "iptal" in haystack


def _signed_amount(record: BillRecord, category: DocumentCategory, return_source_category: DocumentCategory | None = None) -> float | None:
    amount = _primary_amount(record)
    if amount is None:
        return None
    return -abs(amount) if _is_return_record(record, category, return_source_category) else amount


def _is_directly_settled(record: BillRecord, category: DocumentCategory) -> bool:
    if category in {DocumentCategory.HARCAMA_FISI, DocumentCategory.ELDEN_ODEME}:
        return True
    payment_method = str(record.payment_method or "").strip().casefold()
    return payment_method in {"nakit", "kredi karti", "kredi kartı", "banka transferi"}


def _counterparty_name(record: BillRecord, category: DocumentCategory) -> str:
    if category == DocumentCategory.FATURA:
        return str(record.company_name or record.recipient_name or record.buyer_name or _sender_display_name(record) or "")
    if category == DocumentCategory.ELDEN_ODEME:
        return str(record.recipient_name or record.buyer_name or record.company_name or record.description or "")
    if category == DocumentCategory.ODEME_DEKONTU:
        return str(record.recipient_name or record.buyer_name or record.company_name or _sender_display_name(record) or "")
    if category == DocumentCategory.CEK:
        return str(record.recipient_name or record.notes or record.company_name or "")
    return str(record.recipient_name or record.company_name or record.buyer_name or _sender_display_name(record) or "")


def _party_key(record: BillRecord, *, role: str) -> str:
    return ledger.derive_party_key(record.model_dump(mode="json"), role=role)


def _withholding_label(record: BillRecord) -> str:
    if record.withholding_present is True or record.withholding_amount not in (None, 0):
        if record.withholding_rate not in (None, 0):
            return f"EVET (%{_safe(record.withholding_rate)})"
        return "EVET"
    if record.withholding_present is False:
        return "HAYIR"
    return ""


def _iter_invoice_line_items(record: BillRecord) -> list[dict[str, object]]:
    line_items = record.line_items or []
    rows: list[dict[str, object]] = []
    for item in line_items:
        if item is None:
            continue
        data = item.model_dump(mode="json") if hasattr(item, "model_dump") else dict(item)
        rows.append(data)
    if rows:
        return rows
    if any(value is not None for value in (record.line_quantity, record.line_unit, record.unit_price, record.line_amount, record.description)):
        rows.append({
            "description": record.description,
            "quantity": record.line_quantity,
            "unit": record.line_unit,
            "unit_price": record.unit_price,
            "line_amount": record.line_amount or record.subtotal,
        })
    return rows


def _line_item_rows(record: BillRecord) -> list[dict[str, object]]:
    return [
        item
        for item in _iter_invoice_line_items(record)
        if any(item.get(key) not in (None, "") for key in ("description", "quantity", "unit", "unit_price", "line_amount"))
    ]


def _line_item_count(record: BillRecord) -> int:
    return len(_line_item_rows(record))


def _line_item_descriptions(record: BillRecord) -> list[str]:
    descriptions: list[str] = []
    seen: set[str] = set()
    for item in _line_item_rows(record):
        item_description = str(item.get("description") or "").strip()
        if not item_description:
            continue
        key = item_description.casefold()
        if key in seen:
            continue
        seen.add(key)
        descriptions.append(item_description)
    return descriptions


def _line_item_description_summary(record: BillRecord, *, preview_limit: int = 3) -> str:
    descriptions = _line_item_descriptions(record)
    if not descriptions:
        return ""
    if len(descriptions) == 1:
        return descriptions[0]
    summary = ", ".join(descriptions[:preview_limit])
    if len(descriptions) > preview_limit:
        summary += f" +{len(descriptions) - preview_limit} kalem"
    return summary


def _invoice_primary_line_item(record: BillRecord) -> dict[str, object] | None:
    populated_items = _line_item_rows(record)
    if len(populated_items) == 1:
        return populated_items[0]
    return None


def _invoice_quantity_display(record: BillRecord, primary_line_item: dict[str, object] | None = None) -> str:
    primary = primary_line_item or _invoice_primary_line_item(record)
    if primary is not None:
        return _display_quantity(primary.get("quantity", record.line_quantity), primary.get("unit", record.line_unit))
    line_item_count = _line_item_count(record)
    if line_item_count > 1:
        return f"{line_item_count} kalem"
    return _display_quantity(record.line_quantity, record.line_unit)


def _invoice_unit_price_display(record: BillRecord, primary_line_item: dict[str, object] | None = None) -> object:
    primary = primary_line_item or _invoice_primary_line_item(record)
    if primary is not None:
        unit_price = primary.get("unit_price", record.unit_price)
        return unit_price if unit_price not in (None, "") else ""
    if _line_item_count(record) > 1:
        return ""
    return record.unit_price if record.unit_price is not None else ""


def _shipment_product_summary(record: BillRecord) -> str:
    return _line_item_description_summary(record, preview_limit=3) or _coalesce_text(record.description, record.notes)


def _shipment_visible_description(record: BillRecord) -> str:
    parts: list[str] = []
    raw_description = _text_value(record.description)
    product_summary = _shipment_product_summary(record)
    if raw_description and product_summary and raw_description != product_summary:
        parts.append(raw_description)
    line_item_count = _line_item_count(record)
    if line_item_count > 1:
        parts.append(f"{line_item_count} kalem")
    extra_detail = _shipment_extra_detail(record)
    if extra_detail:
        parts.append(extra_detail)
    return " | ".join(parts)


def _shipment_quantity_display(record: BillRecord) -> str:
    quantity = record.product_quantity if record.product_quantity is not None else record.line_quantity
    return _display_quantity(quantity, record.line_unit)


def _is_generic_invoice_description(record: BillRecord, description: str | None) -> bool:
    normalized = " ".join(str(description or "").split()).casefold()
    if not normalized:
        return True

    invoice_type = " ".join(str(record.invoice_type or "").split()).casefold()
    if invoice_type and normalized == invoice_type:
        return True

    return normalized in {
        "fatura",
        "e-fatura",
        "e arşiv",
        "e-arşiv",
        "e arsiv",
        "e-arsiv",
        "satış faturası",
        "satis faturasi",
        "toptan satış faturası",
        "toptan satis faturasi",
        "iade faturası",
        "iade faturasi",
        "toptan satış iade",
        "toptan satis iade",
    }


def _invoice_summary_description(
    record: BillRecord,
    *,
    category: DocumentCategory,
    return_source_category: DocumentCategory | None = None,
) -> str:
    description = str(record.description or "").strip()
    if description and not _is_generic_invoice_description(record, description):
        return description

    line_item_summary = _line_item_description_summary(record, preview_limit=2)
    if line_item_summary:
        return line_item_summary

    if description:
        return description
    if record.invoice_type:
        return str(record.invoice_type)
    if _is_return_record(record, category, return_source_category):
        return _return_source_label(return_source_category)
    return "Fatura"


def _masraf_paid_formula(row_number: int, *, spreadsheet=None, spreadsheet_id: str | None = None) -> str:
    sep = _formula_arg_separator(spreadsheet=spreadsheet, spreadsheet_id=spreadsheet_id)
    settled_ref = f"{_header_letter('Masraf Kayıtları', _HIDDEN_SETTLED_AMOUNT_HEADER)}{row_number}"
    row_id_ref = f"{_header_letter('Masraf Kayıtları', _HIDDEN_ROW_ID_HEADER)}{row_number}"
    debt_col = _header_letter('__Ödeme_Dağıtımları', 'Borç Row ID')
    amount_col = _header_letter('__Ödeme_Dağıtımları', 'Ayrılan Tutar')
    return (
        f"=IFERROR({settled_ref}{sep}0)+"
        f"IFERROR(SUMIF('__Ödeme_Dağıtımları'!{debt_col}:{debt_col}{sep}{row_id_ref}{sep}'__Ödeme_Dağıtımları'!{amount_col}:{amount_col}){sep}0)"
    )


def _masraf_remaining_formula(row_number: int, *, spreadsheet=None, spreadsheet_id: str | None = None) -> str:
    sep = _formula_arg_separator(spreadsheet=spreadsheet, spreadsheet_id=spreadsheet_id)
    balance_ref = f"{_header_letter('Masraf Kayıtları', 'Bakiye (TL)')}{row_number}"
    paid_ref = f"{_header_letter('Masraf Kayıtları', 'Ödenen (TL)')}{row_number}"
    return f"=IFERROR({balance_ref}-{paid_ref}{sep}0)"


def _build_row_for_tab(
    record: BillRecord,
    tab_name: str,
    *,
    category: DocumentCategory,
    row_id: str,
    row_number: int,
    drive_link: Optional[str] = None,
    return_source_category: DocumentCategory | None = None,
    source_doc_id: str | None = None,
    spreadsheet=None,
) -> list:
    source_doc_id = source_doc_id or _document_source_id(record, row_id)
    tax_number = str(record.tax_number or "")
    separator_spreadsheet_id = _registered_spreadsheet_id_for_month(_month_key()) or None
    separator_spreadsheet = spreadsheet
    drive_value = _drive_cell(drive_link, spreadsheet=separator_spreadsheet, spreadsheet_id=separator_spreadsheet_id)

    if tab_name == 'Faturalar':
        primary_line_item = _invoice_primary_line_item(record)
        line_quantity = _invoice_quantity_display(record, primary_line_item)
        unit_price = _invoice_unit_price_display(record, primary_line_item)
        withholding_label = _withholding_label(record) or 'HAYIR'
        withholding_amount = record.withholding_amount
        if withholding_amount is None and withholding_label == 'HAYIR':
            withholding_amount = 0
        line_amount = record.subtotal
        if line_amount is None:
            line_amount = record.line_amount if record.line_amount is not None else (primary_line_item or {}).get('line_amount')
        return [
            _safe(record.invoice_number or record.document_number),
            _safe(record.document_date),
            _safe(record.invoice_type or (_return_source_label(return_source_category) if _is_return_record(record, category, return_source_category) else 'Fatura')),
            _safe(record.company_name),
            _safe(record.tax_number),
            _safe(record.buyer_name or record.recipient_name),
            _safe(_invoice_summary_description(record, category=category, return_source_category=return_source_category)),
            _safe(line_quantity),
            _safe(unit_price),
            _safe(line_amount),
            _safe(record.vat_rate),
            _safe(record.vat_amount),
            withholding_label,
            _safe(withholding_amount),
            _safe(record.payable_amount if record.payable_amount is not None else record.total_amount),
            _safe(record.currency or 'TRY'),
            _safe(_invoice_extra_detail(record)),
            drive_value,
            row_id,
            _party_key(record, role='debt'),
            source_doc_id,
            tax_number,
            'fatura',
        ]

    if tab_name == 'Masraf Kayıtları':
        signed_amount = _signed_amount(record, category, return_source_category)
        settled_amount = signed_amount if (signed_amount is not None and _is_directly_settled(record, category) and not _is_return_record(record, category, return_source_category)) else 0
        return [
            _safe(record.document_date),
            _safe(record.expense_category or record.invoice_type or _return_source_label(return_source_category or category)),
            _safe(_counterparty_name(record, category)),
            _safe(record.description or record.notes),
            _safe(_document_reference(record)),
            _safe(signed_amount),
            _masraf_paid_formula(row_number, spreadsheet=separator_spreadsheet, spreadsheet_id=separator_spreadsheet_id),
            _masraf_remaining_formula(row_number, spreadsheet=separator_spreadsheet, spreadsheet_id=separator_spreadsheet_id),
            drive_value,
            row_id,
            _party_key(record, role='debt'),
            source_doc_id,
            tax_number,
            category.value,
            _safe(settled_amount),
        ]

    if tab_name == 'Sevk Fişleri':
        return [
            _safe(record.document_number or record.receipt_number or record.invoice_number),
            _safe(record.document_date),
            _safe(record.company_name),
            _safe(record.recipient_name or record.buyer_name),
            _safe(_shipment_product_summary(record)),
            _safe(_shipment_quantity_display(record)),
            _safe(record.shipment_destination),
            _safe(_shipment_visible_description(record)),
            drive_value,
            row_id,
            _party_key(record, role='debt'),
            source_doc_id,
            tax_number,
            category.value,
        ]

    if tab_name == '__Raw Belgeler':
        return [
            source_doc_id,
            category.value,
            'EVET' if _is_return_record(record, category, return_source_category) else 'HAYIR',
            _safe(record.company_name),
            _safe(record.tax_number),
            _safe(record.document_number),
            _safe(record.invoice_number),
            _safe(record.receipt_number),
            _safe(record.document_date),
            _safe(record.document_time),
            _safe(_primary_amount(record)),
            _safe(record.currency or 'TRY'),
            _safe(_sender_display_name(record)),
            _safe(record.recipient_name or record.buyer_name),
            _safe(record.description),
            _safe(record.notes),
            _safe(record.iban),
            _safe(record.bank_name or record.cheque_bank_name),
            _safe(record.source_message_id),
            drive_value,
            row_id,
        ]

    if tab_name == '__Çek_Dekont_Detay':
        return [
            source_doc_id,
            category.value,
            _safe(record.company_name),
            _safe(_sender_display_name(record)),
            _safe(record.recipient_name or record.buyer_name),
            _safe(record.document_number or record.invoice_number),
            _safe(record.iban or record.cheque_account_ref),
            _safe(record.bank_name or record.cheque_bank_name),
            _safe(record.cheque_serial_number or record.document_number),
            _safe(record.cheque_bank_name),
            _safe(record.cheque_branch),
            _safe(record.cheque_account_ref),
            _safe(record.cheque_issue_place),
            _safe(record.cheque_issue_date),
            _safe(record.cheque_due_date or record.document_date),
            _safe(record.description or record.notes),
            drive_value,
            row_id,
        ]

    raise KeyError(f'Unsupported row build for tab: {tab_name}')


def _build_invoice_line_rows(record: BillRecord, *, row_id_prefix: str, source_doc_id: str | None = None) -> list[list]:
    source_doc_id = source_doc_id or _document_source_id(record, row_id_prefix)
    rows: list[list] = []
    for index, item in enumerate(_iter_invoice_line_items(record), start=1):
        rows.append([
            source_doc_id,
            index,
            _safe(item.get('description')),
            _safe(item.get('quantity')),
            _safe(item.get('unit')),
            _safe(item.get('unit_price')),
            _safe(item.get('line_amount')),
            f'{row_id_prefix}__line{index}',
        ])
    return rows


def _build_payment_allocation_row(
    *,
    party_name: str,
    description: str,
    reference_number: str,
    sender_name: str,
    payment_amount: float | int | str | None,
    payment_date: str,
    remaining_balance: float | int | str | None,
    status: str,
    drive_link: Optional[str],
    row_id: str,
    party_key: str,
    source_doc_id: str,
    debt_row_id: str,
    tax_number: str = '',
    allocation_id: str = '',
    spreadsheet=None,
) -> list:
    return [
        _safe(party_name),
        _safe(description),
        _safe(reference_number),
        _safe(sender_name),
        _safe(payment_amount),
        _safe(payment_date),
        _safe(remaining_balance),
        _safe(status),
        _drive_cell(drive_link, spreadsheet=spreadsheet),
        row_id,
        _safe(party_key),
        _safe(source_doc_id),
        _safe(source_doc_id),
        _safe(debt_row_id),
        _safe(allocation_id),
        _safe(tax_number),
        'odeme',
    ]


def _build_allocation_detail_row(
    *,
    allocation_id: str,
    party_key: str,
    debt_row_id: str,
    payment_doc_id: str,
    payment_date: str,
    debt_date: str,
    debt_amount: float | int | str | None,
    allocated_amount: float | int | str | None,
    remaining_amount: float | int | str | None,
    status: str,
) -> list:
    return [
        allocation_id,
        _safe(party_key),
        _safe(debt_row_id),
        _safe(payment_doc_id),
        _safe(payment_date),
        _safe(debt_date),
        _safe(debt_amount),
        _safe(allocated_amount),
        _safe(remaining_amount),
        _safe(status),
        allocation_id,
    ]

def _worksheet_rows_as_dicts(ws, tab_name: str, *, value_render_option: str | None = None) -> list[dict[str, object]]:
    last_col = _internal_row_id_column_letter(tab_name)
    try:
        rows = _get_range_values(ws, f"A3:{last_col}", value_render_option=value_render_option)
    except Exception:
        return []

    headers = _headers(tab_name)
    visible_count = _visible_header_count(tab_name)
    result: list[dict[str, object]] = []
    for row in rows:
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        if not any(str(cell or "").strip() for cell in padded[:visible_count]):
            continue
        result.append({headers[idx]: padded[idx] if idx < len(padded) else "" for idx in range(len(headers))})
    return result


def _split_aliases(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set)):
        parts = [str(item).strip() for item in value if str(item or '').strip()]
    else:
        parts = [part.strip() for part in str(value).split(',') if part.strip()]
    seen: list[str] = []
    for part in parts:
        if part not in seen:
            seen.append(part)
    return tuple(seen)


def _load_party_card_map(sh) -> dict[str, dict[str, object]]:
    ws = _ensure_tab_exists(sh, '__Cari_Kartlar', lightweight=True)
    rows = _worksheet_rows_as_dicts(ws, '__Cari_Kartlar')
    result: dict[str, dict[str, object]] = {}
    for row in rows:
        party_key = str(row.get('Party Key') or '').strip()
        if not party_key:
            continue
        result[party_key] = {
            'display_name': str(row.get('Görünen Ad') or '').strip(),
            'tax_number': str(row.get('Vergi No') or '').strip(),
            'aliases': _split_aliases(row.get('Aliaslar')),
        }
    return result


def _party_card_row(*, party_key: str, display_name: str, tax_number: str = '', aliases: tuple[str, ...] = ()) -> list:
    return [party_key, display_name, tax_number, ', '.join(alias for alias in aliases if alias), party_key]


def _upsert_party_cards(sh, cards: list[dict[str, object]]) -> None:
    if not cards:
        return
    ws = _ensure_tab_exists(sh, '__Cari_Kartlar', lightweight=True)
    existing = _load_party_card_map(sh)
    rows_to_append: list[list] = []
    for card in cards:
        party_key = str(card.get('party_key') or '').strip()
        display_name = str(card.get('display_name') or '').strip()
        if not party_key or not display_name or party_key in existing:
            continue
        rows_to_append.append(_party_card_row(
            party_key=party_key,
            display_name=display_name,
            tax_number=str(card.get('tax_number') or '').strip(),
            aliases=_split_aliases(card.get('aliases')),
        ))
        existing[party_key] = card
    if rows_to_append:
        _retry_on_rate_limit(lambda: ws.append_rows(rows_to_append, value_input_option='USER_ENTERED'))


def _load_expense_debt_state(sh) -> list[dict[str, object]]:
    ws = _ensure_tab_exists(sh, 'Masraf Kayıtları', lightweight=True)
    rows = _worksheet_rows_as_dicts(ws, 'Masraf Kayıtları', value_render_option='UNFORMATTED_VALUE')
    cards = _load_party_card_map(sh)
    result: list[dict[str, object]] = []
    for index, row in enumerate(rows):
        row_id = str(row.get(_HIDDEN_ROW_ID_HEADER) or '').strip()
        if not row_id:
            continue
        party_key = str(row.get(_HIDDEN_PARTY_KEY_HEADER) or '').strip()
        display_name = str(row.get('Alıcı / Tedarikçi') or '').strip()
        tax_number = ledger.normalize_tax_number(row.get(_HIDDEN_TAX_NUMBER_HEADER))
        aliases = cards.get(party_key, {}).get('aliases', ()) if party_key else ()
        try:
            original_amount = float(row.get('Bakiye (TL)') or 0)
        except Exception:
            original_amount = 0.0
        try:
            remaining_amount = float(row.get('Kalan Borç (TL)') or 0)
        except Exception:
            remaining_amount = 0.0
        result.append({
            'row_id': row_id,
            'party_key': party_key or f'row:{row_id}',
            'display_name': display_name,
            'tax_number': tax_number,
            'date': str(row.get('Tarih') or '').strip(),
            'original_amount': original_amount,
            'remaining_amount': remaining_amount,
            'aliases': aliases,
            'sort_index': index,
        })
    return result


def _payment_matching_rows(
    debt_state: list[dict[str, object]],
    *,
    balance_kind: str = 'any',
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for debt in debt_state:
        remaining_amount = float(debt.get('remaining_amount') or 0)
        if balance_kind == 'payable' and remaining_amount <= 0:
            continue
        if balance_kind == 'receivable' and remaining_amount >= 0:
            continue
        rows.append({
            'row_id': debt['row_id'],
            'party_key': debt['party_key'],
            'company_name': debt['display_name'],
            'recipient_name': debt['display_name'],
            'tax_number': debt['tax_number'],
            'aliases': debt.get('aliases', ()),
            'amount': abs(remaining_amount) if balance_kind == 'receivable' else remaining_amount,
            'date': debt['date'],
        })
    return rows


def _party_balance_rows(
    balance_state: list[dict[str, object]],
    *,
    party_key: str,
    balance_kind: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for balance in balance_state:
        if balance.get('party_key') != party_key:
            continue
        remaining_amount = float(balance.get('remaining_amount') or 0)
        if balance_kind == 'payable' and remaining_amount > 0:
            rows.append(balance)
        if balance_kind == 'receivable' and remaining_amount < 0:
            rows.append(balance)
    rows.sort(key=lambda balance: (str(balance.get('date') or ''), int(balance.get('sort_index') or 0)))
    return rows


def _party_display_name_from_state(balance_state: list[dict[str, object]], *, party_key: str) -> str:
    for balance in balance_state:
        if balance.get('party_key') != party_key:
            continue
        display_name = _text_value(balance.get('display_name'))
        if display_name:
            return display_name
    return ''


def _receivable_counterparty_name(record: BillRecord) -> str:
    return _coalesce_text(
        record.sender_name,
        record.source_sender_name,
        record.company_name,
        record.recipient_name,
        record.buyer_name,
    )


def _receivable_match_record(record: BillRecord) -> dict[str, object]:
    primary_name = _receivable_counterparty_name(record)
    aliases: list[str] = []
    for candidate in (
        primary_name,
        record.sender_name,
        record.source_sender_name,
        record.company_name,
        record.recipient_name,
        record.buyer_name,
        record.notes,
    ):
        text = _text_value(candidate)
        if text and text not in aliases:
            aliases.append(text)
    return {
        'tax_number': ledger.normalize_tax_number(record.tax_number),
        'recipient_name': primary_name,
        'company_name': primary_name,
        'buyer_name': _coalesce_text(record.recipient_name, record.buyer_name),
        'sender_name': _sender_display_name(record),
        'aliases': tuple(aliases),
    }


def _signed_payment_value(source_amount: float, value: float) -> float:
    return -value if source_amount < 0 else value


def _allocation_status(remaining_amount: float, allocated_amount: float) -> str:
    if allocated_amount <= 0:
        return ledger.STATUS_ACIK
    if remaining_amount <= 0:
        return ledger.STATUS_KAPANDI
    return ledger.STATUS_KISMI


def _build_payment_projection_rows(
    *,
    record: BillRecord,
    category: DocumentCategory,
    item_id: str,
    debt_state: list[dict[str, object]],
    drive_link: Optional[str],
    spreadsheet=None,
) -> tuple[list[list], list[list], list[dict[str, object]]]:
    source_doc_id = item_id
    amount = float(_primary_amount(record) or 0)
    payment_magnitude = abs(amount)
    payment_date = str(record.document_date or record.cheque_due_date or record.cheque_issue_date or '')
    description = str(record.description or record.notes or _document_reference(record) or '')
    payment_party_name = _counterparty_name(record, category)
    receivable_party_name = _receivable_counterparty_name(record)
    payment_reference = _document_reference(record)
    payment_sender_name = _display_sender_or_company(record)
    payment_tax_number = ledger.normalize_tax_number(record.tax_number)

    payable_match = ledger.match_payment_party(
        record.model_dump(mode='json'),
        _payment_matching_rows(debt_state),
    )
    payable_rows = (
        _party_balance_rows(debt_state, party_key=payable_match.party_key, balance_kind='payable')
        if payable_match.party_key else []
    )

    receivable_match = None
    receivable_rows: list[dict[str, object]] = []
    if amount > 0 and category in {DocumentCategory.ODEME_DEKONTU, DocumentCategory.CEK}:
        receivable_match = ledger.match_payment_party(
            _receivable_match_record(record),
            _payment_matching_rows(debt_state),
        )
        if receivable_match.party_key:
            receivable_rows = _party_balance_rows(
                debt_state,
                party_key=receivable_match.party_key,
                balance_kind='receivable',
            )

    visible_rows: list[list] = []
    allocation_rows: list[list] = []
    party_cards: list[dict[str, object]] = []

    selected_mode = 'unmatched'
    selected_match = None
    matched_display_name = payment_party_name
    matched_alias = payment_party_name

    if category == DocumentCategory.CEK and receivable_rows:
        selected_mode = 'receivable'
        selected_match = receivable_match
        matched_display_name = receivable_match.display_name or receivable_party_name
        matched_alias = receivable_party_name or matched_display_name
    elif payable_rows:
        selected_mode = 'payable'
        selected_match = payable_match
        matched_display_name = payable_match.display_name or payment_party_name
        matched_alias = payment_party_name or matched_display_name
    elif receivable_rows:
        selected_mode = 'receivable'
        selected_match = receivable_match
        matched_display_name = receivable_match.display_name or receivable_party_name
        matched_alias = receivable_party_name or matched_display_name
    elif payable_match.party_key:
        selected_mode = 'matched_no_open_balance'
        selected_match = payable_match
        matched_display_name = payable_match.display_name or payment_party_name
        matched_alias = payment_party_name or matched_display_name
    elif receivable_match and receivable_match.party_key:
        selected_mode = 'matched_no_open_balance'
        selected_match = receivable_match
        matched_display_name = receivable_match.display_name or receivable_party_name
        matched_alias = receivable_party_name or matched_display_name

    if selected_match and selected_match.party_key:
        state_display_name = _party_display_name_from_state(debt_state, party_key=selected_match.party_key)
        if state_display_name:
            matched_display_name = state_display_name

    if selected_match and selected_match.party_key:
        party_cards.append({
            'party_key': selected_match.party_key,
            'display_name': matched_display_name,
            'tax_number': selected_match.tax_number or payment_tax_number,
            'aliases': tuple(filter(None, [matched_alias])),
        })

    remaining_payment = payment_magnitude
    allocation_index = 0

    if selected_mode == 'payable' and selected_match and selected_match.party_key:
        party_debts = payable_rows
        for debt in party_debts:
            if remaining_payment <= 0:
                break
            open_amount = float(debt['remaining_amount'])
            if open_amount <= 0:
                continue
            applied = min(open_amount, remaining_payment)
            if applied <= 0:
                continue
            allocation_index += 1
            debt['remaining_amount'] = round(open_amount - applied, 2)
            remaining_payment = round(remaining_payment - applied, 2)
            allocation_id = f"{item_id}__alloc{allocation_index}"
            visible_row_id = f"{item_id}__row{allocation_index}"
            status = _allocation_status(float(debt['remaining_amount']), applied)
            visible_rows.append(_build_payment_allocation_row(
                party_name=matched_display_name,
                description=description,
                reference_number=payment_reference,
                sender_name=payment_sender_name,
                payment_amount=_signed_payment_value(amount, applied),
                payment_date=payment_date,
                remaining_balance=debt.get('remaining_amount'),
                status=status,
                drive_link=drive_link,
                row_id=visible_row_id,
                party_key=selected_match.party_key,
                source_doc_id=source_doc_id,
                debt_row_id=str(debt.get('row_id') or ''),
                tax_number=str(debt.get('tax_number') or payment_tax_number or ''),
                allocation_id=allocation_id,
                spreadsheet=spreadsheet,
            ))
            allocation_rows.append(_build_allocation_detail_row(
                allocation_id=allocation_id,
                party_key=selected_match.party_key,
                debt_row_id=str(debt.get('row_id') or ''),
                payment_doc_id=source_doc_id,
                payment_date=payment_date,
                debt_date=str(debt.get('date') or ''),
                debt_amount=debt.get('original_amount'),
                allocated_amount=applied,
                remaining_amount=debt.get('remaining_amount'),
                status=status,
            ))

    if selected_mode == 'receivable' and selected_match and selected_match.party_key:
        for debt in receivable_rows:
            if remaining_payment <= 0:
                break
            open_amount = abs(float(debt['remaining_amount']))
            if open_amount <= 0:
                continue
            applied = min(open_amount, remaining_payment)
            if applied <= 0:
                continue
            allocation_index += 1
            debt['remaining_amount'] = round(float(debt['remaining_amount']) + applied, 2)
            remaining_payment = round(remaining_payment - applied, 2)
            remaining_receivable = abs(float(debt['remaining_amount']))
            visible_row_id = f"{item_id}__row{allocation_index}"
            status = _allocation_status(remaining_receivable, applied)
            visible_rows.append(_build_payment_allocation_row(
                party_name=matched_display_name,
                description=description,
                reference_number=payment_reference,
                sender_name=payment_sender_name,
                payment_amount=applied,
                payment_date=payment_date,
                remaining_balance=remaining_receivable,
                status=status,
                drive_link=drive_link,
                row_id=visible_row_id,
                party_key=selected_match.party_key,
                source_doc_id=source_doc_id,
                debt_row_id='receivable:' + str(debt.get('row_id') or ''),
                tax_number=str(debt.get('tax_number') or payment_tax_number or ''),
                allocation_id=f'{item_id}__alloc{allocation_index}',
                spreadsheet=spreadsheet,
            ))

    if selected_mode == 'unmatched':
        visible_rows.append(_build_payment_allocation_row(
            party_name=matched_display_name,
            description=description,
            reference_number=payment_reference,
            sender_name=payment_sender_name,
            payment_amount=amount,
            payment_date=payment_date,
            remaining_balance=amount,
            status=ledger.STATUS_ESLESMEDI,
            drive_link=drive_link,
            row_id=f'{item_id}__row1',
            party_key='',
            source_doc_id=source_doc_id,
            debt_row_id='',
            tax_number=payment_tax_number,
            allocation_id=f'{item_id}__alloc0',
            spreadsheet=spreadsheet,
        ))
    elif allocation_index == 0:
        visible_rows.append(_build_payment_allocation_row(
            party_name=matched_display_name,
            description=description,
            reference_number=payment_reference,
            sender_name=payment_sender_name,
            payment_amount=amount,
            payment_date=payment_date,
            remaining_balance=0,
            status=ledger.STATUS_BORC_YOK,
            drive_link=drive_link,
            row_id=f'{item_id}__row1',
            party_key=selected_match.party_key if selected_match else '',
            source_doc_id=source_doc_id,
            debt_row_id='',
            tax_number=(selected_match.tax_number if selected_match else '') or payment_tax_number,
            allocation_id=f'{item_id}__alloc0',
            spreadsheet=spreadsheet,
        ))
    elif remaining_payment > 0:
        visible_rows.append(_build_payment_allocation_row(
            party_name=matched_display_name,
            description=description,
            reference_number=payment_reference,
            sender_name=payment_sender_name,
            payment_amount=_signed_payment_value(amount, remaining_payment),
            payment_date=payment_date,
            remaining_balance=remaining_payment,
            status=ledger.STATUS_FAZLA_ODEME,
            drive_link=drive_link,
            row_id=f'{item_id}__row{allocation_index + 1}',
            party_key=selected_match.party_key if selected_match else '',
            source_doc_id=source_doc_id,
            debt_row_id='',
            tax_number=(selected_match.tax_number if selected_match else '') or payment_tax_number,
            allocation_id=f'{item_id}__alloc{allocation_index + 1}',
            spreadsheet=spreadsheet,
        ))

    return visible_rows, allocation_rows, party_cards


def reset_current_month_spreadsheet_data(*, spreadsheet_id: Optional[str] = None) -> int:
    """Clear current-month data rows while preserving headers, formulas, and formatting."""
    client = _get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable.")

    with _lock:
        sh = _open_spreadsheet_by_key(client, spreadsheet_id) if spreadsheet_id else _get_or_create_spreadsheet(client)
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
        "tab_name": _canonical_tab_name(tab_name),
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
    if not isinstance(raw, list):
        return []

    normalized_items: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        targets = []
        for target in item.get("targets", []):
            if not isinstance(target, dict):
                continue
            tab_name = str(target.get("tab_name") or "")
            if tab_name in _LEGACY_IADE_TITLES or _is_ignored_orphan_title(tab_name):
                continue
            normalized_target = _normalize_drive_link_target(target)
            if _is_ignored_orphan_title(str(normalized_target.get("tab_name") or "")):
                continue
            targets.append(normalized_target)
        if not targets:
            continue
        normalized_item = dict(item)
        normalized_item["targets"] = targets
        normalized_items.append(normalized_item)
    return normalized_items


def _save_pending_drive_uploads(items: list[dict]) -> None:
    _pending_drive_uploads_state_path().write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _normalize_drive_link_target(target: dict[str, str | int]) -> dict[str, str | int]:
    normalized: dict[str, str | int] = {
        "spreadsheet_id": str(target["spreadsheet_id"]),
        "tab_name": _canonical_tab_name(str(target["tab_name"])),
        "row_number": int(target["row_number"]),
    }
    row_id = str(target.get("row_id") or "").strip()
    if row_id:
        normalized["row_id"] = row_id
    return normalized


def _drive_link_target_key(target: dict[str, str | int]) -> tuple[str, str, str, str | int]:
    spreadsheet_id = str(target.get("spreadsheet_id") or "")
    tab_name = _canonical_tab_name(str(target.get("tab_name") or ""))
    row_id = str(target.get("row_id") or "").strip()
    if row_id:
        return (spreadsheet_id, tab_name, "row_id", row_id)
    return (spreadsheet_id, tab_name, "row_number", int(target.get("row_number") or 0))


def _resolve_drive_link_target_row_number(ws, target: dict[str, str | int]) -> int:
    tab_name = _canonical_tab_name(str(target["tab_name"]))
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
    tab_name = _canonical_tab_name(str(target["tab_name"]))

    sh = client.open_by_key(spreadsheet_id)
    ws = _ensure_tab_exists(sh, tab_name, lightweight=True)
    row_number = _resolve_drive_link_target_row_number(ws, target)
    cell_a1 = f"{_drive_column_letter(tab_name)}{row_number}"

    _retry_on_rate_limit(
        lambda: ws.update([[_drive_cell(drive_link, spreadsheet=ws.spreadsheet)]], cell_a1, value_input_option="USER_ENTERED")
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

    normalized_source_message_id = _root_source_message_id(source_message_id)

    storage_guard.prune_stale_transient_storage()
    if storage_guard.should_stop_payload_writes():
        logger.warning(
            "Skipping pending Drive upload queue for message id=%s because disk pressure forbids transient writes.",
            normalized_source_message_id or "?",
        )
        return

    normalized_targets = [_normalize_drive_link_target(target) for target in targets]

    with _pending_drive_uploads_lock:
        items = _load_pending_drive_uploads()
        if source_message_id:
            for existing in items:
                if (
                    str(existing.get("source_message_id") or "") == normalized_source_message_id
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
                        normalized_source_message_id,
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
        "source_message_id": normalized_source_message_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "attempts": 0,
    }

    with _pending_drive_uploads_lock:
        items = _load_pending_drive_uploads()
        items.append(item)
        _save_pending_drive_uploads(items)

    logger.warning(
        "Queued pending Drive upload for message id=%s with %d target cell(s).",
        normalized_source_message_id,
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
    if not isinstance(raw, list):
        return []

    normalized_items: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        tab_name = str(item.get("tab_name") or "")
        if tab_name in _LEGACY_IADE_TITLES or _is_ignored_orphan_title(tab_name):
            continue
        normalized_item = dict(item)
        normalized_item["tab_name"] = _canonical_tab_name(tab_name)
        if str(normalized_item.get("category") or "") == DocumentCategory.IADE.value:
            normalized_item["category"] = DocumentCategory.FATURA.value
            normalized_item["tab_name"] = "Faturalar"
            normalized_item["return_source_category"] = ""
        normalized_items.append(normalized_item)
    return normalized_items


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


def _root_source_message_id(source_message_id: str | None) -> str:
    raw = (source_message_id or "").strip()
    prefix, separator, suffix = raw.rpartition("__doc")
    if prefix and separator and suffix.isdigit():
        return prefix
    return raw


def _shared_pending_sheet_payload_path(
    *,
    source_message_id: str | None,
    filename: str | None,
    mime_type: str | None,
) -> Path:
    key = "|".join([
        _root_source_message_id(source_message_id),
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
    storage_guard.prune_stale_transient_storage()
    if storage_guard.should_stop_payload_writes():
        logger.warning(
            "Skipping pending sheet payload for message id=%s because disk pressure forbids transient writes.",
            source_message_id or "?",
        )
        return ""

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
    feedback_target: dict[str, str] | None = None,
    is_visible_tab: bool = False,
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
    feedback = feedback_target or {}
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
        "is_visible_tab": bool(is_visible_tab),
        "feedback_platform": str(feedback.get("platform") or ""),
        "feedback_chat_id": str(feedback.get("chat_id") or ""),
        "feedback_recipient_type": str(feedback.get("recipient_type") or ""),
        "feedback_message_id": str(feedback.get("message_id") or ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "attempts": 0,
        "next_attempt_at": time.time(),
    }


def _pending_sheet_retry_delay_seconds(attempts: int) -> float:
    exponent = max(attempts - 1, 0)
    return min(_PENDING_SHEET_WORKER_RETRY_DELAY_SECONDS * (2 ** exponent), 300.0)


def _pending_sheet_item_is_ready(item: dict) -> bool:
    return float(item.get("next_attempt_at") or 0.0) <= time.time()


def _pending_sheet_batch_priority(item: dict) -> tuple[int, int]:
    tab_name = str(item.get("tab_name") or "")
    if tab_name in {"Masraf Kayıtları", "Faturalar", "Sevk Fişleri"}:
        return (0, 0)
    if tab_name == "Banka Ödemeleri":
        return (1, 0)
    if tab_name.startswith("__") and tab_name != "__Raw Belgeler":
        return (2, 0)
    if tab_name == "__Raw Belgeler":
        return (3, 0)
    return (0, 0)


def _select_pending_sheet_batch(*, batch_size: int) -> list[dict]:
    with _pending_sheet_appends_lock:
        items = _load_pending_sheet_appends()

    if not items:
        return []

    ready_candidates = [
        (index, item)
        for index, item in enumerate(items)
        if _pending_sheet_item_is_ready(item)
    ]
    if not ready_candidates:
        return []

    _, first_ready = min(
        ready_candidates,
        key=lambda pair: (_pending_sheet_batch_priority(pair[1]), pair[0]),
    )

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


def has_pending_visible_appends(*, message_id: str, chat_id: str | None = None, platform: str | None = None) -> bool:
    normalized_message_id = (message_id or "").strip()
    if not normalized_message_id:
        return False

    with _pending_sheet_appends_lock:
        items = _load_pending_sheet_appends()

    for item in items:
        if not bool(item.get("is_visible_tab")):
            continue
        if str(item.get("feedback_message_id") or "").strip() != normalized_message_id:
            continue
        if chat_id is not None and str(item.get("feedback_chat_id") or "").strip() != chat_id:
            continue
        if platform is not None and str(item.get("feedback_platform") or "").strip() != platform:
            continue
        return True
    return False


def _pending_sheet_feedback_key(item: dict) -> tuple[str, str, str]:
    return (
        str(item.get("feedback_platform") or "").strip(),
        str(item.get("feedback_chat_id") or "").strip(),
        str(item.get("feedback_message_id") or "").strip(),
    )


def _send_visible_sheet_success_feedback(item: dict) -> None:
    platform = str(item.get("feedback_platform") or "").strip()
    chat_id = str(item.get("feedback_chat_id") or "").strip()
    message_id = str(item.get("feedback_message_id") or "").strip()
    recipient_type = str(item.get("feedback_recipient_type") or "").strip() or "individual"
    if not platform or not message_id:
        return

    try:
        if platform == "periskope":
            from app.services.providers import periskope

            periskope.react_to_message(message_id, "✅")
            return

        from app.services.providers import whatsapp

        whatsapp.send_reaction_message(
            chat_id,
            message_id,
            "✅",
            recipient_type=recipient_type,
        )
    except Exception as exc:
        logger.warning(
            "Failed to send visible-sheet success reaction for platform=%s chat_id=%s message_id=%s: %s",
            platform,
            chat_id,
            message_id,
            exc,
            exc_info=True,
        )


def _dispatch_visible_sheet_success_feedback(batch: list[dict]) -> None:
    feedback_items: dict[tuple[str, str, str], dict] = {}
    for item in batch:
        if not bool(item.get("is_visible_tab")):
            continue
        key = _pending_sheet_feedback_key(item)
        if not all(key):
            continue
        feedback_items.setdefault(key, item)

    if not feedback_items:
        return

    with _pending_sheet_appends_lock:
        remaining_items = _load_pending_sheet_appends()

    remaining_keys = {
        _pending_sheet_feedback_key(item)
        for item in remaining_items
        if bool(item.get("is_visible_tab"))
    }
    for key, item in feedback_items.items():
        if key in remaining_keys:
            continue
        _send_visible_sheet_success_feedback(item)


def _existing_row_numbers_by_pending_id(ws, tab_name: str, pending_ids: set[str]) -> dict[str, int]:
    if not pending_ids:
        return {}

    row_id_col = _internal_row_id_column_letter(tab_name)
    source_doc_col = _header_letter(tab_name, _HIDDEN_SOURCE_DOC_ID_HEADER)
    range_end = source_doc_col or row_id_col
    try:
        values = ws.get(f"A3:{range_end}")
    except Exception:
        return {}

    headers = _headers(tab_name)
    rows: dict[str, int] = {}
    for row_number, row in enumerate(values, start=3):
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        row_map = {headers[idx]: padded[idx] for idx in range(min(len(headers), len(padded)))}
        for key in (
            str(row_map.get(_HIDDEN_ROW_ID_HEADER) or '').strip(),
            str(row_map.get(_HIDDEN_SOURCE_DOC_ID_HEADER) or '').strip(),
        ):
            if key in pending_ids:
                rows[key] = row_number
    return rows


def _existing_drive_targets_by_pending_id(
    ws,
    tab_name: str,
    spreadsheet_id: str,
    pending_ids: set[str],
) -> dict[str, list[dict[str, str | int]]]:
    if not pending_ids:
        return {}
    last_col = _internal_row_id_column_letter(tab_name)
    try:
        rows = ws.get(f"A3:{last_col}")
    except Exception:
        return {}

    headers = _headers(tab_name)
    result: dict[str, list[dict[str, str | int]]] = {pending_id: [] for pending_id in pending_ids}
    for row_number, row in enumerate(rows, start=3):
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        row_map = {headers[idx]: padded[idx] for idx in range(len(headers))}
        row_id = str(row_map.get(_HIDDEN_ROW_ID_HEADER) or '').strip()
        source_doc_id = str(row_map.get(_HIDDEN_SOURCE_DOC_ID_HEADER) or '').strip()
        for key in (row_id, source_doc_id):
            if key and key in pending_ids:
                result.setdefault(key, []).append(
                    _build_drive_link_target(
                        spreadsheet_id=spreadsheet_id,
                        tab_name=tab_name,
                        row_number=row_number,
                        row_id=row_id or key,
                    )
                )
    return {key: value for key, value in result.items() if value}


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
            row_targets_by_item: dict[str, list[dict[str, str | int]]] = {}

            with _lock:
                sh = _open_spreadsheet_by_key(client, spreadsheet_id)
                audit_tabs = {tab_name, '📊 Özet'}
                if tab_name == 'Banka Ödemeleri':
                    audit_tabs.update({'Masraf Kayıtları', '__Ödeme_Dağıtımları', '__Cari_Kartlar'})
                _audit_spreadsheet_layout(sh, repair=True, target_tabs=audit_tabs)
                ws = _ensure_tab_exists(sh, tab_name)

                existing_targets = _existing_drive_targets_by_pending_id(
                    ws,
                    tab_name,
                    spreadsheet_id,
                    {str(item.get('id') or '') for item in batch},
                )
                row_targets_by_item.update(existing_targets)

                new_items = [
                    item for item in batch
                    if str(item.get('id') or '') not in existing_targets
                ]

                if new_items:
                    if tab_name == 'Banka Ödemeleri':
                        allocation_ws = _ensure_tab_exists(sh, '__Ödeme_Dağıtımları', lightweight=True)
                        debt_state = _load_expense_debt_state(sh)
                        start_row_number = len(ws.col_values(1)) + 1
                        visible_rows: list[list] = []
                        allocation_rows: list[list] = []
                        party_cards: list[dict[str, object]] = []

                        for item in new_items:
                            item_id = str(item.get('id') or '')
                            record = BillRecord.model_validate(item.get('record') or {})
                            category_raw = str(item.get('category') or '').strip()
                            item_category = DocumentCategory(category_raw) if category_raw else DocumentCategory.ODEME_DEKONTU
                            built_visible_rows, built_allocation_rows, built_cards = _build_payment_projection_rows(
                                record=record,
                                category=item_category,
                                item_id=item_id,
                                debt_state=debt_state,
                                drive_link=str(item.get('drive_link') or '') or None,
                                spreadsheet=sh,
                            )
                            row_targets_by_item[item_id] = []
                            row_start = start_row_number + len(visible_rows)
                            row_id_idx = _header_index('Banka Ödemeleri', _HIDDEN_ROW_ID_HEADER)
                            for offset, row in enumerate(built_visible_rows):
                                row_id = str(row[row_id_idx] if row_id_idx is not None else item_id)
                                row_targets_by_item[item_id].append(
                                    _build_drive_link_target(
                                        spreadsheet_id=spreadsheet_id,
                                        tab_name='Banka Ödemeleri',
                                        row_number=row_start + offset,
                                        row_id=row_id,
                                    )
                                )
                            visible_rows.extend(built_visible_rows)
                            allocation_rows.extend(built_allocation_rows)
                            party_cards.extend(built_cards)

                        if visible_rows:
                            _retry_on_rate_limit(lambda: ws.append_rows(visible_rows, value_input_option='USER_ENTERED'))
                        if allocation_rows:
                            _retry_on_rate_limit(lambda: allocation_ws.append_rows(allocation_rows, value_input_option='USER_ENTERED'))
                        _upsert_party_cards(sh, party_cards)

                    elif tab_name == '__Fatura Kalemleri':
                        rows: list[list] = []
                        for item in new_items:
                            item_id = str(item.get('id') or '')
                            record = BillRecord.model_validate(item.get('record') or {})
                            row_targets_by_item[item_id] = []
                            rows.extend(_build_invoice_line_rows(record, row_id_prefix=item_id, source_doc_id=item_id))
                        if rows:
                            _retry_on_rate_limit(lambda: ws.append_rows(rows, value_input_option='USER_ENTERED'))

                    else:
                        start_row_number = len(ws.col_values(1)) + 1
                        rows: list[list] = []
                        party_cards: list[dict[str, object]] = []
                        row_id_idx = _header_index(tab_name, _HIDDEN_ROW_ID_HEADER)

                        for item in new_items:
                            item_id = str(item.get('id') or '')
                            record = BillRecord.model_validate(item.get('record') or {})
                            category = DocumentCategory(str(item.get('category') or DocumentCategory.BELIRSIZ.value))
                            return_source_raw = str(item.get('return_source_category') or '').strip()
                            return_source_category = DocumentCategory(return_source_raw) if return_source_raw else None
                            row_number = start_row_number + len(rows)
                            row = _build_row_for_tab(
                                record,
                                tab_name,
                                category=category,
                                row_id=item_id,
                                row_number=row_number,
                                drive_link=str(item.get('drive_link') or '') or None,
                                return_source_category=return_source_category,
                                source_doc_id=item_id,
                                spreadsheet=sh,
                            )
                            rows.append(row)
                            if _header_index(tab_name, _VISIBLE_DRIVE_LINK_HEADER) is not None or _header_index(tab_name, _HIDDEN_DRIVE_LINK_HEADER) is not None:
                                row_id = str(row[row_id_idx] if row_id_idx is not None else item_id)
                                row_targets_by_item[item_id] = [
                                    _build_drive_link_target(
                                        spreadsheet_id=spreadsheet_id,
                                        tab_name=tab_name,
                                        row_number=row_number,
                                        row_id=row_id,
                                    )
                                ]
                            else:
                                row_targets_by_item[item_id] = []

                            if tab_name == 'Masraf Kayıtları':
                                party_cards.append({
                                    'party_key': _party_key(record, role='debt'),
                                    'display_name': _counterparty_name(record, category),
                                    'tax_number': str(record.tax_number or ''),
                                    'aliases': tuple(filter(None, [record.company_name, record.recipient_name, record.buyer_name, record.sender_name])),
                                })

                        if rows:
                            _retry_on_rate_limit(lambda: ws.append_rows(rows, value_input_option='USER_ENTERED'))
                        if party_cards:
                            _upsert_party_cards(sh, party_cards)

            payloads_to_cleanup: set[str] = set()
            for item in batch:
                item_id = str(item.get('id') or '')
                targets = row_targets_by_item.get(item_id, [])

                payload_path_raw = str(item.get('document_payload_path') or '')
                if str(item.get('drive_link') or '').strip():
                    if payload_path_raw:
                        payloads_to_cleanup.add(payload_path_raw)
                    continue

                if not payload_path_raw or not targets:
                    if payload_path_raw and tab_name == '__Fatura Kalemleri':
                        payloads_to_cleanup.add(payload_path_raw)
                    continue

                payload_path = Path(payload_path_raw)
                if not payload_path.exists():
                    raise RuntimeError(f"Queued sheet append payload is missing: {payload_path}")

                queue_pending_document_upload(
                    file_bytes=payload_path.read_bytes(),
                    filename=str(item.get('document_filename') or payload_path.name),
                    mime_type=str(item.get('document_mime_type') or 'application/octet-stream'),
                    targets=targets,
                    source_message_id=str(item.get('source_message_id') or '') or None,
                )
                payloads_to_cleanup.add(payload_path_raw)

            _remove_pending_sheet_appends({str(item.get('id') or '') for item in batch})
            for payload_path_raw in payloads_to_cleanup:
                _cleanup_pending_sheet_payload_if_unused(payload_path_raw)
            _dispatch_visible_sheet_success_feedback(batch)
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
    feedback_target: dict[str, str] | None = None,
) -> list[dict[str, str | int]]:
    """
    Queue *record* for append into the correct Google Sheets tab for *category*.

    drive_link: Google Drive web-view URL for the original document.
                Shown as a clickable "Görüntüle" link in the Belge column.
    Return documents remain in their base category tab; no separate active İadeler tab is used.
    All errors are caught so CSV persistence is never disrupted.
    """
    client = _get_client()
    if client is None:
        return []

    try:
        normalized_category = DocumentCategory.FATURA if category == DocumentCategory.IADE else category
        visible_tab = _CATEGORY_VISIBLE_TAB.get(normalized_category, "Faturalar")
        payload_bytes = None if drive_link else pending_document_bytes
        payload_filename = None if drive_link else pending_document_filename
        payload_mime_type = None if drive_link else pending_document_mime_type

        items: list[dict] = []

        def _queue(tab_name: str) -> None:
            is_visible_tab = not tab_name.startswith("__")
            items.append(
                _queue_pending_sheet_append_item(
                    record=record,
                    category=normalized_category,
                    tab_name=tab_name,
                    drive_link=drive_link,
                    return_source_category=category if category == DocumentCategory.IADE else None,
                    document_payload=payload_bytes,
                    document_filename=payload_filename,
                    document_mime_type=payload_mime_type,
                    feedback_target=feedback_target if is_visible_tab else None,
                    is_visible_tab=is_visible_tab,
                )
            )

        _queue('__Raw Belgeler')
        _queue(visible_tab)

        if normalized_category == DocumentCategory.FATURA:
            _queue('Masraf Kayıtları')
            if _iter_invoice_line_items(record):
                _queue('__Fatura Kalemleri')
        elif normalized_category in {DocumentCategory.HARCAMA_FISI, DocumentCategory.ELDEN_ODEME}:
            if visible_tab != 'Masraf Kayıtları':
                _queue('Masraf Kayıtları')
        elif normalized_category in {DocumentCategory.ODEME_DEKONTU, DocumentCategory.CEK}:
            _queue('__Çek_Dekont_Detay')

        with _pending_sheet_appends_lock:
            pending_items = _load_pending_sheet_appends()
            pending_items.extend(items)
            _save_pending_sheet_appends(pending_items)

        logger.info(
            "Queued %d Google Sheets append(s) for category=%s is_return=%s message_id=%s.",
            len(items),
            normalized_category,
            is_return,
            record.source_message_id,
        )
        start_pending_sheet_append_worker()
        return items
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
                logger.info("Spreadsheet %s was prepared recently; running lightweight repair.", sh.id)
                _audit_spreadsheet_layout(sh, repair=True, refresh_formatting=False)
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
        sh = _open_spreadsheet_by_key(client, spreadsheet_id) if spreadsheet_id else _get_or_create_spreadsheet(client)
        _repair_monthly_spreadsheet_layout(sh)
        logger.info("📊 Özet tab ensured.")
    except Exception as exc:
        logger.warning("Could not ensure Özet tab: %s", exc)
