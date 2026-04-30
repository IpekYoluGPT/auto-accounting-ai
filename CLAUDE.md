# CLAUDE.md - Project Context for AI Assistants

## Project Identity

- **Name:** auto-accounting-ai
- **Stack:** Python 3.11+ / FastAPI / Gemini AI / Google Sheets API / gspread
- **Deployment:** Railway (auto-deploy from GitHub `main`)
- **Production URL:** `https://auto-accounting-ai-production.up.railway.app`
- **Tests:** 88 passing (`python -m pytest tests/ -q`)

## What This Project Does

Automated Turkish accounting pipeline:
1. WhatsApp documents (fatura, fiş, çek, dekont) arrive via **Periskope webhook**
2. **Gemini AI** classifies → categorizes → extracts structured fields
3. Records are persisted to SQLite (`canonical_store.sqlite3`) + written to **Google Sheets** (monthly spreadsheets)
4. Confirmation messages sent back to WhatsApp

## Current Architecture

```
WhatsApp → Periskope webhook → FastAPI
                                  │
                    ┌─────────────┤
                    ▼             ▼
              bill_classifier  doc_classifier
              (is financial?)  (which category?)
                    │             │
                    ▼             ▼
              gemini_extractor (multi-document aware)
                    │
                    ├── canonical_store (SQLite — source of truth)
                    ├── record_store (CSV + dedup)
                    └── google_sheets (monthly spreadsheets via OAuth)
```

## Key Design Decisions

### SQLite is the Source of Truth — NOT Google Sheets

`canonical_store.sqlite3` at `$STORAGE_DIR/state/canonical_store.sqlite3` is the authoritative store.
Google Sheets is a **projection/view** rebuilt from SQLite. Any manual edit to the sheet
**will be overwritten** the next time the projection worker runs (every 30 seconds, or on
any new incoming message). The only exception is fields listed in `_AUTHORITATIVE_FIELDS`
(see below) which are captured as overrides.

### Projection Worker Rewrites Everything

`_write_visible_projection_rows()` in `google_sheets.py` **clears the entire tab** (rows 3–1000)
and rewrites all rows from SQLite on every flush. This means:

- **Manual row reordering is lost** — rows are always sorted by `(document_date, created_at, source_doc_id)`
- **Manual cell edits are lost** unless the column is in `_AUTHORITATIVE_FIELDS` for that tab
- **Each new WhatsApp message triggers a full rewrite** of all visible tabs (~12 Sheets API requests)

### _AUTHORITATIVE_FIELDS — What Survives Manual Edits

Only columns listed here survive projection overwrites (captured by 5-min override sync):

```python
# google_sheets.py
_AUTHORITATIVE_FIELDS = {
    "Masraf Kayıtları": ("Kategori", "Alıcı / Tedarikçi", "Açıklama", "Belge No / Referans"),
    "Banka Ödemeleri":  ("Alıcı / Tedarikçi", "Açıklama", "Referans No", "Gönderen", "Gönderen IBAN", "Alıcı IBAN", "Banka"),
    "Çekler":           ("Lehdar", "Açıklama", "Çek No", "Çeki Düzenleyen", "Banka"),
    "Faturalar":        ("Fatura Tipi", "Alıcı", "Açıklama / Hizmet"),
    "Sevk Fişleri":     ("Tarih", "Satıcı", "Alıcı", "Ürün Cinsi", "Ürün Miktarı", "Sevk Yeri", "Açıklama"),
}
```

**Tarih (date) is authoritative only for Sevk Fişleri.** For all other tabs, manually editing
a date cell will be overwritten on the next projection. To permanently fix a date on any tab,
patch SQLite using `/setup/patch-record-date` or `/setup/bulk-patch-dates`.

### _document_month_key() — Checked BEFORE document_date

```python
# google_sheets.py:1295
for candidate in (
    getattr(document, "created_at", None),      # ← checked FIRST
    getattr(record, "document_date", None),
    ...
):
```

`created_at` (the WhatsApp message arrival time) is used before `document_date`. This means
a document with a wrong extracted year (e.g. 2020 instead of 2026) still lands in the current
month's projection scope because `created_at` is always correct. After patching `document_date`
in SQLite, the record continues to appear in the correct month's sheet.

### Two Railway Services Share /data Volume

There are **two Railway services** for this project:

| Service | Status | SQLite path |
|---------|--------|-------------|
| `muhasebe-api` | Online (current) | `/data/storage/state/canonical_store.sqlite3` |
| `muhasebe` | Offline (legacy) | Somewhere under `/data/` |

Both mount the same `/data` persistent volume. The legacy `muhasebe` service periodically
wakes up and re-projects from its own SQLite to the same Google Sheet (every ~3 hours, visible
in Sheet version history as SA writes). **This causes manual sheet corrections to revert.**

The startup migration in `app/services/accounting/migrations.py` patches ALL SQLite files found
under `/data` on every boot, which fixes the legacy service's database too.

### Startup Migration (migrations.py)

`run_sevk_date_fix()` runs as the **first step** in `_run_google_sheets_startup_tasks()`.
It is idempotent — records already at the correct date are skipped. Add future one-time
data fixes here as new functions called from `_run_google_sheets_startup_tasks()`.

### sheets_registry.json

Maps `YYYY-MM` → spreadsheet ID. If this gets corrupted or points to the wrong sheet,
the projection worker writes to the wrong spreadsheet. Fix with `/setup/update-sheet-registry`.
File is stored at `$STORAGE_DIR/state/sheets_registry.json`.

### Multi-Document Extraction
- A single photo may contain multiple documents (e.g. 3 cheques side by side)
- `extract_bills()` returns a `list[BillRecord]` with sub-indexed message IDs (`msg_123__doc1`, `msg_123__doc2`)
- The old `extract_bill()` wrapper still exists for backward compatibility

### Monthly Spreadsheets (OAuth)
- Service accounts CANNOT create Google Workspace files (403 permission error)
- OAuth2 user credentials are used for spreadsheet/folder creation
- Service account handles all read/write operations after sharing
- Each month auto-creates: subfolder "Belgeler — {Ay Yıl}" + spreadsheet "Muhasebe — {Ay Yıl}"
- `sheets_registry.json` tracks month → spreadsheet_id mapping
- Duplicate prevention: Drive search before auto-creating

### Rate Limit Handling
- Google Sheets 429 errors are retried with exponential backoff (5s→10s→20s→40s→80s)
- Gemini API calls use tenacity retry (5 attempts with exponential backoff)

### Chat Allowlist
- `PERISKOPE_ALLOWED_CHAT_IDS` controls which WhatsApp groups/chats are processed
- Empty allowlist = reject ALL (safety default, prevents bot running in wrong groups)

## File Layout

```
app/
├── config.py                          # All env vars (Pydantic Settings)
├── main.py                            # FastAPI app, routers, health/export endpoints
├── models/schemas.py                  # BillRecord, AIExtractionResult, AIMultiExtractionResult, ClassificationResult, DocumentCategory
├── routes/
│   ├── webhooks.py                    # Meta Cloud API webhook (legacy, not primary)
│   ├── periskope.py                   # Periskope webhook + tool endpoints (PRIMARY)
│   ├── groups.py                      # Official Meta group management (dormant)
│   ├── setup.py                       # One-time OAuth2 setup flow
│   └── setup_admin.py                 # Admin/ops endpoints (token-protected)
├── services/
│   ├── gemini_client.py               # Shared Gemini API wrapper with retry + fallback
│   ├── accounting/
│   │   ├── intake.py                  # Central message processing pipeline
│   │   ├── bill_classifier.py         # Is this a financial document? (text keywords + Gemini vision)
│   │   ├── doc_classifier.py          # Which category? (fatura/dekont/fiş/çek/malzeme/iade)
│   │   ├── gemini_extractor.py        # Multi-doc extraction → list[BillRecord]
│   │   ├── migrations.py              # Startup data migrations (idempotent, run every boot)
│   │   ├── record_store.py            # CSV persistence + dedup + message claim tracking
│   │   ├── canonical_store.py         # SQLite source of truth for all documents
│   │   └── exporter.py                # CSV/XLSX export formatting
│   └── providers/
│       ├── google_sheets.py           # Monthly sheet management, OAuth, retry, tab bootstrap
│       ├── google_sheets_projection.py # Projection worker, pending append queue
│       ├── periskope.py               # Periskope API client (send/fetch/media)
│       └── whatsapp.py                # Meta Cloud API client
└── utils/
    ├── logging.py
    └── file_storage.py
```

## Document Categories

| Category | Tab Name | Description |
|----------|----------|-------------|
| fatura | 🧾 Faturalar | Official invoices with KDV |
| odeme_dekontu | 💳 Dekontlar | Bank transfers, EFT, FAST |
| harcama_fisi | ⛽ Harcama Fişleri | POS receipts, fuel, market |
| cek | 📝 Çekler | Bank cheques |
| elden_odeme | 💵 Elden Ödemeler | Cash payments (manager text) |
| malzeme | 🏗️ Malzeme → "Sevk Fişleri" tab | Delivery notes, materials |
| iade | ↩️ İadeler | Returns and cancellations |
| belirsiz | 🧾 Faturalar | Fallback |

**Note:** The category is `malzeme` in code but the tab is named **Sevk Fişleri** in the sheet.
Do not call it "Malzeme" when talking to the user.

## Important Env Vars

| Variable | Purpose |
|----------|---------|
| `GEMINI_API_KEY` | Gemini AI (classification + extraction) |
| `GEMINI_LEHDAR_REFINEMENT_MODEL` | Model used for the cheque-only Lehdar second pass (default `gemini-3.1-flash-lite-preview`) |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Base64-encoded SA credentials (read/write sheets) |
| `GOOGLE_OAUTH_CLIENT_ID` | OAuth2 for spreadsheet creation |
| `GOOGLE_OAUTH_CLIENT_SECRET` | OAuth2 for spreadsheet creation |
| `GOOGLE_OAUTH_REFRESH_TOKEN` | OAuth2 refresh token (obtained via /setup/google-auth) |
| `GOOGLE_DRIVE_PARENT_FOLDER_ID` | Parent Drive folder for monthly subfolders |
| `PERISKOPE_API_KEY` | Periskope outbound messaging |
| `PERISKOPE_ALLOWED_CHAT_IDS` | Comma-separated allowed WhatsApp chat IDs |
| `MANAGER_PHONE_NUMBER` | Phone number for elden odeme text entries |
| `STORAGE_DIR` | Persistent storage root (production: `/data/storage`, mount: `/data`) |

## Service Account Info

- **SA email (new):** `auto-accounting-ai@muhasebe-494114.iam.gserviceaccount.com`
- **SA email (old):** `whatsappsheet@whatsapp-account-manager-ai.iam.gserviceaccount.com`
- **Owner email:** `yilmazatakan4423@gmail.com`
- **GCP Project:** muhasebe-494114

## Admin Endpoints (all require `x-api-key: $PERISKOPE_TOOL_TOKEN`)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/setup/storage-status` | GET | Queue depths, flush counts, disk usage |
| `/setup/debug-storage` | GET | Find all SQLite files on volume, show document counts |
| `/setup/lookup-record` | GET `?q=` | Search SQLite by message_id or source_doc_id |
| `/setup/patch-record-date` | POST | Fix a date in SQLite (triggers re-projection) |
| `/setup/patch-sheet-date-direct` | POST | Fix a date cell directly in the Sheet (bypasses SQLite — temporary) |
| `/setup/bulk-patch-dates` | POST | Fix dates across ALL SQLite files on volume (use for legacy-service revertion issues) |
| `/setup/update-sheet-registry` | POST | Point a month to a different spreadsheet ID |
| `/setup/reprocess-message` | POST | Clear dedup so a message can be reprocessed |
| `/setup/drain-queues` | POST | Manually flush pending projection + inbound queues |
| `/setup/repair-sheet` | POST | Fix tab layout and formulas |
| `/setup/rewrite-belge-links` | POST | Force-rewrite Drive hyperlinks in the sheet |

## Testing

```bash
python -m pytest tests/ -q          # Full suite (88 tests, ~3 min)
python -m pytest tests/ -x -q       # Stop on first failure
```

Test files mirror the source structure. Mocks are used for Gemini API and Periskope API calls.

## Common Patterns

- All Gemini calls go through `gemini_client.generate_structured_content()` with Pydantic response schemas
- `_retry_on_rate_limit()` wraps Google Sheets writes with exponential backoff
- `record_store.claim_message_processing()` prevents duplicate processing of the same message
- `_safe_send_text_message()` catches send failures so processing continues
- Tab bootstrap order matters: data tabs first, then Ozet summary formulas

### Cheque Lehdar Two-Pass Extraction

For `category_hint == DocumentCategory.CEK`, `extract_bills()` runs a second focused
Gemini call (`_refine_missing_cheque_lehdars` in `app/services/accounting/gemini_extractor.py`)
when the primary pass leaves any record's `recipient_name` blank. The refinement uses the
faster `GEMINI_LEHDAR_REFINEMENT_MODEL` (Flash) and only fills blank slots — populated
lehdars are never overwritten. Refinement failures log a warning and leave records as-is.

### Date Two-Pass Extraction

For Sevk Fişleri and other handwritten documents, `extract_bills()` runs a second Gemini
call (`_refine_suspicious_dates` in `gemini_extractor.py`) when any extracted year falls
outside `{current_year-1, current_year, current_year+1}`. The today's date is also injected
into the primary extraction prompt to reduce year misreads. See PR #18.
