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
3. Records are persisted to daily CSV + written to **Google Sheets** (monthly spreadsheets)
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
                    ├── record_store (CSV + dedup)
                    └── google_sheets (monthly spreadsheets via OAuth)
```

## Key Design Decisions

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
│   └── setup.py                       # One-time OAuth2 setup flow
├── services/
│   ├── gemini_client.py               # Shared Gemini API wrapper with retry + fallback
│   ├── accounting/
│   │   ├── intake.py                  # Central message processing pipeline
│   │   ├── bill_classifier.py         # Is this a financial document? (text keywords + Gemini vision)
│   │   ├── doc_classifier.py          # Which category? (fatura/dekont/fiş/çek/malzeme/iade)
│   │   ├── gemini_extractor.py        # Multi-doc extraction → list[BillRecord]
│   │   ├── record_store.py            # CSV persistence + dedup + message claim tracking
│   │   └── exporter.py                # CSV/XLSX export formatting
│   └── providers/
│       ├── google_sheets.py           # Monthly sheet management, OAuth, retry, tab bootstrap
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
| malzeme | 🏗️ Malzeme | Delivery notes, materials |
| iade | ↩️ İadeler | Returns and cancellations |
| belirsiz | 🧾 Faturalar | Fallback |

## Important Env Vars

| Variable | Purpose |
|----------|---------|
| `GEMINI_API_KEY` | Gemini AI (classification + extraction) |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Base64-encoded SA credentials (read/write sheets) |
| `GOOGLE_OAUTH_CLIENT_ID` | OAuth2 for spreadsheet creation |
| `GOOGLE_OAUTH_CLIENT_SECRET` | OAuth2 for spreadsheet creation |
| `GOOGLE_OAUTH_REFRESH_TOKEN` | OAuth2 refresh token (obtained via /setup/google-auth) |
| `GOOGLE_DRIVE_PARENT_FOLDER_ID` | Parent Drive folder for monthly subfolders |
| `PERISKOPE_API_KEY` | Periskope outbound messaging |
| `PERISKOPE_ALLOWED_CHAT_IDS` | Comma-separated allowed WhatsApp chat IDs |
| `MANAGER_PHONE_NUMBER` | Phone number for elden odeme text entries |

## Service Account Info

- **SA email:** `whatsappsheet@whatsapp-account-manager-ai.iam.gserviceaccount.com`
- **Owner email:** `yilmazatakan4423@gmail.com`
- **GCP Project:** whatsapp-account-manager-ai

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
