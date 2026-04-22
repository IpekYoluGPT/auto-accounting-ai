# Setup Guide

## Prerequisites

- Python 3.11+
- One inbound provider:
  - Meta Developer App with WhatsApp Cloud API access, or
  - Periskope with a connected WhatsApp number
- Google Gemini API access
- Google credentials for the Sheets / Drive projection path
- Persistent storage for `STORAGE_DIR` in any real deployment

## Local Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload --port 8000
```

The checked-in `.env.example` covers the core runtime knobs, but it does not currently enumerate every Google Sheets / Drive variable used by `app/config.py`. Add the missing Google variables below manually when preparing a real environment.

## Environment Variables

### Core runtime

| Variable | Required | Notes |
| --- | --- | --- |
| `GEMINI_API_KEY` | Yes | Required for the live hot path. Classification and extraction are Gemini-first. |
| `GEMINI_CLASSIFIER_MODEL` | No | Defaults to `gemini-3.1-pro-preview`. |
| `GEMINI_EXTRACTOR_MODEL` | No | Defaults to `gemini-3.1-pro-preview`. |
| `GEMINI_VALIDATION_MODEL` | No | Shared Gemini config; monitored at startup even though it is not the main media hot path today. |
| `BUSINESS_TIMEZONE` | No | Used for monthly workbook rollover. Defaults to `Europe/Istanbul`. |
| `STORAGE_DIR` | Yes in production | Must point to persistent storage. Queue state, CSV exports, canonical SQLite state, and delayed Drive uploads all live here. |
| `LOG_LEVEL` | No | Defaults to `INFO`. |
| `MANAGER_PHONE_NUMBER` | No | Enables the special text-to-`elden_odeme` path for the configured manager. |

### Inbound provider: Meta Cloud API

| Variable | Required | Notes |
| --- | --- | --- |
| `WHATSAPP_VERIFY_TOKEN` | Yes for Meta | Used for `GET /webhook` verification. |
| `WHATSAPP_ACCESS_TOKEN` | Yes for Meta | Used for outbound replies and media download. |
| `WHATSAPP_PHONE_NUMBER_ID` | Yes for Meta | Cloud API phone number ID. |
| `WHATSAPP_GROUPS_ONLY` | Recommended | Defaults to `true`; direct 1:1 intake is blocked unless explicitly disabled. |

### Inbound provider: Periskope

| Variable | Required | Notes |
| --- | --- | --- |
| `PERISKOPE_API_KEY` | Yes for Periskope | Required for replies, reactions, and private notes. |
| `PERISKOPE_PHONE` | Yes for Periskope | Used as the `x-phone` header. |
| `PERISKOPE_SIGNING_KEY` | Strongly recommended | Used to verify `x-periskope-signature`. |
| `PERISKOPE_ALLOWED_CHAT_IDS` | Yes for Periskope | Empty means reject all chats by design. |
| `PERISKOPE_TOOL_TOKEN` | If using custom tools | Protects `/integrations/periskope/tools/*`. |
| `PERISKOPE_API_BASE_URL` | No | Defaults to `https://api.periskope.app/v1`. |
| `PERISKOPE_MEDIA_BASE_URL` | No | Defaults to `https://api.periskope.app`. |

### Google Sheets and Drive projection

| Variable | Required | Notes |
| --- | --- | --- |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Yes | Base64-encoded service account JSON. The live Sheets/Drive client path expects base64 input. |
| `GOOGLE_SHEETS_SPREADSHEET_ID` | Optional | Seed spreadsheet for the current month. If set and accessible, it is registered for the active month. |
| `GOOGLE_DRIVE_PARENT_FOLDER_ID` | Recommended | Parent folder for monthly workbook placement and monthly `Fişler` document folders. |
| `GOOGLE_SHEETS_OWNER_EMAIL` | Recommended | Email to share new spreadsheets with when they are created automatically. |
| `GOOGLE_OAUTH_CLIENT_ID` | Optional but preferred | Enables workbook and folder creation as the real Google user instead of only the service account. |
| `GOOGLE_OAUTH_CLIENT_SECRET` | Optional but preferred | Used with the refresh token below. |
| `GOOGLE_OAUTH_REFRESH_TOKEN` | Optional but preferred | Obtained through `/setup/google-auth`. |

Notes:

- The live Sheets output is a projection from canonical SQLite state, not a direct per-message append.
- Drive upload is intentionally delayed. The workbook can show the visible row first and receive the `Belge` hyperlink later.
- If `GOOGLE_DRIVE_PARENT_FOLDER_ID` is missing, Drive upload/backfill is skipped.

### Optional Google Document AI / OCR

| Variable | Required | Notes |
| --- | --- | --- |
| `GOOGLE_DOCUMENT_AI_PROJECT_ID` | Optional | Needed only if using Document AI OCR helpers. |
| `GOOGLE_DOCUMENT_AI_LOCATION` | Optional | Defaults to `eu`. |
| `GOOGLE_DOCUMENT_AI_FORM_PROCESSOR_ID` | Optional | Primary Form Parser processor. |
| `GOOGLE_DOCUMENT_AI_OCR_PROCESSOR_ID` | Optional | Enterprise OCR fallback processor. |
| `OCR_MIN_TEXT_CHARS` | Optional | OCR quality threshold. |
| `OCR_MIN_PARSE_SCORE` | Optional | OCR parsing threshold. |
| `OCR_MIN_QUALITY_SCORE` | Optional | OCR quality threshold. |

These OCR variables are not required for the current Gemini-first hot path, but the OCR stack remains present and usable.

### Queue and retry tuning

| Variable | Required | Notes |
| --- | --- | --- |
| `INBOUND_RETRY_MAX_ATTEMPTS` | No | Defaults to `20`. |
| `INBOUND_RETRY_MAX_AGE_HOURS` | No | Defaults to `24`. |
| `INBOUND_WORKER_POLL_SECONDS` | No | Defaults to `5`. |
| `INBOUND_MAX_ACTIVE_JOBS` | No | Defaults to `2`. |
| `GEMINI_MAX_CONCURRENCY` | No | Defaults to `1`. |
| `PENDING_PAYLOAD_STORAGE_LIMIT_MB` | No | Queue payload storage guard. |
| `STORAGE_SOFT_PRESSURE_BYTES` | No | Soft storage pressure threshold. |
| `STORAGE_HARD_REJECT_BYTES` | No | Rejects new media jobs when exceeded. |
| `STORAGE_EMERGENCY_STOP_BYTES` | No | Stops transient payload writes when exceeded. |
| `STORAGE_MIN_FREE_BYTES` | No | Minimum required free disk budget. |

## Google Setup

### Service account

1. Create or reuse a Google service account with access to Sheets and Drive.
2. Put the credentials into `GOOGLE_SERVICE_ACCOUNT_JSON`.
3. Share the target Drive folder with that service account if you use a user-owned folder.

### Optional OAuth bootstrap

OAuth is the preferred path when you want new spreadsheets and folders to be created as the real Google user.

1. Create an OAuth client in Google Cloud.
2. Set:
   - `GOOGLE_OAUTH_CLIENT_ID`
   - `GOOGLE_OAUTH_CLIENT_SECRET`
3. Visit `GET /setup/google-auth`.
4. Complete the consent flow.
5. Store the returned refresh token in `GOOGLE_OAUTH_REFRESH_TOKEN`.

The application can still fall back to service-account-driven creation, but OAuth usually makes ownership and Drive visibility cleaner.

### Monthly workbook behavior

At startup and around the month boundary, the app prepares the current month's workbook. The registry for month-to-spreadsheet mapping is kept in `state/sheets_registry.json`. If a sheet already exists for the current month, it is reused; otherwise the app tries, in order:

1. the registry entry for the current month,
2. `GOOGLE_SHEETS_SPREADSHEET_ID`,
3. an existing Drive file with the expected monthly title,
4. auto-creation in Drive.

## Webhook Setup

### Meta Cloud API

- Verification URL: `GET /webhook`
- Delivery URL: `POST /webhook`

Typical development flow:

```bash
ngrok http 8000
```

Then register `https://<your-tunnel>/webhook` in Meta.

### Periskope

- Delivery URL: `POST /integrations/periskope/webhook`
- Recommended auth: `x-periskope-signature` verified with `PERISKOPE_SIGNING_KEY`

If `PERISKOPE_ALLOWED_CHAT_IDS` is empty, the backend rejects all Periskope chats as a safety default.

## Periskope Custom Tools

Create these tools if you use the Periskope agent workflow:

- `POST /integrations/periskope/tools/create_accounting_record`
- `POST /integrations/periskope/tools/get_submission_status`
- `POST /integrations/periskope/tools/assign_to_human`

Use `PERISKOPE_TOOL_TOKEN` as the shared Bearer token.

## Deployment Notes

For customer-owned managed deployments and billing handoff, see:

- [docs/customer-handoff.md](docs/customer-handoff.md)

### Persistent storage is mandatory

Do not deploy this app with ephemeral-only storage if you expect reliable media retries and reconciliation. The following live state would otherwise be lost on restart:

- inbound queue jobs,
- cached inbound payload bytes,
- processed message registry,
- CSV dedupe fingerprints,
- canonical SQLite store,
- pending Drive backfills,
- month-to-spreadsheet registry.

### Railway example

Start command:

```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

If you deploy on Railway, align `STORAGE_DIR` with the mounted volume path. The app logs a warning when it detects that storage is outside the persistent volume.

### Railway test reset (clear sheet values only)

Use this when you want a clean workbook for manual testing without clearing queue/canonical files in `STORAGE_DIR`.

1. Verify you are targeting the API service URL:

```bash
curl -i "$APP_BASE_URL/health"
```

Expect `200` with `{"status":"ok"}`. If you get an HTML page or `404`, you are likely pointing to the wrong Railway service/domain.

2. Clear workbook values only:

```bash
curl -X POST "$APP_BASE_URL/setup/reset-sheet" \
  -H "Authorization: Bearer $PERISKOPE_TOOL_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"clear_storage": false}'
```

`"clear_storage": false` keeps queue/canonical runtime state and only resets visible workbook values.
