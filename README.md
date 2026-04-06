# auto-accounting-ai

> Code-first AI backend that receives WhatsApp bill/invoice/receipt images, ignores junk messages, extracts accounting data with Gemini, and exports spreadsheet-ready output 24/7.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Problem Statement

Small businesses and freelancers in Turkey share **fatura** (invoice), **fiş** (cash receipt), and **makbuz** (payment receipt) photos in a WhatsApp group. They also send unrelated messages, memes, and greetings in the same group. A human assistant currently spends hours every week manually sorting these messages and entering data into a spreadsheet.

**auto-accounting-ai** automates the entire pipeline:

1. Receives every WhatsApp message via webhook.
2. Classifies whether the message contains a real financial document.
3. Downloads and analyses qualifying media with the Gemini AI API.
4. Extracts structured accounting fields.
5. Exports spreadsheet-ready CSV/XLSX with Turkish column names plus source sender/group metadata.

The backend now supports two ingress modes:
- official Meta WhatsApp Cloud API webhooks at `POST /webhook`
- Periskope webhooks for WhatsApp Web-connected groups/chats at `POST /integrations/periskope/webhook`

When a webhook message includes group context, the bot treats that as the reply target and keeps the participant identity as export metadata.

---

## Architecture

```
WhatsApp Group
      │  (fatura / fiş / makbuz photo)
      ▼
Meta Cloud API / Periskope  ──────►  POST /webhook or POST /integrations/periskope/webhook
                              │
                    ┌─────────▼──────────┐
                    │ Shared Intake Flow │  ← keyword heuristics + Gemini flash
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Record Store      │  ← claim check + CSV + dedup state
                    └────────────────────┘
```

See [docs/architecture.md](docs/architecture.md) for the full module breakdown.

---

## Repository Structure

```
auto-accounting-ai/
├── app/
│   ├── main.py                   # FastAPI app entry point
│   ├── config.py                 # Settings (pydantic-settings)
│   ├── routes/
│   │   ├── groups.py             # Official WhatsApp group onboarding/management
│   │   ├── periskope.py          # Periskope webhook + tool endpoints
│   │   └── webhooks.py           # GET/POST /webhook
│   ├── services/
│   │   ├── intake.py             # Shared inbound accounting pipeline
│   │   ├── periskope.py          # Periskope API client
│   │   ├── whatsapp.py           # WhatsApp Cloud API client
│   │   ├── bill_classifier.py    # Keyword + Gemini classification
│   │   ├── gemini_client.py      # Shared Gemini structured-output helper
│   │   ├── gemini_extractor.py   # AI extraction + normalisation
│   │   ├── record_store.py       # CSV persistence + message claims/dedup
│   │   └── exporter.py           # CSV / XLSX export helpers
│   ├── models/
│   │   └── schemas.py            # Pydantic models
│   └── utils/
│       ├── logging.py            # Structured logging
│       └── file_storage.py       # Temp file helpers
├── docs/
│   ├── architecture.md
│   ├── setup.md
│   └── data-schema.md
├── examples/
│   ├── sample_bill_result.json
│   └── sample_accounting_rows.csv
├── tests/
│   ├── test_classifier.py
│   ├── test_config.py
│   ├── test_extractor.py
│   ├── test_groups.py
│   ├── test_record_store.py
│   └── test_webhooks.py
├── .env.example
├── pytest.ini
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/IpekYoluGPT/auto-accounting-ai.git
cd auto-accounting-ai
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Fill in your credentials
```

### 3. Run

```bash
uvicorn app.main:app --reload --port 8000
```

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `PORT` | HTTP server port | `8000` |
| `WHATSAPP_VERIFY_TOKEN` | Webhook verification token | *(required)* |
| `WHATSAPP_ACCESS_TOKEN` | Meta permanent access token | *(required)* |
| `WHATSAPP_PHONE_NUMBER_ID` | Meta phone number ID | *(required)* |
| `WHATSAPP_GROUPS_ONLY` | Process only official group messages; direct 1:1 intake stays disabled | `true` |
| `PERISKOPE_API_KEY` | Periskope API key used for outbound replies and notes | *(optional)* |
| `PERISKOPE_PHONE` | Periskope `x-phone` header value (phone number or phone_id) | *(optional)* |
| `PERISKOPE_API_BASE_URL` | Periskope REST base URL | `https://api.periskope.app/v1` |
| `PERISKOPE_MEDIA_BASE_URL` | Base URL used to resolve relative media paths from Periskope webhooks | `https://api.periskope.app` |
| `PERISKOPE_SIGNING_KEY` | HMAC signing key for `x-periskope-signature` | *(optional but recommended)* |
| `PERISKOPE_TOOL_TOKEN` | Shared secret for custom tool endpoints | *(optional but recommended)* |
| `GEMINI_API_KEY` | Google Gemini API key | *(required)* |
| `GEMINI_CLASSIFIER_MODEL` | Gemini model used for media classification | `gemini-flash-lite-latest` |
| `GEMINI_EXTRACTOR_MODEL` | Gemini model used for field extraction | `gemini-flash-lite-latest` |
| `STORAGE_DIR` | Directory for temp files, exports, and processed-message state | `./storage` |
| `LOG_LEVEL` | `DEBUG` / `INFO` / `WARNING` / `ERROR` | `INFO` |

---

## Group Onboarding API

The backend now exposes official group-management endpoints for onboarding tenants:

- `POST /groups/onboard` creates a new official WhatsApp group and tries to fetch its invite link immediately.
- `GET /groups` lists active groups for the configured business number.
- `GET /groups/{group_id}` fetches group metadata.
- `GET /groups/{group_id}/invite-link` returns the current invite link.
- `POST /groups/{group_id}/invite-link/reset` resets the invite link.
- `GET /groups/{group_id}/join-requests` lists open join requests.
- `POST /groups/{group_id}/join-requests/approve` approves join requests.

By default, direct 1:1 user chats are not processed. If someone messages the bot outside the official group, they are told to use the accounting group instead.

---

## Periskope Integration

If you connect a normal WhatsApp/WhatsApp Business app number to Periskope, configure:

- webhook endpoint: `POST /integrations/periskope/webhook`
- custom tool endpoints:
  - `POST /integrations/periskope/tools/create_accounting_record`
  - `POST /integrations/periskope/tools/get_submission_status`
  - `POST /integrations/periskope/tools/assign_to_human`

`/integrations/periskope/webhook` processes inbound `message.created` events, ignores self-sent messages to avoid loops, verifies `x-periskope-signature` when configured, downloads media from Periskope storage, and sends confirmations back through Periskope’s `/message/send` queue.

---

## WhatsApp Webhook Flow

```
Meta Platform
    │
    │  POST /webhook  { "object": "whatsapp_business_account", ... }
    ▼
FastAPI route  →  parse payload  →  background task per message
                                          │
                              ┌───────────┴───────────┐
                              │ text message?          │ image/doc?
                              │                        │
                         keyword classify        claim message id
                              │                        │
                         is_bill=True?           download media
                              │                        │
                         prompt user             Gemini classify
                         to send photo               │
                                               Gemini extract
                                                    │
                                            append to CSV once
                                                    │
                                       mark handled + reply ✅
```

### Sample Turkish User Messages

| Event | Message |
|---|---|
| Bill accepted | ✅ Belgeniz alındı ve muhasebe kaydına eklendi. Firma: ABC Market · Toplam: 100.0 TRY |
| Non-bill ignored | *(no reply — silent discard)* |
| Text that looks like a bill | 📄 Fatura/fiş metin olarak algılandı. Lütfen belge fotoğrafını gönderin. |
| Direct 1:1 chat while disabled | 🔒 Bu bot şimdilik yalnızca resmi WhatsApp muhasebe grubunda çalışıyor. Lütfen belgeyi grup içinden gönderin. |
| Extraction failure | ⚠️ Belgeniz işlenirken bir hata oluştu. Lütfen daha sonra tekrar deneyin. |

---

## Gemini Extraction Flow

1. The message ID is claimed before expensive AI work so duplicate deliveries do not fan out across workers.
2. Image bytes + MIME type are sent to `gemini-flash-lite-latest` via the `google-genai` SDK with structured JSON output.
3. Response is parsed and normalised:
   - Turkish date formats (`DD.MM.YYYY`) → ISO 8601 (`YYYY-MM-DD`)
   - Turkish numbers (`1.234,56`) → float (`1234.56`)
   - Currency defaults to `TRY`
4. A `BillRecord` Pydantic model validates all fields.
5. The record is appended to `storage/exports/records_YYYY-MM-DD.csv`, and the message is marked complete so later duplicates are skipped.

---

## Example Input / Output

### Input (WhatsApp image of a market receipt)

*A JPEG photo of a supermarket cash receipt showing ABC Market, 100.00 TL total.*

### Output (`sample_bill_result.json`)

```json
{
  "company_name": "ABC Market",
  "tax_number": "9876543210",
  "document_date": "2024-03-10",
  "currency": "TRY",
  "subtotal": 84.75,
  "vat_rate": 18.0,
  "vat_amount": 15.25,
  "total_amount": 100.00,
  "payment_method": "Nakit",
  "expense_category": "Yemek",
  "confidence": 0.91
}
```

### Export row (`sample_accounting_rows.csv`)

| Firma Adı | Tarih | Para Birimi | Genel Toplam | Gider Kategorisi | Güven Skoru |
|---|---|---|---|---|---|
| ABC Market | 2024-03-10 | TRY | 100.0 | Yemek | 0.91 |

---

## Railway Deployment

See [docs/setup.md](docs/setup.md) for step-by-step Railway deployment instructions.

**Start command:**
```
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

---

## Running Tests

```bash
python -m pytest -q
```

---

## Expense Categories

Gemini suggests one of the following Turkish categories for each document:

`Yemek` · `Ulaşım` · `Konaklama` · `Ofis` · `Yazılım` · `Donanım` · `Abonelik` · `Kargo` · `Vergi` · `Diğer`

---

## Roadmap

- [ ] Google Sheets integration (append rows via Sheets API)
- [ ] PostgreSQL / Supabase persistence
- [ ] `/export` HTTP endpoint returning XLSX on demand
- [ ] Multi-group / multi-tenant support
- [ ] Dashboard (read-only, separate service)
- [ ] ERP / e-invoice (e-fatura) integration

---

## License

[MIT](LICENSE)
