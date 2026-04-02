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
5. Exports spreadsheet-ready CSV/XLSX with Turkish column names.

---

## Architecture

```
WhatsApp Group
      │  (fatura / fiş / makbuz photo)
      ▼
Meta Cloud API  ──────►  POST /webhook
                              │
                    ┌─────────▼──────────┐
                    │  Bill Classifier   │  ← keyword heuristics + Gemini flash
                    └─────────┬──────────┘
                     is_bill? │
                    ┌─────────▼──────────┐
                    │  Gemini Extractor  │  ← Gemini 3 Flash, structured JSON
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Normalizer        │  ← Pydantic, Turkish date/number format
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
│   │   └── webhooks.py           # GET/POST /webhook
│   ├── services/
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
| `GEMINI_API_KEY` | Google Gemini API key | *(required)* |
| `GEMINI_CLASSIFIER_MODEL` | Gemini model used for media classification | `gemini-3-flash-preview` |
| `GEMINI_EXTRACTOR_MODEL` | Gemini model used for field extraction | `gemini-3-flash-preview` |
| `STORAGE_DIR` | Directory for temp files, exports, and processed-message state | `./storage` |
| `LOG_LEVEL` | `DEBUG` / `INFO` / `WARNING` / `ERROR` | `INFO` |

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
| Extraction failure | ⚠️ Belgeniz işlenirken bir hata oluştu. Lütfen daha sonra tekrar deneyin. |

---

## Gemini Extraction Flow

1. The message ID is claimed before expensive AI work so duplicate deliveries do not fan out across workers.
2. Image bytes + MIME type are sent to `gemini-3-flash-preview` via the `google-genai` SDK with structured JSON output.
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
