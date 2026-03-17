# Architecture

## Overview

```
WhatsApp Group
      │
      │  (photo / document / text)
      ▼
Meta Cloud API  ──────►  POST /webhook
                              │
                    ┌─────────▼──────────┐
                    │  Webhook Router     │
                    │  (FastAPI)          │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Bill Classifier   │
                    │  (keyword + Gemini)│
                    └─────────┬──────────┘
                     is_bill? │
                    ┌─────────▼──────────┐
                    │  Media Downloader  │
                    │  (WhatsApp API)    │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Gemini Extractor  │
                    │  (gemini-1.5-pro)  │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Normalizer        │
                    │  (Pydantic)        │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Exporter          │
                    │  CSV / XLSX        │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Storage           │
                    │  ./storage/exports │
                    └────────────────────┘
```

## Modules

| Module | Responsibility |
|---|---|
| `app/main.py` | FastAPI app, startup |
| `app/config.py` | Settings from env vars |
| `app/routes/webhooks.py` | HTTP webhook endpoints |
| `app/services/whatsapp.py` | WhatsApp Cloud API client |
| `app/services/bill_classifier.py` | Keyword + Gemini classification |
| `app/services/gemini_extractor.py` | Gemini extraction + normalisation |
| `app/services/exporter.py` | CSV / XLSX export |
| `app/models/schemas.py` | Pydantic models |
| `app/utils/logging.py` | Structured logging |
| `app/utils/file_storage.py` | Temp file helpers |

## Data Flow

1. Meta sends a POST to `/webhook` for every new message.
2. The webhook handler validates the payload and enqueues each message for background processing.
3. For **text** messages: keyword classification determines if it might be a bill.
4. For **image/document** messages:
   a. Media bytes are downloaded from the WhatsApp CDN.
   b. A lightweight Gemini call classifies the image (is it a real bill?).
   c. If yes: a full Gemini extraction call produces structured JSON.
   d. The JSON is normalised into a `BillRecord` Pydantic model.
   e. The record is appended to a daily CSV in `./storage/exports/`.
5. The sender receives a Turkish confirmation message.

## Design Principles

- **Background tasks**: Media download + AI extraction run in FastAPI `BackgroundTasks` so the webhook returns 200 immediately (prevents Meta retries).
- **Idempotency**: Records are keyed by `source_message_id`; duplicate webhook deliveries append a duplicate row (future enhancement: dedup check).
- **Retry logic**: `tenacity` retries Gemini and WhatsApp API calls with exponential back-off.
- **Separation of concerns**: Routes only parse and enqueue; all business logic is in services.
