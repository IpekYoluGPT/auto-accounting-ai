# Architecture

## Overview

```
WhatsApp Group
      |
      |  (photo / document / text)
      v
Meta Cloud API ------> POST /webhook
                              |
                    +---------v---------+
                    |  Webhook Router   |
                    |  (FastAPI)        |
                    +---------+---------+
                              |
                    +---------v---------+
                    | Bill Classifier   |
                    | keyword + Gemini  |
                    +---------+---------+
                     is_bill? |
                    +---------v---------+
                    | Media Downloader  |
                    | WhatsApp API      |
                    +---------+---------+
                              |
                    +---------v---------+
                    | Gemini Extractor  |
                    | Gemini 3 Flash    |
                    +---------+---------+
                              |
                    +---------v---------+
                    | Normalizer        |
                    | Pydantic          |
                    +---------+---------+
                              |
                    +---------v---------+
                    | Record Store      |
                    | claim + CSV state |
                    +---------+---------+
                              |
                    +---------v---------+
                    | Storage           |
                    | ./storage/*       |
                    +-------------------+
```

## Modules

| Module | Responsibility |
|---|---|
| `app/main.py` | FastAPI app, startup |
| `app/config.py` | Settings from env vars |
| `app/routes/webhooks.py` | HTTP webhook endpoints |
| `app/services/whatsapp.py` | WhatsApp Cloud API client |
| `app/services/bill_classifier.py` | Keyword + Gemini classification |
| `app/services/gemini_client.py` | Shared Gemini 3 client wrapper |
| `app/services/gemini_extractor.py` | Gemini extraction + normalisation |
| `app/services/record_store.py` | Message claim lifecycle + CSV persistence + completion registry |
| `app/services/exporter.py` | CSV / XLSX export |
| `app/models/schemas.py` | Pydantic models |
| `app/utils/logging.py` | Structured logging |

## Data Flow

1. Meta sends a POST to `/webhook` for every new message.
2. The webhook handler validates the payload and enqueues each message for background processing.
3. Each background task claims the WhatsApp message ID before expensive work starts.
4. For text messages, keyword classification determines if it might be a bill.
5. For image/document messages:
   a. Media bytes are downloaded from the WhatsApp CDN.
   b. Gemini 3 Flash classifies the media as bill or non-bill with structured JSON output.
   c. If accepted, Gemini 3 Flash extracts structured accounting fields.
   d. The extracted payload is normalised into a `BillRecord`.
   e. The record is appended to the daily CSV export exactly once.
6. Successful outcomes are marked complete; failures release the claim so a retry can try again later.
7. The sender receives a Turkish success or retry/error reply.

## Design Principles

- **Background tasks**: Media download and AI work run in FastAPI `BackgroundTasks` so the webhook returns 200 immediately.
- **Fail-closed media handling**: AI failures do not create export rows; the system asks for retry rather than guessing.
- **Cross-worker idempotency**: Message IDs are claimed before AI work and marked complete after successful handling so duplicate deliveries are skipped across processes that share storage.
- **Retry logic**: `tenacity` retries Gemini and WhatsApp API calls with exponential backoff.
- **Separation of concerns**: Routes parse and orchestrate; services own API access, extraction, and persistence.
