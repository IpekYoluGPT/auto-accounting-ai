# Architecture

## Overview

```
WhatsApp Group / 1:1 Chat
      |
      |  (photo / document / text)
      v
Meta Cloud API or Periskope ------> POST /webhook or POST /integrations/periskope/webhook
                                               |
                                     +---------v---------+
                                     |  Ingress Router   |
                                     |  (FastAPI)        |
                                     +---------+---------+
                                               |
                                     +---------v---------+
                                     | Shared Intake     |
                                     | keyword + Gemini  |
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
| `app/routes/periskope.py` | Periskope webhook + custom tool endpoints |
| `app/services/providers/whatsapp.py` | WhatsApp Cloud API client |
| `app/services/providers/periskope.py` | Periskope API client |
| `app/services/accounting/intake.py` | Shared inbound accounting pipeline |
| `app/services/accounting/bill_classifier.py` | Keyword + Gemini classification |
| `app/services/gemini_client.py` | Shared Gemini 3 client wrapper |
| `app/services/accounting/gemini_extractor.py` | Gemini extraction + normalisation |
| `app/services/accounting/record_store.py` | Message claim lifecycle + CSV persistence + completion registry |
| `app/services/accounting/exporter.py` | CSV / XLSX export |
| `app/models/schemas.py` | Pydantic models |
| `app/utils/logging.py` | Structured logging |

## Data Flow

1. Meta sends a POST to `/webhook`, or Periskope sends a POST to `/integrations/periskope/webhook`.
2. The ingress route validates provider-specific signatures/tokens, resolves whether the message came from a direct chat or group, and enqueues it for background processing.
3. Periskope self-authored messages are ignored so outbound confirmations do not loop back into the AI pipeline.
4. If groups-only mode is enabled and the message is a direct 1:1 chat, the bot sends a "use the group" warning and stops there.
5. Each remaining background task claims the source message ID before expensive work starts.
6. For text messages, keyword classification determines if it might be a bill.
7. For image/document messages:
   a. Media bytes are downloaded from the WhatsApp CDN.
   b. Meta media is downloaded from the WhatsApp CDN; Periskope media is downloaded from Periskope storage.
   c. Gemini 3 Flash classifies the media as bill or non-bill with structured JSON output.
   d. If accepted, Gemini 3 Flash extracts structured accounting fields.
   e. The extracted payload is normalised into a `BillRecord`, including source sender/group metadata.
   f. The record is appended to the daily CSV export exactly once.
8. Successful outcomes are marked complete; failures release the claim so a retry can try again later.
9. Replies are sent back to the originating chat through the provider that delivered the message.
10. Periskope AI custom tools can call dedicated backend endpoints to create a manual accounting record, query submission status, or assign the chat to a human via private note.

## Design Principles

- **Background tasks**: Media download and AI work run in FastAPI `BackgroundTasks` so the webhook returns 200 immediately.
- **Groups-only gate**: Direct 1:1 intake is disabled by default so the workflow stays aligned to official accounting groups.
- **Multi-provider ingress**: Meta and Periskope each have thin provider-specific routes, but both feed the same classifier/extractor/store path.
- **Fail-closed media handling**: AI failures do not create export rows; the system asks for retry rather than guessing.
- **Cross-worker idempotency**: Message IDs are claimed before AI work and marked complete after successful handling so duplicate deliveries are skipped across processes that share storage.
- **Group-aware routing**: Incoming `group_id` is treated as the chat target, while `from` remains the sender identity for audit/export metadata.
- **Loop prevention**: Periskope `from_me` / private-note events are ignored to prevent outbound confirmations from retriggering intake.
- **Retry logic**: `tenacity` retries Gemini, Periskope, and WhatsApp API calls with exponential backoff.
- **Separation of concerns**: Routes parse and orchestrate; services own API access, extraction, and persistence.
