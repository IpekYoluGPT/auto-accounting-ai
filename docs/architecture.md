# Architecture

## Overview

The live system is a queued, multi-stage bookkeeping pipeline. Media messages are accepted quickly at the webhook edge, persisted into a durable inbound queue, processed by Gemini workers, deduplicated into a daily CSV export, upserted into a canonical SQLite store, projected into monthly Google Sheets tabs, and only then acknowledged as fully successful.

```text
Meta Cloud API / Periskope
            |
            v
      FastAPI ingress
            |
            v
   durable inbound queue
            |
            v
 Gemini classify / extract
            |
            v
 CSV dedupe + processed-id registry
            |
            v
 canonical SQLite store
            |
            v
 Google Sheets projection worker
            |
            +----> visible workbook tabs
            |
            v
 delayed Drive upload worker
            |
            v
 staged feedback back to WhatsApp
```

## Core Components

| Component | Responsibility |
| --- | --- |
| `app/routes/webhooks.py` | Meta webhook ingress. Media is enqueued instead of being processed inline. |
| `app/routes/periskope.py` | Periskope webhook ingress plus custom tool endpoints. |
| `app/services/accounting/inbound_queue.py` | Durable media queue, retry scheduling, payload persistence, terminal failure logging, and queue-driven feedback timing. |
| `app/services/accounting/intake.py` | Shared hot-path orchestration for classification, extraction, persistence, and reaction/text messages. |
| `app/services/accounting/doc_classifier.py` | Gemini-first document triage: financial/non-financial, category, return flag, document count, and retry hint. |
| `app/services/accounting/gemini_extractor.py` | Gemini structured extraction for one or more bookkeeping records from the media payload. |
| `app/services/accounting/record_store.py` | Daily CSV export, duplicate suppression, processed message registry, in-flight claims, and warning throttling. |
| `app/services/accounting/canonical_store.py` | SQLite-backed canonical document store, pending projection queue, manual override state, and feedback lookup metadata. |
| `app/services/providers/google_sheets.py` | Monthly workbook management, full visible projection flushes, Drive upload queue, delayed Drive link backfill, and success feedback after visible rows land. |
| `app/services/providers/google_document_ai.py` | Google Document AI OCR integration. Present and test-covered, but not on the current synchronous hot path. |
| `app/services/accounting/ocr.py` | OCR preparation, caching, deterministic parsing helpers, and OCR quality assessment utilities. |

## Runtime Data Stores

All persistent runtime state lives under `STORAGE_DIR`.

| Path | Purpose |
| --- | --- |
| `state/pending_inbound_jobs.json` | Durable inbound media jobs waiting for fetch/retry/processing. |
| `state/pending_inbound_jobs/*.bin` | Cached inbound payload bytes so retries survive transient upstream failures. |
| `state/inbound_failures.json` | Terminal inbound queue failures after retry exhaustion or expiry. |
| `state/processed_message_ids.txt` | Message IDs that already completed handling. |
| `state/inflight_message_ids.json` | Cross-worker claim registry to prevent concurrent duplicate processing. |
| `state/content_fingerprints.txt` | Strong content-level dedupe fingerprints for CSV writes. |
| `exports/records_YYYY-MM-DD.csv` | Append-only daily export. |
| `state/canonical_store.sqlite3` | Canonical document rows, pending projection docs, sheet overrides, and worker state. |
| `state/pending_drive_uploads.json` | Delayed Google Drive upload and link backfill queue. |
| `state/pending_drive_uploads/*.bin` | Stored payload bytes for delayed Drive upload. |
| `state/sheets_registry.json` | Month-to-spreadsheet registry for the active workbook set. |

## Live Processing Model

### 1. Ingress and durable queue

- Text messages still flow through `intake.process_incoming_message()`.
- Media messages from Meta or Periskope are enqueued first through `inbound_queue.enqueue_media_job()`.
- The webhook sends the initial `⌛` reaction as soon as the media job is accepted.
- Queue jobs are persisted on disk so retries can survive worker restarts, transient provider failures, or slow upstream dependencies.

### 2. Gemini-first hot path

- The queue worker fetches media and calls `intake.process_media_payload()`.
- `doc_classifier.analyze_document()` is Gemini-first. It decides whether the media is financial, its category, whether it is a return, how many documents are visible, and whether the sender should retry with a clearer image.
- `gemini_extractor.extract_bills()` then produces one or more normalized `BillRecord` objects.
- Multi-document media can be split into canonical document IDs such as `wamid-123__doc1`, `wamid-123__doc2`.

### 3. CSV dedupe gate

- `record_store.persist_record_once()` writes to the daily CSV only once.
- Dedupe is not only message-ID based. It also checks content fingerprints derived from record content, which protects the CSV export from repeated deliveries or near-duplicate submissions.
- Only records that pass this dedupe gate continue into the canonical Sheets pipeline.

### 4. Canonical store and projection

- `google_sheets.append_record()` no longer means "write a visible row immediately".
- Instead it upserts the normalized document into `canonical_store.sqlite3` and marks that document as needing projection.
- The projection worker rebuilds the visible Sheets snapshot from canonical state, then writes the visible business tabs:
  - `Masraf Kayıtları`
  - `Banka Ödemeleri`
  - `Faturalar`
  - `Sevk Fişleri`
- Hidden technical tabs such as `__Raw Belgeler`, `__Ödeme_Dağıtımları`, `__Fatura Kalemleri`, `__Çek_Dekont_Detay`, and `__Cari_Kartlar` support the workbook but are not the operator-facing output.

### 5. Drive backfill

- Document file upload to Google Drive is intentionally decoupled from the visible Sheets projection.
- The visible row can be projected first with an empty `Belge` cell.
- A later worker uploads the original image/PDF, stores the Drive link against the canonical document, and backfills the correct visible row even if rows were reordered meanwhile.

### 6. Staged feedback

- `⌛`: sent when the media job is accepted by the inbound queue.
- delay notice text: sent once if the job becomes retry-waiting because Gemini or media fetch failed transiently.
- `📝`: sent when extraction/export succeeded but visible Sheets rows are still pending.
- `✅`: sent only after `process_pending_sheet_appends()` flushes the visible projection and the feedback target is no longer pending.
- `⚠️`: sent for terminal failures, including retry exhaustion and non-retryable processing errors.

## Projection Semantics

The workbook is a projection, not the source of truth.

- Canonical documents live in SQLite.
- Visible Sheets rows are regenerated from canonical state.
- Certain visible fields are treated as authoritative operator overrides.
- The projection worker periodically syncs those operator edits back into canonical override state and re-marks affected documents as dirty.

This is why the system can tolerate re-projection, row movement, delayed Drive links, and month bootstrap repairs without losing the underlying bookkeeping record.

## Category-to-Tab Mapping

| Category | Visible tab |
| --- | --- |
| `fatura` | `Faturalar` |
| `belirsiz` | `Faturalar` |
| `iade` | `Faturalar` with return metadata preserved in canonical state |
| `odeme_dekontu` | `Banka Ödemeleri` |
| `cek` | `Banka Ödemeleri` |
| `harcama_fisi` | `Masraf Kayıtları` |
| `elden_odeme` | `Masraf Kayıtları` |
| `malzeme` | `Sevk Fişleri` |

## Google Document AI and OCR

Google Document AI is still part of the codebase and can produce OCR bundles with Form Parser plus Enterprise OCR fallback. The OCR helpers in `app/services/accounting/ocr.py` also provide deterministic parsing and quality assessment. However, the current hot path described above does not depend on OCR to classify or extract documents. Production intake currently runs Gemini-first for both triage and extraction, with OCR infrastructure retained as supporting capability rather than the main ingestion path.

## Startup and Recovery

At startup the app launches background workers and bootstrap tasks for:

- monthly workbook preparation,
- pending Sheets projection flushes,
- pending Drive backfills,
- inbound queue normalization and replay.

Because the queue, CSV dedupe state, and canonical store are all persisted under `STORAGE_DIR`, recovery after restart depends on that directory being backed by persistent storage in production.
