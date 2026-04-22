# Runtime Pipeline

## Hot Path at a Glance

```text
webhook ingress
    |
    v
durable inbound queue
    |
    v
media fetch + Gemini classification
    |
    v
Gemini extraction
    |
    v
CSV dedupe gate
    |
    v
canonical SQLite upsert
    |
    v
visible Sheets projection flush
    |
    v
delayed Drive upload + link backfill
    |
    v
final success feedback
```

## Stage-by-Stage

### 1. Webhook acceptance

Media messages do not run the expensive pipeline inline.

- Meta ingress: `app/routes/webhooks.py`
- Periskope ingress: `app/routes/periskope.py`

For image and document messages the route:

1. builds a `MessageRoute`,
2. calls `inbound_queue.enqueue_media_job()`,
3. sends the initial `⌛` reaction if the enqueue succeeds.

The goal is to return control to the provider quickly while preserving the job on disk.

### 2. Durable inbound queue

`app/services/accounting/inbound_queue.py` persists queue state under `STORAGE_DIR/state`.

Important artifacts:

- `pending_inbound_jobs.json`
- `pending_inbound_jobs/*.bin`
- `inbound_failures.json`

Behavior:

- jobs are retried with exponential backoff,
- payload bytes are cached locally when storage pressure allows it,
- jobs are reclaimed if a worker dies mid-processing,
- queue retries stop after the configured age / attempt ceiling,
- the queue sends one delay notice if a job becomes retry-waiting.

This queue is the durable front door for the live media pipeline.

### 3. Gemini document analysis

The queue worker loads the media and calls `intake.process_media_payload()`.

`doc_classifier.analyze_document()` is Gemini-first and answers:

- is this a financial document?
- which category does it belong to?
- is it a return?
- how many separate documents are visible?
- should the sender retry with a better image?

This is the current hot path. OCR is not required at this stage.

### 4. Gemini extraction

If the document passes triage, `gemini_extractor.extract_bills()` produces one or more normalized `BillRecord` objects.

Details that matter operationally:

- multi-document images can produce multiple records,
- split documents receive derived source IDs such as `wamid__doc1`,
- extraction may run a stricter retry prompt if the detected document count does not match the extracted count.

If Gemini cannot extract a usable record, the user receives a retry-oriented error instead of a guessed row.

### 5. CSV dedupe gate

Each extracted record passes through `record_store.persist_record_once()`.

This step:

- appends the record to the daily CSV export,
- records the processed message ID,
- stores strong content fingerprints,
- prevents duplicate CSV rows across retries, duplicate webhook deliveries, and repeated submissions of the same content.

If a record fails this dedupe gate, it does not continue into the canonical Sheets pipeline.

## Canonical and Projection Layers

### 6. Canonical store

After the CSV gate, `google_sheets.append_record()` writes the normalized record into `canonical_store.sqlite3`.

The canonical store holds:

- the normalized bookkeeping record,
- document category and return metadata,
- any known Drive link,
- feedback routing metadata,
- the pending projection queue,
- sheet override state.

This SQLite store is the durable source of truth for everything downstream of the CSV export.

### 7. Visible Sheets projection

The Sheets worker does not append one visible row per message in place. Instead, it rebuilds the visible workbook snapshot from canonical state.

Visible tabs:

- `Masraf Kayıtları`
- `Banka Ödemeleri`
- `Faturalar`
- `Sevk Fişleri`

Hidden support tabs:

- `__Raw Belgeler`
- `__Ödeme_Dağıtımları`
- `__Fatura Kalemleri`
- `__Çek_Dekont_Detay`
- `__Cari_Kartlar`

Implications:

- visible rows can be regenerated safely,
- Drive links can arrive later,
- month repair/bootstrap can reapply formatting and layout,
- operator edits can be synced back as overrides.

### 8. Delayed Drive backfill

Drive upload is intentionally decoupled from the visible Sheets flush.

`queue_pending_document_upload()` stores:

- payload bytes for the original media,
- the canonical document IDs that should receive the Drive link,
- optional visible row targets for direct backfill.

`process_pending_document_uploads()` then:

1. uploads the original file to the current month's Drive folder,
2. writes the Drive link back into canonical state,
3. backfills the `Belge` hyperlink in the correct visible row.

The worker can recover even if rows moved after projection because it can resolve by hidden row ID instead of only the original row number.

## Feedback Lifecycle

User feedback is intentionally staged.

| Moment | User-visible effect |
| --- | --- |
| enqueue accepted | `⌛` reaction |
| retryable queue delay | one text notice explaining processing is delayed |
| extraction succeeded but visible projection still pending | `📝` reaction |
| visible projection flush completed | `✅` reaction |
| terminal failure or retry exhaustion | `⚠️` reaction plus text explanation |

The key point is that `✅` means the record made it through the visible Sheets projection stage, not merely that Gemini extraction finished.

## Idempotency and Recovery

The pipeline uses multiple idempotency layers because no single key is enough:

- inbound queue dedupe by message ID before expensive work,
- inter-process claim tracking for in-flight messages,
- processed message registry,
- CSV content fingerprints,
- canonical `source_doc_id` upserts,
- pending projection tracking by canonical document ID,
- delayed Drive uploads that can reuse cached links and merged targets.

This layered design lets the system survive:

- duplicate provider deliveries,
- worker restarts,
- transient Gemini or provider failures,
- delayed Drive uploads,
- visible sheet rewrites and repairs.

## Special Paths

### Manager text messages

Text from `MANAGER_PHONE_NUMBER` can bypass the media path and become an `elden_odeme` entry if Gemini can extract a cash payment amount from the text.

### Non-financial or low-quality messages

- non-financial media is rejected with a warning,
- unusable images request a clearer retry,
- group-only mode can reject direct chats before the expensive stages,
- retryable upstream failures stay in the inbound queue instead of being dropped.

## Google Document AI / OCR Position

The project still includes:

- `app/services/providers/google_document_ai.py`
- `app/services/accounting/ocr.py`

That stack can normalize media, call Document AI Form Parser, fall back to Enterprise OCR, serialize OCR bundles, and score extraction quality. It is present and maintained, but it is not on the current production hot path for document classification or extraction. The live path is Gemini-first, with OCR retained as supporting infrastructure rather than the main synchronous pipeline.
