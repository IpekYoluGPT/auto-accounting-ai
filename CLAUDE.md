# Context Handoff

## Purpose Of This Document

This file is a full project handoff for the current state of `auto-accounting-ai`.

It is intended to answer:

- what this project is
- why it exists
- how it evolved
- what is currently working
- what is not being used right now
- how the repo is organized
- how the live deployment is wired
- which provider is actually in use
- what operational assumptions the system depends on
- what fixes were made recently
- what risks and caveats still exist

Secrets are intentionally omitted.

## Repository Identity

- Repository name: `auto-accounting-ai`
- Workspace path: `/mnt/c/Users/ariba/OneDrive/Documenti/Software Projects/AI Projects/auto-accounting-ai/auto-accounting-ai`
- Main branch: `main`
- Current pushed HEAD at handoff time: `44d515e`
- Current git state at handoff creation time: clean working tree
- Test status at handoff time: `82 passed in 13.55s`

## High-Level Product Summary

This project is a FastAPI backend that receives WhatsApp-originated accounting messages, classifies whether a message contains a financial document, extracts accounting fields with Gemini, stores normalized records, and exports accounting-ready CSV/XLSX output.

The original product idea was:

- users send receipt/invoice/payment images in WhatsApp
- the system filters noise from real accounting material
- the system extracts structured fields
- the extracted data becomes spreadsheet-ready accounting output

The project now supports two ingress paths in code:

- Meta WhatsApp Cloud API webhook ingress
- Periskope webhook ingress

The current live user path is Periskope, not Meta Cloud API.

## Business Context

The target workflow is Turkish small-business accounting intake.

Typical supported document types:

- fatura
- fiş
- makbuz
- payment proof
- bank transfer confirmation
- invoice-like receipt/document images

Typical unsupported content:

- casual group chat
- greetings
- memes
- unrelated screenshots
- non-accounting photos

## Current Live Reality

### What Is Actually Being Used

The system is currently operating through Periskope, using a WhatsApp app-connected phone.

The live production backend URL is:

- `https://auto-accounting-ai-production.up.railway.app`

The live production Periskope webhook URL is:

- `https://auto-accounting-ai-production.up.railway.app/integrations/periskope/webhook`

### Current Chat Restriction

The backend was explicitly changed so it does not respond in every group.

It now supports an allowlist via:

- `PERISKOPE_ALLOWED_CHAT_IDS`

The current intended allowed group is:

- group name used during testing: `Test`
- group chat id: `120363410789660631@g.us`

The intended live value is:

```text
PERISKOPE_ALLOWED_CHAT_IDS=120363410789660631@g.us
```

If that env var is empty, the backend will process all Periskope groups/chats again.

### Important Operational Assumption

Because the current production route is Periskope with a linked WhatsApp app number:

- the Railway backend can stay online independently
- the user computer can be off
- the connected phone still matters

Practical meaning:

- PC off: usually OK
- phone off, disconnected, dead battery, no internet, or unlinked: risky
- Periskope depends on the phone session remaining healthy

## What Is Not The Current Launch Path

The repo still contains official Meta WhatsApp Cloud API and official group-management code.

That code is real and tested, but it is not the primary live path for this user right now.

Reason:

- the user originally wanted a bot inside real WhatsApp groups
- official Meta Groups API access was not practically usable for the current account/number setup
- the user decided to move to a third-party solution
- the chosen third-party path became Periskope

So:

- Meta code remains in repo
- Meta group onboarding endpoints remain in repo
- they are not the path currently driving the live result

## Project Evolution Timeline

### Phase 1: Original Backend

The project started as a Meta Cloud API + Gemini invoice/receipt extraction backend.

Core ingredients:

- FastAPI
- Gemini classification
- Gemini extraction
- CSV/XLSX export
- message id deduplication and throttling

### Phase 2: Group-Aware Meta Support

The project was extended to distinguish:

- direct 1:1 WhatsApp chats
- official Cloud API group messages

It also added:

- group metadata on records
- group-aware reply targeting
- groups-only mode for intake restriction

### Phase 3: Official Group Onboarding API

The repo gained:

- `POST /groups/onboard`
- group invite link handling
- group join request handling
- group listing and metadata endpoints

This was built for official WhatsApp group management via Meta-style APIs.

### Phase 4: Official Meta Groups Blocker

Live testing against the user’s account found that:

- the available phone setup was not eligible for official Groups APIs
- the test number was not enough
- the official route was not practical for the user’s urgency

The major blocker observed during this phase was:

- Graph API error `131215`

### Phase 5: Pivot To Third-Party Periskope

The user chose to use Periskope instead of waiting on Meta’s official group eligibility path.

This required:

- Periskope webhook ingestion
- Periskope outbound reply support
- Periskope message/media handling
- provider-independent intake logic

### Phase 6: Periskope Production Debugging

Several real production issues were found and fixed one by one.

Important fixes made during this phase:

1. Periskope sent `event_type` instead of `event`
2. Periskope sometimes sent `has_media: null`
3. Periskope media URLs sometimes arrived as `storage.googleapis.com/periskope-attachments/...`
4. those media URLs initially failed with `401`
5. media path normalization and fallback retrieval were added
6. external storage URLs stopped receiving Periskope auth headers
7. Gemini incorrectly rejected invoice-like sample documents labeled `ÖRNEK FATURA`
8. Periskope intake was restricted to an explicit allowlisted chat id
9. repo services were reorganized by domain/provider for readability

### Phase 7: Current Working State

The user reported that the bot now works.

The final requested operational constraint was:

- only respond in the `Test` group
- do not respond in every group the user is part of

That behavior is now in code via the Periskope allowlist.

## Current Recent Git History

Recent commits leading to the present state:

- `44d515e` `refactor: organize services by domain and provider`
- `3e54e8a` `feat: restrict periskope intake to allowed chats`
- `50df325` `fix: accept invoice-like sample documents`
- `d9bf6dc` `fix: avoid auth headers on external periskope media urls`
- `97e3e26` `fix: normalize periskope storage urls before download`
- `db69a12` `fix: refresh periskope media path on download auth errors`
- `7b25aae` `fix: accept nullable periskope media flags`
- `45987ed` `fix: accept periskope event_type webhooks`
- `6cf0ce0` `feat: add periskope webhook accounting integration`
- `aa9ab39` `fix: always reply to processed image uploads`
- `3d1d237` `feat: warn direct-chat users about unrelated messages`
- `0acea6d` `feat: notify users while invoices are processing`

## Current Top-Level Repo Structure

```text
auto-accounting-ai/
├── .env.example
├── .gitignore
├── LICENSE
├── README.md
├── railway.toml
├── requirements.txt
├── context_handoff.md
├── app/
│   ├── __init__.py
│   ├── config.py
│   ├── main.py
│   ├── models/
│   │   ├── __init__.py
│   │   └── schemas.py
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── groups.py
│   │   ├── periskope.py
│   │   └── webhooks.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── gemini_client.py
│   │   ├── accounting/
│   │   │   ├── __init__.py
│   │   │   ├── bill_classifier.py
│   │   │   ├── exporter.py
│   │   │   ├── gemini_extractor.py
│   │   │   ├── intake.py
│   │   │   └── record_store.py
│   │   └── providers/
│   │       ├── __init__.py
│   │       ├── periskope.py
│   │       └── whatsapp.py
│   └── utils/
│       ├── __init__.py
│       ├── file_storage.py
│       └── logging.py
├── docs/
│   ├── architecture.md
│   ├── data-schema.md
│   └── setup.md
├── examples/
│   ├── sample_accounting_rows.csv
│   └── sample_bill_result.json
├── scripts/
│   └── whatsapp_groups_smoke_test.py
└── tests/
    ├── __init__.py
    ├── test_classifier.py
    ├── test_config.py
    ├── test_exports.py
    ├── test_extractor.py
    ├── test_gemini_client.py
    ├── test_groups.py
    ├── test_periskope.py
    ├── test_record_store.py
    └── test_webhooks.py
```

## Architectural Overview

The app has three major layers:

1. ingress routes
2. shared accounting pipeline
3. provider-specific transport clients

### Ingress Routes

- `app/routes/webhooks.py`
  - handles Meta Cloud API webhook verification and message ingress
- `app/routes/periskope.py`
  - handles Periskope webhook ingress
  - handles Periskope tool endpoints
- `app/routes/groups.py`
  - handles official Meta group onboarding/management endpoints

### Shared Accounting Pipeline

- `app/services/accounting/intake.py`
  - central flow for text/image/document message processing
- `app/services/accounting/bill_classifier.py`
  - text keyword classification
  - Gemini media classification
  - override for invoice-like sample/template documents
- `app/services/accounting/gemini_extractor.py`
  - structured extraction into `BillRecord`
- `app/services/accounting/record_store.py`
  - message deduplication
  - claim tracking
  - warning throttling
  - CSV persistence
  - export row lookup
- `app/services/accounting/exporter.py`
  - CSV/XLSX formatting and Turkish column mapping

### Provider Clients

- `app/services/providers/whatsapp.py`
  - Meta Cloud API media fetch
  - Meta Cloud API send text
  - official group management helper methods
- `app/services/providers/periskope.py`
  - Periskope message send
  - Periskope note send
  - Periskope media download
  - Periskope message fetch
  - media URL normalization and fallback logic

### Shared AI Client

- `app/services/gemini_client.py`
  - shared Gemini client wrapper used by classifier and extractor

## Main Runtime Entry

`app/main.py` mounts:

- `/webhook`
- `/groups`
- `/integrations/periskope`

It also exposes:

- `GET /health`
- `GET /export.csv`
- `GET /export.xlsx`

## Endpoint Catalog

### Health / Export

- `GET /health`
  - liveness probe
- `GET /export.csv`
  - latest CSV export
- `GET /export.xlsx`
  - latest XLSX export generated from latest CSV

### Meta Cloud API

- `GET /webhook`
  - Meta verification challenge
- `POST /webhook`
  - incoming WhatsApp messages

### Official Group Management

- `GET /groups`
- `POST /groups/onboard`
- `GET /groups/{group_id}`
- `GET /groups/{group_id}/invite-link`
- `POST /groups/{group_id}/invite-link/reset`
- `GET /groups/{group_id}/join-requests`
- `POST /groups/{group_id}/join-requests/approve`

### Periskope Webhook

- `POST /integrations/periskope/webhook`
  - primary live ingress path
  - currently the important route for the user

### Periskope Tool Endpoints

These exist in the backend:

- `POST /integrations/periskope/tools/create_accounting_record`
- `POST /integrations/periskope/tools/get_submission_status`
- `POST /integrations/periskope/tools/assign_to_human`

These are not currently the main live path because the user saw `Custom Tools` as `Coming Soon` in Periskope UI.

## Current Provider Strategy

### Meta Cloud API

Status:

- still implemented
- still tested
- not the user’s current production launch path

Why not primary now:

- official native group bot path was not practical in the user’s setup
- group eligibility and number/account constraints made it unsuitable for immediate launch

### Periskope

Status:

- primary launch path
- webhook ingress works
- message sending works
- live invoice flow works

Current constraints:

- must keep connected phone healthy
- custom tools are present in backend but not practically active in UI
- signing-key support exists but production may still be running without it if the Periskope panel does not expose it

## Current Known Production / Live Ops Context

These items are known from the live debugging session and matter operationally:

- Railway auto-deploys from GitHub `main`
- production URL is the Railway app URL above
- Periskope webhook is active and points to the production webhook path
- backend was tested repeatedly with real logs from production
- Periskope live fixes were validated through logs and then pushed
- `PERISKOPE_ALLOWED_CHAT_IDS` was introduced specifically so only the `Test` group is handled
- current expected allowlist value is the `Test` chat id

### Important Current User-Specific Assumptions

- the user wants only one group active
- that one group is the `Test` group
- other joined groups must be ignored
- this is enforced in backend by env-based allowlist

## Environment Variables

### Server

- `PORT`
  - default `8000`
  - Railway sets it automatically in production

### Meta Cloud API

- `WHATSAPP_VERIFY_TOKEN`
- `WHATSAPP_ACCESS_TOKEN`
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_GROUPS_ONLY`

Notes:

- `WHATSAPP_GROUPS_ONLY=true` keeps direct 1:1 intake disabled
- Meta replies may fail if stale credentials remain in production
- Meta path is not the main live route now

### Periskope

- `PERISKOPE_API_KEY`
  - required for outbound replies and notes
- `PERISKOPE_PHONE`
  - the `x-phone` header value
  - expected format is phone number or phone id
- `PERISKOPE_API_BASE_URL`
  - default `https://api.periskope.app/v1`
- `PERISKOPE_MEDIA_BASE_URL`
  - default `https://api.periskope.app`
- `PERISKOPE_SIGNING_KEY`
  - optional in code
  - recommended if Periskope UI exposes it
  - code accepts unsigned requests if not configured
- `PERISKOPE_TOOL_TOKEN`
  - protects custom tool endpoints
  - backend supports it even though Periskope UI tools were not usable
- `PERISKOPE_ALLOWED_CHAT_IDS`
  - comma-separated allowlist for Periskope chat ids
  - currently the key env var for restricting operation to the `Test` group

Important note:

- `PERISKOPE_ALLOWED_CHAT_IDS` exists in code, but at the time of this handoff it is not yet documented in `.env.example`, `README.md`, or `docs/setup.md`
- this is a documentation gap to keep in mind

### Gemini

- `GEMINI_API_KEY`
- `GEMINI_CLASSIFIER_MODEL`
- `GEMINI_EXTRACTOR_MODEL`

Observed live behavior:

- production logs showed `gemini-3-flash-preview` in use during one live run
- the repo default is `gemini-flash-lite-latest`
- actual production model depends on Railway env values

### Storage / Logging

- `STORAGE_DIR`
- `LOG_LEVEL`

## Storage Model

Storage is file-based under `STORAGE_DIR`.

Important directories:

- `storage/exports/`
- `storage/state/`

Important files:

- `exports/records_<YYYY-MM-DD>.csv`
- `state/processed_message_ids.txt`
- `state/inflight_message_ids.json`
- `state/warning_throttle.json`
- `state/.record_store.lock`

Behavior:

- message ids are claimed before expensive work
- completed ids are remembered
- warning messages are throttled per recipient
- duplicate deliveries are skipped

## Export Schema

The exported CSV/XLSX columns are:

- `Firma Adı`
- `Vergi Numarası`
- `Vergi Dairesi`
- `Belge Numarası`
- `Fatura Numarası`
- `Fiş Numarası`
- `Tarih`
- `Saat`
- `Para Birimi`
- `Ara Toplam`
- `KDV Oranı`
- `KDV Tutarı`
- `Genel Toplam`
- `Ödeme Yöntemi`
- `Gider Kategorisi`
- `Açıklama`
- `Notlar`
- `Kaynak Mesaj ID`
- `Kaynak Dosya Adı`
- `Kaynak Türü`
- `Kaynak Gönderen ID`
- `Kaynak Grup ID`
- `Sohbet Türü`
- `Güven Skoru`

## Core Data Models

### `BillRecord`

Represents the normalized accounting record written to export.

It contains:

- company identity fields
- document identity fields
- date/time
- monetary values
- payment method
- expense category
- free-text description/notes
- source metadata
- confidence

### `ClassificationResult`

Represents bill/non-bill classification:

- `is_bill`
- `reason`
- `confidence`

### `AIExtractionResult`

Represents structured Gemini extraction before final normalization.

## Detailed Runtime Flow

### Meta Ingress Flow

1. Meta posts to `POST /webhook`
2. webhook payload is validated
3. each message is queued as a background task
4. route resolves:
   - sender
   - group/direct chat context
   - reply target
5. shared intake decides whether to ignore, warn, extract, or export

### Periskope Ingress Flow

1. Periskope posts to `POST /integrations/periskope/webhook`
2. route verifies signature if a signing key is configured
3. payload parser accepts both:
   - `event`
   - `event_type`
4. payload parser accepts message object under:
   - `data`
   - `current_attributes`
   - `attributes`
   - `message`
5. self-authored and private-note events are ignored
6. route resolves chat context
7. allowlist check runs via `PERISKOPE_ALLOWED_CHAT_IDS`
8. shared intake pipeline processes the message

### Shared Intake Flow

Text path:

1. classify text with regex keyword heuristics
2. if not accounting:
   - send throttled warning or ignore
3. if accounting-like text:
   - ask for photo/document

Media path:

1. send `processing` message
2. download media bytes
3. classify media with Gemini
4. if classified as non-bill:
   - send non-bill warning
5. if classified as bill:
   - extract with Gemini
   - normalize into `BillRecord`
   - persist to CSV once
   - send success confirmation

## Classifier Behavior

### Text Classification

Text classification is regex-based.

Signals for bill-like text include words like:

- fatura
- fiş
- makbuz
- toplam
- KDV
- invoice
- receipt
- tax
- amount

### Image Classification

Image/document classification uses Gemini with a classification-only prompt.

There is now a manual override for invoice-like test/sample documents:

- if Gemini rejects an image because it thinks it is a sample/template
- but the reason clearly still indicates invoice/receipt/fatura-type content
- the backend overrides the rejection and treats it as a bill-like document

This was added because a real user test invoice was labeled `ÖRNEK FATURA` and Gemini rejected it as a demo/template.

Tradeoff:

- fewer false negatives for invoice-like samples
- slight risk of additional false positives for template/demo accounting-looking documents

## Periskope Media Download Behavior

This part was the biggest live-debugging area.

Periskope media can arrive in multiple path forms:

- relative Periskope storage path
- direct absolute URL
- `storage.googleapis.com/periskope-attachments/...`

Current behavior in `app/services/providers/periskope.py`:

1. try the original URL/path
2. if the URL is a Google storage attachment URL, normalize it to a Periskope-style public storage path
3. do not send Periskope auth headers to external Google storage hosts
4. if a `401/403` occurs and a message id is available, fetch the message again from Periskope API and retry using canonical message media path

This logic was added because multiple production failures came from attachment URL access patterns.

## Periskope Tool Endpoints

These routes exist and are fully implemented:

### `create_accounting_record`

Purpose:

- allow AI or external automation to create a structured record directly

Behavior:

- builds a `BillRecord`
- infers group vs individual source context from `chat_id`
- persists record once
- returns `recorded` or `duplicate`

### `get_submission_status`

Purpose:

- query whether a specific message or chat already has exported rows

Behavior:

- accepts `source_message_id` and/or `chat_id`
- looks up rows in export CSV files

### `assign_to_human`

Purpose:

- write a private note into Periskope for manual follow-up

Behavior:

- posts to Periskope note creation endpoint

### Practical Current Status

Although these are implemented in backend, the Periskope UI the user saw showed:

- `Custom Tools`
- `Coming Soon`

So they should be treated as backend-ready but not product-ready in the current UI experience.

## Official Group Onboarding API

`app/routes/groups.py` contains official group management endpoints for Meta-like APIs.

These include:

- create group
- list groups
- get group info
- get/reset invite links
- list/approve join requests

These endpoints are part of the repo because the project originally pursued official WhatsApp group support.

Current practical status:

- code is present
- tests exist
- user is not launching through this path now

## Tests And Coverage

At handoff time the suite passed:

- `82 passed in 13.55s`

Key test files:

- `tests/test_classifier.py`
  - text/image classification behavior
  - sample invoice override
- `tests/test_extractor.py`
  - Gemini extraction normalization
- `tests/test_gemini_client.py`
  - Gemini client wrapper behavior
- `tests/test_record_store.py`
  - message claiming, persistence, lookup
- `tests/test_exports.py`
  - export endpoints / formatting behavior
- `tests/test_webhooks.py`
  - Meta webhook flow
  - groups-only gate
  - classifier and extractor control paths
- `tests/test_groups.py`
  - official group route behavior
- `tests/test_periskope.py`
  - Periskope webhook parsing
  - signature handling
  - self-message ignore
  - null media flags
  - event/event_type support
  - tool routes
  - media normalization/fallback logic
  - allowlist behavior

## Live Production Debugging History

This sequence matters because it explains why the code looks the way it does.

### Bug 1: Webhook Payload Key Mismatch

Problem:

- backend expected `event`
- Periskope sent `event_type`

Fix:

- webhook parser now accepts both

### Bug 2: Message Payload Location Mismatch

Problem:

- backend expected only `data`
- Periskope often sent `current_attributes`

Fix:

- parser now accepts multiple payload containers

### Bug 3: `has_media` Nullable

Problem:

- Periskope sometimes sent `has_media: null`
- strict schema validation rejected the message

Fix:

- `has_media` and `from_me` were relaxed to nullable-compatible behavior

### Bug 4: Media URL 401

Problem:

- attachment download via `storage.googleapis.com/periskope-attachments/...` returned `401`

Fixes:

- canonical message lookup fallback
- normalized storage path fallback
- auth headers removed from external Google storage URL requests

### Bug 5: Classifier False Negative On `ÖRNEK FATURA`

Problem:

- Gemini classified a real invoice-like sample as not-a-bill

Fix:

- override for invoice-like sample/template rejection

### Bug 6: Bot Active In All Groups

Problem:

- Periskope webhook caused backend to react in every group

Fix:

- env-driven allowlist `PERISKOPE_ALLOWED_CHAT_IDS`

## Known Caveats / Risks

### 1. Periskope Signing Key May Still Be Missing

Code supports:

- `PERISKOPE_SIGNING_KEY`

But during live debugging the Periskope UI did not clearly expose it, so the app may currently be operating without signature verification.

Impact:

- less secure webhook ingress

### 2. Periskope Custom Tools Are Not Practical Yet

Backend supports them.

UI availability did not.

Impact:

- webhook path is the real automation path
- custom tools should not be assumed usable today

### 3. Meta Path Still Exists And May Still Produce Noise

Meta routes still exist in app.

During live debugging, stale Meta credentials produced `401 Unauthorized` in logs for some direct-chat attempts.

Impact:

- not the main issue for Periskope flow
- but it can still create confusing logs if Meta remains configured

### 4. Phone Dependency

Periskope path depends on linked phone health.

Impact:

- if phone disconnects, workflow may stop even though Railway stays healthy

### 5. Docs Lag Slightly Behind Current Runtime

The repo docs do not yet fully mention:

- `PERISKOPE_ALLOWED_CHAT_IDS`
- all of the recent Periskope production fixes

This handoff file is currently the most complete context record.

## What Exists Outside The Repo

Some important system context is not versioned in code.

### Periskope AI Prompting / Knowledge Base Work

During setup, prompts/FAQ content for Periskope AI personalization were drafted manually in conversation.

These included:

- Turkish FAQ items for receipt handling
- AI role/instructions for accounting intake
- restrictions for tax/legal/compliance advice
- activation prompt rules

Important:

- this UI content is not stored in the repo
- current saved content inside Periskope may diverge from the drafts used during setup

### Periskope Chat Settings

Also not versioned in repo:

- which chats have AI enabled
- which chats have AI flagging enabled
- which chats have agent replies enabled
- any operating-hours or snooze settings in Periskope

### Railway Secrets

Also not in repo:

- real API keys
- real phone identifiers
- Gemini production key

## Current Recommended Operational Posture

For the current live setup:

- use Periskope webhook as the real runtime path
- keep backend restricted to the allowed `Test` group
- keep AI/chat-level behavior conservative to avoid duplicate replies
- keep the linked phone online and healthy
- use Railway logs as the primary debugging source

## Suggested Near-Term Improvements

These are the next logical improvements, in priority order.

### Priority 1

- document `PERISKOPE_ALLOWED_CHAT_IDS` in `.env.example`
- document `PERISKOPE_ALLOWED_CHAT_IDS` in `README.md`
- document `PERISKOPE_ALLOWED_CHAT_IDS` in `docs/setup.md`

### Priority 2

- add a small admin endpoint or config UI to manage allowlisted chats without redeploying

### Priority 3

- add stronger observability around:
  - media download retries
  - classifier false negatives
  - extraction failures
  - outbound reply failures

### Priority 4

- decide whether the Meta official groups code should remain in active scope or be treated as dormant

### Priority 5

- if Periskope exposes signing keys properly later, enable `PERISKOPE_SIGNING_KEY` in production immediately

### Priority 6

- if Periskope custom tools become available in UI, wire the existing backend endpoints into real AI tool calls

## Recommended Debugging Order If Something Breaks Again

When the system fails, check in this order:

1. Does the message appear in Periskope at all?
2. Does Railway log `POST /integrations/periskope/webhook 200 OK`?
3. Does the log say `Processing periskope message ...`?
4. Does the log show a media download failure?
5. Does the log show Gemini classification failure?
6. Does the log show Gemini extraction failure?
7. Does the log show outbound `POST https://api.periskope.app/v1/message/send` success?

Quick interpretation:

- no webhook log: Periskope webhook config issue
- webhook `ok` but no processing: parser/allowlist/self-message ignore issue
- processing but `bu muhasebe fotoğrafı değil`: classifier issue
- processing then `bir hata oldu`: download/extraction error
- outbound send failure: Periskope API key/phone/auth issue

## Final Snapshot Summary

At the moment of this handoff:

- repo is organized and clean
- `main` is pushed
- tests are green
- the working launch path is Periskope webhook ingress
- official Meta code remains but is not the active strategy
- the backend is restricted to one allowed group
- the user reported the bot now works
- the main remaining operational risks are phone connectivity, Periskope platform limitations, and some documentation drift

## Absolute Most Important Facts To Remember

- the current real integration is Periskope, not official Meta groups
- the production webhook path is `/integrations/periskope/webhook`
- the system should only run in the `Test` group via `PERISKOPE_ALLOWED_CHAT_IDS`
- Periskope custom tools exist in backend but are not the main live mechanism
- the linked phone must stay online
- this handoff file is currently the most complete single-source context summary
