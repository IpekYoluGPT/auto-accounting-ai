# Customer Handoff Runbook

This document defines the supported production handoff model for serving this backend to a customer while keeping the source code private.

## Ownership Model

- The customer owns the production accounts and pays the bills:
  - Railway
  - Google Cloud / Drive / Sheets
  - Periskope
- The vendor keeps the source code private and remains responsible for production deployments.
- The customer should invite the vendor to the customer-owned Railway project as a deployer or admin instead of asking for source-code transfer.

## Company Email

Use a company-owned email account, not a personal mailbox, for:

- Railway workspace ownership
- Google Cloud project ownership
- Google Drive / Google Sheets ownership
- Periskope billing and login

Recommended examples:

- `ops@company.com`
- `accounting@company.com`
- `it@company.com`

If the company email is not already a Google identity, create either:

- a Google Workspace user, or
- a Google account bound to that company email address

## Railway Production Layout

Deploy one always-on Railway service only.

- Plan: `Hobby` is enough to start
- Service type: persistent web service
- Replicas: `1`
- Healthcheck path: `/health`
- Restart policy: on failure
- Volume: attach exactly one persistent volume
- `STORAGE_DIR`: place it inside the mounted volume path, for example `/data/storage`

Important constraints for this repository:

- Queue state, CSV exports, SQLite canonical state, and Drive backfill state live under `STORAGE_DIR`
- Do not use ephemeral-only storage
- Do not enable horizontal scaling / multiple replicas
- Enable Railway volume backups

## Secrets And Environment

The minimum customer-owned production secrets are:

- `GEMINI_API_KEY`
- `PERISKOPE_API_KEY`
- `PERISKOPE_PHONE`
- `PERISKOPE_SIGNING_KEY`
- `PERISKOPE_ALLOWED_CHAT_IDS`
- `PERISKOPE_TOOL_TOKEN`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_DRIVE_PARENT_FOLDER_ID`
- `GOOGLE_SHEETS_OWNER_EMAIL`

Preferred Google ownership setup:

- `GOOGLE_SHEETS_OWNER_EMAIL` should be a company-owned Google identity
- Configure `GOOGLE_OAUTH_CLIENT_ID`
- Configure `GOOGLE_OAUTH_CLIENT_SECRET`
- Configure `GOOGLE_OAUTH_REFRESH_TOKEN`

If OAuth is configured, monthly workbook and folder creation can happen as the customer's real Google user instead of only the service account.

## Periskope Setup

Recommended starting plan:

- Periskope `Starter`
- 1 user
- 1 connected phone

Webhook target:

- `POST /integrations/periskope/webhook`

Recommended admin / tool endpoints:

- `POST /setup/drain-queues`
- `POST /setup/retry-inbound`
- `GET /setup/storage-status`

## Validation Before Go-Live

Before declaring production ready, verify all of the following:

1. `GET /health` returns `200` with `{"status":"ok"}`
2. Railway `STORAGE_DIR` is inside the mounted volume path
3. A sample image reaches:
   - inbound queue
   - Gemini classification
   - Gemini extraction
   - CSV persistence
   - canonical SQLite persistence
   - Sheets projection
   - Drive link backfill
4. Restarting the Railway service does not lose queued jobs or canonical data
5. The customer can access Railway billing, Google billing, and Periskope billing directly

Run the release preflight from the project root:

```bash
./.venv/bin/python app/delivery_smoke.py
```

## Monthly Cost Snapshot

The following is a practical estimate for a single customer as of April 2026, assuming about 300 document images per month and one operator.

- Railway Hobby with one small always-on service and one small volume: about `$6-$8/month`
- Periskope Starter with 1 user and first phone free: `$20/month`
- Google Sheets API: `$0`
- Google Drive API: `$0`
- Optional Google Workspace seat for company-owned Google identity: about `$7-$8.40/month`
- Gemini API on current `gemini-3.1-pro-preview`: about `$5-$11/month`

Expected total:

- Without a new Google Workspace seat: about `$31-$35/month`
- With one Google Workspace seat: about `$40-$48/month`

Taxes, custom domains, and premium support are separate.
