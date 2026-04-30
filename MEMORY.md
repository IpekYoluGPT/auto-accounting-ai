# MEMORY.md — Hard-Won Lessons for AI Assistants

This file records bugs, root causes, and fixes discovered in production.
Read this before diagnosing any sheet/data issue.

---

## 1. "Dates reverted after I fixed them manually in the sheet"

**Root cause:** Google Sheets is a projection — not the source of truth. The projection
worker clears and rewrites every tab every 30 seconds (or on every new WhatsApp message).
Manual cell edits are overwritten unless the column is in `_AUTHORITATIVE_FIELDS`.

**Fix:** Patch the date in SQLite, not in the sheet.
- Use `POST /setup/patch-record-date` with `{"message_id": "...", "new_date": "YYYY-MM-DD"}`
- If `docs_found: 0` (record not in current SQLite), use `POST /setup/bulk-patch-dates`
  which scans ALL SQLite files on the `/data` volume including the legacy service's DB.

**Do NOT** just patch the sheet directly (`patch-sheet-date-direct`) and call it done —
that fix lasts at most 30 seconds until the next projection flush.

---

## 2. "Manual row reordering in the sheet disappeared"

**Root cause:** Same as above. Rows are always rewritten in the order:
`(document_date ASC, created_at ASC, source_doc_id ASC)` from SQLite.
Row order in the sheet cannot be preserved manually. It must be changed in code
(`list_documents()` sort key in `canonical_store.py`).

---

## 3. "patch-record-date returns docs_found: 0 even though the row exists in the sheet"

**Root cause:** Two separate issues were encountered:

1. **Records from a previous deploy** — old deploy may have had records in its SQLite.
   New deploy starts with fresh SQLite (empty). The rows in the sheet are being projected
   by the **legacy `muhasebe` service** (offline Railway service sharing the same `/data` volume).

2. **source_message_id was empty** — some old records were stored with an empty
   `source_message_id` column. Fix: SQL query uses `OR source_doc_id = ?` as a fallback.
   Both are now handled in `canonical_store.patch_document_date_by_message_id()`.

**Fix:** Use `POST /setup/bulk-patch-dates` — it scans every SQLite on `/data`, not just
the current service's DB.

---

## 4. "The sheet points to the wrong spreadsheet / Görüntüle links lead nowhere"

**Root cause:** `sheets_registry.json` maps `YYYY-MM` → spreadsheet ID. If this gets
corrupted or if a new spreadsheet was manually created without updating the registry,
the projection worker writes to the wrong sheet or the Drive links embed the wrong ID.

**Diagnosis:** `GET /setup/storage-status` → check `last_visible_flush_at` and whether
the sheet URL in the registry matches the actual production spreadsheet.

**Fix:** `POST /setup/update-sheet-registry` with `{"month": "2026-04", "spreadsheet_id": "..."}`.
Then `POST /setup/drain-queues` to force an immediate re-projection to the correct sheet.

---

## 5. "A message was processed but the row doesn't appear in the sheet"

**Checklist:**
1. Is the chat in `PERISKOPE_ALLOWED_CHAT_IDS`? Empty = reject ALL.
2. Is the message deduplicated? Check `processed_message_ids.txt`. Use
   `POST /setup/reprocess-message` to clear dedup and let the customer resend.
3. Is `canonical_store.sqlite3` empty? Run `GET /setup/debug-storage` to see all DBs
   and their document counts.
4. Is the registry pointing to the correct spreadsheet? (See #4 above.)
5. Are queues backed up? `GET /setup/storage-status` → check `pending_sheet_appends`.

---

## 6. "The legacy muhasebe service keeps overwriting the sheet"

**Background:** There are two Railway services sharing the same `/data` volume:
- `muhasebe-api` — current, online service
- `muhasebe` — old offline service; cannot be deleted (not admin)

The old service wakes up every ~3 hours and re-projects from its own SQLite
(visible in Sheet version history as SA writes every ~3h).

**Fix:** The startup migration in `migrations.py` patches ALL SQLite files under `/data`
on every boot of the new service. This patches the old service's DB too. After the migration
runs, both services read the same (correct) data.

If the old service somehow gets new wrong data, run `POST /setup/bulk-patch-dates` manually.

---

## 7. "Gemini extracted year 2020/2024 instead of 2026 on handwritten documents"

**Root cause:** Handwritten dates on delivery notes (Sevk Fişleri) are ambiguous.
Gemini reads them without date context and picks a wrong year.

**Fix implemented (PR #18):**
- Today's date injected into the extraction prompt
- Two-pass refinement: if extracted year is outside `{current_year-1, current_year, current_year+1}`,
  a second focused Gemini call re-reads the image for just the date

**The 13 wrong-date records from April 2026** were permanently fixed by the startup
migration added in PR #26. See `migrations.py._SEVK_DATE_FIXES` for the full list.

---

## 8. "Tarih (date) column edit in Sevk Fişleri tab is not persisted"

**Root cause:** `Tarih` was not in `_AUTHORITATIVE_FIELDS["Sevk Fişleri"]`, so the
5-minute override sync never captured date edits. The field was added in PR #26.

For ALL OTHER tabs, date columns are still not authoritative — manual date edits there
will be overwritten. The permanent fix for those is always SQLite.

---

## 9. "_document_month_key() uses created_at before document_date"

```python
# google_sheets.py:1295
for candidate in (
    getattr(document, "created_at", None),   # ← WhatsApp arrival time, always correct
    getattr(record, "document_date", None),  # ← Gemini-extracted date, may be wrong year
    ...
):
```

A record with wrong year (e.g. document_date=2020-04-22) still lands in the April 2026
projection scope because `created_at` is correct. This is intentional — records always
appear in the month they arrived via WhatsApp, regardless of the document's date.

**Implication:** Patching `document_date` in SQLite fixes the displayed date cell but does
NOT move the record to a different month's spreadsheet. The record stays in the month of
its `created_at`.

---

## 10. Service Account / OAuth

- **SA email:** `auto-accounting-ai@muhasebe-494114.iam.gserviceaccount.com`
- SA can **read/write** sheets but **cannot create** Google Workspace files (403)
- Sheet/folder **creation** uses OAuth2 user credentials (`GOOGLE_OAUTH_REFRESH_TOKEN`)
- If OAuth token expires, run `/setup/google-auth` flow to get a new refresh token

---

## 11. Storage Paths

| What | Path |
|------|------|
| SQLite DB | `$STORAGE_DIR/state/canonical_store.sqlite3` |
| Sheet registry | `$STORAGE_DIR/state/sheets_registry.json` |
| Inbound queue | `$STORAGE_DIR/state/inbound_queue.sqlite3` |
| CSV exports | `$STORAGE_DIR/exports/` |
| Dedup files | `$STORAGE_DIR/processed_message_ids.txt` etc. |

In production: `STORAGE_DIR=/data/storage`, Railway volume mount: `/data`.

---

## 12. Diagnosing "is the data in SQLite or not?"

```bash
# Via admin API:
curl -H "x-api-key: $TOKEN" https://auto-accounting-ai-production.up.railway.app/setup/debug-storage
# → shows every .sqlite3 found on /data with document counts

curl -H "x-api-key: $TOKEN" "https://auto-accounting-ai-production.up.railway.app/setup/lookup-record?q=3AD0FD9D"
# → searches by message_id substring
```

If `total_documents_in_db: 0` on the current service's DB but rows exist in the sheet,
**the legacy muhasebe service is the one projecting those rows.**
