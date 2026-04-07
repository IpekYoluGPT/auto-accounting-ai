# Context Handoff — auto-accounting-ai

**Date:** 2026-04-07
**Commit:** `4cd2b6f` (main)
**Tests:** 88 passed
**Deploy:** Railway auto-deploy from GitHub main

---

## 1. What This System Does

WhatsApp accounting automation for a Turkish small business:

1. Documents (fatura, fis, cek, dekont) arrive in a WhatsApp group
2. Periskope forwards them as webhooks to our FastAPI backend on Railway
3. Gemini AI classifies, categorizes, and extracts structured fields
4. Data is written to monthly Google Sheets + daily CSV backup
5. Confirmation messages are sent back to WhatsApp

---

## 2. Current Working State

Everything below is **live and working in production:**

- Periskope webhook ingress (primary path)
- Gemini 2.5-flash for classification + extraction
- Multi-document detection (e.g. 3 cheques in one photo → 3 separate records)
- OAuth2 monthly spreadsheet auto-creation
- Service account read/write to shared spreadsheets
- 429 rate limit retry with exponential backoff
- Duplicate spreadsheet prevention (Drive search before creation)
- Chat allowlist (empty = reject all)
- Manager elden odeme text extraction
- Document categories: fatura, dekont, harcama_fisi, cek, elden_odeme, malzeme, iade

---

## 3. Key Infrastructure

| Component | Details |
|-----------|---------|
| **Backend** | FastAPI on Railway (`https://auto-accounting-ai-production.up.railway.app`) |
| **AI** | Gemini 2.5-flash (classifier + extractor) |
| **Sheets** | OAuth2 for creation, service account for read/write |
| **WhatsApp** | Periskope webhook → Periskope API for replies |
| **SA Email** | `whatsappsheet@whatsapp-account-manager-ai.iam.gserviceaccount.com` |
| **Owner** | `yilmazatakan4423@gmail.com` |
| **GCP Project** | whatsapp-account-manager-ai |

---

## 4. Monthly Spreadsheet Flow

Each month auto-creates on first document arrival:

```
GOOGLE_DRIVE_PARENT_FOLDER_ID/
└── Belgeler — Nisan 2026/          (subfolder, auto-created)
    └── Muhasebe — Nisan 2026       (spreadsheet, auto-created)
        ├── Ozet                    (summary with SUM formulas)
        ├── Faturalar
        ├── Dekontlar
        ├── Harcama Fisleri
        ├── Cekler
        ├── Elden Odemeler
        ├── Malzeme
        └── Iadeler
```

**Creation chain:** OAuth credentials create file → share with service account → service account handles all subsequent writes.

**Registry:** `storage/state/sheets_registry.json` maps `"2026-04"` → spreadsheet ID.

---

## 5. Multi-Document Extraction

- `extract_bills()` asks Gemini to detect ALL documents in an image
- Returns `list[BillRecord]` via `AIMultiExtractionResult` schema
- Sub-indexed message IDs for dedup: `msg_123__doc1`, `msg_123__doc2`
- Single document images work identically (list of 1)
- `extract_bill()` backward-compat wrapper returns first record only

---

## 6. Allowed Chat IDs

```
PERISKOPE_ALLOWED_CHAT_IDS=120363423064785066@g.us,120363045948478087@g.us,120363410789660631@g.us
```

Empty value = reject ALL chats (safety default).

---

## 7. Rate Limit Protection

- **Google Sheets 429:** `_retry_on_rate_limit()` with exponential backoff (5s, 10s, 20s, 40s, 80s)
- **Gemini API:** tenacity retry (5 attempts, exponential 2-30s)
- **Duplicate spreadsheets:** `_find_existing_spreadsheet_in_drive()` searches before creating

---

## 8. Known Limitations

1. **Gemini fallback model:** `gemini-1.5-flash` returns 404 in v1beta — fallback doesn't work, primary model retries handle most failures
2. **PERISKOPE_SIGNING_KEY:** not configured in production (accepts unsigned webhooks)
3. **Phone dependency:** Periskope requires linked phone to stay online
4. **Google OAuth token:** If GCP app is in "Testing" mode, refresh token expires in 7 days. Must be "In production" or "Internal".

---

## 9. Project Evolution

1. **Phase 1:** Meta Cloud API + Gemini extraction + CSV export
2. **Phase 2:** Group-aware Meta support + groups-only mode
3. **Phase 3:** Official Meta group onboarding API
4. **Phase 4:** Meta groups blocked (Graph API 131215) → pivot decision
5. **Phase 5:** Periskope integration (webhook + media + replies)
6. **Phase 6:** Production debugging (event_type, has_media, media 401, ORNEK FATURA, allowlist)
7. **Phase 7:** Google Sheets integration (service account)
8. **Phase 8:** OAuth2 for monthly spreadsheet creation (service accounts can't create files)
9. **Phase 9:** Multi-document extraction + 429 retry + duplicate prevention

---

## 10. Debugging Checklist

When something fails, check in this order:

1. Does Railway log show `POST /integrations/periskope/webhook 200 OK`?
2. Is the chat_id in the allowed list?
3. Does it say `Processing periskope message ...`?
4. Media download success?
5. Gemini classification result? (`is_bill=True/False`)
6. Gemini extraction result? (`N document(s) found`)
7. Google Sheets append success? (check for 429 retry logs)
8. Outbound `POST https://api.periskope.app/v1/message/send` success?
