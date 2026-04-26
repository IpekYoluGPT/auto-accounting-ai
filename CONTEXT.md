# Context Handoff — Cheque Lehdar Extraction Fix

**Date:** 2026-04-26
**Branch:** `claude/fix-lehdar-extraction-SqOIJ`
**PR:** [#13](https://github.com/IpekYoluGPT/2026-auto-accounting-ai/pull/13)
**Tests:** 289 passing (`python -m pytest tests/ -q`)

---

## 1. Problem

In production cheque sheets, the **Lehdar** (payee) column was empty (`-`) for most
rows even though the same images extracted the Lehdar correctly in silent/sandbox tests.
User screenshot showed many cheques with `Çek No`, `Çeki Düzenleyen`, `Banka` populated
but `Lehdar` blank.

## 2. Root Cause

Commit `186b34a` ("Improve cheque lehdar extraction") added a Document AI–based OCR hint
to the cheque extraction prompt. The hint is built only when Google Document AI is
configured AND succeeds (`app/services/accounting/gemini_extractor.py` `_build_ocr_hint`).
- **In sandbox/tests:** the OCR call is mocked to return useful text → Gemini extracts
  Lehdar reliably.
- **In production:** Document AI is not configured (or fails), so no hint is added.
  Gemini's vision-only pass often returns `null` for handwritten payees → cells fall
  through to the `-` placeholder in `_placeholder_missing_visible_values`.

## 3. Fix Implemented

Three commits on the branch:

### `3e14f4e` — Refine missing cheque lehdar names
- Added `_refine_missing_cheque_lehdars()` second pass in
  `app/services/accounting/gemini_extractor.py`. Triggered only when at least one cheque
  record in the batch has a blank `recipient_name`.
- New Pydantic schemas `_ChequeLehdarEntry` / `_ChequeLehdarRefinement` (lehdar list).
- Strengthened the primary cheque prompt: `recipient_name` is now `ZORUNLU` (required)
  with explicit instructions to return a best-effort partial reading rather than blank.
- Refinement is wrapped in try/except — any failure (API error, malformed payload) logs
  a warning and leaves records untouched.
- 3 new tests covering: refinement fills only blanks, refinement skipped when all
  lehdars present, refinement failure preserves originals.

### `2a8b315` — Run lehdar refinement on faster Flash model
- Added `gemini_lehdar_refinement_model` setting in `app/config.py`.
- The refinement call now runs on the Flash model instead of the Pro extractor model.

### `88ecdac` — Fix lehdar refinement model name to gemini-3.1-flash-lite-preview
- First default `gemini-3.1-flash-preview` returned 404 NOT_FOUND in production.
- Corrected default to `gemini-3.1-flash-lite-preview` (per user).

## 4. Outstanding Issue (UNRESOLVED)

**The user reports the pipeline is still slow even after switching to Flash Lite.**
They shared a log file at:
```
/root/.claude/uploads/394384d6-ad2c-47e0-b502-fc48a316294c/019dc6e4-logs.1777158346376.log.txt
```
The file is 33,644 tokens (>25k limit) — needs to be read in chunks via `Read` with
`offset`/`limit`, or grep'd via Bash.

**Investigation TODO for next session:**
1. Read the log in chunks; look for timing patterns (`HTTP Request: POST` lines for
   Gemini calls, `Sending media` log lines, intake start/end markers).
2. Suspects to verify:
   - Is `_build_ocr_hint` blocking on a slow Document AI call before the primary Gemini
     extraction even starts? (`ocr.prepare_document` → `google_document_ai.process_document`)
   - Are images being processed sequentially when several arrive in a short window?
     (`_gemini_call_semaphore` size = `settings.gemini_max_concurrency`)
   - Is Gemini Pro itself slow on cheque images regardless of refinement?
   - Periskope retries / webhook re-deliveries inflating apparent latency?
3. If the primary Pro call is the bottleneck (not the refinement), consider:
   - Moving the *primary* extractor to Flash Lite for cheques specifically.
   - Returning the Periskope webhook ack immediately and processing async.
   - Caching Document AI results so repeated images skip OCR.

## 5. Key Files Touched

| File | Purpose |
|------|---------|
| `app/services/accounting/gemini_extractor.py` | Primary + refinement extraction logic, prompts, OCR hint |
| `app/config.py` | New `gemini_lehdar_refinement_model` setting |
| `tests/test_extractor.py` | 3 new refinement tests + tightened existing cheque tests |
| `CLAUDE.md` | Added env var row + "Cheque Lehdar Two-Pass Extraction" pattern note |

## 6. How to Resume

```bash
git checkout claude/fix-lehdar-extraction-SqOIJ
python -m pytest tests/ -q          # 289 passing baseline

# Inspect the slow-path log
grep -E "HTTP Request|Sending media|Extraction complete|Lehdar refinement" \
  /root/.claude/uploads/394384d6-ad2c-47e0-b502-fc48a316294c/019dc6e4-logs.1777158346376.log.txt \
  | head -100
```

PR #13 is open and ready for review/merge once the latency question is settled.
