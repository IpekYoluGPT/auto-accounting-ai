#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from io import BytesIO
from urllib import error, parse, request

from PIL import Image, ImageDraw, ImageFont


def _http_json(method: str, base_url: str, path: str, *, token: str | None = None, payload: dict | None = None, query: dict | None = None) -> dict:
    url = base_url.rstrip("/") + path
    if query:
        url += "?" + parse.urlencode(query, doseq=True)

    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = request.Request(url, data=data, method=method.upper())
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")

    try:
        with request.urlopen(req, timeout=90) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed with {exc.code}: {body}") from exc


def _wait_for_health(base_url: str, timeout_seconds: int = 180) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            payload = _http_json("GET", base_url, "/health")
        except Exception:
            time.sleep(3)
            continue
        if payload.get("status") == "ok":
            return
        time.sleep(3)
    raise RuntimeError("Service did not become healthy before timeout.")


def _font(size: int):
    for candidate in ("DejaVuSans.ttf", "Arial.ttf"):
        try:
            return ImageFont.truetype(candidate, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _render_document(lines: list[str], *, width: int = 1600, height: int = 1200) -> bytes:
    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)
    title_font = _font(52)
    body_font = _font(38)

    y = 50
    for index, line in enumerate(lines):
        font = title_font if index == 0 else body_font
        draw.text((60, y), line, fill="black", font=font)
        y += 70 if index == 0 else 54

    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _invoice_png(invoice_no: str, amount: str, company: str) -> bytes:
    return _render_document([
        "SATIS FATURASI",
        f"Firma: {company}",
        f"Vergi No: 1234567890",
        f"Fatura No: {invoice_no}",
        "Tarih: 2026-04-11",
        "Saat: 14:35",
        "Urun: Hazir Beton",
        "KDVsiz: 10000.00 TRY",
        "KDV: 2000.00 TRY",
        f"GENEL TOPLAM: {amount} TRY",
        "Odeme Yontemi: Havale",
        "Aciklama: Sandbox test faturasi",
    ])


def _non_document_png() -> bytes:
    image = Image.new("RGB", (1200, 800), "#2d6a4f")
    draw = ImageDraw.Draw(image)
    draw.ellipse((120, 120, 500, 500), fill="#95d5b2")
    draw.rectangle((700, 180, 1050, 560), fill="#40916c")
    draw.text((90, 640), "BU BIR MUHASEBE BELGESI DEGIL", fill="white", font=_font(42))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _media_payload(session_id: str, message_id: str, image_bytes: bytes, filename: str) -> dict:
    return {
        "session_id": session_id,
        "message_id": message_id,
        "msg_type": "image",
        "filename": filename,
        "mime_type": "image/png",
        "media_base64": base64.b64encode(image_bytes).decode("ascii"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Railway sandbox E2E validation against the accounting service.")
    parser.add_argument("--base-url", default=os.getenv("RAILWAY_BASE_URL", "https://auto-accounting-ai-production.up.railway.app"))
    parser.add_argument("--token", default=os.getenv("PERISKOPE_TOOL_TOKEN", ""))
    parser.add_argument("--session-id", default=os.getenv("SANDBOX_SESSION_ID", "railway-e2e"))
    args = parser.parse_args()

    if not args.token:
        print("PERISKOPE_TOOL_TOKEN is required for sandbox routes.", file=sys.stderr)
        return 2

    _wait_for_health(args.base_url)

    ensure = _http_json("POST", args.base_url, "/setup/sandbox/ensure", token=args.token, payload={"session_id": args.session_id})
    session_id = ensure["session_id"]
    reset = _http_json("POST", args.base_url, "/setup/sandbox/reset", token=args.token, payload={"session_id": session_id})

    invoice_one = _invoice_png("SANDBOX-001", "12000.00", "KARAKAYA INSAAT")
    invoice_two = _invoice_png("SANDBOX-002", "8450.00", "KARAKAYA INSAAT")
    non_document = _non_document_png()

    scenarios = [
        ("invoice_one", _media_payload(session_id, "sandbox-invoice-1", invoice_one, "invoice-1.png")),
        ("invoice_two", _media_payload(session_id, "sandbox-invoice-2", invoice_two, "invoice-2.png")),
        ("invoice_duplicate", _media_payload(session_id, "sandbox-invoice-3", invoice_one, "invoice-1-dup.png")),
        ("non_document", _media_payload(session_id, "sandbox-non-document", non_document, "not-a-document.png")),
    ]

    scenario_results = []
    for name, payload in scenarios:
        result = _http_json("POST", args.base_url, "/setup/sandbox/intake", token=args.token, payload=payload)
        scenario_results.append({"name": name, "result": result})

    initial_audit = _http_json("GET", args.base_url, "/setup/sandbox/audit", token=args.token, query={"session_id": session_id, "repair": "true"})

    drift_results = []
    drifts = [
        {"action": "corrupt_total_row", "tab_name": "🧾 Faturalar"},
        {"action": "clear_hidden_row_ids", "tab_name": "🧾 Faturalar", "row_count": 5},
        {"action": "reorder_rows", "tab_name": "🧾 Faturalar", "row_count": 5},
        {"action": "rename_data_tab", "tab_name": "🧾 Faturalar", "replacement_name": "RENAMED FATURALAR"},
        {"action": "delete_summary_tab"},
    ]
    for drift in drifts:
        payload = {"session_id": session_id, **drift}
        applied = _http_json("POST", args.base_url, "/setup/sandbox/drift", token=args.token, payload=payload)
        audit_query = {
            "session_id": session_id,
            "repair": "true",
            "tab_name": applied.get("recommended_audit_tabs") or [],
        }
        audit = _http_json("GET", args.base_url, "/setup/sandbox/audit", token=args.token, query=audit_query)
        drift_results.append({"drift": applied, "audit": audit})

    final_audit = _http_json("GET", args.base_url, "/setup/sandbox/audit", token=args.token, query={"session_id": session_id, "repair": "true"})

    report = {
        "ensure": ensure,
        "reset": reset,
        "scenarios": scenario_results,
        "initial_audit": initial_audit,
        "drift_results": drift_results,
        "final_audit": final_audit,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
