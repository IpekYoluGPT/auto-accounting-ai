"""
Tests for export download endpoints.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app


def test_export_csv_returns_latest_export_file():
    with TemporaryDirectory() as tmpdir:
        export_dir = Path(tmpdir) / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        filepath = export_dir / "records_2026-04-03.csv"
        filepath.write_text("Firma Adı,Genel Toplam\nABC Market,100.0\n", encoding="utf-8-sig")

        client = TestClient(app)
        with patch("app.main.settings.storage_dir", tmpdir):
            response = client.get("/export.csv")

    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "ABC Market" in response.text


def test_export_xlsx_returns_downloadable_workbook():
    with TemporaryDirectory() as tmpdir:
        export_dir = Path(tmpdir) / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        filepath = export_dir / "records_2026-04-03.csv"
        filepath.write_text("Firma Adı,Genel Toplam\nABC Market,100.0\n", encoding="utf-8-sig")

        client = TestClient(app)
        with patch("app.main.settings.storage_dir", tmpdir):
            response = client.get("/export.xlsx")

    assert response.status_code == 200
    assert (
        response.headers["content-type"]
        == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert response.headers["content-disposition"].endswith('"records_2026-04-03.xlsx"')
    assert response.content[:2] == b"PK"


def test_export_endpoints_return_404_when_no_export_exists():
    with TemporaryDirectory() as tmpdir:
        client = TestClient(app)
        with patch("app.main.settings.storage_dir", tmpdir):
            csv_response = client.get("/export.csv")
            xlsx_response = client.get("/export.xlsx")

    assert csv_response.status_code == 404
    assert xlsx_response.status_code == 404
