"""
Workbook layout helpers delegated from the main Google Sheets provider module.
"""

from __future__ import annotations


def ensure_projection_workbook_layout(sheets, spreadsheet) -> None:
    sheets._ensure_projection_workbook_layout(spreadsheet)


def bootstrap_projection_tabs(sheets, spreadsheet) -> None:
    try:
        summary_ws = spreadsheet.sheet1
        summary_ws.update_title("📊 Özet")
        for tab_name in sheets._projection_target_tabs():
            headers = sheets._headers(tab_name)
            new_ws = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=len(headers) + 2)
            sheets._setup_worksheet(new_ws, tab_name, lightweight=True)
        sheets._setup_summary_tab(summary_ws, sheets._month_label(), lightweight=True)
        sheets._mark_recently_prepared(spreadsheet)
        sheets.logger.info("Bootstrapped projection tabs on new spreadsheet.")
    except Exception as exc:
        sheets.logger.warning("Could not bootstrap projection tabs: %s", exc)
