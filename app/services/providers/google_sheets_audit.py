"""
Audit and repair helpers delegated from the main Google Sheets provider module.
"""

from __future__ import annotations


def force_rewrite_drive_links(sheets, *, spreadsheet_id=None, target_tabs=None) -> dict[str, int]:
    client = sheets._get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable.")

    with sheets._lock:
        sh = sheets._open_spreadsheet_by_key(client, spreadsheet_id) if spreadsheet_id else sheets._get_or_create_spreadsheet(client)
        resolved_tabs = []
        if target_tabs:
            for tab_name in target_tabs:
                canonical = sheets._canonical_tab_name(tab_name)
                if canonical not in resolved_tabs:
                    resolved_tabs.append(canonical)
        else:
            for tab_name in sheets._TABS:
                if sheets._header_index(tab_name, sheets._VISIBLE_DRIVE_LINK_HEADER) is None:
                    continue
                if sheets._tab_spec(tab_name).hidden_tab:
                    continue
                resolved_tabs.append(tab_name)

        raw_drive_links = sheets._raw_document_drive_link_map(sh)
        repaired_by_tab: dict[str, int] = {}
        for tab_name in resolved_tabs:
            if sheets._header_index(tab_name, sheets._VISIBLE_DRIVE_LINK_HEADER) is None:
                repaired_by_tab[tab_name] = 0
                continue

            ws = sheets._ensure_tab_exists(sh, tab_name, lightweight=True)
            row_formulas: list[tuple[int, str]] = []
            for row_number, row_map in sheets._iter_visible_row_maps(ws, tab_name, value_render_option="FORMULA"):
                url = sheets._extract_drive_link_from_cell_value(row_map.get(sheets._VISIBLE_DRIVE_LINK_HEADER))
                if not url:
                    source_doc_id = sheets._coalesce_text(row_map.get(sheets._HIDDEN_SOURCE_DOC_ID_HEADER))
                    url = raw_drive_links.get(source_doc_id, "")
                if not url:
                    continue
                row_formulas.append((row_number, sheets._drive_cell(url, spreadsheet=ws.spreadsheet)))

            repaired = sheets._rewrite_drive_cells(ws, tab_name, row_formulas)
            repaired_by_tab[tab_name] = repaired
            if repaired:
                sheets.logger.info("Force-rewrote %d Drive link cell(s) on '%s'.", repaired, tab_name)

        return repaired_by_tab


def hide_nonvisible_tabs(sheets, *, spreadsheet_id=None) -> dict[str, int]:
    client = sheets._get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable.")

    with sheets._lock:
        sh = sheets._open_spreadsheet_by_key(client, spreadsheet_id) if spreadsheet_id else sheets._get_or_create_spreadsheet(client)
        hidden_tabs: dict[str, int] = {}
        for ws in sheets._list_worksheets(sh):
            title = str(ws.title)
            canonical_title = sheets._canonical_tab_name(title)
            should_hide = canonical_title not in sheets._VISIBLE_TABS
            if should_hide:
                sheets._set_worksheet_hidden(ws, hidden=True)
                hidden_tabs[title] = hidden_tabs.get(title, 0) + 1
        return hidden_tabs


def audit_current_month_spreadsheet(
    sheets,
    *,
    spreadsheet_id=None,
    repair: bool = False,
    target_tabs=None,
    refresh_formatting: bool = False,
) -> dict[str, object]:
    client = sheets._get_client()
    if client is None:
        raise RuntimeError("Google Sheets client unavailable.")

    with sheets._lock:
        sh = sheets._open_spreadsheet_by_key(client, spreadsheet_id) if spreadsheet_id else sheets._get_or_create_spreadsheet(client)
        findings = sheets._audit_spreadsheet_layout(
            sh,
            repair=repair,
            target_tabs=target_tabs,
            refresh_formatting=refresh_formatting,
        )
        return {
            "spreadsheet_id": sh.id,
            "month_key": sheets._month_key(),
            "findings": findings,
            "queue": sheets.queue_status(),
        }
