"""
Smoke test the official WhatsApp Groups API using the configured app settings.

Read-only mode:
    ./.venv-codex/Scripts/python.exe scripts/whatsapp_groups_smoke_test.py --read-only

Create a labeled test group and fetch its invite link:
    ./.venv-codex/Scripts/python.exe scripts/whatsapp_groups_smoke_test.py --create
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import settings
from app.services import whatsapp


def _pretty(data: object) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)


def _require_credentials() -> None:
    missing: list[str] = []
    if not settings.whatsapp_access_token:
        missing.append("WHATSAPP_ACCESS_TOKEN")
    if not settings.whatsapp_phone_number_id:
        missing.append("WHATSAPP_PHONE_NUMBER_ID")

    if missing:
        names = ", ".join(missing)
        raise SystemExit(f"Missing required environment variables: {names}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="Only call list_groups and skip group creation.",
    )
    parser.add_argument(
        "--create",
        action="store_true",
        help="Create a labeled smoke-test group and fetch its metadata/invite link.",
    )
    parser.add_argument(
        "--subject",
        default="Codex Smoke Test",
        help="Base subject to use when creating a test group.",
    )
    args = parser.parse_args()

    if not args.read_only and not args.create:
        parser.error("Choose at least one of --read-only or --create.")

    _require_credentials()

    print("== list_groups(limit=5) ==")
    groups = whatsapp.list_groups(limit=5)
    print(_pretty(groups))

    if not args.create:
        return 0

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    subject = f"{args.subject} {timestamp}"
    description = "Temporary group created by Codex smoke test."

    print("\n== create_group ==")
    created = whatsapp.create_group(
        subject=subject,
        description=description,
        join_approval_mode="auto_approve",
    )
    print(_pretty(created))

    group_id = created.get("group_id") or created.get("id")
    if not isinstance(group_id, str) or not group_id:
        raise SystemExit("Create response did not include a group ID.")

    print("\n== get_group_info ==")
    info = whatsapp.get_group_info(
        group_id,
        fields=(
            "subject,description,participants,join_approval_mode,"
            "total_participant_count,suspended,creation_timestamp"
        ),
    )
    print(_pretty(info))

    print("\n== get_group_invite_link ==")
    invite = whatsapp.get_group_invite_link(group_id)
    print(_pretty(invite))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
