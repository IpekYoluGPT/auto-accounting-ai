from __future__ import annotations

import hashlib
from types import SimpleNamespace

from app.services.accounting.intake_types import MediaPayload, MessageRoute
from app.services.accounting.media_pipeline import process_media_payload


def _route() -> MessageRoute:
    return MessageRoute(
        platform="periskope",
        sender_id="905551112233@c.us",
        sender_name="Ahmet",
        chat_id="120363410789660631@g.us",
        chat_type="group",
        recipient_type="group",
        group_id="120363410789660631@g.us",
        reply_to_message_id="peri-msg-1",
    )


def test_process_media_payload_skips_known_media_before_gemini():
    raw_bytes = b"already-seen-image"
    expected_sha = hashlib.sha256(raw_bytes).hexdigest()

    class FakeRecordStore:
        def is_media_processed(self, media_sha256: str) -> bool:
            assert media_sha256 == expected_sha
            return True

    def fail_prepare_media(*args, **kwargs):
        raise AssertionError("known duplicate media should not be sent through Gemini")

    intake_module = SimpleNamespace(
        record_store=FakeRecordStore(),
        media_prep=SimpleNamespace(prepare_media=fail_prepare_media),
        logger=SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    result = process_media_payload(
        intake_module=intake_module,
        payload=MediaPayload(
            message_id="peri-msg-1",
            route=_route(),
            raw_bytes=raw_bytes,
            mime_type="image/jpeg",
            filename="receipt.jpg",
            source_type="image",
        ),
    )

    assert result.outcome == "already_exported"
    assert result.stage == "media_dedupe"
