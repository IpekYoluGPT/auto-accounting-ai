"""
Shared intake types and callback contracts.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol


@dataclass(frozen=True)
class MessageRoute:
    platform: Literal["meta_whatsapp", "periskope"]
    sender_id: str
    chat_id: str
    chat_type: Literal["individual", "group"]
    recipient_type: str
    sender_name: str | None = None
    group_id: str | None = None
    reply_to_message_id: str | None = None


SendTextFn = Callable[[MessageRoute, str], None]
SendReactionFn = Callable[[MessageRoute, str], None]
FetchMediaFn = Callable[[], bytes]


@dataclass(frozen=True)
class MediaProcessingResult:
    outcome: str
    exported_count: int = 0
    retryable: bool = False
    user_message: str | None = None
    stage: str = "processing"


class OutboundMessenger(Protocol):
    def send_text(self, route: MessageRoute, text: str) -> None: ...

    def send_reaction(self, route: MessageRoute, emoji: str) -> None: ...


@dataclass(frozen=True)
class MediaPayload:
    message_id: str
    route: MessageRoute
    raw_bytes: bytes
    mime_type: str
    filename: str
    source_type: str
    attachment_url: str | None = None
