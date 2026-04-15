from __future__ import annotations

import re
from typing import Iterable, Optional

_CANONICAL_UNIT_ALIASES: dict[str, tuple[str, ...]] = {
    "adet": ("AD", "ADET", "adet", "ad"),
    "torba": ("TRB", "TORBA", "torba", "trb"),
    "paket": ("PK", "PAKET", "paket", "pk"),
    "koli": ("KOLI", "KOLİ", "koli"),
    "çuval": ("CUVAL", "ÇUVAL", "çuval", "cuval"),
    "m3": ("M3", "m3", "m³", "M³"),
    "m2": ("M2", "m2", "m²", "M²"),
    "kg": ("KG", "kg"),
    "gr": ("GR", "gr", "G", "g"),
    "lt": ("LT", "lt", "L", "l"),
    "ton": ("TON", "ton"),
    "mt": ("MT", "mt", "METRE", "metre", "METR", "metr"),
    "m": ("M", "m"),
}

_COMPACT_DISPLAY_UNITS = {"m3", "m2", "m", "mt", "kg", "gr", "lt", "ton"}


def _fold_unit(value: object) -> str:
    return str(value or "").strip().casefold()


_ALIAS_TO_CANONICAL: dict[str, str] = {
    _fold_unit(alias): canonical
    for canonical, aliases in _CANONICAL_UNIT_ALIASES.items()
    for alias in aliases
}

_RAW_ALIASES: tuple[str, ...] = tuple(
    alias
    for aliases in _CANONICAL_UNIT_ALIASES.values()
    for alias in aliases
)

_SORTED_ALIASES = sorted({alias for alias in _RAW_ALIASES}, key=len, reverse=True)
_UNIT_TOKEN_PATTERN = "|".join(re.escape(alias) for alias in _SORTED_ALIASES)

QUANTITY_WITH_UNIT_TOKEN_RE = re.compile(
    rf"^\s*(?P<quantity>\d+(?:[.,]\d+)?)\s*(?P<unit>{_UNIT_TOKEN_PATTERN})\s*$",
    re.IGNORECASE,
)
LINE_ITEM_LEADING_QUANTITY_RE = re.compile(
    rf"^\s*(?P<quantity>\d+(?:[.,]\d+)?)\s*(?P<unit>{_UNIT_TOKEN_PATTERN})\b[\s:;,.\-]*(?P<rest>.+?)\s*$",
    re.IGNORECASE,
)


def canonical_unit(value: object) -> Optional[str]:
    folded = _fold_unit(value)
    if not folded:
        return None
    return _ALIAS_TO_CANONICAL.get(folded)


def display_unit(value: object, *, compact: bool = False) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    canonical = canonical_unit(raw)
    if canonical in _COMPACT_DISPLAY_UNITS:
        return canonical
    return raw


def units_share_canonical(values: Iterable[object]) -> bool:
    canonical_values = {
        canonical_unit(value)
        for value in values
        if str(value or "").strip()
    }
    canonical_values.discard(None)
    return len(canonical_values) == 1 and bool(canonical_values)
