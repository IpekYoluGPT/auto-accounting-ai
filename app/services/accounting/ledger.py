
"""
Pure-Python customer ledger helpers.

The functions in this module are intentionally independent from Google Sheets
or any other persistence layer. They normalize counterparty identities,
match payments to debt parties with clear precedence, and allocate payments
using FIFO.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha1
import unicodedata
from typing import Any, Sequence

MONEY_QUANT = Decimal("0.01")

STATUS_KAPANDI = "Kapandı"
STATUS_KISMI = "Kısmi"
STATUS_ACIK = "Açık"
STATUS_FAZLA_ODEME = "Fazla Ödeme"
STATUS_ESLESMEDI = "Eşleşmedi"
STATUS_BORC_YOK = "Borç Yok"

_TURKISH_TRANSLATION = str.maketrans(
    {
        "ç": "c",
        "ğ": "g",
        "ı": "i",
        "İ": "i",
        "ö": "o",
        "ş": "s",
        "ü": "u",
        "Ç": "c",
        "Ğ": "g",
        "Ö": "o",
        "Ş": "s",
        "Ü": "u",
    }
)

_LEGAL_SUFFIX_TOKENS = {
    "a",
    "s",
    "a.s",
    "as",
    "anonim",
    "company",
    "co",
    "corp",
    "corporation",
    "group",
    "inc",
    "limited",
    "ltd",
    "ltd.",
    "ltd.sti",
    "ltdsti",
    "sanayi",
    "sirketi",
    "şirketi",
    "sti",
    "tic",
    "ticaret",
    "ve",
}

_PRIMARY_NAME_FIELDS_BY_ROLE = {
    "auto": (
        "company_name",
        "party_name",
        "recipient_name",
        "buyer_name",
        "payee_name",
        "beneficiary_name",
        "counterparty_name",
        "sender_name",
        "name",
    ),
    "debt": (
        "company_name",
        "party_name",
        "recipient_name",
        "buyer_name",
        "payee_name",
        "beneficiary_name",
        "counterparty_name",
        "sender_name",
        "name",
    ),
    "payment": (
        "recipient_name",
        "payee_name",
        "beneficiary_name",
        "company_name",
        "party_name",
        "buyer_name",
        "counterparty_name",
        "sender_name",
        "name",
    ),
}

_ALIAS_FIELDS = (
    "aliases",
    "alias",
    "alternate_names",
    "alternate_name",
    "sender_name",
    "recipient_name",
    "buyer_name",
    "payee_name",
    "beneficiary_name",
    "counterparty_name",
    "company_name",
    "party_name",
    "name",
)

_TAX_FIELDS = ("tax_number", "tax_id", "vkn", "tckn", "tax_no", "tax")
_MANUAL_KEY_FIELDS = ("manual_party_key", "party_key_override", "party_key")
_DATE_FIELDS = ("date", "document_date", "due_date", "payment_date")
_AMOUNT_FIELDS = ("amount", "total_amount", "debt_amount", "payment_amount", "balance")


def _get_value(record: Mapping[str, Any] | object, key: str) -> Any:
    if isinstance(record, Mapping):
        return record.get(key)
    return getattr(record, key, None)


def _iter_text_values(value: Any) -> Iterable[str]:
    if value is None:
        return ()
    if isinstance(value, str):
        text = value.strip()
        return (text,) if text else ()
    if isinstance(value, Mapping):
        return ()
    if isinstance(value, Iterable):
        texts: list[str] = []
        for item in value:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                texts.append(text)
        return tuple(texts)
    text = str(value).strip()
    return (text,) if text else ()


def _first_non_empty(record: Mapping[str, Any] | object, fields: Sequence[str]) -> str | None:
    for field in fields:
        value = _get_value(record, field)
        for text in _iter_text_values(value):
            if text:
                return text
    return None


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def _transliterate(value: str) -> str:
    value = value.translate(_TURKISH_TRANSLATION)
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def normalize_alias(value: str | None) -> str:
    """Return a relaxed alias form suitable for exact alias comparisons."""
    if value is None:
        return ""

    text = _normalize_whitespace(_transliterate(value).casefold())
    cleaned = [char if char.isalnum() else " " for char in text]
    return _normalize_whitespace("".join(cleaned))


def normalize_name(value: str | None) -> str:
    """Return a company/person name in canonical form."""
    alias = normalize_alias(value)
    if not alias:
        return ""

    tokens = alias.split()
    while tokens:
        if len(tokens) >= 2 and tokens[-2:] in (['a', 's'], ['ltd', 'sti']):
            tokens = tokens[:-2]
            continue
        if tokens[-1] in _LEGAL_SUFFIX_TOKENS:
            tokens.pop()
            continue
        break

    if not tokens:
        return alias

    return " ".join(tokens)


def normalize_tax_number(value: str | None) -> str:
    if value is None:
        return ""
    return "".join(char for char in str(value) if char.isdigit())


def _normalize_date_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return ""

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text


def _money(value: Any) -> Decimal:
    if value is None:
        return Decimal("0.00")
    if isinstance(value, Decimal):
        amount = value
    elif isinstance(value, int):
        amount = Decimal(value)
    elif isinstance(value, float):
        amount = Decimal(str(value))
    else:
        text = str(value).strip().replace(" ", "")
        if not text:
            return Decimal("0.00")
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text:
            text = text.replace(".", "").replace(",", ".")
        amount = Decimal(text)
    return amount.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


def _sort_key(record: Mapping[str, Any] | object, index: int) -> tuple[int, str, int]:
    date_value = _normalize_date_value(_first_non_empty(record, _DATE_FIELDS))
    if date_value:
        return (0, date_value, index)
    return (1, "", index)


def _manual_party_key(record: Mapping[str, Any] | object) -> str | None:
    value = _first_non_empty(record, _MANUAL_KEY_FIELDS)
    if value:
        return value.strip()
    return None


def _extract_primary_name(record: Mapping[str, Any] | object, *, role: str = "auto") -> str | None:
    fields = _PRIMARY_NAME_FIELDS_BY_ROLE.get(role, _PRIMARY_NAME_FIELDS_BY_ROLE["auto"])
    value = _first_non_empty(record, fields)
    return value.strip() if value else None


def _iter_alias_candidates(record: Mapping[str, Any] | object, *, role: str = "auto") -> tuple[str, ...]:
    aliases: list[str] = []
    manual_key = _manual_party_key(record)
    manual_normalized = normalize_alias(manual_key) if manual_key else ""

    for field in _ALIAS_FIELDS:
        value = _get_value(record, field)
        for text in _iter_text_values(value):
            alias = normalize_alias(text)
            if not alias:
                continue
            if manual_normalized and alias == manual_normalized:
                continue
            if alias not in aliases:
                aliases.append(alias)

    return tuple(aliases)


def derive_party_key(
    record: Mapping[str, Any] | object,
    *,
    role: str = "auto",
) -> str:
    """Derive a stable party key from record fields."""

    manual_key = _manual_party_key(record)
    if manual_key:
        return manual_key

    tax_number = normalize_tax_number(_first_non_empty(record, _TAX_FIELDS))
    if tax_number:
        return f"tax:{tax_number}"

    primary_name = normalize_name(_extract_primary_name(record, role=role))
    if primary_name:
        return f"name:{primary_name}"

    aliases = _iter_alias_candidates(record, role=role)
    if aliases:
        return f"alias:{aliases[0]}"

    signature_fields = (
        "source_message_id",
        "document_number",
        "invoice_number",
        "receipt_number",
        "document_date",
        "document_time",
        "total_amount",
        "description",
        "notes",
    )
    signature_parts = []
    for field in signature_fields:
        value = _get_value(record, field)
        if value is None:
            continue
        if field == "total_amount":
            signature_parts.append(f"{field}={_money(value)}")
        elif field == "document_date":
            signature_parts.append(f"{field}={_normalize_date_value(value)}")
        else:
            signature_parts.append(f"{field}={normalize_alias(str(value))}")

    signature = "|".join(signature_parts) or repr(sorted(signature_fields))
    digest = sha1(signature.encode("utf-8")).hexdigest()[:12]
    return f"record:{digest}"


def _extract_tax_number(record: Mapping[str, Any] | object) -> str:
    return normalize_tax_number(_first_non_empty(record, _TAX_FIELDS))


@dataclass(frozen=True)
class PartyProfile:
    party_key: str
    display_name: str | None
    tax_number: str | None
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class PartyMatch:
    party_key: str | None
    matched_by: str
    display_name: str | None = None
    tax_number: str | None = None


@dataclass(frozen=True)
class AllocationLink:
    debt_row_id: str
    payment_row_id: str
    party_key: str
    amount: Decimal


@dataclass(frozen=True)
class DebtAllocation:
    row_id: str
    party_key: str
    original_amount: Decimal
    allocated_amount: Decimal
    remaining_amount: Decimal
    status: str


@dataclass(frozen=True)
class PaymentAllocation:
    row_id: str
    matched_party_key: str | None
    matched_by: str
    original_amount: Decimal
    allocated_amount: Decimal
    remaining_amount: Decimal
    status: str


@dataclass(frozen=True)
class PartySummary:
    party_key: str
    display_name: str | None
    tax_number: str | None
    total_debt: Decimal
    allocated_debt: Decimal
    remaining_debt: Decimal
    total_payment: Decimal
    excess_payment: Decimal
    status: str


@dataclass(frozen=True)
class LedgerAllocation:
    debt_rows: tuple[DebtAllocation, ...]
    payment_rows: tuple[PaymentAllocation, ...]
    links: tuple[AllocationLink, ...]
    party_summaries: tuple[PartySummary, ...]


def _build_party_profiles(debt_rows: Sequence[Mapping[str, Any] | object]) -> dict[str, PartyProfile]:
    profiles: dict[str, dict[str, Any]] = {}
    for record in debt_rows:
        party_key = derive_party_key(record, role="debt")
        tax_number = _extract_tax_number(record) or None
        display_name = normalize_name(_extract_primary_name(record, role="debt")) or None
        aliases = _iter_alias_candidates(record, role="debt")

        profile = profiles.setdefault(
            party_key,
            {
                "party_key": party_key,
                "display_name": display_name,
                "tax_number": tax_number,
                "aliases": set(),
            },
        )

        if profile["display_name"] is None and display_name:
            profile["display_name"] = display_name
        if profile["tax_number"] is None and tax_number:
            profile["tax_number"] = tax_number
        profile["aliases"].update(aliases)

    return {
        party_key: PartyProfile(
            party_key=party_key,
            display_name=profile["display_name"],
            tax_number=profile["tax_number"],
            aliases=tuple(sorted(profile["aliases"])),
        )
        for party_key, profile in profiles.items()
    }


def _index_party_fields(profiles: Mapping[str, PartyProfile]) -> tuple[
    dict[str, str],
    dict[str, str],
    dict[str, str],
]:
    tax_index: dict[str, str] = {}
    name_index: dict[str, str] = {}
    alias_candidates: dict[str, set[str]] = {}

    for profile in profiles.values():
        if profile.tax_number:
            tax_index.setdefault(profile.tax_number, profile.party_key)
        if profile.display_name:
            name_index.setdefault(profile.display_name, profile.party_key)
        for alias in profile.aliases:
            alias_candidates.setdefault(alias, set()).add(profile.party_key)

    alias_index = {
        alias: next(iter(party_keys))
        for alias, party_keys in alias_candidates.items()
        if len(party_keys) == 1
    }
    return tax_index, name_index, alias_index


def match_payment_party(
    payment_record: Mapping[str, Any] | object,
    debt_rows: Sequence[Mapping[str, Any] | object],
) -> PartyMatch:
    """Match one payment record to an existing debt party."""

    profiles = _build_party_profiles(debt_rows)
    tax_index, name_index, alias_index = _index_party_fields(profiles)

    manual_key = _manual_party_key(payment_record)
    if manual_key:
        profile = profiles.get(manual_key)
        return PartyMatch(
            party_key=manual_key,
            matched_by="manual_override",
            display_name=profile.display_name if profile else None,
            tax_number=profile.tax_number if profile else None,
        )

    payment_tax_number = _extract_tax_number(payment_record)
    if payment_tax_number and payment_tax_number in tax_index:
        party_key = tax_index[payment_tax_number]
        profile = profiles[party_key]
        return PartyMatch(
            party_key=party_key,
            matched_by="tax_number",
            display_name=profile.display_name,
            tax_number=profile.tax_number,
        )

    payment_name = normalize_name(_extract_primary_name(payment_record, role="payment"))
    if payment_name and payment_name in name_index:
        party_key = name_index[payment_name]
        profile = profiles[party_key]
        return PartyMatch(
            party_key=party_key,
            matched_by="normalized_name",
            display_name=profile.display_name,
            tax_number=profile.tax_number,
        )

    for alias in _iter_alias_candidates(payment_record, role="payment"):
        party_key = alias_index.get(alias)
        if party_key:
            profile = profiles[party_key]
            return PartyMatch(
                party_key=party_key,
                matched_by="unique_alias",
                display_name=profile.display_name,
                tax_number=profile.tax_number,
            )

    return PartyMatch(party_key=None, matched_by=STATUS_ESLESMEDI)


def _row_id(record: Mapping[str, Any] | object, index: int) -> str:
    value = _first_non_empty(record, ("row_id", "id", "line_id", "source_message_id", "document_number"))
    if value:
        return value
    return f"row-{index + 1}"


def _status_for_debt(total: Decimal, allocated: Decimal) -> str:
    if total <= Decimal("0.00"):
        return STATUS_BORC_YOK
    if allocated <= Decimal("0.00"):
        return STATUS_ACIK
    if allocated < total:
        return STATUS_KISMI
    return STATUS_KAPANDI


def _status_for_party_summary(
    *,
    total_debt: Decimal,
    allocated_debt: Decimal,
    total_payment: Decimal,
    matched: bool,
) -> str:
    remaining_debt = max(total_debt - allocated_debt, Decimal("0.00"))
    excess_payment = max(total_payment - allocated_debt, Decimal("0.00"))

    if not matched and total_debt <= Decimal("0.00"):
        return STATUS_ESLESMEDI
    if total_debt <= Decimal("0.00"):
        return STATUS_BORC_YOK
    if excess_payment > Decimal("0.00") and remaining_debt <= Decimal("0.00"):
        return STATUS_FAZLA_ODEME
    if remaining_debt <= Decimal("0.00"):
        return STATUS_KAPANDI
    if allocated_debt <= Decimal("0.00"):
        return STATUS_ACIK
    return STATUS_KISMI


def allocate_fifo(
    debt_rows: Sequence[Mapping[str, Any] | object],
    payment_rows: Sequence[Mapping[str, Any] | object],
) -> LedgerAllocation:
    """Allocate payments against debts in FIFO order."""

    profiles = _build_party_profiles(debt_rows)

    normalized_debts: list[dict[str, Any]] = []
    for index, record in enumerate(debt_rows):
        party_key = derive_party_key(record, role="debt")
        normalized_debts.append(
            {
                "record": record,
                "row_id": _row_id(record, index),
                "party_key": party_key,
                "amount": _money(_first_non_empty(record, _AMOUNT_FIELDS)),
                "sort_key": _sort_key(record, index),
            }
        )

    normalized_debts.sort(key=lambda item: (item["party_key"], item["sort_key"]))

    debt_remaining: dict[str, Decimal] = {
        item["row_id"]: item["amount"] for item in normalized_debts
    }
    debt_allocated: dict[str, Decimal] = {
        item["row_id"]: Decimal("0.00") for item in normalized_debts
    }
    debt_order_by_party: dict[str, list[dict[str, Any]]] = {}
    for item in normalized_debts:
        debt_order_by_party.setdefault(item["party_key"], []).append(item)

    payment_matches: list[PartyMatch] = []
    payment_links: list[AllocationLink] = []
    payment_allocated: dict[str, Decimal] = {}
    payment_remaining: dict[str, Decimal] = {}
    payment_statuses: dict[str, str] = {}

    for index, record in enumerate(payment_rows):
        row_id = _row_id(record, index)
        amount = _money(_first_non_empty(record, _AMOUNT_FIELDS))
        match = match_payment_party(record, debt_rows)
        payment_matches.append(match)

        allocated = Decimal("0.00")
        remaining_payment = amount

        if match.party_key and match.party_key in debt_order_by_party:
            for debt in debt_order_by_party[match.party_key]:
                if remaining_payment <= Decimal("0.00"):
                    break
                debt_row_id = debt["row_id"]
                debt_open = debt_remaining[debt_row_id]
                if debt_open <= Decimal("0.00"):
                    continue
                applied = min(debt_open, remaining_payment)
                if applied <= Decimal("0.00"):
                    continue
                debt_remaining[debt_row_id] = (debt_open - applied).quantize(MONEY_QUANT)
                debt_allocated[debt_row_id] = (debt_allocated[debt_row_id] + applied).quantize(MONEY_QUANT)
                remaining_payment = (remaining_payment - applied).quantize(MONEY_QUANT)
                allocated = (allocated + applied).quantize(MONEY_QUANT)
                payment_links.append(
                    AllocationLink(
                        debt_row_id=debt_row_id,
                        payment_row_id=row_id,
                        party_key=match.party_key,
                        amount=applied,
                    )
                )

        payment_allocated[row_id] = allocated
        payment_remaining[row_id] = remaining_payment

        if match.party_key is None:
            payment_statuses[row_id] = STATUS_ESLESMEDI
        elif match.party_key not in debt_order_by_party:
            payment_statuses[row_id] = STATUS_BORC_YOK
        elif remaining_payment > Decimal("0.00") and allocated > Decimal("0.00"):
            payment_statuses[row_id] = STATUS_FAZLA_ODEME
        elif allocated <= Decimal("0.00"):
            payment_statuses[row_id] = STATUS_ACIK
        elif remaining_payment <= Decimal("0.00"):
            party_open_debt = sum(
                debt_remaining[item["row_id"]] for item in debt_order_by_party[match.party_key]
            )
            payment_statuses[row_id] = STATUS_KAPANDI if party_open_debt <= Decimal("0.00") else STATUS_KISMI
        else:
            payment_statuses[row_id] = STATUS_KISMI

    debt_allocations = tuple(
        DebtAllocation(
            row_id=item["row_id"],
            party_key=item["party_key"],
            original_amount=item["amount"],
            allocated_amount=debt_allocated[item["row_id"]],
            remaining_amount=debt_remaining[item["row_id"]],
            status=_status_for_debt(item["amount"], debt_allocated[item["row_id"]]),
        )
        for item in normalized_debts
    )

    payment_allocations = tuple(
        PaymentAllocation(
            row_id=_row_id(record, index),
            matched_party_key=payment_matches[index].party_key,
            matched_by=payment_matches[index].matched_by,
            original_amount=_money(_first_non_empty(record, _AMOUNT_FIELDS)),
            allocated_amount=payment_allocated[_row_id(record, index)],
            remaining_amount=payment_remaining[_row_id(record, index)],
            status=payment_statuses[_row_id(record, index)],
        )
        for index, record in enumerate(payment_rows)
    )

    party_summaries: list[PartySummary] = []
    for party_key, profile in profiles.items():
        debt_rows_for_party = debt_order_by_party.get(party_key, [])
        total_debt = sum((item["amount"] for item in debt_rows_for_party), Decimal("0.00"))
        allocated_debt = sum((debt_allocated[item["row_id"]] for item in debt_rows_for_party), Decimal("0.00"))
        total_payment = sum(
            _money(_first_non_empty(record, _AMOUNT_FIELDS))
            for index, record in enumerate(payment_rows)
            if payment_matches[index].party_key == party_key
        )
        remaining_debt = max(total_debt - allocated_debt, Decimal("0.00"))
        excess_payment = max(total_payment - allocated_debt, Decimal("0.00"))
        matched = any(match.party_key == party_key for match in payment_matches)
        party_summaries.append(
            PartySummary(
                party_key=party_key,
                display_name=profile.display_name,
                tax_number=profile.tax_number,
                total_debt=total_debt,
                allocated_debt=allocated_debt,
                remaining_debt=remaining_debt,
                total_payment=total_payment,
                excess_payment=excess_payment,
                status=_status_for_party_summary(
                    total_debt=total_debt,
                    allocated_debt=allocated_debt,
                    total_payment=total_payment,
                    matched=matched,
                ),
            )
        )

    return LedgerAllocation(
        debt_rows=debt_allocations,
        payment_rows=payment_allocations,
        links=tuple(payment_links),
        party_summaries=tuple(sorted(party_summaries, key=lambda item: item.party_key)),
    )
