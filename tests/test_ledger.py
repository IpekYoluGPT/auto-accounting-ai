
from decimal import Decimal

from app.services.accounting.ledger import (
    STATUS_ACIK,
    STATUS_BORC_YOK,
    STATUS_ESLESMEDI,
    STATUS_FAZLA_ODEME,
    STATUS_KAPANDI,
    STATUS_KISMI,
    allocate_fifo,
    derive_party_key,
    match_payment_party,
    normalize_alias,
    normalize_name,
)


def _debt(**overrides):
    row = {
        "row_id": "d-1",
        "company_name": "ABC Market",
        "amount": Decimal("100.00"),
        "date": "2026-04-01",
    }
    row.update(overrides)
    return row


def _payment(**overrides):
    row = {
        "row_id": "p-1",
        "amount": Decimal("100.00"),
        "date": "2026-04-02",
    }
    row.update(overrides)
    return row


def test_normalization_handles_turkish_characters_and_suffixes():
    assert normalize_alias("Yapı Kredi Elazığ Şubesi") == "yapi kredi elazig subesi"
    assert normalize_name("Kansa Grup Gıda Ticaret ve Sanayi Ltd Şti") == "kansa grup gida"


def test_stable_party_key_prefers_tax_number_over_name():
    record = {
        "company_name": "Different Name",
        "tax_number": " 123-456-7890 ",
    }
    assert derive_party_key(record, role="debt") == "tax:1234567890"


def test_fifo_allocates_multiple_debts_oldest_first():
    debts = [
        _debt(row_id="d-1", amount=Decimal("100.00"), date="2026-04-01", company_name="ABC Market"),
        _debt(row_id="d-2", amount=Decimal("60.00"), date="2026-04-05", company_name="ABC Market"),
    ]
    payments = [
        _payment(row_id="p-1", amount=Decimal("120.00"), date="2026-04-10", recipient_name="ABC Market"),
    ]

    result = allocate_fifo(debts, payments)

    assert [row.row_id for row in result.debt_rows] == ["d-1", "d-2"]
    assert result.debt_rows[0].allocated_amount == Decimal("100.00")
    assert result.debt_rows[0].remaining_amount == Decimal("0.00")
    assert result.debt_rows[0].status == STATUS_KAPANDI
    assert result.debt_rows[1].allocated_amount == Decimal("20.00")
    assert result.debt_rows[1].remaining_amount == Decimal("40.00")
    assert result.debt_rows[1].status == STATUS_KISMI
    assert result.payment_rows[0].allocated_amount == Decimal("120.00")
    assert result.payment_rows[0].remaining_amount == Decimal("0.00")
    assert result.payment_rows[0].status == STATUS_KISMI
    assert result.party_summaries[0].status == STATUS_KISMI


def test_partial_payment_marks_debt_open_and_party_partial():
    debts = [
        _debt(row_id="d-1", amount=Decimal("100.00"), date="2026-04-01", company_name="ABC Market"),
    ]
    payments = [
        _payment(row_id="p-1", amount=Decimal("40.00"), date="2026-04-02", recipient_name="ABC Market"),
    ]

    result = allocate_fifo(debts, payments)

    assert result.debt_rows[0].status == STATUS_KISMI
    assert result.debt_rows[0].remaining_amount == Decimal("60.00")
    assert result.payment_rows[0].status == STATUS_KISMI
    assert result.party_summaries[0].status == STATUS_KISMI


def test_overpayment_marks_excess_and_fazla_odeme():
    debts = [
        _debt(row_id="d-1", amount=Decimal("100.00"), date="2026-04-01", company_name="ABC Market"),
    ]
    payments = [
        _payment(row_id="p-1", amount=Decimal("150.00"), date="2026-04-02", recipient_name="ABC Market"),
    ]

    result = allocate_fifo(debts, payments)

    assert result.debt_rows[0].status == STATUS_KAPANDI
    assert result.payment_rows[0].status == STATUS_FAZLA_ODEME
    assert result.payment_rows[0].remaining_amount == Decimal("50.00")
    assert result.party_summaries[0].status == STATUS_FAZLA_ODEME
    assert result.party_summaries[0].excess_payment == Decimal("50.00")


def test_tax_id_match_takes_precedence_over_name_and_alias():
    debts = [
        _debt(
            row_id="d-1",
            amount=Decimal("75.00"),
            date="2026-04-01",
            company_name="ABC Market",
            tax_number="1234567890",
            aliases=("Other Alias",),
        ),
        _debt(
            row_id="d-2",
            amount=Decimal("30.00"),
            date="2026-04-02",
            company_name="Different Party",
            tax_number="9999999999",
        ),
    ]
    payment = _payment(
        row_id="p-1",
        amount=Decimal("10.00"),
        tax_number="1234567890",
        recipient_name="Different Party",
    )

    match = match_payment_party(payment, debts)

    assert match.party_key == "tax:1234567890"
    assert match.matched_by == "tax_number"


def test_normalized_name_match_finds_company_name():
    debts = [
        _debt(
            row_id="d-1",
            amount=Decimal("80.00"),
            date="2026-04-01",
            company_name="Kansa Grup Gıda Ticaret ve Sanayi Ltd Şti",
        ),
    ]
    payment = _payment(
        row_id="p-1",
        amount=Decimal("80.00"),
        recipient_name="Kansa Grup Gida",
    )

    match = match_payment_party(payment, debts)

    assert match.party_key == "name:kansa grup gida"
    assert match.matched_by == "normalized_name"


def test_unique_alias_match_is_used_when_name_does_not_match():
    debts = [
        _debt(
            row_id="d-1",
            amount=Decimal("55.00"),
            date="2026-04-01",
            company_name="Main Party",
            aliases=("Yapı Kredi Elazığ Şubesi",),
        ),
    ]
    payment = _payment(
        row_id="p-1",
        amount=Decimal("55.00"),
        recipient_name="Yapı Kredi Elazığ Şubesi",
    )

    match = match_payment_party(payment, debts)

    assert match.party_key == "name:main party"
    assert match.matched_by == "unique_alias"


def test_unmatched_payment_returns_eslesmedi_and_no_allocation():
    debts = [
        _debt(row_id="d-1", amount=Decimal("100.00"), date="2026-04-01", company_name="ABC Market"),
    ]
    payments = [
        _payment(row_id="p-1", amount=Decimal("25.00"), recipient_name="Completely Different"),
    ]

    result = allocate_fifo(debts, payments)

    assert result.payment_rows[0].status == STATUS_ESLESMEDI
    assert result.payment_rows[0].matched_party_key is None
    assert result.links == ()
    assert result.party_summaries[0].status == STATUS_ACIK
    assert result.party_summaries[0].total_debt == Decimal("100.00")
    assert result.party_summaries[0].allocated_debt == Decimal("0.00")
