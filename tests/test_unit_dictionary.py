import pytest

from app.services.accounting import unit_dictionary


@pytest.mark.parametrize(
    ("raw_unit", "expected_canonical"),
    [
        ("TRB", "torba"),
        ("TORBA", "torba"),
        ("torba", "torba"),
        ("PK", "paket"),
        ("PAKET", "paket"),
        ("M3", "m3"),
        ("m³", "m3"),
        ("m3", "m3"),
    ],
)
def test_canonical_unit_merges_aliases(raw_unit, expected_canonical):
    assert unit_dictionary.canonical_unit(raw_unit) == expected_canonical


def test_display_unit_preserves_packaging_abbreviations_but_compacts_measurements():
    assert unit_dictionary.display_unit("TRB") == "TRB"
    assert unit_dictionary.display_unit("PK") == "PK"
    assert unit_dictionary.display_unit("M3", compact=True) == "m3"
    assert unit_dictionary.display_unit("m³", compact=True) == "m3"
