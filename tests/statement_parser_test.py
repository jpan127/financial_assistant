from pathlib import Path

from category import Category
from statement_parser import parse
from transaction import Transaction


def test_parse() -> None:
    """Tests [parse]."""
    transactions = parse(Path(__file__).parent / "./dummy.ofx")
    assert transactions == [
        Transaction(
            date="2021-12-25",
            description="DOORDASH1",
            category=Category.Unknown,
            amount=-123.45,
            id="1111111111111111111111111111111",
        ),
        Transaction(
            date="2012-01-01",
            description="SAFEWAY",
            category=Category.Unknown,
            amount=-678.90,
            id="2222222222222222222222222222222",
        ),
    ]
