from pathlib import Path

from category import Category
from statement_parser import parse
from transaction import Transaction


def test_parse() -> None:
    """Tests [parse]."""
    transactions = parse(Path(__file__).parent / "./2021_dummy.ofx")
    balance: float = 1234.56
    assert transactions == [
        Transaction(
            date="2021-12-25",
            description="DOORDASH1",
            category=Category.Unknown,
            amount=-123.45,
            id="91bdf53f4fca6363a9f576799d48c6c6",
            tags=["2021", "dummy"],
            balance=(balance + 678.90),
        ),
        Transaction(
            date="2021-01-01",
            description="SAFEWAY",
            category=Category.Unknown,
            amount=-678.90,
            id="5dba7d80fdb2fae8dc189c2eca02a2ea",
            tags=["2021", "dummy"],
            balance=balance,
        ),
    ]
