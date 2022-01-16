import csv
from pathlib import Path
from typing import Callable

from category import Category
from bank_history_parser import parse
from transaction import Transaction


def _run_n(f: Callable[[], None], n: int = 10) -> None:
    """
    Calls the function [f], [n] times for idempotency.

    Args:
        f: The callback to call.
        n: The number of times to call.
    """
    for _ in range(n):
        f()


def test_parse_csv(tmp_path: Path) -> None:
    """Tests [parse]."""
    path = tmp_path / "2021_superbank.csv"
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Date", "Description", "Amount", "Balance"])
        writer.writeheader()
        rows = (
            {"Date": "01/01/2021", "Description": "january", "Amount": 111.11, "Balance": 111.11},
            {"Date": "02/02/2021", "Description": "february", "Amount": 222.22, "Balance": 222.22},
            {"Date": "03/03/2021", "Description": "march", "Amount": 333.33, "Balance": 333.33},
        )
        for row in rows:
            # Minify multiline strings to single line with single whitespaces
            writer.writerow(row)

    expected_transactions = [
        Transaction(date="2021-01-01", description="january", category=Category.Bank, amount=111.11, id="d8e934b3abc42e14f54d897a7257870f", tags=["2021", "superbank"], balance=111.11),
        Transaction(date="2021-02-02", description="february", category=Category.Bank, amount=222.22, id="4583f54e63bd2c4bc57ad2c3bae635a3", tags=["2021", "superbank"], balance=222.22),
        Transaction(date="2021-03-03", description="march", category=Category.Bank, amount=333.33, id="3fa54c2253fbfc8536e41a82a007c158", tags=["2021", "superbank"], balance=333.33),
    ]

    # Check stability of IDs

    def test() -> None:
        transactions = parse(path)
        assert transactions == expected_transactions
    _run_n(test)


def test_parse_ofx() -> None:
    """Tests [parse]."""
    balance: float = 1234.56
    expected_transactions = [
        Transaction(
            date="2021-12-25",
            description="DOORDASH1",
            category=Category.Bank,
            amount=-123.45,
            id="64d3a77e38151d1ca35c05286ad7f43f",
            tags=["2021", "dummy"],
            balance=(balance + 678.90),
        ),
        Transaction(
            date="2021-01-01",
            description="SAFEWAY",
            category=Category.Bank,
            amount=-678.90,
            id="7f86b8c9c4a88f94116828b5381bdb68",
            tags=["2021", "dummy"],
            balance=balance,
        ),
    ]

    # Check stability of IDs
    def test() -> None:
        transactions = parse(Path(__file__).parent / "./2021_dummy.ofx")
        assert transactions == expected_transactions
    _run_n(test)
