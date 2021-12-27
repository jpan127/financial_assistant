from pathlib import Path
from typing import List

import pytest

import database
from category import Category
from transaction import Transaction


# Transactions used in the tests below
TRANSACTIONS = [
    Transaction(
        date="2021-01-01",
        description="DOORDASH",
        category=Category.Food,
        amount=111.11,
        id="111",
    ),
    Transaction(
        date="2021-02-02",
        description="PG&E",
        category=Category.BillsUtilities,
        amount=222.22,
        id="222",
    ),
    Transaction(
        date="2021-03-03",
        description="TODDSNYDER",
        category=Category.Shopping,
        amount=333.33,
        id="333",
    ),
    Transaction(
        date="2021-04-04",
        description="GARBAGE",
        category=Category.Unknown,
        amount=444.44,
        id="444",
    ),
]


def test_write() -> None:
    """Tests the [write] function."""

    def write_another_transaction(**kwargs) -> None:
        """Writes a transaction to the database supplied by kwargs."""
        database.write(
            **kwargs,
            transactions=[TRANSACTIONS[3]],
        )

    # Create a database with 3 transactions
    db = database.write(
        path=":memory:",
        transactions=TRANSACTIONS[:3],
    )

    # What rows we expect the database to have
    expected_transactions: List[Transaction] = []

    # Check the database rows against the expected rows
    def test() -> None:
        assert (
            database.to_transactions(db.execute("select * from transactions"))
            == expected_transactions
        )

    # After the first write, there should be 3 rows for 3 transactions
    expected_transactions = TRANSACTIONS[:3]
    test()

    # Write a 4th transaction, but to a new database, not reusing the initial database
    # Nothing changes with the current database
    write_another_transaction(path=":memory:")
    test()

    # Write a 4th transaction to the initial database
    # Expect another row
    write_another_transaction(db=db)
    expected_transactions = TRANSACTIONS[:4]
    test()

    # Write the same 4th transaction again and expect it does not get duplicatedf
    write_another_transaction(db=db)
    test()


def test_read_unknown_categories() -> None:
    """Tests the [read_unknown_categories] function."""
    # These paths do not exist yet
    with pytest.raises(RuntimeError):
        database.read_unknown_categories(path=Path(":memory:"))
    with pytest.raises(RuntimeError):
        database.read_unknown_categories(path=Path("nonexistent_database.db"))

    # Write multiple known categorized transaction, and one unknown
    db = database.write(
        path=Path(":memory:"),
        transactions=TRANSACTIONS,
    )

    # Function under test should only return the unknown
    assert database.read_unknown_categories(db=db) == [TRANSACTIONS[3]]
