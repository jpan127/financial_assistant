import copy
import sqlite3
from pathlib import Path
from typing import Any, List

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
        tags=["doordash", "food"],
    ),
    Transaction(
        date="2021-02-02",
        description="PG&E",
        category=Category.BillsUtilities,
        amount=222.22,
        id="222",
        tags=["bills"],
    ),
    Transaction(
        date="2021-03-03",
        description="TODDSNYDER",
        category=Category.Shopping,
        amount=333.33,
        id="333",
        tags=["clothes"],
    ),
    Transaction(
        date="2021-04-04",
        description="GARBAGE",
        category=Category.Unknown,
        amount=444.44,
        id="444",
        tags=["bills"],
    ),
]


def check(db: sqlite3.Connection, transactions: List[Transaction]) -> None:
    assert database.to_transactions(db.execute("select * from transactions")) == transactions


def test_session() -> None:
    """Tests the [session] function."""
    write = lambda db: database.write(db=db, transactions=TRANSACTIONS)

    # Normal usages
    with database.session(":memory:") as db:
        write(db)
        check(db, TRANSACTIONS)

    db = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
    with database.session(db=db) as db:
        write(db)
        check(db, TRANSACTIONS)
    # After above context closes, the database is closed, writing to it will raise
    with pytest.raises(sqlite3.ProgrammingError):
        write(db)

    # Writing to a read only database will raise
    with database.session(":memory:", read_only=True) as db:
        with pytest.raises(sqlite3.OperationalError):
            write(db)

    # After first context closes, the database is closed, writing to it will raise
    with database.session(":memory:") as db:
        with database.session(db=db) as db:
            pass
        with pytest.raises(sqlite3.ProgrammingError):
            write(db)


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
        assert database.to_transactions(db.execute("select * from transactions")) == expected_transactions

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


def test_tag(monkeypatch: Any) -> None:
    """Tests the [tag] function."""
    response: str = ""
    monkeypatch.setattr("builtins.input", lambda _: response)

    # The original (first) transaction already has the tag (food) being added
    # Add a transaction that matches '%{transactions[0].description}'
    #  This transaction has an empty set of tags
    # Add a transaction that matches '{transactions[0].description}%'
    #  This transaction has one tag
    transactions = TRANSACTIONS.copy()
    transactions.append(copy.deepcopy(transactions[0]))
    transactions[-1].id += "1"  # Set unique ID
    transactions[-1].tags = []
    transactions[-1].description = f"ABCD {transactions[0].description}"  # Add a prefix
    transactions.append(copy.deepcopy(transactions[0]))
    transactions[-1].id += "2"  # Set unique ID
    transactions[-1].tags = ["doordash"]
    transactions[-1].description = f"{transactions[0].description} ABCD"  # Add a suffix

    expected_transactions = copy.deepcopy(transactions)

    with database.session(Path(":memory:")) as db:
        database.write(
            db=db,
            transactions=transactions,
        )

        # No matches
        database.tag(db, format_str="", tag_str="food")
        assert database.to_transactions(db.execute("select * from transactions")) == transactions

        # 3 matches (rejected)
        response = "n"
        database.tag(db, format_str="%DOORDASH%", tag_str="food")
        assert database.to_transactions(db.execute("select * from transactions")) == transactions

        # 3 matches (accepted)
        # expected_transactions[0] should not duplicate the food tag
        response = "y"
        database.tag(db, format_str="%DOORDASH%", tag_str="food")
        expected_transactions[-1].tags.append("food")
        expected_transactions[-2].tags.append("food")
        assert database.to_transactions(db.execute("select * from transactions")) == expected_transactions

        # 2 matches (accepted)
        database.tag(db, format_str="DOORDASH%", tag_str="stupid")
        expected_transactions[0].tags.append("stupid")
        expected_transactions[-1].tags.append("stupid")
        assert database.to_transactions(db.execute("select * from transactions")) == expected_transactions
