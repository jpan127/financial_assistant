import sqlite3
import dataclasses
from pathlib import Path
from typing import List, Type, Mapping, Optional

from category import Category
from transaction import Transaction

TYPE_MAP: Mapping[Type, str] = {
    str: "TINYTEXT",
    float: "FLOAT",
    Category: "Category",
}
TRANSACTION_SCHEMA = {f.name: TYPE_MAP[f.type] for f in dataclasses.fields(Transaction)}
PRIMARY_KEY = Transaction.unique_field()
COLUMN_NAMES = ",".join(
    TRANSACTION_SCHEMA.keys()
)  # .replace("category", "category Category")
COLUMN_HEADERS = ", ".join(
    f"{name} {type}" for name, type in TRANSACTION_SCHEMA.items()
).replace(
    f"{PRIMARY_KEY} {TRANSACTION_SCHEMA[PRIMARY_KEY]}",
    f"{PRIMARY_KEY} {TRANSACTION_SCHEMA[PRIMARY_KEY]} primary key",
)
VALUES_FORMAT_STRING = ",".join(["?"] * len(TRANSACTION_SCHEMA))


def write(
    transactions: List[Transaction],
    path: Optional[Path] = None,
    db: Optional[sqlite3.Connection] = None,
    do_overwrite: bool = False,
) -> sqlite3.Connection:
    """Writes the transactions to the table.

    Params:
        transactions : The list of transactions to write to the database
        path         : The path to the database file (will create if does not exist) (mutually exclusive with db)
        db           : An already connected database (mutually exclusive with path)
    Returns:
        The opened database connection.
    """
    # Only transactions with valid IDs can be inserted
    for transaction in transactions:
        if not transaction.id:
            raise RuntimeError(
                f"Tried to write {str(transaction)} which has an invalid ID"
            )

    # One of these flags is required and are mutually exclusive
    assert (path is not None) ^ (db is not None)
    if not db:
        db = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)

    # Only create the table if it doesn't already exist
    db.execute(f"create table if not exists transactions ( {COLUMN_HEADERS} )")

    # Insert into database
    with db:
        count_fn = lambda: list(db.execute("select count(*) from transactions"))[0][0]
        rows = count_fn()
        policy = "replace" if do_overwrite else "ignore"
        try:
            db.executemany(
                f"insert or {policy} into transactions({COLUMN_NAMES}) values ({VALUES_FORMAT_STRING})",
                [
                    tuple(dataclasses.asdict(transaction).values())
                    for transaction in transactions
                ],
            )
        except sqlite3.IntegrityError:
            pass
        print(f"Inserted {count_fn() - rows} rows")
    return db


def read_unknown_categories(
    path: Optional[Path] = None, db: Optional[sqlite3.Connection] = None
) -> List[Transaction]:
    """
    Reads transactions with an Unknown value for the category column.

    Params:
        path : The path to the database file (will create if does not exist) (mutually exclusive with db)
        db   : An already connected database (mutually exclusive with path)
    Returns:
        All transactions (rows) that match the criteria.
    """
    # One of these flags is required and are mutually exclusive
    assert (path is not None) ^ (db is not None)
    if not db:
        if not path.exists():
            raise RuntimeError(f"{path} is expected to exist")
        db = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)

    transactions = to_transactions(
        db.execute(
            "select * from transactions where category = ?", (Category.Unknown.name,)
        )
    )
    db.close()
    return transactions


def to_transactions(cursor: sqlite3.Cursor) -> List[Transaction]:
    """
    Converts an iterable of tuples to the Transaction dataclass.

    Params:
        cursor: The sqlite iterable of values.
    Returns:
        A list of Transactions.
    """
    return [
        Transaction(
            date=values[0],
            description=values[1],
            category=values[2],
            amount=values[3],
            id=values[4],
        )
        for values in cursor
    ]
