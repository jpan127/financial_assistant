import dataclasses
import re
import pprint
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import List, Type, Mapping, Optional

from category import Category
from transaction import Transaction

TYPE_MAP: Mapping[Type, str] = {
    str: "TINYTEXT",
    float: "FLOAT",
    Category: "Category",
    List[str]: "StringList",
}
TRANSACTION_SCHEMA = {f.name: TYPE_MAP[f.type] for f in dataclasses.fields(Transaction)}
PRIMARY_KEY = Transaction.unique_field()
COLUMN_NAMES = ",".join(TRANSACTION_SCHEMA.keys())
COLUMN_HEADERS = ", ".join(
    f"{name} {type}" for name, type in TRANSACTION_SCHEMA.items()
).replace(
    f"{PRIMARY_KEY} {TRANSACTION_SCHEMA[PRIMARY_KEY]}",
    f"{PRIMARY_KEY} {TRANSACTION_SCHEMA[PRIMARY_KEY]} primary key",
)
VALUES_FORMAT_STRING = ",".join(["?"] * len(TRANSACTION_SCHEMA))

# Register [list[str]] with sqlite serialization/deserialization
sqlite3.register_adapter(list, lambda l: ",".join(l))
sqlite3.register_converter("StringList", lambda s: s.decode().split(","))


@contextmanager
def session(*args, read_only: bool = False, **kwargs) -> sqlite3.Connection:
    """Creates a session with the [detect_types] arg set."""
    if "db" in kwargs:
        db = kwargs.get("db")
    else:
        if len(args) != 1:
            raise ValueError(f"Expecting one positional argument (path) if 'db' is not in kwargs: {args}")
        path: Path = args[0]
        if read_only:
            path = Path(f"file:{path}?mode=ro")
            kwargs["uri"] = True
        db = sqlite3.connect(path, **kwargs, detect_types=sqlite3.PARSE_DECLTYPES)

    # Connect a regex function to python's implementation
    db.create_function("regexp", 2, lambda expression, s: re.compile(expression).search(s) is not None)
    yield db
    db.close()


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
            tags=values[5] or [], # Use empty list over None
        )
        for values in cursor
    ]


def tag(db: sqlite3.Connection, *, format_str: str, tag_str: str, width=150) -> None:
    """Populates matched transactions' tag column.

    Args:
        db         : An externally created database connection.
        format_str : A SQL search format string (i.e. %ABC% to match *ABC*).
        tag_str    : An alphanumeric tag to add to the tag column.
        width      : The width to print matched transactions.
    """
    if not tag_str.isalnum():
        raise ValueError(f"Expecting tag ({tag_str}) to be alphanumeric")

    # Match the description first, then match rows that do not already have this tag
    match_clause: str = f"description like '{format_str}' and tags not regexp '\\b{tag_str}\\b'"
    with db:
        # Preview the rows that are matched
        matches = to_transactions(db.execute(f"select * from transactions where {match_clause}"))
        if not matches:
            print("No matches")
            return
        # TODO: Make a config for this
        pprint.pprint(matches, width=width)

        # Make sure the user is ok with the previewed changes
        while True:
            match input("Would you like to continue (y/n)?").lower():
                case 'y': break
                case 'n': return

        # Update the database
        # Append the tag, with a comma, if there already exists tags, otherwise just set the entire string
        db.execute(f"""update transactions
                    set tags = iif(
                        tags = '' or tags is null,
                        '{tag_str}',
                        tags || ',{tag_str}')
                    where {match_clause}
                    """)
