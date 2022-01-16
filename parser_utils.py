from datetime import timezone
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple

from ofxtools.Parser import OFXTree

from category import Category
from transaction import Transaction, Transactions, hash_transactions

# These are institution payment transaction descriptions and shouldn't be relevant
IGNORED_TRANSACTION_KEYS = (
    "AUTO PAYMENT",
    "AUTOMATIC PAYMENT - THANK",
    "AUTOMATIC PAYMENT - THANK",
    "AUTOPAY PAYMENT - THANK YOU",
    "AUTOPAY PAYMENT THANK YOU",
    "MOBILE PAYMENT THANK YOU",
    "MOBILE PAYMENT - THANK YOU",
    "Payment Thank You - Web",
    "Payment Thank You-Mobile",
)


def parse_ofx(
    path: Path,
    *,
    category: Category,
    tags: List[str],
) -> Transactions:
    """
    Parses an OFX file and converts it to a list of transactions.

    Args:
        path     : The path to the OFX file.
        category : The category to set for all transactions.
        tags     : The tags to set for all transactions.

    Returns:
        The converted list of transactions.
    """

    def parse_id(transaction: Any) -> str:
        """Prefer the REFNUM attribute, if it exists, otherwise the FITID.

        Currently, it is not supported for an OFX file to not have either.
        """
        return transaction.refnum or transaction.fitid

    # Parse in binary mode (required by library)
    parser = OFXTree()
    with path.open("rb") as f:
        parser.parse(f)

    transactions: Transactions = []
    ofx = parser.convert()
    statements = ofx.statements
    if len(statements) != 1:
        raise ValueError(f"Only 1 statement is expected, found {len(statements)}")

    for transaction in statements[0].transactions:
        # Skip blacklisted transactions
        # Some of these strings have leading whitespace
        if transaction.name.strip() in IGNORED_TRANSACTION_KEYS:
            continue
        dtime = transaction.dtposted.replace(tzinfo=timezone.utc).astimezone(tz=None)
        transactions.append(
            Transaction(
                date=dtime.date().isoformat(),
                description=transaction.name,
                category=category,
                amount=float(transaction.trnamt),
                id=parse_id(transaction),
                tags=tags,
            )
        )

        # Generate a unique hash using the transaction object without the ID, and the existing ID separately
        transactions[-1].id = hash_transactions([transactions[-1]], extra_inputs=[transactions[-1].id])

    # If a balance value doesn't exist, ignore it
    try:
        balance = float(statements[0].availbal.balamt)
    except AttributeError:
        return transactions

    # Set the balance appropriately
    for transaction in reversed(transactions):
        transaction.balance = round(balance, 2)
        balance -= transaction.amount
    return transactions


ParseFn = Callable[[Path, str, str], Transactions]


def parse(path: Path, parse_csv_fn: ParseFn, parse_ofx_fn: ParseFn) -> Transactions:
    """
    Parses a CSV/OFX file and converts it to a list of transactions.

    Params:
        path: The path to the CSV/OFX file. Expected to be in <YEAR>_<NAME>.<EXT> format.

    Returns:
        The converted list of transactions.
    """
    try:
        year, name = str(path.with_suffix("").name).split("_", maxsplit=1)
        name = name.lower()
        if not 2000 <= int(year) <= 3000:
            raise ValueError(f"{year} is an unexpected value for year")
    except ValueError as e:
        raise ValueError(f"{path} is not in the format <YEAR>_<NAME>.<EXT>") from e

    match path.suffix.lower():
        case ".csv":
            return parse_csv_fn(path=path, year=year, name=name)
        case (".ofx" | ".qfx"):
            return parse_ofx_fn(path=path, year=year, name=name)
    raise RuntimeError(f"Only CSV/OFX files are supported, not {path.suffix}")
