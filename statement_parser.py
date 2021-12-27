import csv
from datetime import timezone
from typing import List
from pathlib import Path

import click
from ofxtools.Parser import OFXTree

from category import Category
from category_lookup_table import STRING_CATEGORY_MAP
from transaction import Transaction


# These are chase payment transaction descriptions and shouldn't be relevant
IGNORED_TRANSACTION_KEYS = (
    "AUTOMATIC PAYMENT - THANK",
    "Payment Thank You-Mobile",
    "Payment Thank You - Web",
)


def _parse_ofx(path: Path) -> List[Transaction]:
    """
    Parses an OFX file and converts it to a list of transactions.

    Params:
        path: The path to the OFX file.

    Returns:
        The converted list of transactions.
    """
    # Parse in binary mode (required by library)
    parser = OFXTree()
    with path.open("rb") as f:
        parser.parse(f)

    transactions = []
    ofx = parser.convert()
    statements = ofx.statements
    for statement in statements:
        for transaction in statement.transactions:
            # Skip blacklisted transactions
            if transaction.name in IGNORED_TRANSACTION_KEYS:
                continue
            dtime = transaction.dtposted.replace(tzinfo=timezone.utc).astimezone(
                tz=None
            )
            transactions.append(
                Transaction(
                    date=dtime.date().isoformat(),
                    description=transaction.name,
                    category=Category.Unknown,
                    amount=float(transaction.trnamt),
                    id=transaction.fitid,
                )
            )

    return transactions


def _parse_csv(path: Path) -> List[Transaction]:
    """
    Parses a CSV file and converts it to a list of transactions.

    Params:
        path: The path to the CSV file.

    Returns:
        The converted list of transactions.
    """
    with path.open("r") as f:
        reader = csv.DictReader(f, delimiter=",")
        return [
            Transaction(
                date=transaction["Transaction Date"],
                description=transaction["Description"],
                category=STRING_CATEGORY_MAP[transaction["Category"]],
                amount=float(transaction["Amount"]),
                id="",
            )
            for transaction in reader
            if transaction["Description"] not in IGNORED_TRANSACTION_KEYS
        ]


def parse(path: Path) -> List[Transaction]:
    """
    Parses a CSV/OFX file and converts it to a list of transactions.

    Params:
        path: The path to the CSV/OFX file.

    Returns:
        The converted list of transactions.
    """
    match path.suffix.lower():
        case ".csv": return _parse_csv(path)
        case ".ofx": return _parse_ofx(path)
    raise RuntimeError(f"Only CSV/OFX files are supported, not {path.suffix}")


@click.command()
@click.argument("path", type=click.Path(exists=True))
def cli(path: str) -> None:
    """CLI to manually test the functions."""
    import pprint
    pprint.pprint(parse(Path(path)))

if __name__ == "__main__":
    cli()
