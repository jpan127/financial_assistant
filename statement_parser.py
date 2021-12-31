import csv
from datetime import timezone
from typing import Any, List, Mapping
from pathlib import Path

import click
from ofxtools.Parser import OFXTree

from category import Category
from category_lookup_table import STRING_CATEGORY_MAP
from transaction import Transaction


# These are chase payment transaction descriptions and shouldn't be relevant
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


class SchemaMismatchError(Exception):
    """The schema of the file was unexpected."""


def _parse_ofx(path: Path) -> List[Transaction]:
    """
    Parses an OFX file and converts it to a list of transactions.

    Params:
        path: The path to the OFX file.

    Returns:
        The converted list of transactions.
    """

    def parse_id(transaction: Any) -> str:
        """Prefer the REFNUM attribute, if it exists, otherwise the FITID."""
        return transaction.refnum or transaction.fitid

    # Parse in binary mode (required by library)
    parser = OFXTree()
    with path.open("rb") as f:
        parser.parse(f)

    transactions: List[Transaction] = []
    ofx = parser.convert()
    statements = ofx.statements
    for statement in statements:
        for transaction in statement.transactions:
            # Skip blacklisted transactions
            # Some of these strings have leading whitespace
            if transaction.name.strip() in IGNORED_TRANSACTION_KEYS:
                continue
            dtime = transaction.dtposted.replace(tzinfo=timezone.utc).astimezone(tz=None)
            transactions.append(
                Transaction(
                    date=dtime.date().isoformat(),
                    description=transaction.name,
                    category=Category.Unknown,
                    amount=float(transaction.trnamt),
                    id=parse_id(transaction),
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

    def parse_date(transaction: Mapping[str, Any]) -> str:
        """Try any of these keys that are used from various banks."""
        for key in ("Transaction Date", "Date", "Trans Date"):
            if key in transaction:
                return transaction[key]
        raise NotImplementedError(f"Schema is missing a transaction date column {transaction}")

    def parse_amount(amount: str) -> float:
        """Target uses a weird $(123.45) format, so sanitize that."""
        for c in ("$", "(", ")"):
            amount = amount.replace(c, "")
        return float(amount)

    def parse_id(transaction: Mapping[str, Any]) -> str:
        """Some banks produce CSVs already with the REFNUM."""
        for key in ("Reference", "Reference Number"):
            if key in transaction:
                return transaction[key].replace("'", "")
        return ""

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=",")
        try:
            return [
                Transaction(
                    date=parse_date(transaction),
                    description=transaction["Description"],
                    category=STRING_CATEGORY_MAP[transaction["Category"]],
                    amount=parse_amount(transaction["Amount"]),
                    id=parse_id(transaction),
                )
                for transaction in reader
                if transaction["Description"] not in IGNORED_TRANSACTION_KEYS
            ]
        except NotImplementedError as e:
            raise SchemaMismatchError(f"CSV headers mismatch: {reader.fieldnames}") from e


def parse(path: Path) -> List[Transaction]:
    """
    Parses a CSV/OFX file and converts it to a list of transactions.

    Params:
        path: The path to the CSV/OFX file.

    Returns:
        The converted list of transactions.
    """
    match path.suffix.lower():
        case ".csv":
            return _parse_csv(path)
        case (".ofx" | ".qfx"):
            return _parse_ofx(path)
    raise RuntimeError(f"Only CSV/OFX files are supported, not {path.suffix}")


@click.command()
@click.argument("path", type=click.Path(exists=True))
def cli(path: str) -> None:
    """CLI to manually test the functions."""
    import pprint

    pprint.pprint(parse(Path(path)))


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter
