import csv
from typing import Any, List, Mapping
from pathlib import Path

import click

import parser_utils as utils
from category import Category
from category_lookup_table import STRING_CATEGORY_MAP
from transaction import Transaction, Transactions
from parser_utils import IGNORED_TRANSACTION_KEYS


class SchemaMismatchError(Exception):
    """The schema of the file was unexpected."""


def _parse_ofx(path: Path, *, year: str, name: str) -> List[Transaction]:
    """
    Parses an OFX file and converts it to a list of transactions.

    Args:
        path: The path to the OFX file.
        year: The expected year of the transactions, also filtered by year.
        name: The name of the institution the transactions came from.

    Returns:
        The converted list of transactions.
    """
    return [t for t in utils.parse_ofx(path, category=Category.Unknown, tags=[year, name]) if t.date.split("-")[0] == year]


def _parse_csv(path: Path, *, year: str, name: str) -> List[Transaction]:
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
                    tags=[year, name],
                )
                for transaction in reader
                if transaction["Description"] not in IGNORED_TRANSACTION_KEYS
            ]
        except NotImplementedError as e:
            raise SchemaMismatchError(f"CSV headers mismatch: {reader.fieldnames}") from e


def parse(path: Path) -> Transactions:
    """
    Parses a CSV/OFX file and converts it to a list of transactions.

    Params:
        path: The path to the CSV/OFX file. Expected to be in <YEAR>_<NAME>.<EXT> format.

    Returns:
        The converted list of transactions.
    """
    return utils.parse(path, _parse_csv, _parse_ofx)


@click.command()
@click.argument("path", type=click.Path(exists=True, path_type=Path))
def cli(path: Path) -> None:
    """CLI to manually test the functions."""
    import pprint

    pprint.pprint(parse(path))


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter
