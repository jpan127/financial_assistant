import csv
from pathlib import Path
from typing import Any, Mapping

import click

from analytics import calc_bank_stats, split_transactions_by_month, print_bank_stats
from transaction import Transaction, Transactions, hash_transactions
from category import Category


def _check_year(year: Any) -> None:
    """Sanity checks the year value."""
    year = int(year)
    if not 2000 <= year <= 3000:
        raise ValueError(f"{year} is an unexpected value for year")


def _parse_date(row: Mapping[str, Any]) -> str:
    """Parses the 'date' field of a bank history CSV file.

    Args:
        row: A mapping for a row of a CSV file.
    Returns:
        The parsed value.
    """
    for key in ("Date", "Posting Date"):
        try:
            month, day, year = row[key].split("/")
            _check_year(year)
            return f"{year}-{month}-{day}"
        except KeyError:
            continue
        except ValueError:
            print(f"{row[key]} is not in the expected month/day/year format")  # @TODO: This can be handled more generically
            raise
    raise RuntimeError(f"{row} does not contain a known date column")


def _parse_balance(row: Mapping[str, Any]) -> str:
    """Parses the 'balance' field of a bank history CSV file.

    Args:
        row: A mapping for a row of a CSV file.
    Returns:
        The parsed value.
    """
    for key in ("Balance", "RunningBalance"):
        try:
            return float(row[key].replace("$", "").replace(",", ""))
        except KeyError:
            continue
    raise RuntimeError(f"{row} does not contain a known balance column")


def _parse_amount(row: Mapping[str, Any]) -> str:
    """Parses the 'amount' field of a bank history CSV file.

    Args:
        row: A mapping for a row of a CSV file.
    Returns:
        The parsed value.
    """
    if "Amount" in row:
        return float(row["Amount"].replace("$", "").replace(",", ""))
    if "Withdrawal (-)" in row and row["Withdrawal (-)"]:
        return -float(row["Withdrawal (-)"].replace("$", "").replace(",", ""))
    if "Deposit (+)" in row and row["Deposit (+)"]:
        return float(row["Deposit (+)"].replace("$", "").replace(",", ""))
    raise RuntimeError(f"{row} does not contain a known amount column")


def parse(path: Path, bank: str) -> Transactions:
    """Parses a CSV file that represents a bank's history."""
    if path.suffix.lower() != ".csv":
        raise ValueError(f"{path} is expected to be a csv file")
    if not path.exists():
        raise ValueError(f"{path} is expected to exist")

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=",")
        transactions: Transactions = [
            Transaction(
                date=_parse_date(row),
                description=" ".join(row["Description"].split()),  # Remove consecutive spaces
                category=Category.Bank,
                amount=_parse_amount(row),
                id="",
                tags=[bank],
                balance=_parse_balance(row),
            )
            for row in reader
        ]

    # If the ordering of the file is descending, reverse it
    if transactions[0].date > transactions[-1].date:
        transactions = list(reversed(transactions))

    # Check that only 1 year is present in the transactions
    for transaction in transactions:
        year = transaction.date.split("-")[0]
        first_year = transactions[0].date.split("-")[0]
        if year != first_year:
            raise NotImplementedError("Currently, only annual bank history reports are supported")

    # Set a unique ID for each transaction
    # Since it is possible for two entries to have the same values:
    #   For example:
    #       1. 01/01/3000,DESCRIPTION,1.00,1.00
    #       2. 01/01/3000,DESCRIPTION,-1.00,0.00
    #       3. 01/01/3000,DESCRIPTION,1.00,1.00
    #       4. 01/02/3000,DESCRIPTION,1.00,2.00
    #   If the balance goes down and back up by the same amount on the same day, there's a duplicate entry
    #   1 and 3 are the same exact values with their ordering being the only difference
    # This logic is meant to take into account the ordering relative to the day for hashing
    # 1 is hash combined with 2 and 3 transactions, in order (it is assumed the ordering of the CSV file is correct)
    # 2 is hash combined with 3
    # 3 is hashed by itself
    hashes = set()
    for i, transaction in enumerate(transactions):
        transactions_to_hash = [transaction]
        for other_transaction in transactions[i + 1 :]:
            if transaction.date != other_transaction.date:
                break
            transactions_to_hash.append(other_transaction)
        transaction.id = hash_transactions(transactions_to_hash)
        hashes.add(transaction.id)

    # Sanity check no collisions
    assert len(hashes) == len(transactions)

    return transactions


@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.argument("bank", type=str)
def cli(path: str, bank: str) -> None:
    """CLI to manually test the functions."""
    print_bank_stats(calc_bank_stats(split_transactions_by_month(parse(Path(path), bank=bank))))


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter
