import csv
import click
import collections
import os
import tabulate
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any

from statement_parser import Transaction, parse_csv

BLACKLISTED_TRANSACTION_DESCRIPTIONS = ("AUTOMATIC PAYMENT - THANK", )


def sum_transactions(transactions: List[Transaction]) -> int:
    return sum(
        round(float(transaction.amount)) for transaction in transactions)


def print_transactions_by_category(transactions: List[Transaction]):
    transactions_by_category = collections.defaultdict(list)
    for transaction in transactions:
        category = transaction.category
        transactions_by_category[category].append(transaction)

    def to_verbose_category(category: str,
                            transactions: List[Transaction]) -> str:
        transactions_str = "\n".join(f" - {transaction.short_description()}"
                                     for transaction in transactions)
        return f"{category}\n{transactions_str}"

    sum_by_category = sorted([[
        to_verbose_category(category, transactions_for_category),
        str(sum_transactions(transactions_for_category))
    ] for category, transactions_for_category in
                              transactions_by_category.items()],
                             key=lambda x: float(x[1]),
                             reverse=True)
    sum_by_category.append(["Sum", sum_transactions(transactions)])
    print(tabulate.tabulate(sum_by_category, tablefmt='fancy_grid'))


def calculate_stats(transactions: List[Dict[Any, Any]]) -> None:
    # Remove all blacklisted transactions
    transactions = [
        transaction for transaction in transactions
        if not any(k in transaction.description
                   for k in BLACKLISTED_TRANSACTION_DESCRIPTIONS)
    ]

    # Since all non-refund transactions are negative, change them to positive to make calculations more intuitive
    for transaction in transactions:
        transaction.amount = -transaction.amount

    print_transactions_by_category(transactions)


# Idea: use a directory structure to discover files
#       cache already calculated files if it becomes slow
@click.command()
def main() -> None:
    for path, directory, files in os.walk("data"):
        if path == os.path.join("data", "all"):
            continue
        for file in files:
            if Path(file).suffix == ".CSV":
                transactions = parse_csv(Path(path) / file)
                calculate_stats(transactions)


if __name__ == "__main__":
    main()
