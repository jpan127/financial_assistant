"""Performs analytical operations on transactions in the database."""

import calendar
import dataclasses
import datetime
import sqlite3
from abc import ABC, abstractmethod
from typing import List, Dict, Iterator, Tuple, Mapping

import tabulate

from category import Category
from transaction import Transactions

import database


CategorySumMap = Dict[Category, float]
CategoryTransactionsMap = Dict[Category, Transactions]

MonthlyCategorySumMap = Dict[str, CategorySumMap]
MonthlyCategoryTransactionsMap = Dict[str, CategoryTransactionsMap]


@dataclasses.dataclass(frozen=True)
class BankStats:
    """Stats calculated over a bank's transaction history."""

    diff: float
    begin: float
    end: float
    gain: float
    lose: float
    days: int


def _month_iterator(*, year: str = "202_", suffix: str = "%") -> Iterator[Tuple[str, str]]:
    """Iterates over the calendar months.

    Args:
        year   : The 4-digit year to put into the date pattern.
        suffix : A suffix to append to the "ABCD-EF-" date pattern.
    Returns:
        The month in string form, the date match pattern.
    """
    for ii in range(1, 12 + 1):
        # Accept any year 202X, and make sure month number is 2-digit aligned, and accept any day
        date_pattern: str = f"{year}-{str(ii).zfill(2)}-{suffix}"
        month: str = calendar.month_name[ii]
        yield month, date_pattern


class _Operation(ABC):
    def __init__(self, db: sqlite3.Connection, user: str, **kwargs) -> None:
        """Performs the operation on the matched transactions.

        Args:
            db     : An externally created database connection.
            user   : The table name.
            kwargs : Any args that [database.select] accept.
        """
        self.db = db
        self.user = user
        self.kwargs = kwargs

    @abstractmethod
    def __call__(self) -> None:
        """Performs the operation on the transactions that match one or more of the columns.

        Returns:
            The sum and the matched transactions.
        """
        ...

    @abstractmethod
    def by_month(self) -> None:
        """Performs the operation on the transactions that match one or more of the columns, per month.

        Returns:
            The sum and the matched transactions, per month.
        Raises:
            ValueError: If 'date_pattern' is specified.
        """
        if "date_pattern" in self.kwargs:
            raise ValueError("This function does not take a 'date_pattern' arg.")

    @abstractmethod
    def categories_by_month(self) -> None:
        """Performs the operation on the transactions that match one or more of the columns, per month, by category.

        Returns:
            The sum and the matched transactions, per month, by category.
        Raises:
            ValueError: If 'date_pattern' is specified.
        """
        for key in ("date_pattern", "category"):
            if key in self.kwargs:
                raise ValueError(f"This function does not take a '{key}' arg.")


class Accumulate(_Operation):
    """Accumulates transaction amounts from the matched transactions."""

    def __call__(self, **kwargs) -> Tuple[float, Transactions]:
        """Refer to base class."""
        transactions = database.select(self.db, self.user, **(self.kwargs | kwargs))
        transactions.sort(key=lambda t: t.date)

        sum_amount: float = sum(round(float(transaction.amount)) for transaction in transactions)
        return sum_amount, transactions

    def by_month(self) -> Tuple[Dict[str, float], Dict[str, Transactions]]:
        """Refer to base class."""
        super().by_month()

        sums_by_month = {}
        transactions_by_month = {}
        for month, date_pattern in _month_iterator():
            sums_by_month[month], transactions_by_month[month] = self(date_pattern=date_pattern)
            if sums_by_month[month] == 0.0:
                del sums_by_month[month]
                del transactions_by_month[month]
        return sums_by_month, transactions_by_month

    def categories_by_month(self) -> Tuple[MonthlyCategorySumMap, MonthlyCategoryTransactionsMap]:
        """Refer to base class."""
        super().categories_by_month()

        sums_by_month = {}
        transactions_by_month = {}
        for month, date_pattern in _month_iterator():
            sums_by_month[month], transactions_by_month[month] = {}, {}
            for category in Category:
                sums_by_month[month][category], transactions_by_month[month][category] = self(date_pattern=date_pattern, category=category)
                if sums_by_month[month][category] == 0.0:
                    del sums_by_month[month][category]
                    del transactions_by_month[month][category]
            if not sums_by_month[month]:
                del sums_by_month[month]
                del transactions_by_month[month]

        return sums_by_month, transactions_by_month


class Top(_Operation):
    """Accumulates transaction amounts from the matched transactions."""

    def __call__(self, **kwargs) -> Transactions:
        """Refer to base class."""
        if all(key not in self.kwargs for key in ("num", "top")):
            raise ValueError(f"Expected a 'num'/'top' kwarg: {self.kwargs}")
        # Convert generic 'num' arg to [database.select]'s specific 'top' arg naming
        if "top" not in self.kwargs:
            self.kwargs["top"] = self.kwargs["num"]
            del self.kwargs["num"]
        return database.select(self.db, self.user, **(self.kwargs | kwargs))

    def by_month(self) -> Dict[str, Transactions]:
        """Refer to base class."""
        super().by_month()

        transactions_by_month = {}
        for month, date_pattern in _month_iterator():
            transactions_by_month[month] = self(date_pattern=date_pattern)
            if not transactions_by_month[month]:
                del transactions_by_month[month]
        return transactions_by_month

    def categories_by_month(self) -> MonthlyCategoryTransactionsMap:
        """Refer to base class."""
        super().categories_by_month()

        transactions_by_month = {}
        for month, date_pattern in _month_iterator():
            transactions_by_month[month] = {}
            for category in Category:
                transactions_by_month[month][category] = self(date_pattern=date_pattern, category=category)
                if not transactions_by_month[month][category]:
                    del transactions_by_month[month][category]
            if not transactions_by_month[month]:
                del transactions_by_month[month]
        return transactions_by_month


def split_transactions_by_month(transactions: Transactions) -> Dict[str, Transactions]:
    """Splits the input transactions into buckets by month.

    Args:
        transactions: A list of unbucketed transactions.
    Returns:
        A mapping from month to transactions in that month.
    Raises:
        ValueError: If the input is invalid or empty.
    """
    if not transactions:
        raise ValueError("Expected at least one transaction")
    year: str = transactions[0].date.split("-")[0]
    if not all(transaction.date.split("-")[0] == year for transaction in transactions):
        raise ValueError(f"Expected all transactions to be in the same year {year}")

    transactions_by_month = {}
    for month, date_pattern in _month_iterator(year=year, suffix=""):
        start_idx = None
        end_idx = None
        for idx, transaction in enumerate(transactions):
            if transaction.date.startswith(date_pattern):
                if start_idx is None:
                    start_idx = idx
                end_idx = idx
            elif end_idx is not None:
                break
        # There are no transactions for this month
        if start_idx is None:
            continue
        # There's only 1 transaction
        if end_idx is None:
            end_idx = start_idx
        month_transactions = transactions[start_idx : end_idx + 1]
        transactions_by_month[month] = month_transactions

    # Make sure that all transactions are accounted for
    assert sum(len(ts) for ts in transactions_by_month.values()) == len(transactions)
    return transactions_by_month


def calc_bank_stats(transactions_by_month: Dict[str, Transactions]) -> Dict[str, BankStats]:
    """Calculates stats from the transactions.

    Args:
        transactions_by_month: Transactions bucketed by month.
    Returns:
        Stats calculated by month, plus a set of stats calculated over the entire year.
    Raises:
        ValueError: If the input is empty.
    """
    if not transactions_by_month:
        raise ValueError("Expected at least one transaction")

    round_float = lambda f: float(round(f, 2))
    to_date = lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date()
    stats: Dict[str, BankStats] = {
        month: BankStats(
            diff=round_float(sum(transaction.amount for transaction in month_transactions)),
            begin=month_transactions[0].balance - month_transactions[0].amount,
            end=month_transactions[-1].balance,
            gain=round_float(sum(transaction.amount for transaction in month_transactions if transaction.amount > 0)),
            lose=round_float(sum(transaction.amount for transaction in month_transactions if transaction.amount < 0)),
            days=calendar.monthrange(to_date(month_transactions[0].date).year, to_date(month_transactions[0].date).month)[1],
        )
        for month, month_transactions in transactions_by_month.items()
    }

    # Aggregate over the year
    transactions_by_month_list: List[BankStats] = list(transactions_by_month.values())
    first_transaction = transactions_by_month_list[0][0]
    last_transaction = transactions_by_month_list[-1][-1]
    stats["Year"] = BankStats(
        diff=round_float(sum(s.diff for s in stats.values())),
        begin=first_transaction.balance - first_transaction.amount,
        end=last_transaction.balance,
        gain=round_float(sum(s.gain for s in stats.values())),
        lose=round_float(sum(s.lose for s in stats.values())),
        days=sum(s.days for s in stats.values()),
    )
    return stats


def print_bank_stats(stats_by_month: Mapping[str, BankStats]) -> None:
    """Pretty prints the bank stats by month in a table.

    Args:
        stats_by_month: The stats calculated per month.
    """
    rows: List[List[str]] = [["Month"] + [f.name for f in dataclasses.fields(BankStats)]] + [[month] + list(dataclasses.asdict(stats).values()) for month, stats in stats_by_month.items()]
    print(tabulate.tabulate(rows, tablefmt="fancy_grid"))
