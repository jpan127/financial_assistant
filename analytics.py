"""Performs analytical operations on transactions in the database."""

import calendar
import sqlite3
from abc import ABC, abstractmethod
from typing import Dict, Generator, Tuple

from category import Category
from transaction import Transactions

import database


CategorySumMap = Dict[Category, float]
CategoryTransactionsMap = Dict[Category, Transactions]

MonthlyCategorySumMap = Dict[str, CategorySumMap]
MonthlyCategoryTransactionsMap = Dict[str, CategoryTransactionsMap]


def _month_iterator() -> Generator[Tuple[str, str], None, None]:
    """Iterates over the calendar months.

    Returns:
        The month in string form, the date match pattern.
    """
    for ii in range(1, 12 + 1):
        # Accept any year 202X, and make sure month number is 2-digit aligned, and accept any day
        date_pattern: str = f"202_-{str(ii).zfill(2)}-%"
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
