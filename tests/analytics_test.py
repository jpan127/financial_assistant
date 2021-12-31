import pytest

import analytics
import database
from category import Category
from transaction import Transaction

# Transactions used in the tests below
TRANSACTIONS = [
    Transaction(date="2021-01-01", description="DOORDASH", category=Category.Food, amount=-111.11, id="111", tags=["doordash", "food"]),
    Transaction(date="2021-02-02", description="PG&E", category=Category.BillsUtilities, amount=-222.22, id="222", tags=["bills"]),
    Transaction(date="2021-03-03", description="TODDSNYDER", category=Category.Shopping, amount=-333.33, id="333", tags=["clothes"]),
    Transaction(date="2021-04-04", description="GARBAGE", category=Category.Unknown, amount=-444.44, id="444", tags=["bills"]),
]

USER: str = "PyTest"


def test_Accumulate() -> None:
    """Tests the [Accumulate] class."""
    with database.session(":memory:") as db:
        database.write(USER, db=db, transactions=TRANSACTIONS)

        op = analytics.Accumulate(db, USER, tags=["bills"])
        sum_amount, transactions = op()
        assert sum_amount == -666
        assert transactions == [TRANSACTIONS[1], TRANSACTIONS[3]]
        sums_by_month, transactions_by_month = op.by_month()
        assert sums_by_month == {"February": -222, "April": -444}
        assert transactions_by_month == {"February": [TRANSACTIONS[1]], "April": [TRANSACTIONS[3]]}
        sums_by_month, transactions_by_month = op.categories_by_month()
        assert sums_by_month == {"February": {Category.BillsUtilities: -222}, "April": {Category.Unknown: -444}}
        assert transactions_by_month == {
            "February": {Category.BillsUtilities: [TRANSACTIONS[1]]},
            "April": {Category.Unknown: [TRANSACTIONS[3]]},
        }

        op = analytics.Top(db, USER, description_pattern="%", top=2)
        transactions = op()
        assert transactions == [TRANSACTIONS[3], TRANSACTIONS[2]]
        transactions_by_month = op.by_month()
        assert transactions_by_month == {
            "January": [TRANSACTIONS[0]],
            "February": [TRANSACTIONS[1]],
            "March": [TRANSACTIONS[2]],
            "April": [TRANSACTIONS[3]],
        }
        transactions_by_month = op.categories_by_month()
        assert transactions_by_month == {
            "January": {Category.Food: [TRANSACTIONS[0]]},
            "February": {Category.BillsUtilities: [TRANSACTIONS[1]]},
            "March": {Category.Shopping: [TRANSACTIONS[2]]},
            "April": {Category.Unknown: [TRANSACTIONS[3]]},
        }
