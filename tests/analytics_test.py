import calendar

import analytics
import database
from analytics import calc_bank_stats, split_transactions_by_month, BankStats
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


def test_calc_bank_stats() -> None:
    """Tests [calc_bank_stats]."""

    def make(**kwargs) -> Transaction:
        return Transaction(description="", category=Category.Bank, id="", tags=[], **kwargs)

    stats = calc_bank_stats(
        split_transactions_by_month(
            [
                make(date="2021-01-01", amount=1.00, balance=1.00),
                make(date="2021-02-02", amount=2.00, balance=3.00),
                make(date="2021-03-03", amount=3.00, balance=6.00),
            ]
        )
    )
    assert stats == {
        "January": BankStats(diff=1.00, begin=0.00, end=1.00, gain=1.00, lose=0.00, days=31),
        "February": BankStats(diff=2.00, begin=1.00, end=3.00, gain=2.00, lose=0.00, days=28),
        "March": BankStats(diff=3.00, begin=3.00, end=6.00, gain=3.00, lose=0.00, days=31),
        "Year": BankStats(diff=6.00, begin=0.00, end=6.00, gain=6.00, lose=0.00, days=90),
    }

    stats = calc_bank_stats(
        split_transactions_by_month(
            [
                make(date="2021-01-01", amount=1.00, balance=1.00),
                make(date="2021-03-03", amount=3.00, balance=4.00),
                make(date="2021-12-12", amount=-5.00, balance=-1.00),
            ]
        )
    )
    assert stats == {
        "January": BankStats(diff=1.00, begin=0.00, end=1.00, gain=1.00, lose=0.00, days=31),
        "March": BankStats(diff=3.00, begin=1.00, end=4.00, gain=3.00, lose=0.00, days=31),
        "December": BankStats(diff=-5.00, begin=4.00, end=-1.00, gain=0.00, lose=-5.00, days=31),
        "Year": BankStats(diff=-1.00, begin=0.00, end=-1.00, gain=4.00, lose=-5.00, days=93),
    }

    # Test different combinations of months with 2 transactions in the first month, and 1 in the last
    for month_a_idx in range(1, 12 + 1):
        for month_b_idx in range(month_a_idx + 1, 12 + 1):
            month_a = calendar.month_name[month_a_idx]
            month_b = calendar.month_name[month_b_idx]
            month_a_idx_formatted = str(month_a_idx).zfill(2)
            month_b_idx_formatted = str(month_b_idx).zfill(2)
            month_a_days = calendar.monthrange(2021, month_a_idx)[1]
            month_b_days = calendar.monthrange(2021, month_b_idx)[1]
            stats = calc_bank_stats(
                split_transactions_by_month(
                    [
                        make(date=f"2021-{month_a_idx_formatted}-14", amount=-1_000.00, balance=999_000.00),
                        make(date=f"2021-{month_a_idx_formatted}-28", amount=-99_000.00, balance=900_000.00),
                        make(date=f"2021-{month_b_idx_formatted}-12", amount=27_272.72, balance=927_272.72),
                    ]
                )
            )
            assert stats == {
                month_a: BankStats(diff=-100_000.00, begin=1_000_000.00, end=900_000.00, gain=0.00, lose=-100_000.00, days=month_a_days),
                month_b: BankStats(diff=27_272.72, begin=900_000.00, end=927_272.72, gain=27_272.72, lose=0.00, days=month_b_days),
                "Year": BankStats(diff=-72_727.28, begin=1_000_000.00, end=927_272.72, gain=27_272.72, lose=-100_000.00, days=month_a_days + month_b_days),
            }
