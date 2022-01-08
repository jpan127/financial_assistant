import csv
from pathlib import Path

from category import Category
from bank_history_parser import parse
from transaction import Transaction


def test_parse(tmp_path: Path) -> None:
    """Tests [parse]."""
    path = tmp_path / "history.csv"
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Date", "Description", "Amount", "Balance"])
        writer.writeheader()
        rows = (
            {"Date": "01/01/2021", "Description": "january", "Amount": 111.11, "Balance": 111.11},
            {"Date": "02/02/2021", "Description": "february", "Amount": 222.22, "Balance": 222.22},
            {"Date": "03/03/2021", "Description": "march", "Amount": 333.33, "Balance": 333.33},
        )
        for row in rows:
            # Minify multiline strings to single line with single whitespaces
            writer.writerow(row)

    with path.open("r") as f:
        print(f.read())

    expected_transactions = [
        Transaction(date="2021-01-01", description="january", category=Category.Bank, amount=111.11, id="073463d01f89bb12a649189a08421a55", tags=["superbank"], balance=111.11),
        Transaction(date="2021-02-02", description="february", category=Category.Bank, amount=222.22, id="95bbbcc6b3d5b73ddb831d346feba2d9", tags=["superbank"], balance=222.22),
        Transaction(date="2021-03-03", description="march", category=Category.Bank, amount=333.33, id="51dd7bb6516298e6479e5853eb8bcb3c", tags=["superbank"], balance=333.33),
    ]

    # Check stability of IDs
    for _ in range(10):
        transactions = parse(path, bank="superbank")
        assert transactions == expected_transactions
