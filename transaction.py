import dataclasses
import hashlib
from typing import Any, List, Optional, Sequence

from category import Category


@dataclasses.dataclass
class Transaction:
    date: str
    description: str
    category: Category
    amount: float
    id: str = dataclasses.field(repr=False)  # Some unique identifier
    tags: List[str] = dataclasses.field(default_factory=list)
    balance: float = 0.0  # Only used for bank transactions

    def short_description(self) -> str:
        return f"{self.description} ({self.amount})"

    @staticmethod
    def unique_field() -> str:
        # Make sure this field exists
        assert any(f.name == "id" for f in dataclasses.fields(Transaction))
        return "id"


Transactions = List[Transaction]


def hash_transactions(
    transactions: Transactions,
    extra_inputs: Optional[Sequence[Any]] = None,
) -> str:
    """Hashes a list of transactions.

    Args:
        transactions: The transactions to hash.
        extra_inputs: Additional string-convertible objects to hash.
    Returns:
        An MD5 hex-string hash.
    """
    hasher = hashlib.md5()
    for transaction in transactions:
        hasher.update(str(transaction).encode("utf-8"))
    if extra_inputs:
        for extra_input in extra_inputs:
            hasher.update(str(extra_input).encode("utf-8"))
    return hasher.hexdigest()
