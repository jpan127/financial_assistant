import dataclasses
from typing import List

from category import Category


@dataclasses.dataclass
class Transaction:
    date: str
    description: str
    category: Category
    amount: float
    id: str = dataclasses.field(repr=False)  # Some unique identifier
    tags: List[str] = dataclasses.field(default_factory=list)

    def short_description(self) -> str:
        return f"{self.description} ({self.amount})"

    @staticmethod
    def unique_field() -> str:
        # Make sure this field exists
        assert any(f.name == "id" for f in dataclasses.fields(Transaction))
        return "id"
