import dataclasses

from category import Category


@dataclasses.dataclass
class Transaction:
    date: str
    description: str
    category: Category
    amount: float
    id: str  # Some unique identifier

    def short_description(self) -> str:
        return f"{self.description} ({self.amount})"

    @staticmethod
    def unique_field() -> str:
        # Make sure this field exists
        assert any(f.name == "id" for f in dataclasses.fields(Transaction))
        return "id"
