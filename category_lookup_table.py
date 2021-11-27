from pathlib import Path
from enum import Enum, auto
from typing import Optional

from ruamel.yaml import YAML, yaml_object

yaml = YAML()


@yaml_object(yaml)
class Category(Enum):
    Automotive = auto()
    BillsUtilities = auto()
    Education = auto()
    Entertainment = auto()
    FeesAdjustments = auto()
    Food = auto()
    Gas = auto()
    Gifts = auto()
    Groceries = auto()
    Health = auto()
    Home = auto()
    Miscellaneous = auto()
    Personal = auto()
    ProfessionalServices = auto()
    Shopping = auto()
    Travel = auto()

    @classmethod
    def to_yaml(cls, representer, node):
        return representer.represent_scalar("!Category", node.name)

    @classmethod
    def from_yaml(cls, constructor, node):
        return cls(Category[node.value])


STRING_CATEGORY_MAP = {
    "Automotive": Category.Automotive,
    "Bills & Utilities": Category.BillsUtilities,
    "Education": Category.Education,
    "Entertainment": Category.Entertainment,
    "Fees & Adjustments": Category.FeesAdjustments,
    "Food & Drink": Category.Food,
    "Gas": Category.Gas,
    "Gifts & Donations": Category.Gifts,
    "Groceries": Category.Groceries,
    "Health & Wellness": Category.Health,
    "Home": Category.Home,
    "Miscellaneous": Category.Miscellaneous,
    "Personal": Category.Personal,
    "Professional Services": Category.ProfessionalServices,
    "Shopping": Category.Shopping,
    "Travel": Category.Travel,
}


# @TODO: Add hints
class CategoryLookupTable:
    def __init__(self, path: Path) -> None:
        if not path.exists():
            with path.open("w"):
                pass
        self._path = path
        self._table = yaml.load(path)
        if not self._table:
            self._table = {}

    def __enter__(self) -> "CategoryLookupTable":
        return self

    def __exit__(self, *args) -> None:
        self.flush()

    def flush(self) -> None:
        """Flushes the table to file."""
        yaml.dump(self._table, self._path)

    def load(self, key: str) -> Optional[Category]:
        """Looks up the string in the table."""
        return self._table[key]

    def store(self, key: str, category: Category) -> None:
        """Stores a key to category mapping in the table."""
        if category == Category.Unknown:
            raise RuntimeError(f"Trying to store {key} with an unknown category")
        self._table[key] = category
