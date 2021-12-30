import sqlite3
from enum import Enum, auto

from ruamel.yaml import YAML, yaml_object

yaml = YAML(typ="safe")


@yaml_object(yaml)
class Category(Enum):
    Unknown = auto()
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
        return representer.represent_scalar(u"!Category", node.name)

    @classmethod
    def from_yaml(cls, constructor, node):
        return cls(Category[node.value])

    def __repr__(self):
        """Much cleaner than the generated repr: '<Category.XYZ: 1>'."""
        return self.name


# Register [Category] with sqlite serialization/deserialization
sqlite3.register_adapter(Category, lambda category: category.name)
sqlite3.register_converter("Category", lambda s: Category[s.decode()])
