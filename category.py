from enum import Enum, auto

from ruamel.yaml import YAML, yaml_object

yaml = YAML()


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
