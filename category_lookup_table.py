from pathlib import Path
from typing import Optional, Mapping, List

import nltk
from nltk.tag.perceptron import PerceptronTagger
from ruamel.yaml import YAML

from category import Category

yaml = YAML(typ="safe")


CategoryMap = Mapping[str, Category]

CATEGORY_LOOKUP_TABLE_PATH = Path("./data/category_lookup_table.yaml")
CATEGORY_HINT_CONFIG_PATH = Path("./data/category_hint_config.yaml")
STRING_CATEGORY_MAP: CategoryMap = {
    "": Category.Unknown,
    # Chase strings
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
    # AMEX strings
    "Business Services-Advertising Services": Category.Entertainment,
    "Business Services-Health Care Services": Category.Health,
    "Business Services-Mailing & Shipping": Category.ProfessionalServices,
    "Business Services-Other Services": Category.Miscellaneous,
    "Business Services-Printing & Publishing": Category.ProfessionalServices,
    "Business Services-Professional Services": Category.ProfessionalServices,
    "Communications-Cable & Internet Comm": Category.Entertainment,
    "Entertainment-Associations": Category.Entertainment,
    "Entertainment-General Attractions": Category.Entertainment,
    "Entertainment-General Events": Category.Entertainment,
    "Entertainment-Other Entertainment": Category.Entertainment,
    "Entertainment-Theatrical Events": Category.Entertainment,
    "Entertainment-Theme Parks": Category.Entertainment,
    "Fees & Adjustments-Fees & Adjustments": Category.FeesAdjustments,
    "Merchandise & Supplies-Appliance Stores": Category.Shopping,
    "Merchandise & Supplies-Arts & Jewelry": Category.Shopping,
    "Merchandise & Supplies-Book Stores": Category.Shopping,
    "Merchandise & Supplies-Clothing Stores": Category.Shopping,
    "Merchandise & Supplies-Computer Supplies": Category.Shopping,
    "Merchandise & Supplies-Department Stores": Category.Shopping,
    "Merchandise & Supplies-Florists & Garden": Category.Shopping,
    "Merchandise & Supplies-Furnishing": Category.Shopping,
    "Merchandise & Supplies-General Retail": Category.Shopping,
    "Merchandise & Supplies-Groceries": Category.Groceries,
    "Merchandise & Supplies-Hardware Supplies": Category.Shopping,
    "Merchandise & Supplies-Internet Purchase": Category.Shopping,
    "Merchandise & Supplies-Mail Order": Category.Entertainment,
    "Merchandise & Supplies-Pharmacies": Category.Health,
    "Merchandise & Supplies-Sporting Goods Stores": Category.Shopping,
    "Merchandise & Supplies-Wholesale Stores": Category.Shopping,
    "Other-Government Services": Category.ProfessionalServices,
    "Other-Miscellaneous": Category.Miscellaneous,
    "Restaurant-Bar & Café": Category.Food,
    "Restaurant-Restaurant": Category.Food,
    "Transportation-Auto Services": Category.Automotive,
    "Transportation-Fuel": Category.Automotive,
    "Transportation-Other Transportation": Category.Travel,
    "Transportation-Parking Charges": Category.ProfessionalServices,
    "Transportation-Taxis & Coach": Category.Travel,
    "Travel-Airline": Category.Travel,
    "Travel-Lodging": Category.Travel,
    "Travel-Travel Agencies": Category.Travel,
}


class CategoryHinter:
    """
    Maintains a config of keywords to category mapping.
    These categories can be learned either directly from a bank history or manually by the user.
    """

    def __init__(self, config_path: Path = CATEGORY_HINT_CONFIG_PATH) -> None:
        """Initializes the config.
        Params:
            config_path: The path to the config file that this class maintains.
        """
        self._config_path = config_path

        if not config_path.exists():
            with config_path.open("w"):
                pass

        self._config = yaml.load(config_path)
        if not self._config:
            self._config = {}

    def __enter__(self) -> "CategoryLookupTable":
        """Enter context."""
        return self

    def __exit__(self, *_args) -> None:
        """Exit context; flush contents to file."""
        self.flush()

    def flush(self) -> None:
        """Flushes the table to file."""
        yaml.dump(self._config, self._config_path)

    def _split_description(self, description: str) -> List[str]:
        """Tokenizes the description and strips non alphanumeric characters/whitespaces."""
        description = "".join(c if str.isalnum(c) or str.isspace(c) or c == "&" else " " for c in description)

        def is_valid_word(s: str) -> bool:
            return len(s) > 0 and all(str.isalnum(c) or c == "&" for c in s) and s != "&"

        return list(filter(is_valid_word, description.split(" ")))

    def hint(self, description: str) -> Optional[CategoryMap]:
        """
        Scans the words in the description to determine if there are any key words that are in the config.
        If there are, return the categories mapped to each key word found.
        """
        category_map: CategoryMap = {}
        for word in self._split_description(description):
            if word in self._config:
                category_map[word] = self._config[word]
        return category_map

    def store(self, key: str, category: Category) -> None:
        """Stores a key to category mapping in the table."""
        if " " in key:
            raise RuntimeError(f"{key} can not contain whitespaces, it must be 1 word")
        self._config[key] = category

    def build_hints(self, description_category_map: CategoryMap, do_flush: bool) -> None:
        """
        Builds a category map of individual words from the input category map of descriptions.
        Params:
            description_category_map : A map of descriptions to categories.
            do_flush                 : True to write to file, false to print results.
        """
        # Ensure the necessary databases are downloaded
        nltk.download("punkt", quiet=True)
        nltk.download("averaged_perceptron_tagger", quiet=True)

        # The part of speech tagger instance
        tagger = PerceptronTagger()

        def criteria(word: str) -> bool:
            """
            Each word must meet this criteria to be eligible for hinting.
                1. The word must not be a single letter.
                2. The word must be a noun.
            """
            tag = tagger.tag([word])[0][1]
            # https://stackoverflow.com/questions/15388831/what-are-all-possible-pos-tags-of-nltk/15389153
            return len(word) > 1 and tag.startswith("NN")

        hints: CategoryMap = {}
        for description, category in description_category_map.items():
            for word in self._split_description(description):
                if criteria(word):
                    hints[word] = category

        n: int = 0
        for word, category in hints.items():
            if word not in self._config:
                n += 1
        print(f"{n} new hints discovered")

        # Merge the current set of hints with the new set
        self._config |= hints
        print(f"{len(self._config)} total hints")

        if not do_flush:
            print(self._config)
            return

        # Write to file
        yaml.dump(self._config, self._config_path)


class CategoryLookupTable:
    """
    Builds and maintains a table of keywords to category mapping.
    """

    def __init__(self, config_path: Path = CATEGORY_LOOKUP_TABLE_PATH) -> None:
        """Initializes the config.
        Params:
            config_path: The path to the config file that this class maintains.
        """
        self._config_path = config_path

        if not config_path.exists():
            with config_path.open("w"):
                pass

        self._table: CategoryMap = yaml.load(config_path)
        if not self._table:
            self._table = {}

    def __enter__(self) -> "CategoryLookupTable":
        """Enter context."""
        return self

    def __exit__(self, *_args) -> None:
        """Exit context; flush contents to file."""
        self.flush()

    def flush(self) -> None:
        """Flushes the table to file."""
        yaml.dump(self._table, self._config_path)

    def load(self, key: str) -> Optional[Category]:
        """Looks up the string in the table."""
        return self._table.get(key)

    def store(self, key: str, category: Category) -> None:
        """Stores a key to category mapping in the table."""
        if category == Category.Unknown:
            raise RuntimeError(f"Trying to store {key} with an unknown category")
        self._table[key] = category
