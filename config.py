"""Application-wide configuration."""

import dataclasses
from pathlib import Path
from typing import Generator, List

from ruamel.yaml import YAML

yaml = YAML(typ="safe")


class ConfigError(Exception):
    """When a config is invalid."""


@dataclasses.dataclass(frozen=True)
class Paths:
    """All application-specific paths."""

    category_hint_config: Path = dataclasses.field(default=Path())
    category_lookup_table: Path = dataclasses.field(default=Path())
    command_history: Path = dataclasses.field(default=Path())
    database: Path = dataclasses.field(default=Path())
    database_backup: Path = dataclasses.field(default=Path())

    @staticmethod
    def make(root_dir: Path) -> "Paths":
        """Factory function.

        Args:
            root_dir: The root directory for all the paths in this object.
        Returns:
            The constructed object.
        """
        return Paths(
            category_hint_config=Path(f"{root_dir}/category_hint_config.yaml"),
            category_lookup_table=Path(f"{root_dir}/category_lookup_table.yaml"),
            command_history=Path(f"{root_dir}/command_history.txt"),
            database=Path(f"{root_dir}/database.db"),
            database_backup=Path(f"{root_dir}/database_backup.db"),
        )


@dataclasses.dataclass
class Config:
    """A user-defined configuration for this application."""

    terminal_width: int  # The max width for pretty printing
    root_data_dir: Path  # An absolute path to a directory to read/store user-specific files
    users: List[str] = dataclasses.field(default=list)  # A list of predefined users that the transactions belong to

    # This is populated automatically
    paths: Paths = dataclasses.field(default=Paths())

    @staticmethod
    def field_names() -> Generator[List[str], None, None]:
        """Gets names of all fields that are set by the parent.

        Returns:
            A list of all the names.
        """
        for field in dataclasses.fields(Config):
            if field.name != "paths":
                yield field.name

    def __post_init__(self) -> None:
        """Runs after initialization."""
        self.root_data_dir = Path(self.root_data_dir)
        if not self.root_data_dir.exists():
            raise ConfigError(f"{self.root_data_dir} is not a valid directory")
        if self.paths != Paths():
            raise ConfigError("'paths' is not meant to be set on construction")
        self.paths = Paths.make(self.root_data_dir)


def load(path: Path) -> Config:
    """Loads a config from file.

    Args:
        path: The path to the config
    Returns:
        The config if successful.
    Raises:
        ConfigError: If the config did not load successfully.
    """
    with path.open("r") as f:
        data = yaml.load(f)
    try:
        return Config(**{name: data[name] for name in Config.field_names()})
    except KeyError as e:
        raise ConfigError from e
