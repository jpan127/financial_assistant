"""Application-wide configuration."""

import dataclasses
from pathlib import Path
from typing import List

from ruamel.yaml import YAML

yaml = YAML(typ="safe")


@dataclasses.dataclass
class Config:
    """A user-defined configuration for this application."""

    users: List[str] = dataclasses.field(default=list)  # A list of predefined users that the transactions belong to


class ConfigError(Exception):
    """When a config is invalid."""


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
        return Config(users=data["users"])
    except KeyError as e:
        raise ConfigError from e
