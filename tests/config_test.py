from pathlib import Path

import pytest

import config


def _check_config(c: config.Config, tmp_path: Path) -> None:
    assert c.terminal_width == 127
    assert c.root_data_dir == tmp_path
    assert c.users == ["bob", "mary"]
    assert c.paths.category_hint_config == Path(f"{tmp_path}/category_hint_config.yaml")
    assert c.paths.category_lookup_table == Path(f"{tmp_path}/category_lookup_table.yaml")
    assert c.paths.command_history == Path(f"{tmp_path}/command_history.txt")
    assert c.paths.database == Path(f"{tmp_path}/database.db")
    assert c.paths.database_backup == Path(f"{tmp_path}/database_backup.db")


def test_paths() -> None:
    """Tests [Paths]."""
    paths = config.Paths()
    assert paths.category_hint_config == Path()
    assert paths.category_lookup_table == Path()
    assert paths.command_history == Path()
    assert paths.database == Path()
    assert paths.database_backup == Path()

    paths = config.Paths.make("/a/b/c/d")
    assert paths.category_hint_config == Path("/a/b/c/d/category_hint_config.yaml")
    assert paths.category_lookup_table == Path("/a/b/c/d/category_lookup_table.yaml")
    assert paths.command_history == Path("/a/b/c/d/command_history.txt")
    assert paths.database == Path("/a/b/c/d/database.db")
    assert paths.database_backup == Path("/a/b/c/d/database_backup.db")


def test_config(tmp_path: Path) -> None:
    """Tests [Config]."""
    c = config.Config(terminal_width=127, root_data_dir=tmp_path, users=["bob", "mary"])
    _check_config(c, tmp_path)

    # Passed in paths
    with pytest.raises(config.ConfigError):
        c = config.Config(terminal_width=127, root_data_dir=tmp_path, users=["bob", "mary"], paths=c.paths)

    # Invalid path
    with pytest.raises(config.ConfigError):
        c = config.Config(terminal_width=127, root_data_dir="doesnotexist", users=["bob", "mary"])


def test_load(tmp_path: Path) -> None:
    """Tests [load]."""
    config_path: Path = tmp_path / "config.yaml"
    with (config_path).open("w") as f:
        config.yaml.dump(
            {
                "terminal_width": 127,
                "root_data_dir": str(tmp_path),
                "users": ["bob", "mary"],
            },
            f,
        )

    c = config.load(config_path)
    _check_config(c, tmp_path)
