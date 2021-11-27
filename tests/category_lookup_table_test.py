import pytest

import category_lookup_table


def test__split_description(tmp_path):
    hinter = category_lookup_table.CategoryHinter(config_path=(tmp_path / "tmp.yaml"))
    assert ["PG", "E"] == hinter._split_description("PG & E")
    assert ["PGE"] == hinter._split_description("PGE")
    assert ["PG&E"] == hinter._split_description("PG&E")
    assert ["PG", "E", "PGE", "PG&E"] == hinter._split_description("PG & E PGE PG&E")
