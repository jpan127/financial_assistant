"""Cleans up a CSV with multiline-strings and newlines."""

import csv
import pprint
from pathlib import Path

import click


@click.command(name="clean")
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option("-p", "--print-categories", is_flag=True, help="Just print the description + category")
def cli(path: Path, print_categories: bool) -> None:
    """Cleans up a CSV with multiline-strings and newlines."""
    # Read the CSV file and fully parse it
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=",")
        rows = list(reader)
        # Just print the categories and exit
        if print_categories:
            category_map = {}
            for row in rows:
                if row["Category"] not in category_map:
                    category_map[row["Category"]] = set()
                category_map[row["Category"]].add(row["Description"])
            pprint.pprint(category_map, width=150)
            return
        fieldnames = reader.fieldnames

    # Write to the same path
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            # Minify multiline strings to single line with single whitespaces
            writer.writerow({k: " ".join(v.split()) for k, v in row.items()})


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter
