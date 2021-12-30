import cmd
import pprint
import sqlite3
from itertools import permutations
from pathlib import Path
from textwrap import dedent
from typing import List, Optional

import click

import database
from category import Category
from category_lookup_table import (
    CategoryLookupTable,
    CategoryHinter,
    CATEGORY_LOOKUP_TABLE_PATH,
    CATEGORY_HINT_CONFIG_PATH,
)
from statement_parser import parse
from statement_puller import determine_transaction_categories


DATABASE_PATH = Path("./data/database.db")
DATABASE_BACKUP_PATH = Path("./data/database_backup.db")


class CLI(cmd.Cmd):
    intro = "Hi, I am your financial assistant! What would you like to do today?"
    prompt = "> "

    def __init__(self, **kwargs) -> None:
        self._args = kwargs
        super().__init__()

    def help_update_database(self) -> None:
        """Help text for [do_update_database]."""
        print(
            dedent(
                """\
            Provide an OFX file that contains bank transactions.
            These transactions will be parsed and stored into the database.
            Keep in mind that OFX transactions do not specify categories so they will need to be populated subsequently.
            """
            )
        )

    def help_update_categories(self) -> None:
        """Help text for [do_update_categories]."""
        print(
            dedent(
                """\
            Using the current state of the category lookup table (known categories),
            attempt to comb through the database for transactions with unknown categories
            and populate any now-known categories.  This program will prompt the user for
            any remaining transactions with unknown categories.  The result will be saved,
            and the program will learn from it.
            """
            )
        )

    def help_import_categories(self) -> None:
        """Help text for [do_import_categories]."""
        print(
            dedent(
                """\
            Provide a CSV file that contains bank transactions.
            It is expected that the CSV contains categories for each transaction.
            These transactions will be parsed and their categories mapped to their description.
            It will be known that this exact description matches this category.
            The description will be broken down into individual words,
            and these words will be known to suggest but not definitively affirm this category.
            Example:
                Description: DOORDASH * SUPER GOOD SUSHI
                Category: Food
            Result:
                "DOORDASH * SUPER GOOD SUSHI" | will be mapped to Food
                "DOORDASH"                    | will be saved as a keyword suggesting Food
                "SUPER"                       | will be saved as a keyword suggesting Food
                "GOOD"                        | will be saved as a keyword suggesting Food
                "SUSHI"                       | will be saved as a keyword suggesting Food
            """
            )
        )

    def help_exec(self) -> None:
        """Help text for [do_exec]."""
        print(
            dedent(
                """\
            Executes an arbitrary command with the database.
            Args:
                1: A valid SQL command."""
            )
        )

    def help_exec_to_backup(self) -> None:
        """Help text for [do_exec_to_backup]."""
        print(
            dedent(
                """\
            Executes an arbitrary command with a copy of the database and prints the differences.
            Args:
                1: A valid SQL command."""
            )
        )

    def help_tag(self) -> None:
        """Help text for [do_tag]."""
        print(
            dedent(
                """\
            Applies a tag to the transactions found in the database with matching descriptions.
            Args:
                1: A valid SQL match pattern (i.e. %KEYWORD%).
                2: A single alphanumeric tag keyword."""
            )
        )

    def _determine_path(self, arg: str, expected_extensions: List[str]) -> Optional[Path]:
        def check_extension(path: Path) -> Optional[Path]:
            if any(path.suffix.lower() == ext.lower() for ext in expected_extensions):
                return path
            print(f"{path} does not have an expected extension {expected_extensions}")
            return None

        if path := self._args.get("path"):
            print(f"Using previously supplied {path}")
            return check_extension(path)

        if not arg:
            print("Expecting a path argument")
            return None
        path = Path(arg)
        if not path.exists():
            print(f"{path} is not a valid path")
            return None
        return check_extension(path)

    def do_update_database(self, arg: str):
        """Refer to [help_update_database] for documentation."""
        if path := self._determine_path(arg, expected_extensions=[".ofx"]):
            print(f"Updating database with : {path}")
            transactions = parse(path)
            with database.session(db=database.write(path=DATABASE_PATH, transactions=transactions)) as db:
                db.close()

    def do_update_categories(self, _: str):
        """Refer to [help_update_categories] for documentation."""
        unknown_transactions = database.read_unknown_categories(path=DATABASE_PATH)
        print(f"Found {len(unknown_transactions)} transactions with unknown categories")
        determine_transaction_categories(unknown_transactions)
        database.write(path=DATABASE_PATH, transactions=unknown_transactions, do_overwrite=True)
        print(f"Populated {len(unknown_transactions)} transactions with categories")

    def do_import_categories(self, arg: str):
        """Refer to [help_import_categories] for documentation."""
        if path := self._determine_path(arg, expected_extensions=[".csv"]):
            print(f"Importing categories from : {path}")
            with CategoryLookupTable(config_path=CATEGORY_LOOKUP_TABLE_PATH) as table:
                transactions = parse(path)
                print(f"Found {len(transactions)} transactions")
                for transaction in transactions:
                    if transaction.category != Category.Unknown:
                        table.store(transaction.description, transaction.category)

                hinter = CategoryHinter(config_path=CATEGORY_HINT_CONFIG_PATH)
                hinter.build_hints(table._table, do_flush=True)

    def do_exec(self, arg: str) -> None:
        """Refer to [help_exec] for documentation."""
        with database.session(DATABASE_PATH) as db:
            with db:
                db.execute(arg)

    def do_exec_to_backup(self, arg: str) -> None:
        """Refer to [help_exec_to_backup] for documentation."""
        with database.session(DATABASE_PATH, read_only=True) as db, database.session(DATABASE_BACKUP_PATH) as db_backup:
            # Copy to backup database, then execute the command on the backup database
            with db_backup:
                db.backup(db_backup)
                db_backup.execute(arg)
            with db:
                # Attach the databases to each other so the queries can compare them
                db.execute("attach ? as backup", [str(DATABASE_BACKUP_PATH)])
                # Try the different permutations with n=2 (vice versa)
                for a, b in permutations(("main", "backup")):
                    # Determine what is in [a] but not [b]
                    results = database.to_transactions(db.execute(f"select * from (select * from {a}.transactions except select * from {b}.transactions)"))
                    n = len(results)
                    print(f"Removed in backup ({n}):" if a == "main" else f"Added in backup ({n}):")
                    pprint.pprint(results, width=150)  # TODO: Make a config for this

    def do_tag(self, arg: str) -> None:
        """Refer to [help_tag] for documentation."""
        try:
            format_str, tag = arg.split(" ")
            if not tag.isalnum():
                print("Expecting tag to be alphanumeric")
                return
        except ValueError:
            print("Expecting arguments {format_str tag}")
            return

        try:
            with database.session(DATABASE_PATH) as db:
                database.tag(db, format_str=format_str, tag_str=tag)
        except sqlite3.OperationalError as e:
            print("Caught sqlite3.OperationalError:", e)

    def do_exit(self, _: str) -> bool:
        """Exit the program."""
        return True


@click.command()
@click.option("-p", "--path", type=click.Path(exists=True))
def cli(path: str) -> None:
    kwargs = {}
    if path:
        kwargs["path"] = Path(path)
    CLI(**kwargs).cmdloop()


if __name__ == "__main__":
    cli()
