import argparse
import cmd
import glob
import pprint
import readline
import sqlite3
import traceback
from itertools import permutations
from pathlib import Path
from textwrap import dedent
from typing import List, Optional, Set, Tuple

import click

import analytics
import config
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


# @TODO: Make paths agnostic to caller
DATABASE_PATH = Path("./data/database.db")
DATABASE_BACKUP_PATH = Path("./data/database_backup.db")
COMMAND_HISTORY_PATH = Path("./data/command_history.txt")


class UsageError(Exception):
    """The command was used incorrectly."""


def parse_query_args(args: List[str], required: Set[str] = None) -> argparse.Namespace:
    get_required = lambda s: s in required if required else False
    parser = argparse.ArgumentParser(exit_on_error=False)
    parser.add_argument("-t", "--tags", default=None, required=get_required("tags"), type=str, help="A comma separated list of tags to match.")
    parser.add_argument("-c", "--category", default=None, required=get_required("category"), type=str, help="A category to match.")
    parser.add_argument("-n", "--num", default=None, required=get_required("num"), type=int, help="This many to match.")
    args = parser.parse_args(args)
    if args.tags:
        args.tags = args.tags.split(",")
    if args.category:
        args.category = Category[args.category]
    return {k: v for k, v in vars(args).items() if v is not None}


class CLI(cmd.Cmd):
    """The application's main CLI."""

    intro = "Hi, I am your financial assistant! What would you like to do today?"
    prompt = "> "

    def __init__(self, config_path: Path, **kwargs) -> None:
        self._args = kwargs
        self._config = config.load(config_path)
        super().__init__()

        def parse_user_decorator(f):
            def g(arg: str):
                user, args = self._parse_user(arg)
                if not user:
                    return
                f(user, args)

            # Save this so it can be invoked directly if necessary
            g.base_function = f
            return g

        # Do not allow a command to crash the program
        def try_catch_decorator(f):
            def g(arg: str) -> None:
                try:
                    f(arg)
                except Exception:
                    print(traceback.format_exc())

            # Save this so it can be invoked directly if necessary
            g.base_function = f
            return g

        # Decorate methods that require a user arg
        methods_that_require_user: Tuple[str] = (
            "do_accumulate",
            "do_accumulate_by_month",
            "do_accumulate_categories_by_month",
            "do_bootstrap",
            "do_exec_to_backup",
            "do_import_categories",
            "do_tag",
            "do_top",
            "do_top_by_month",
            "do_top_categories_by_month",
            "do_update_categories",
            "do_update_database",
        )
        for method in methods_that_require_user:
            setattr(self, method, try_catch_decorator(parse_user_decorator(getattr(self, method))))
            setattr(self, f"complete_{method.replace('do_', '')}", self._complete_user)

        # Make sure all commands have a corresponding help method
        attributes = dir(self)
        for method in attributes:
            if method.startswith("do_"):
                if method not in ("do_exit", "do_help"):
                    # Make sure all commands have a corresponding help method
                    if f"help_{method.replace('do_', '')}" not in attributes:
                        raise NotImplementedError(f"Missing help method for {method}")
                    # Make sure no commands can crash the program
                    setattr(self, method, try_catch_decorator(getattr(self, method)))

    def preloop(self) -> None:
        """Callback invoked on set up."""
        if COMMAND_HISTORY_PATH.exists():
            readline.read_history_file(COMMAND_HISTORY_PATH)

    def postloop(self) -> None:
        """Callback invoked on tear down."""
        readline.write_history_file(COMMAND_HISTORY_PATH)

    def help_bootstrap(self) -> None:
        """Help text for [do_bootstrap]."""
        print(
            dedent(
                """\
            A command to either initialize or re-initialize a user's database.
            All 3 commands are run:
                1. update_database
                2. import_categories
                3. update_categories

            Args:
                1: The user, must be one of the specified users in the config file.
                2: The root directory to search for transaction files.
                   The directory to find transactions files will be {2}/user."""
            )
        )

    def help_accumulate(self) -> None:
        """Help text for [do_accumulate]."""
        print(
            dedent(
                """\
            Accumulates matched transactions.

            Args:
                1: The user, must be one of the specified users in the config file.
                2: Any of these options: [aggregate]
                    aggregate: Sums up all the transactions that match the tags.
                        3...: Any number of tags to look up transactions for."""
            )
        )

    def help_bootstrap(self) -> None:
        """Help text for [do_bootstrap]."""
        print("TODO")

    def help_update_database(self) -> None:
        """Help text for [do_update_database]."""
        print(
            dedent(
                """\
            Provide an OFX file that contains bank transactions.
            These transactions will be parsed and stored into the database.
            Keep in mind that OFX transactions do not specify categories so they will need to be populated subsequently.

            Args:
                1: The user, must be one of the specified users in the config file.
                2: The path to the OFX file.
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

            Args:
                1: The user, must be one of the specified users in the config file.
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

            Args:
                1: The user, must be one of the specified users in the config file.
                2. The path to the CSV file, if not already provided when starting the program."""
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
                1: The user, must be one of the specified users in the config file.
                2: A valid SQL command."""
            )
        )

    def help_tag(self) -> None:
        """Help text for [do_tag]."""
        print(
            dedent(
                """\
            Applies a tag to the transactions found in the database with matching descriptions.

            Args:
                1: The user, must be one of the specified users in the config file.
                2: A valid SQL match pattern (i.e. %KEYWORD%).
                3: A single alphanumeric tag keyword."""
            )
        )

    def help_accumulate_by_month(self) -> None:
        """Help text for [do_accumulate_by_month]."""
        print(
            dedent(
                """\
                ."""
            )
        )

    def help_accumulate_categories_by_month(self) -> None:
        """Help text for [do_accumulate_categories_by_month]."""
        print(
            dedent(
                """\
                ."""
            )
        )

    def help_top(self) -> None:
        """Help text for [do_top]."""
        print(dedent("""."""))

    def help_top_by_month(self) -> None:
        """Help text for [do_top_by_month]."""
        print(dedent("""."""))

    def help_top_categories_by_month(self) -> None:
        """Help text for [do_top_categories_by_month]."""
        print(dedent("""."""))

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

    def _parse_user(self, arg: str) -> Optional[Tuple[str, List[str]]]:
        args = arg.split(" ")
        if not args:
            print("First arg must be the user")
            return None, []
        user = args[0]
        if user not in self._config.users:
            print(f"'{user}' is not a valid user ({self._config.users})")
            return None, []
        return user, args[1:]

    def do_bootstrap(self, user: str, args: List[str]) -> None:
        """Refer to [help_bootstrap] for documentation."""
        if not args:
            print("Expected a root path arg")
            return
        root_dir = Path(args[0]) / user
        if not root_dir.exists():
            print(f"Expected {root_dir} to exist")
            return
        ofx_files = glob.glob("*.ofx", root_dir=root_dir)
        qfx_files = glob.glob("*.qfx", root_dir=root_dir)
        csv_files = glob.glob("*.csv", root_dir=root_dir)
        for path in ofx_files + qfx_files:
            self.do_update_database.base_function(user, args=[root_dir / path])
        for path in csv_files:
            self.do_import_categories.base_function(user, args=[root_dir / path])
        self.do_update_categories.base_function(user, [])

    def do_update_database(self, user: str, args: List[str]):
        """Refer to [help_update_database] for documentation."""
        maybe_path: Optional[str] = args[0] if args else None
        if path := self._determine_path(maybe_path, expected_extensions=[".ofx", ".qfx"]):
            print(f"Updating database with : {path}")
            transactions = parse(path)
            print(f"Found {len(transactions)} transactions")
            with database.session(db=database.write(user, path=DATABASE_PATH, transactions=transactions)):
                pass

    def do_update_categories(self, user: str, _: List[str]):
        """Refer to [help_update_categories] for documentation."""
        unknown_transactions = database.read_unknown_categories(user, path=DATABASE_PATH)
        print(f"Found {len(unknown_transactions)} transactions with unknown categories")
        unknown_transactions = determine_transaction_categories(unknown_transactions, do_prompt=False)
        try:
            unknown_transactions = determine_transaction_categories(unknown_transactions, do_prompt=True)
        except KeyboardInterrupt:
            # Allow cancelling the prompting
            pass

        database.write(user, path=DATABASE_PATH, transactions=unknown_transactions, do_overwrite=True)
        print(f"Populated {len(unknown_transactions)} transactions with categories")

    def do_import_categories(self, user: str, args: List[str]):
        """Refer to [help_import_categories] for documentation."""
        maybe_path: Optional[str] = args[0] if args else None
        if path := self._determine_path(maybe_path, expected_extensions=[".csv"]):
            print(f"Importing categories from : {path}")
            with CategoryLookupTable(config_path=CATEGORY_LOOKUP_TABLE_PATH) as table:
                transactions = parse(path)
                print(f"Found {len(transactions)} transactions")
                for transaction in transactions:  # pylint: disable=not-an-iterable
                    if transaction.category != Category.Unknown:
                        table.store(transaction.description, transaction.category)

                hinter = CategoryHinter(config_path=CATEGORY_HINT_CONFIG_PATH)
                hinter.build_hints(table._table, do_flush=True)

            count: int = 0
            transaction_map = {transaction.id: transaction for transaction in transactions}  # pylint: disable=not-an-iterable
            unknown_transactions = database.read_unknown_categories(user, path=DATABASE_PATH)
            for unknown_transaction in unknown_transactions:
                try:
                    unknown_transaction.category = transaction_map[unknown_transaction.id].category
                    count += 1
                except KeyError:
                    pass
            if count > 0:
                with database.session(DATABASE_PATH) as db:
                    database.write(user, db=db, transactions=unknown_transactions, do_overwrite=True)
                print(f"Updated {count} existing transactions with categories")

    def do_exec(self, arg: str) -> None:
        """Refer to [help_exec] for documentation."""
        with database.session(DATABASE_PATH) as db:
            # Backup database first
            with database.session(DATABASE_BACKUP_PATH) as db_backup:
                with db_backup:
                    db.backup(db_backup)
            with db:
                cursor = db.execute(arg)
                is_query: bool = arg.lower().startswith("select ")
                if is_query:
                    pprint.pprint(database.to_transactions(cursor), width=150)  # TODO: Make a config for this

    def do_exec_to_backup(self, user: str, args: List[str]) -> None:
        """Refer to [help_exec_to_backup] for documentation."""
        if not args:
            print("Expected a command arg")
            return
        with database.session(DATABASE_PATH, read_only=True) as db, database.session(DATABASE_BACKUP_PATH) as db_backup:
            # Copy to backup database, then execute the command on the backup database
            with db_backup:
                db.backup(db_backup)
                cursor = db_backup.execute(" ".join(args))
                is_query: bool = args[0].lower() == "select"
                if is_query:
                    pprint.pprint(database.to_transactions(cursor), width=150)  # TODO: Make a config for this
                    return

            with db:
                # Attach the databases to each other so the queries can compare them
                db.execute("attach ? as backup", [str(DATABASE_BACKUP_PATH)])
                # Try the different permutations with n=2 (vice versa)
                for a, b in permutations(("main", "backup")):
                    # Determine what is in [a] but not [b]
                    results = database.to_transactions(db.execute(f"select * from (select * from {a}.{user} except select * from {b}.{user})"))
                    n = len(results)
                    print(f"Removed in backup ({n}):" if a == "main" else f"Added in backup ({n}):")
                    pprint.pprint(results, width=150)  # TODO: Make a config for this

    def do_tag(self, user: str, args: List[str]):
        """Refer to [help_tag] for documentation."""
        try:
            format_str, tag = args[0], args[1]
            if not tag.isalnum():
                print("Expecting tag to be alphanumeric")
                return
        except IndexError:
            print("Expecting arguments {format_str tag}")
            return

        try:
            with database.session(DATABASE_PATH) as db:
                database.tag(db, user, format_str=format_str, tag_str=tag)
        except sqlite3.OperationalError as e:
            print("Caught sqlite3.OperationalError:", e)

    def _do_accumulate(self, user: str, argv: List[str], callback: str) -> None:
        if not argv:
            raise UsageError("Expected multiple args")
        args = parse_query_args(argv)
        with database.session(DATABASE_PATH) as db:
            sums, transactions = getattr(analytics.Accumulate(db, user, **args), callback)()
        pprint.pprint(transactions, width=150)
        pprint.pprint(sums, width=150)

    def do_accumulate(self, user: str, args: List[str]) -> None:
        """Refer to [help_accumulate] for documentation."""
        self._do_accumulate(user, args, "__call__")

    def do_accumulate_by_month(self, user: str, args: List[str]) -> None:
        """Refer to [help_accumulate_by_month] for documentation."""
        self._do_accumulate(user, args, "by_month")

    def do_accumulate_categories_by_month(self, user: str, args: List[str]) -> None:
        """Refer to [help_accumulate_categories_by_month] for documentation."""
        if len(args) > 1 and args[0] == "category":
            raise UsageError("This command does not accept a 'category' argument")
        self._do_accumulate(user, args, "categories_by_month")

    def _do_top(self, user: str, argv: List[str], callback: str) -> None:
        if not argv:
            raise UsageError("Expected multiple args")
        args = parse_query_args(argv, required=set(("num")))
        with database.session(DATABASE_PATH) as db:
            transactions = getattr(analytics.Top(db, user, **args), callback)()
        pprint.pprint(transactions, width=150)

    def do_top(self, user: str, args: List[str]) -> None:
        """Refer to [help_top] for documentation."""
        self._do_top(user, args, "__call__")

    def do_top_by_month(self, user: str, args: List[str]) -> None:
        """Refer to [help_top_by_month] for documentation."""
        self._do_top(user, args, "by_month")

    def do_top_categories_by_month(self, user: str, args: List[str]) -> None:
        """Refer to [help_top_categories_by_month] for documentation."""
        if len(args) > 1 and args[0] == "category":
            raise UsageError("This command does not accept a 'category' argument")
        self._do_top(user, args, "categories_by_month")

    def do_exit(self, _: str) -> bool:
        """Exit the program."""
        return True

    def _complete_user(self, text, line, *_) -> List[str]:
        """Tab completion for commands where the first argument must be the user.

        Refer to the [cmd] documentation for the args.
        """
        # Once the first word is completed, don't auto complete anymore
        if line.count(" ") > 1:
            return []
        # Return all users that match the prefix
        return [user for user in self._config.users if user.startswith(text)] if text else self._config.users


@click.command()
@click.option("-p", "--path", type=click.Path(exists=True))
@click.option("-c", "--config_path", type=click.Path(exists=True), default="./data/config.yaml")
def main(path: str, config_path: str) -> None:
    """The CLI entrypoint."""
    kwargs = {}
    if path:
        kwargs["path"] = Path(path)

    cli = CLI(config_path=Path(config_path), **kwargs)
    try:
        cli.cmdloop()
    finally:
        # Make sure to always call this to save the command history
        cli.postloop()


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
