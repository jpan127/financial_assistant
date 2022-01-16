import argparse
import cmd
import glob
import pprint
import readline
import sqlite3
import sys
import traceback
from itertools import permutations
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional, Set, Tuple

import click

import analytics
import config
import database
import statement_parser
import bank_history_parser
from category import Category
from category_lookup_table import CategoryLookupTable, CategoryHinter
from statement_puller import determine_transaction_categories


class UsageError(Exception):
    """The command was used incorrectly."""


def _make_parser(required: Set[str] = None) -> argparse.ArgumentParser:
    """Creates a generic parser with database query options.

    Args:
        required: Names of arguments to make as required.

    Returns:
        The parser.
    """
    get_required = lambda s: required and s in required
    parser = argparse.ArgumentParser(exit_on_error=False)
    parser.add_argument("-t", "--tags", default=None, required=get_required("tags"), type=str, help="A comma separated list of tags to match.")
    parser.add_argument("--not-tags", default=None, required=get_required("tags"), type=str, help="A comma separated list of tags to NOT match.")
    parser.add_argument("-c", "--category", default=None, required=get_required("category"), type=str, help="A category to match.")
    parser.add_argument("-n", "--num", default=None, required=get_required("num"), type=int, help="This many to match.")
    parser.add_argument("-p", "--path", default=None, required=get_required("path"), type=click.Path(exists=True, path_type=Path), help="Path to the file to read.")
    parser.add_argument("-d", "--description_pattern", default=None, required=get_required("description_pattern"), type=str, help="The description pattern to match.")
    return parser


def parse_query_args(parser: argparse.ArgumentParser, argv: List[str]) -> Dict[str, Any]:
    """Parses the arguments with the parser.

    Args:
        argv: The argument list to parse.
    Returns:
        The parsed args.
    """
    try:
        args = parser.parse_args(argv)
    except SystemExit:
        # Do not allow the parser to terminate the program
        raise UsageError("Refer to the above argparse error") from None

    # Post process the args
    if args.tags:
        args.tags = args.tags.split(",")
    if args.not_tags:
        args.not_tags = args.not_tags.split(",")
    if args.category:
        args.category = Category[args.category]
    # Strip out unspecified args and convert to a dictionary
    return {k: v for k, v in vars(args).items() if v is not None}


def _help_with_arg_parser(parser: argparse.ArgumentParser, base_help_text: str) -> None:
    """Produce a CLI command help text that also includes the corresponding parser's help menu.

    Args:
        parser: The parser to extrac the help text from.
        base_help_text: The help text without the parser help text, but with a '{}' for where the parser help text should go.
    """
    # It is assumed that the parser help text goes at the end of the [base_help_text],
    # so we only care about the indentation of the last line
    last_line: str = base_help_text.split("\n")[-1]
    num_leading_spaces = len(last_line) - len(last_line.lstrip())
    leading_spaces = num_leading_spaces * " "

    parser_help_text = parser.format_help()
    # Add the leading spaces to all of the lines except the first (will be stripped below)
    indent_generator = (f"{leading_spaces}{line}" if line.strip() else "" for line in parser_help_text.split("\n"))
    parser_help_text = "\n".join(indent_generator)
    # Strip leading/trailing spaces from the parser help text (not per line), and remove the prefix
    parser_help_text = parser_help_text.replace("usage: cli.py ", "").strip()
    print(dedent(base_help_text.format(parser_help_text)))


class CLI(cmd.Cmd):
    """The application's main CLI."""

    intro = "Hi, I am your financial assistant! What would you like to do today?"
    prompt = "> "

    def __init__(self, **kwargs) -> None:
        self._args = kwargs
        self._config = config.load(kwargs["config_path"])
        self._accumulate_parser = _make_parser()
        self._top_parser = _make_parser(required={"num"})
        self._import_bank_history_parser = _make_parser()
        self._debug = kwargs.get("debug", False)
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
                    exception: str = traceback.format_exc() if self._debug else traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1])[0]
                    print(f"Caught exception:\n  {exception}")

            # Save this so it can be invoked directly if necessary
            if base_function := getattr(f, "base_function", None):
                g.base_function = base_function
            else:
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
            "do_import_statement",
            "do_import_bank_history",
        )
        for method in methods_that_require_user:
            setattr(self, method, parse_user_decorator(getattr(self, method)))
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
        if self._config.paths.command_history.exists():
            readline.read_history_file(self._config.paths.command_history)

    def postloop(self) -> None:
        """Callback invoked on tear down."""
        readline.write_history_file(self._config.paths.command_history)

    def help_bootstrap(self) -> None:
        """Help text for [do_bootstrap]."""
        print(
            dedent(
                """\
            A command to either initialize or re-initialize a user's database.
            All 3 commands are run:
                1. import_statement
                2. import_categories
                3. update_categories

            Args:
                1: The user, must be one of the specified users in the config file.
                2: The root directory to search for transaction files.
                   The directory to find transactions files will be {2}/user."""
            )
        )

    def help_import_statement(self) -> None:
        """Help text for [do_import_statement]."""
        print(
            dedent(
                """\
            Provide an OFX file that contains card transactions.
            These transactions will be parsed and stored into the database.
            Keep in mind that OFX transactions do not specify categories so they will need to be populated subsequently.

            Args:
                1: The user, must be one of the specified users in the config file.
                2: The path to the OFX file.
            """
            )
        )

    def help_import_bank_history(self) -> None:
        """Help text for [do_import_bank_history]."""
        print(
            dedent(
                """\
            Provide a CSV file that contains bank history.
            These transactions will be parsed and stored into the database.

            Args:
                1: The user, must be one of the specified users in the config file.
                2: The path to the CSV file.
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
            Provide a CSV file that contains card transactions.
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

    def help_accumulate(self) -> None:
        """Help text for [do_accumulate]."""
        _help_with_arg_parser(
            self._accumulate_parser,
            base_help_text="""\
            Accumulates matched transactions.

            Args:
                1: The user, must be one of the specified users in the config file.
                2...: {}""",
        )

    def help_accumulate_by_month(self) -> None:
        """Help text for [do_accumulate_by_month]."""
        _help_with_arg_parser(
            self._accumulate_parser,
            base_help_text="""\
            Accumulates matched transactions, by month.

            Args:
                1: The user, must be one of the specified users in the config file.
                2...: {}""",
        )

    def help_accumulate_categories_by_month(self) -> None:
        """Help text for [do_accumulate_categories_by_month]."""
        _help_with_arg_parser(
            self._accumulate_parser,
            base_help_text="""\
            Accumulates matched transactions, by month, by category.

            Args:
                1: The user, must be one of the specified users in the config file.
                2...: {}""",
        )

    def help_top(self) -> None:
        """Help text for [do_top]."""
        _help_with_arg_parser(
            self._top_parser,
            base_help_text="""\
            Shows the top N matched transactions.

            Args:
                1: The user, must be one of the specified users in the config file.
                2...: {}""",
        )

    def help_top_by_month(self) -> None:
        """Help text for [do_top_by_month]."""
        _help_with_arg_parser(
            self._top_parser,
            base_help_text="""\
            Shows the top N matched transactions, by month.

            Args:
                1: The user, must be one of the specified users in the config file.
                2...: {}""",
        )

    def help_top_categories_by_month(self) -> None:
        """Help text for [do_top_categories_by_month]."""
        _help_with_arg_parser(
            self._top_parser,
            base_help_text="""\
            Shows the top N matched transactions, by month, by category.

            Args:
                1: The user, must be one of the specified users in the config file.
                2...: {}""",
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
            self.do_import_statement.base_function(user, args=[root_dir / path])
        for path in csv_files:
            self.do_import_categories.base_function(user, args=[root_dir / path])
        self.do_update_categories.base_function(user, [])

    def do_import_statement(self, user: str, args: List[str]):
        """Refer to [help_import_statement] for documentation."""
        maybe_path: Optional[str] = args[0] if args else None
        if path := self._determine_path(maybe_path, expected_extensions=[".ofx", ".qfx"]):
            print(f"Updating database with : {path}")
            transactions = statement_parser.parse(path)
            print(f"Found {len(transactions)} transactions")
            with database.session(db=database.write(user, path=self._config.paths.database, transactions=transactions)):
                pass

    def do_import_bank_history(self, user: str, argv: List[str]):
        """Refer to [help_import_bank_history] for documentation."""
        args = parse_query_args(self._import_bank_history_parser, argv)
        if path := self._determine_path(args.get("path", None), expected_extensions=[".csv"]):
            print(f"Updating database with : {path}")
            transactions = bank_history_parser.parse(path)
            print(f"Found {len(transactions)} transactions")
            with database.session(db=database.write(user, path=self._config.paths.database, transactions=transactions)):
                pass
            analytics.print_bank_stats(analytics.calc_bank_stats(analytics.split_transactions_by_month(transactions)))

    def do_update_categories(self, user: str, _: List[str]):
        """Refer to [help_update_categories] for documentation."""
        unknown_transactions = database.read_unknown_categories(user, path=self._config.paths.database)
        print(f"Found {len(unknown_transactions)} transactions with unknown categories")
        with CategoryLookupTable(config_path=self._config.paths.category_lookup_table) as table, CategoryHinter(config_path=self._config.paths.category_hint_config) as hinter:
            # Determine the known categories for known transactions from the set of unknown transactions, and write to database
            unknown_transactions = determine_transaction_categories(table, hinter, unknown_transactions, do_prompt=False)
            known_transactions = [t for t in unknown_transactions if t.category != Category.Unknown]
            num: int = len(known_transactions)
            database.write(user, path=self._config.paths.database, transactions=known_transactions, do_overwrite=True)  # @TODO: Would be good to only overwrite certain columns

            # Prompt the user for the rest of the unknown transactions
            unknown_transactions[:] = [t for t in unknown_transactions if t.category == Category.Unknown]
            try:
                unknown_transactions = determine_transaction_categories(table, hinter, unknown_transactions, do_prompt=True)
            except KeyboardInterrupt:
                # Allow cancelling the prompting
                pass
            known_transactions[:] = [t for t in unknown_transactions if t.category != Category.Unknown]
            num += len(known_transactions)
            database.write(user, path=self._config.paths.database, transactions=known_transactions, do_overwrite=True)  # @TODO: Would be good to only overwrite certain columns

        print(f"Populated {num} transactions with categories")

    def do_import_categories(self, user: str, args: List[str]):
        """Refer to [help_import_categories] for documentation."""
        maybe_path: Optional[str] = args[0] if args else None
        if path := self._determine_path(maybe_path, expected_extensions=[".csv"]):
            print(f"Importing categories from : {path}")
            with CategoryLookupTable(config_path=self._config.paths.category_lookup_table) as table, CategoryHinter(config_path=self._config.paths.category_hint_config) as hinter:
                transactions = statement_parser.parse(path)
                print(f"Found {len(transactions)} transactions")
                for transaction in transactions:  # pylint: disable=not-an-iterable
                    if transaction.category != Category.Unknown:
                        table.store(transaction.description, transaction.category)

                hinter.build_hints(table._table, do_flush=True)

            count: int = 0
            transaction_map = {transaction.id: transaction for transaction in transactions}  # pylint: disable=not-an-iterable
            unknown_transactions = database.read_unknown_categories(user, path=self._config.paths.database)
            for unknown_transaction in unknown_transactions:
                try:
                    unknown_transaction.category = transaction_map[unknown_transaction.id].category
                    count += 1
                except KeyError:
                    pass
            if count > 0:
                with database.session(self._config.paths.database) as db:
                    database.write(user, db=db, transactions=unknown_transactions, do_overwrite=True)  # @TODO: Would be good to only overwrite certain columns
                print(f"Updated {count} existing transactions with categories")

    def do_exec(self, arg: str) -> None:
        """Refer to [help_exec] for documentation."""
        with database.session(self._config.paths.database) as db:
            # Backup database first
            with database.session(self._config.paths.database_backup) as db_backup:
                with db_backup:
                    db.backup(db_backup)
            with db:
                cursor = db.execute(arg)
                is_query: bool = arg.lower().startswith("select ")
                if is_query:
                    pprint.pprint(database.to_transactions(cursor), width=self._config.terminal_width)

    def do_exec_to_backup(self, user: str, args: List[str]) -> None:
        """Refer to [help_exec_to_backup] for documentation."""
        if not args:
            print("Expected a command arg")
            return
        with database.session(self._config.paths.database, read_only=True) as db, database.session(self._config.paths.database_backup) as db_backup:
            # Copy to backup database, then execute the command on the backup database
            with db_backup:
                db.backup(db_backup)
                cursor = db_backup.execute(" ".join(args))
                is_query: bool = args[0].lower() == "select"
                if is_query:
                    pprint.pprint(database.to_transactions(cursor), width=self._config.terminal_width)
                    return

            with db:
                # Attach the databases to each other so the queries can compare them
                db.execute("attach ? as backup", [str(self._config.paths.database_backup)])
                # Try the different permutations with n=2 (vice versa)
                for a, b in permutations(("main", "backup")):
                    # Determine what is in [a] but not [b]
                    results = database.to_transactions(db.execute(f"select * from (select * from {a}.{user} except select * from {b}.{user})"))
                    n = len(results)
                    print(f"Removed in backup ({n}):" if a == "main" else f"Added in backup ({n}):")
                    pprint.pprint(results, width=self._config.terminal_width)

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
            with database.session(self._config.paths.database) as db:
                database.tag(db, user, format_str=format_str, tag_str=tag, width=self._config.terminal_width)
        except sqlite3.OperationalError as e:
            print("Caught sqlite3.OperationalError:", e)

    def _do_accumulate(self, user: str, argv: List[str], callback: str) -> None:
        args = parse_query_args(self._accumulate_parser, argv)
        with database.session(self._config.paths.database) as db:
            sums, transactions = getattr(analytics.Accumulate(db, user, **args), callback)()
        pprint.pprint(transactions, width=self._config.terminal_width)
        print("Sums: ", end="")
        pprint.pprint(sums, width=self._config.terminal_width)

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
        args = parse_query_args(self._top_parser, argv)
        with database.session(self._config.paths.database) as db:
            transactions = getattr(analytics.Top(db, user, **args), callback)()
        pprint.pprint(transactions, width=self._config.terminal_width)

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
@click.argument("config_path", type=click.Path(exists=True, path_type=Path))
@click.option("-d", "--debug", is_flag=True, help="True to enable stack traces for caught exceptions")
@click.option("-p", "--path", type=click.Path(exists=True, path_type=Path), help="A path to be used for any command that require a path (to avoid typing it interactively).")
def main(**kwargs) -> None:
    """The CLI entrypoint.

    config_path: The path to the application level config.
    """
    cli = CLI(**kwargs)
    try:
        cli.cmdloop()
    finally:
        # Make sure to always call this to save the command history
        cli.postloop()


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
