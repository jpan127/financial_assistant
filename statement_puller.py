import datetime
from pathlib import Path
from typing import List, Tuple

import click

from ofxclient.account import Account
from ofxclient.config import OfxConfig

from category import Category
from category_lookup_table import CategoryLookupTable, CategoryHinter
from transaction import Transaction
from statement_parser import parse


def _prompt_user_for_category(hinter: CategoryHinter, table: CategoryLookupTable, description: str, description_suffix: str) -> Category:
    """
    Prompts the user so the user can decide what category this transaction belongs in.
    The user can either:
        1. Skip (do nothing, category is Unknown)
        2. Select the hint for one of the keywords in the description
           The keyword to category mapping will be saved in the hints config
           The description to category mapping will be saved in the lookup table
        3. Select the category for the entire description
           The description to category mapping will be saved in the lookup table

    Params:
        hinter             : Produces hints for the potential categories based on the description.
        table              : Lookup table to store the category for the description.
        description        : The transaction's description.
        description_suffix : A suffix to append to the description.
    Returns:
        The category if chosen.
    """
    SKIP_KEYWORD = "SKIP"
    hints = hinter.hint(description)

    # Add an option to skip
    options: List[Tuple[Category, str]] = [(Category.Unknown, SKIP_KEYWORD)]
    # Add the hints first
    # This will save the category for the word
    options.extend((c, word) for word, c in hints.items())
    # Add the options for all categories last
    # This will save the category for the whole description
    options.extend(((c, "") for c in list(Category)))

    # Format the options
    # Add enumerations for each option
    format_word = lambda word: f"({word})" if word else ""
    option_strs = [f"({i}) {c.name} {format_word(word)}" for i, (c, word) in enumerate(options)]
    option_lines = "\n".join(option_strs)
    options_str = f"\nDescription: {description} {description_suffix}\nChoose which category best fits the description.\n{option_lines}\n"

    # Prompt the user for which category this transaction belongs in
    # Keep prompting until successful
    while True:
        option = input(options_str)
        try:
            category, word = options[int(option)]
            # If skipping, use the unknown category
            if word == SKIP_KEYWORD:
                return category
            # If the selection was for a word, store it as a word hint
            if word:
                hinter.store(word, category)
            # Always save the category for this description
            table.store(description, category)
            return category
        except (ValueError, KeyError):
            pass


def determine_transaction_categories(transactions: List[Transaction], do_prompt: bool) -> List[Transaction]:
    """
    Determines the categories for all unknown categories.

    Args:
        transactions : All the transactions to determine categories for.
        do_prompt    : True to prompt the user for which category is best.
    Returns:
        The modified transactions.
    """
    with CategoryLookupTable() as table, CategoryHinter() as hinter:
        for i, transaction in enumerate(transactions):
            # Try to load from the lookup table first
            if category := table.load(transaction.description):
                transactions[i].category = category
                continue

            # Otherwise prompt the user for what the category should be
            if do_prompt:
                transactions[i].category = _prompt_user_for_category(hinter, table, transaction.description, description_suffix=f"({i} / {len(transactions)})")

    return transactions


def _download(account: Account) -> Path:
    date = datetime.datetime.now().date().strftime("%Y-%m-%d")
    path = Path(__file__).parent / Path("data", date + ".ofx")

    if path.exists():
        print(f"Found cached statement {path}")
        return path

    print(f"Downloading statement to {path}")
    data = account.download(days=365)
    with path.open("wb") as f:
        f.write(data.getvalue().encode())
    return path


def pull(username: str, account_name: str) -> List[Transaction]:
    """
    Pulls the OFX transactions from the specified account.
    The [ofxclient] config must be initialized already by running it from the command line.

    Params:
        username     : The name of the account's owner.
        account_name : The name of the account to pull from.
    Returns:
        The transaction if the account was found.
    """
    # Load the config and the accounts that belong to the config
    config = OfxConfig()
    accounts = list(
        filter(
            lambda a: (a.institution.username == username) and (a.description == account_name),
            config.accounts(),
        )
    )
    if not accounts:
        raise KeyError(f"Did not find the {account_name} account under user {username}")
    if len(accounts) > 1:
        raise RuntimeError(f"Found {len(accounts)} accounts for {account_name} under user {username}")

    # Try to download as much as possible, but Chase for example only returns 30 days
    account = accounts[0]
    statement_path = _download(account)

    # Parse the transactions, this requires reading from the file in binary mode
    transactions = parse(fname=statement_path)
    determine_transaction_categories(transactions)

    return transactions


@click.command()
@click.argument("username", type=str)
@click.argument("account_name", type=str)
def cli(username: str, account_name: str) -> None:
    """CLI for testing the [pull] function."""
    import pprint

    pprint.pprint(pull(username=username, account_name=account_name))


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter
