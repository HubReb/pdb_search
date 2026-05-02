"""Top-level Typer application — the ``pdbsearch`` console-script entry point.

Resolves :class:`paper_sorts.config.Settings` from the global flags
(``--config`` / ``--key`` / ``--database-url`` / ``--log-level``), runs
:func:`configure_logging` once, builds a SQLAlchemy engine and
sessionmaker from ``settings.database_url``, and stores the sessionmaker
on ``ctx.obj`` so each subcommand can open its own short-lived
:func:`with_session` transaction.

When invoked without a subcommand, drops into the four-option
interactive menu preserved verbatim from the legacy
``UserInteraction.interact`` (per ``contracts/cli-commands.md`` § "Why
only four options"). Delete and import are reachable as Typer
subcommands but deliberately absent from the menu — friction by design.

The menu's ``q`` shortcut is honoured via the ``quit_alias`` parameter
on :func:`paper_sorts.cli.prompts.ask_choice`, which is the only module
permitted to read user input (constitution Principle III v1.3.0).

Dispatching from the menu uses ``ctx.invoke(subcommand)`` so each
subcommand runs with the same Typer context (no fresh CLI parsing) and
its parameters fall back to their defaults — which triggers the
interactive prompt sequence inside the subcommand.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, cast

import typer

from paper_sorts.cli import add as add_cmd
from paper_sorts.cli import delete as delete_cmd
from paper_sorts.cli import importer as import_cmd
from paper_sorts.cli import migrate as migrate_cmd
from paper_sorts.cli import search as search_cmd
from paper_sorts.cli import update as update_cmd
from paper_sorts.cli.prompts import ask_choice
from paper_sorts.config import Settings
from paper_sorts.db.session import make_engine, make_session_factory
from paper_sorts.logging_config import configure_logging

app = typer.Typer(
    name="pdbsearch",
    help="Off-line paper-database searcher.",
    no_args_is_help=False,
)


@app.callback(invoke_without_command=True)
def _root(
    ctx: typer.Context,
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Path to the Fernet-encrypted INI config file.",
        ),
    ] = None,
    key: Annotated[
        Path | None,
        typer.Option(
            "--key",
            "-k",
            help="Path to the Fernet decryption key file (required iff --config is set).",
        ),
    ] = None,
    database_url: Annotated[
        str | None,
        typer.Option(
            "--database-url",
            help="Direct database URL override (highest precedence).",
        ),
    ] = None,
    log_level: Annotated[
        str | None,
        typer.Option("--log-level", help="DEBUG / INFO / WARNING / ERROR."),
    ] = None,
) -> None:
    """Resolve settings, set up logging, and seed the session factory on ``ctx.obj``."""
    kwargs: dict[str, Any] = {}
    if config is not None:
        kwargs["fernet_config"] = config
    if key is not None:
        kwargs["fernet_key"] = key
    if database_url is not None:
        kwargs["database_url"] = database_url
    if log_level is not None:
        kwargs["log_level"] = log_level

    settings = Settings(**kwargs)
    configure_logging(settings)

    engine = make_engine(cast(str, settings.database_url))
    ctx.obj = make_session_factory(engine)

    if ctx.invoked_subcommand is None:
        _run_top_menu(ctx)


def _run_top_menu(ctx: typer.Context) -> None:
    """Drive the legacy four-option menu, dispatching to the subcommand callables.

    ``ctx.invoke(cb)`` is *not* used: typer subcommand functions take
    ``ctx`` as their first positional argument, but click's ``invoke`` only
    auto-injects context when the callable is a registered ``Command``
    (and at this call site the references are the raw functions, not the
    ``app.command``-wrapped ones). Calling each function directly with
    ``ctx`` lets the per-subcommand defaults (``--by``, ``--bib-file``,
    etc.) fall through to ``None`` so each command's interactive prompt
    sequence runs.
    """
    options = [
        "Search the database",
        "Add an entry",
        "Update an entry",
        "(Q)uit",
    ]
    while True:
        print("\nWhat do you want to do?")
        choice = ask_choice("Your choice", options, quit_alias="q")
        match choice:
            case 1:
                search_cmd.search(ctx)
            case 2:
                add_cmd.add(ctx)
            case 3:
                update_cmd.update(ctx)
            case 4:
                return


# Register the UX subcommands as Typer commands. Migrate (T040) and import
# (T044) are registered as subcommands only — deliberately absent from the
# top-level menu, since both are admin/scripted operations, not part of the
# four-option interactive UX (see contracts/cli-commands.md § "Why only
# four options").
app.command(name="search", help="Search papers by author or title.")(search_cmd.search)
app.command(name="add", help="Add a paper to the database.")(add_cmd.add)
app.command(name="update", help="Update a single editable field.")(update_cmd.update)
app.command(name="delete", help="Delete a paper after confirmation.")(delete_cmd.delete)
app.command(name="migrate", help="Apply pending schema migrations.")(migrate_cmd.migrate)
app.command(name="import", help="Bulk-import papers from a .tex + .bib pair.")(import_cmd.import_)


def main() -> None:
    """Console-script entry point — wired in pyproject as ``pdbsearch``."""
    app()


if __name__ == "__main__":
    main()
