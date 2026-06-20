"""Root Typer application for pdbsearch.

Registers all subcommands and implements the interactive four-option
top-level menu when invoked with no subcommand.

Startup sequence:
1. Parse global options (--database-url, --log-level, --config, --key).
2. Build Settings (four-source priority chain).
3. Configure logging.
4. Create SQLAlchemy engine.
5. Route to subcommand, or drop into the interactive menu.
"""

import logging
from pathlib import Path

import typer
from sqlalchemy.engine import Engine

from paper_sorts.logging_config import configure_logging

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="pdbsearch",
    help="Off-line paper-database searcher.",
    invoke_without_command=True,
)


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    database_url: str | None = typer.Option(
        None,
        "--database-url",
        envvar="PDBSEARCH_DATABASE_URL",
        help="PostgreSQL connection URL (overrides all other sources).",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        envvar="PDBSEARCH_LOG_LEVEL",
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    ),
    config: Path | None = typer.Option(
        None,
        "--config",
        help="Path to Fernet-encrypted INI config file.",
    ),
    key: Path | None = typer.Option(
        None,
        "--key",
        help="Path to Fernet decryption key file.",
    ),
) -> None:
    """pdbsearch — paper database CLI.

    Invoke without a subcommand to enter the interactive menu.

    :param ctx: Typer context.
    :param database_url: Explicit PostgreSQL URL (highest priority).
    :param log_level: Logging level string.
    :param config: Path to encrypted INI credentials.
    :param key: Path to Fernet key file.
    """
    configure_logging(log_level)

    # Resolve database URL from sources
    url = _resolve_database_url(database_url, config, key)

    if not url:
        if ctx.invoked_subcommand is not None:
            # Subcommands handle missing URL themselves
            ctx.ensure_object(dict)
            ctx.obj["engine"] = None
            return

        print(
            "Error: no database URL configured.\n"
            "Pass --database-url, set PDBSEARCH_DATABASE_URL, or provide "
            "--config + --key for the encrypted credentials file."
        )
        raise typer.Exit(1)

    # Create engine
    from paper_sorts.db.session import get_engine

    engine = get_engine(url)
    ctx.ensure_object(dict)
    ctx.obj["engine"] = engine

    if ctx.invoked_subcommand is not None:
        # Delegate to subcommand
        return

    # Interactive top-level menu
    _run_interactive_menu(engine)


def _resolve_database_url(
    explicit: str | None,
    config_file: Path | None,
    key_file: Path | None,
) -> str:
    """Return the database URL from the highest-priority available source.

    Priority: explicit CLI flag > environment / .env > Fernet INI.

    :param explicit: URL passed directly via --database-url.
    :param config_file: Path to Fernet-encrypted INI.
    :param key_file: Path to Fernet key.
    :returns: Database URL string, or empty string if not found.
    """
    if explicit:
        return explicit

    # Try pydantic-settings (env vars + .env + Fernet INI)
    try:
        from pydantic_settings import PydanticBaseSettingsSource

        from paper_sorts.config import FernetIniSettingsSource, Settings

        if config_file is not None and key_file is not None:
            # Build settings with Fernet source enabled
            _cfg = config_file
            _key = key_file

            class SettingsWithFernet(Settings):
                """Settings variant that adds the Fernet INI source."""

                @classmethod
                def customise_sources(
                    cls,
                    init_settings: PydanticBaseSettingsSource,
                    env_settings: PydanticBaseSettingsSource,
                    dotenv_settings: PydanticBaseSettingsSource,
                    file_secret_settings: PydanticBaseSettingsSource,
                ) -> tuple[PydanticBaseSettingsSource, ...]:
                    """Add Fernet INI as the lowest-priority source."""
                    fernet_source = FernetIniSettingsSource(
                        cls,
                        config_file=_cfg,
                        key_file=_key,
                    )
                    return (
                        init_settings,
                        env_settings,
                        dotenv_settings,
                        fernet_source,
                    )

            settings: Settings = SettingsWithFernet(
                config_file=config_file, key_file=key_file
            )
        else:
            settings = Settings()

        return settings.database_url
    except Exception as exc:
        logger.debug("Settings resolution error: %s", exc)
        return ""


def _run_interactive_menu(engine: Engine) -> None:
    """Run the four-option interactive top-level menu.

    Loops until the user chooses Quit.

    :param engine: Active SQLAlchemy engine.
    """
    from paper_sorts.cli.add import run_add
    from paper_sorts.cli.delete import run_delete
    from paper_sorts.cli.prompts import ask_choice
    from paper_sorts.cli.search import run_search
    from paper_sorts.cli.update import run_update

    print("Welcome! Connected to the database.")

    while True:
        idx = ask_choice(
            ["Search the database", "Add an entry", "Update an entry", "Delete an entry"],
            prompt="Your choice: ",
            quit_label="(Q)uit",
        )

        if idx == -1:
            print("Closing connection...")
            break
        elif idx == 0:
            run_search(engine)
        elif idx == 1:
            run_add(engine)
        elif idx == 2:
            run_update(engine)
        elif idx == 3:
            run_delete(engine)


# Register subcommands
from paper_sorts.cli.add import app as add_app  # noqa: E402
from paper_sorts.cli.delete import app as delete_app  # noqa: E402
from paper_sorts.cli.migrate import app as migrate_app  # noqa: E402
from paper_sorts.cli.search import app as search_app  # noqa: E402
from paper_sorts.cli.update import app as update_app  # noqa: E402

app.add_typer(search_app, name="search")
app.add_typer(add_app, name="add")
app.add_typer(update_app, name="update")
app.add_typer(delete_app, name="delete")
app.add_typer(migrate_app, name="migrate")

# import subcommand registered at end to avoid circular imports
try:
    from paper_sorts.cli.importer import app as importer_app

    app.add_typer(importer_app, name="import")
except ImportError:
    pass
