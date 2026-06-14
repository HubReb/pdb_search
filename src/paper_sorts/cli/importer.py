"""Import subcommand for paper_sorts CLI.

Bulk-imports papers from a LaTeX .tex file + BibTeX .bib file pair.
Per-paper commits ensure partial failures leave inserted papers intact.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from paper_sorts.db.session import with_session
from paper_sorts.services import import_service, paper_service

logger = logging.getLogger(__name__)
app = typer.Typer(help="Bulk-import papers from a LaTeX + BibTeX file pair.")
_console = Console()


@app.callback(invoke_without_command=True)
def import_callback(
    ctx: typer.Context,
    tex: Annotated[Path, typer.Option("--tex", help="Path to .tex file")] = Path(""),
    bib: Annotated[Path, typer.Option("--bib", help="Path to .bib file")] = Path(""),
) -> None:
    """Run bulk import when invoked as subcommand.

    Args:
        ctx: Typer context.
        tex: Path to the LaTeX file.
        bib: Path to the BibTeX file.
    """
    if ctx.invoked_subcommand is None:
        from paper_sorts.cli.app import get_database_url

        run_import(get_database_url(), tex_path=tex, bib_path=bib)


def run_import(
    database_url: str,
    tex_path: Path,
    bib_path: Path,
) -> None:
    """Bulk import from a .tex + .bib file pair.

    Uses per-paper sessions so a failure on one paper leaves previously
    imported papers intact (spec US5 acceptance 3).

    Args:
        database_url: SQLAlchemy connection string.
        tex_path: Path to the LaTeX .tex file.
        bib_path: Path to the BibTeX .bib file.
    """
    if not tex_path or not str(tex_path):
        _console.print("[red]Please provide --tex path.[/red]")
        raise typer.Exit(1)
    if not bib_path or not str(bib_path):
        _console.print("[red]Please provide --bib path.[/red]")
        raise typer.Exit(1)

    try:
        tex_content = tex_path.read_text(encoding="utf-8")
        bib_content = bib_path.read_text(encoding="utf-8")
    except OSError as exc:
        _console.print(f"[red]Could not read file: {exc}[/red]")
        raise typer.Exit(1) from exc

    papers_iter = import_service.extract_papers_from_tex_bib(tex_content, bib_content)

    imported = 0
    skipped = 0
    for paper in papers_iter:
        try:
            with with_session(database_url) as session:
                paper_service.add_paper(session, paper)
            imported += 1
        except Exception as exc:
            logger.warning("Failed to import paper %r: %s", paper.bibtex_id, exc)
            skipped += 1

    _console.print(f"[green]Imported {imported} papers. Skipped {skipped} entries.[/green]")
