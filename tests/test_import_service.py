"""Bulk-import tests (US5): extraction, per-paper commit, skip + idempotency."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine

from paper_sorts.cli.importer import run_import
from paper_sorts.services.import_service import extract_papers_from_tex_bib
from paper_sorts.services.paper_service import PaperService

TEX = r"""
\begin{itemize}
\item \textbf{First Paper Title} \cite{first2020}:
A summary of the first paper.
\item \textbf{Second Paper Title} \cite{second2021}:
A summary of the second paper.
\item \textbf{Unmatched Paper Title} \cite{missing2099}:
This one has no bib entry.
\end{itemize}
"""

BIB = """
@article{first2020,
  title={First Paper Title},
  author={Alpha, A. and Beta, B.},
  year={2020}
}
@article{second2021,
  title={Second Paper Title},
  author={Gamma, G.},
  year={2021}
}
"""


def _write_pair(tmp_path: Path) -> tuple[str, str]:
    """Write the fixture ``.tex`` and ``.bib`` files.

    :param tmp_path: pytest temp directory.
    :returns: ``(tex_path, bib_path)`` as strings.
    """
    tex = tmp_path / "lit.tex"
    bib = tmp_path / "refs.bib"
    tex.write_text(TEX, encoding="utf-8")
    bib.write_text(BIB, encoding="utf-8")
    return str(tex), str(bib)


def test_extractor_skips_unmatched_keys(tmp_path: Path) -> None:
    """Only cited entries with a matching bib record are yielded."""
    tex_path, bib_path = _write_pair(tmp_path)
    papers = list(extract_papers_from_tex_bib(tex_path, bib_path))
    keys = {p.bibtex_id for p in papers}
    assert keys == {"first2020", "second2021"}
    first = next(p for p in papers if p.bibtex_id == "first2020")
    assert "Alpha, A." in first.authors


def test_import_inserts_and_is_idempotent(engine: Engine, tmp_path: Path) -> None:
    """Import inserts matched papers; a rerun does not duplicate them."""
    tex_path, bib_path = _write_pair(tmp_path)
    inserted = run_import(engine, tex_path, bib_path)
    assert inserted == 2

    service = PaperService(engine)
    assert len(service.search_by_title("First Paper Title")) == 1
    assert len(service.search_by_author("Gamma, G.")) == 1

    # Rerun: existing keys skipped, no duplicates created.
    reinserted = run_import(engine, tex_path, bib_path)
    assert reinserted == 0
    assert len(service.search_by_title("First Paper Title")) == 1
