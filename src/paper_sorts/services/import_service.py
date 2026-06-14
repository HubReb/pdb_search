"""Bulk-import service for paper_sorts.

Parses a LaTeX literature overview (``.tex``) and a matching BibTeX file
(``.bib``) and yields one :class:`~paper_sorts.db.repositories.PaperCreate`
DTO per matched entry.

Design decisions:
    - Pure data pipeline — no I/O beyond file reads, no logging to stdout,
      no database interaction.
    - Iterator interface: the caller (``cli/importer.py``) commits per-paper
      inside individual ``with_session`` calls, satisfying constitution
      Principle IV (per-paper commit, partial failure recoverable on rerun).
    - Unmatched cite keys are skipped with a logged warning (spec US5, AC2).
    - BibTeX round-trip is delegated to pybtex; LaTeX conversion to
      pylatexenc.  No custom parsing.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

from pybtex.database import parse_file as parse_bib_file  # type: ignore[import-untyped]
from pylatexenc.latex2text import LatexNodes2Text  # type: ignore[import-untyped]

from paper_sorts.db.repositories import PaperCreate

log = logging.getLogger(__name__)


def _parse_tex(tex_path: str | Path) -> dict[str, dict[str, str]]:
    """Parse a LaTeX literature overview and return a title->metadata dict.

    Expects each entry to follow the pattern::

        \\item *Title \\cite{BibTeXKey}: description

    :param tex_path: path to the ``.tex`` file.
    :returns: dict mapping title -> {``bibtex_id``: key, ``contents``: description}.
    """
    path = Path(tex_path)
    raw = path.read_text(encoding="utf-8")
    text_lines = LatexNodes2Text().latex_to_text(raw).split("\n")
    text_lines = [line for line in text_lines if line.strip()]

    papers: dict[str, dict[str, str]] = defaultdict(lambda: defaultdict(str))  # noqa: PGH003
    title: str | None = None
    bibtex_key: str | None = None

    for line in text_lines:
        if "*" in line and "<cit.>" in line:  # title line
            # Try split[1] first (legacy format: "Title <cit.>: Description" per line)
            after_cit = line.split("<cit.>")[1].rstrip(":").strip()
            before_cit = line.split("<cit.>")[0]
            if after_cit and after_cit != ":":
                # Legacy single-line format: title follows cit somehow; fall back
                # to extracting from before cit.
                title = before_cit.split("*")[-1].strip().rstrip(":")
            else:
                # Normal format: "* *Title <cit.>:" — title is before <cit.>
                title = before_cit.split("*")[-1].strip().rstrip(":")

            # Extract cite key from raw LaTeX
            bibtex_key = None
            for latex_line in raw.split("\n"):
                if title in latex_line and r"\cite{" in latex_line:
                    after_cite = latex_line.split(r"\cite{")[1]
                    bibtex_key = after_cite.split("}")[0]
                    break
        else:
            description = line.strip()
            if description and title and bibtex_key:
                papers[title]["bibtex_id"] = bibtex_key
                papers[title]["contents"] = description
                title = None
                bibtex_key = None

    return papers


def extract_papers_from_tex_bib(
    tex_path: str | Path,
    bib_path: str | Path,
) -> Iterator[PaperCreate]:
    """Parse a ``.tex`` + ``.bib`` file pair and yield one DTO per matched entry.

    Entries whose cite key has no matching record in the ``.bib`` file are
    skipped with a logged warning (constituting per-spec behaviour for
    unmatched keys, US5 AC2).

    :param tex_path: path to the LaTeX literature overview file.
    :param bib_path: path to the BibTeX file containing matching entries.
    :yields: :class:`~paper_sorts.db.repositories.PaperCreate` DTOs ready
        for insertion by the caller.
    """
    papers_dict = _parse_tex(tex_path)
    bib_graph = parse_bib_file(str(bib_path), bib_format="bibtex")

    for title, meta in papers_dict.items():
        key = meta.get("bibtex_id", "")
        if not key:
            log.warning("Skipping entry '%s': no cite key found in .tex.", title)
            continue

        if key not in bib_graph.entries:
            log.warning(
                "Skipping entry '%s' (key '%s'): no matching record in .bib file.",
                title,
                key,
            )
            continue

        entry = bib_graph.entries[key]
        bibtex_str: str = entry.to_string("bibtex")

        authors: list[str] = []
        for person in entry.persons.get("author", []):
            last = person.last_names[0] if person.last_names else ""
            first = person.first_names[0] if person.first_names else ""
            authors.append(f"{last}, {first}" if first else last)

        yield PaperCreate(
            title=title,
            contents=meta.get("contents", ""),
            bibtex_id=key,
            bibtex=bibtex_str,
            authors=authors,
        )
