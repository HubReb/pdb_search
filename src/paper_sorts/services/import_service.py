"""Bulk import service for paper-sorts.

Provides :func:`extract_papers_from_tex_bib` — a generator that parses a
``.tex`` file and a ``.bib`` file and yields :class:`PaperCreate` objects for
each cited paper that has a matching BibTeX record.

Papers whose citation key has no matching ``.bib`` entry are skipped with a
logged warning (spec US5, acceptance scenario 2).

This module has **no** SQLAlchemy imports.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from pathlib import Path

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger(__name__)


def _extract_citation_keys(tex_path: str) -> list[str]:
    r"""Extract all citation keys from a ``.tex`` file.

    Finds ``\cite{key}`` and ``\cite{key1,key2}`` patterns.

    :param tex_path: Filesystem path to the ``.tex`` file.
    :returns: List of citation keys (may contain duplicates).
    :raises FileNotFoundError: If the file does not exist.
    :raises OSError: If the file cannot be read.
    """
    text = Path(tex_path).read_text(encoding="utf-8")
    # Match \cite{key} and \cite{key1,key2,...}
    pattern = re.compile(r"\\cite\{([^}]+)\}")
    keys: list[str] = []
    for match in pattern.finditer(text):
        for key in match.group(1).split(","):
            stripped = key.strip()
            if stripped:
                keys.append(stripped)
    return keys


def _parse_bib_file(bib_path: str) -> dict[str, dict[str, object]]:
    """Parse a ``.bib`` file and return a dict keyed by citation key.

    :param bib_path: Filesystem path to the ``.bib`` file.
    :returns: Dict mapping citation key → entry info dict with keys
        ``bibtex_text``, ``authors``, ``title``.
    :raises FileNotFoundError: If the file does not exist.
    :raises OSError: If the file cannot be read.
    """
    from pybtex.database import parse_file

    bib_graph = parse_file(bib_path, bib_format="bibtex")
    result: dict[str, dict[str, object]] = {}
    for key, entry in bib_graph.entries.items():
        bibtex_text: str = entry.to_string("bibtex")
        authors: list[str] = []
        for person in entry.persons.get("author", []):
            last = " ".join(person.last_names) if person.last_names else ""
            first = " ".join(person.first_names) if person.first_names else ""
            name = f"{last}, {first}".strip(", ")
            if name:
                authors.append(name)
        title = ""
        if "title" in entry.fields:
            from pylatexenc.latex2text import LatexNodes2Text

            title = LatexNodes2Text().latex_to_text(entry.fields["title"])
        result[str(key)] = {
            "bibtex_text": bibtex_text,
            "authors": authors,
            "title": title,
        }
    return result


def extract_papers_from_tex_bib(
    tex_path: str, bib_path: str
) -> Iterator[PaperCreate]:
    """Yield :class:`PaperCreate` for each cited entry found in the ``.bib`` file.

    For each ``\\cite{key}`` reference in *tex_path*, looks up *key* in
    *bib_path*.  If found, yields a :class:`PaperCreate`.  If not found,
    logs a warning and skips the entry (spec US5 acceptance scenario 2).

    Duplicate citation keys in the ``.tex`` are silently skipped after the
    first occurrence.

    :param tex_path: Filesystem path to the ``.tex`` file.
    :param bib_path: Filesystem path to the ``.bib`` file.
    :yields: :class:`PaperCreate` objects for each matched entry.
    :raises FileNotFoundError: If either file does not exist.
    :raises OSError: If either file cannot be read.
    """
    keys = _extract_citation_keys(tex_path)
    bib_entries = _parse_bib_file(bib_path)
    seen: set[str] = set()
    for key in keys:
        if key in seen:
            continue
        seen.add(key)
        if key not in bib_entries:
            logger.warning("Citation key %r not found in %s — skipping", key, bib_path)
            continue
        entry = bib_entries[key]
        raw_authors = entry["authors"]
        authors_list: list[str] = list(raw_authors) if isinstance(raw_authors, list) else []
        yield PaperCreate(
            title=str(entry["title"]) or key,
            authors=authors_list,
            bibtex_key=key,
            summary="",  # .tex bulk import does not capture per-paper summaries
            bibtex_text=str(entry["bibtex_text"]),
        )
