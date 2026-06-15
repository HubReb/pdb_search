"""Bulk-import service: extract papers from a ``.tex`` + ``.bib`` pair.

``extract_papers_from_tex_bib`` yields one :class:`PaperCreate` per cited entry that has a
matching BibTeX record. Cited keys with no matching ``.bib`` record are skipped with a logged
warning rather than failing the whole import. This mirrors the legacy ``get_data.py`` pipeline:
pylatexenc decodes the LaTeX overview, pybtex parses the BibTeX, and author names are normalised
to ``"Last, First"``.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

from pybtex.database import parse_file
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger(__name__)


def _parse_tex(tex_path: Path) -> dict[str, dict[str, str]]:
    """Extract ``{title: {bibtex_id, contents}}`` from a LaTeX overview file.

    :param tex_path: path to the ``.tex`` file.
    :return: a mapping from title to its BibTeX key and one-line summary.
    """
    raw = tex_path.read_text(encoding="utf-8")
    lines = [
        line.strip() for line in LatexNodes2Text().latex_to_text(raw).split("\n") if line.strip()
    ]
    papers: dict[str, dict[str, str]] = defaultdict(lambda: defaultdict(str))
    title: str | None = None
    bibtex_id: str | None = None
    for line in lines:
        if "*" in line and "<cit.>" in line:
            title = line.split("<cit.>")[1].rstrip(":").strip()
            if not title:
                title = line.split("<cit.>")[0].split("*")[1].strip()
            bibtex_id = _find_cite_key(raw, title)
            continue
        if title is not None and bibtex_id is not None:
            papers[title]["bibtex_id"] = bibtex_id
            papers[title]["contents"] = line
            title, bibtex_id = None, None
    return papers


def _find_cite_key(raw_tex: str, title: str) -> str | None:
    """Find the ``\\cite{...}`` key on the raw LaTeX line containing ``title``.

    :param raw_tex: the raw LaTeX source.
    :param title: the (decoded) title to locate.
    :return: the citation key, or ``None`` if not found.
    """
    for raw_line in raw_tex.split("\n"):
        if title in raw_line and r"\cite{" in raw_line:
            after = raw_line.split(r"\cite{")[1]
            return after.split("}")[0]
    return None


def extract_papers_from_tex_bib(tex: Path, bib: Path) -> Iterator[PaperCreate]:
    """Yield a :class:`PaperCreate` for each cited entry with a matching BibTeX record.

    :param tex: path to the LaTeX literature-overview file.
    :param bib: path to the BibTeX file.
    :return: an iterator of papers ready to persist; unmatched keys are skipped (logged).
    """
    titles_by_key: dict[str, tuple[str, str]] = {}
    for title, values in _parse_tex(tex).items():
        key = values.get("bibtex_id", "")
        if key:
            titles_by_key[key] = (title, values.get("contents", ""))

    bib_data = parse_file(str(bib), bib_format="bibtex")
    for key, (title, contents) in titles_by_key.items():
        if key not in bib_data.entries:
            logger.warning("citation key %s has no matching bib entry - skipping", key)
            continue
        entry = bib_data.entries[key]
        authors = [
            f"{person.last_names[0]}, {person.first_names[0]}"
            if person.first_names
            else person.last_names[0]
            for person in entry.persons.get("author", [])
        ]
        yield PaperCreate(
            title=title,
            summary=contents,
            bibtex_id=key,
            bibtex=entry.to_string("bibtex"),
            authors=authors,
        )
