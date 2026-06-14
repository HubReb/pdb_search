"""Bulk-import service for paper_sorts.

Parses a LaTeX overview file (``*.tex``) and the corresponding BibTeX file
(``*.bib``) and yields one :class:`~paper_sorts.db.repositories.PaperCreate`
per matched entry.  No database access occurs here.

Ported from ``paper_sorts/helpers.py::get_data`` +
``get_bibtex_information`` + ``get_bibtex_information_in_entry``.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

from pybtex.database import parse_file as btex_parse_file
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger(__name__)


def _parse_tex(tex_path: Path) -> dict[str, dict[str, str]]:
    """Extract title → bibtex_id and title → contents from a LaTeX file.

    Looks for rendered text lines containing ``*`` and ``<cit.>`` as title
    markers.  The title is everything between the last ``*`` and the first
    ``<cit.>`` on that line.  The citation key is extracted from the raw LaTeX
    source via ``\\cite{key}`` matching.

    :param tex_path: Path to the ``.tex`` file.
    :return: Nested dict ``{title: {"bibtex_id": ..., "contents": ...}}``.
    """
    raw = tex_path.read_text(encoding="utf-8")
    text_lines = LatexNodes2Text().latex_to_text(raw).split("\n")
    text_lines = [line for line in text_lines if line.strip()]

    papers: dict[str, dict[str, str]] = defaultdict(lambda: defaultdict(str))
    title: str | None = None
    bibtex_key: str | None = None

    for line in text_lines:
        if "*" in line and "<cit.>" in line:
            # Extract title: everything between the last '*' and first '<cit.>'
            before_cit = line.split("<cit.>")[0]
            parts = before_cit.split("*")
            title = parts[-1].rstrip(":").strip()
            if not title:
                continue

            # Find the \cite{} key from the raw source
            for raw_line in raw.split("\n"):
                if title in raw_line and r"\cite{" in raw_line:
                    cite_parts = raw_line.split(r"\cite{")
                    if len(cite_parts) > 1:
                        bibtex_key = cite_parts[1].split("}")[0]
                    break
        else:
            description = line.strip()
            if description and title:
                papers[title]["bibtex_id"] = bibtex_key or ""
                papers[title]["contents"] = description
                title = None
                bibtex_key = None

    return dict(papers)


def extract_papers_from_tex_bib(
    tex_path: str | Path, bib_path: str | Path
) -> Iterator[PaperCreate]:
    """Yield one :class:`~paper_sorts.db.repositories.PaperCreate` per matched entry.

    Parses ``tex_path`` for title/citation pairs and ``bib_path`` for BibTeX
    records.  For each title whose citation key has a matching BibTeX entry,
    yields a :class:`~paper_sorts.db.repositories.PaperCreate`.  Unmatched
    citations are logged as warnings and skipped.

    :param tex_path: Path to the ``.tex`` overview file.
    :param bib_path: Path to the ``.bib`` file.
    :yields: :class:`~paper_sorts.db.repositories.PaperCreate` for each matched entry.
    """
    tex_path = Path(tex_path)
    bib_path = Path(bib_path)

    papers_dict = _parse_tex(tex_path)
    bib_db = btex_parse_file(str(bib_path), bib_format="bibtex")

    bib_keys = set(bib_db.entries.keys())

    for title, meta in papers_dict.items():
        bib_key: str = meta.get("bibtex_id", "")
        if not bib_key:
            logger.warning("No citation key found for title %r — skipping", title)
            continue
        if bib_key not in bib_keys:
            logger.warning(
                "Citation key %r has no matching .bib entry — skipping", bib_key
            )
            continue

        entry = bib_db.entries[bib_key]
        bibtex_str = entry.to_string("bibtex")

        authors: list[str] = []
        for person in entry.persons.get("author", []):
            last = person.last_names[0] if person.last_names else ""
            first = person.first_names[0] if person.first_names else ""
            if last and first:
                authors.append(f"{last}, {first}")
            elif last:
                authors.append(last)

        contents = meta.get("contents", "")

        yield PaperCreate(
            title=title,
            contents=contents,
            bibtex_id=bib_key,
            bibtex=bibtex_str,
            authors=authors,
        )
