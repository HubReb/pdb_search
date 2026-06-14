"""Bulk import service for paper_sorts.

Extracts paper metadata from a LaTeX literature overview + BibTeX file pair.
Returns an iterator of PaperCreate DTOs suitable for inserting via paper_service.

Constraints (constitution Principle I):
- No sqlalchemy imports — pure transformation, no DB access.
- No rich, no I/O — caller provides file contents as strings.
- No config imports.

Usage::

    with open("overview.tex") as f:
        tex = f.read()
    with open("references.bib") as f:
        bib = f.read()

    for paper in extract_papers_from_tex_bib(tex, bib):
        paper_service.add_paper(db_url, paper)
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator

from pybtex.database import parse_string
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger(__name__)


def _parse_tex(tex_content: str) -> dict[str, dict[str, str]]:
    """Extract {title: {bibtex_id, contents}} from a LaTeX file.

    Parses the LaTeX content using the same heuristic as the legacy
    ``helpers.get_data()`` / ``get_data.get_data()``:
    - Lines with ``*`` and ``<cit.>`` are titles.
    - The following non-empty line is the paper summary.
    - The BibTeX key is extracted from the ``\\cite{...}`` in the source line.

    Args:
        tex_content: Raw LaTeX file content as a string.

    Returns:
        Mapping of title string → dict with keys 'bibtex_id' and 'contents'.
    """
    text_lines = LatexNodes2Text().latex_to_text(tex_content).split("\n")
    text_lines = [line for line in text_lines if line.strip()]

    papers: dict[str, dict[str, str]] = defaultdict(dict)
    title: str | None = None
    bibtex_key: str | None = None

    for line in text_lines:
        if "*" in line and "<cit.>" in line:
            # Title line: format is "* <title> <cit.>[: <description>]"
            # The title is the text between the last '*' and '<cit.>'
            parts = line.split("<cit.>")
            # Extract title from the part before <cit.>
            before_cit = parts[0]
            # Strip leading asterisks and whitespace
            raw_title = before_cit.split("*")[-1].strip()
            title = raw_title if raw_title else None

            # Extract inline description if present after ':'
            inline_desc: str | None = None
            if len(parts) > 1:
                after_cit = parts[1].strip()
                if after_cit.startswith(":"):
                    inline_desc = after_cit[1:].strip() or None

            # Find the cite key in the original LaTeX source
            bibtex_key = None
            for latex_line in tex_content.split("\n"):
                if title and title in latex_line and r"\cite{" in latex_line:
                    after_cite = latex_line.split(r"\cite{")[1]
                    bibtex_key = after_cite.split("}")[0]
                    break

            # If we have an inline description, record the paper now
            if title and bibtex_key and inline_desc:
                papers[title]["bibtex_id"] = bibtex_key
                papers[title]["contents"] = inline_desc
                title = None
                bibtex_key = None
                inline_desc = None

        elif title and line.strip():
            # Description line (when description is on the next line)
            contents = line.strip()
            if bibtex_key:
                papers[title]["bibtex_id"] = bibtex_key
                papers[title]["contents"] = contents
            title = None
            bibtex_key = None

    return papers


def extract_papers_from_tex_bib(
    tex_content: str,
    bib_content: str,
) -> Iterator[PaperCreate]:
    """Extract PaperCreate DTOs from a LaTeX file and its corresponding BibTeX file.

    For each paper entry identified in the LaTeX file, looks up the matching
    BibTeX record by citation key. Entries with no matching BibTeX record are
    skipped with a logged WARNING (per spec US5 acceptance scenario 2).

    Args:
        tex_content: Full text of the LaTeX literature overview file.
        bib_content: Full text of the BibTeX bibliography file.

    Yields:
        PaperCreate DTO for each paper that has a matching BibTeX record.

    Notes:
        No I/O is performed — callers are responsible for reading the files.
        No database access — callers insert via paper_service.add_paper().
    """
    papers_dict = _parse_tex(tex_content)

    bib_db = parse_string(bib_content, bib_format="bibtex")

    for title, meta in papers_dict.items():
        bibtex_id = meta.get("bibtex_id", "")
        contents = meta.get("contents", "")

        if not bibtex_id:
            logger.warning("No BibTeX key found for paper %r — skipping.", title)
            continue

        if bibtex_id not in bib_db.entries:
            logger.warning(
                "BibTeX key %r (for paper %r) has no matching .bib record — skipping.",
                bibtex_id,
                title,
            )
            continue

        entry = bib_db.entries[bibtex_id]
        bibtex_str = entry.to_string("bibtex")

        authors: list[str] = []
        for person in entry.persons.get("author", []):
            last = " ".join(person.last_names)
            first = " ".join(person.first_names)
            authors.append(f"{last}, {first}" if first else last)

        yield PaperCreate(
            title=title,
            contents=contents,
            bibtex_id=bibtex_id,
            bibtex=bibtex_str,
            authors=authors,
        )
