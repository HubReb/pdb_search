"""Bulk import service for paper_sorts.

Extracts PaperCreate DTOs from a LaTeX file + BibTeX file pair.  Pure
extraction logic — no database access, no I/O prompts.

The LaTeX parsing logic is a modernized port of get_data() from the legacy
helpers.py.  BibTeX parsing uses pybtex; LaTeX-to-text conversion uses
pylatexenc.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator

from pybtex.database import parse_string
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger(__name__)


def _parse_tex_citations(tex_content: str) -> dict[str, dict[str, str]]:
    """Extract title → bibtex_id + contents mapping from LaTeX source.

    Parses a literature-overview .tex file that follows the convention:
      "* <cit.> <title>:" on one line, description on the next.

    Args:
        tex_content: Raw LaTeX source string.

    Returns:
        Dict of {title: {"bibtex_id": key, "contents": description}}.
    """
    text_lines = LatexNodes2Text().latex_to_text(tex_content).split("\n")
    text_lines = [line for line in text_lines if line.strip()]
    papers: dict[str, dict[str, str]] = defaultdict(lambda: defaultdict(str))
    title: str | None = None
    bibtex_key: str | None = None

    for line in text_lines:
        if "*" in line and "<cit.>" in line:
            # Title line: extract title text after <cit.>
            after_cit = line.split("<cit.>")[1].rstrip(":")
            if after_cit == ":":
                after_cit = line.split("<cit.>")[0].split("*")[1]
            title = after_cit.strip()
            # Re-scan raw LaTeX for the \cite{key}
            bibtex_key = None
            for raw_line in tex_content.split("\n"):
                if title in raw_line:
                    parts = raw_line.split(r"\cite{")
                    if len(parts) > 1:
                        candidate = parts[1].split("}")[0]
                        bibtex_key = candidate if "\\item" in parts[0] else parts[0]
                        bibtex_key = bibtex_key.split("}")[0]
                        break
        else:
            description = line.strip()
            if description and title:
                papers[title]["bibtex_id"] = bibtex_key or ""
                papers[title]["contents"] = description
                title = None
                bibtex_key = None

    return papers


def extract_papers_from_tex_bib(
    tex_content: str, bib_content: str
) -> Iterator[PaperCreate]:
    """Extract PaperCreate DTOs from LaTeX + BibTeX source strings.

    Skips entries whose BibTeX key has no matching record in bib_content,
    logging a WARNING for each skip (spec US5 acceptance 2).

    Args:
        tex_content: Full content of the .tex file.
        bib_content: Full content of the .bib file.

    Yields:
        PaperCreate DTOs for entries that have both tex citation and bib record.
    """
    papers_meta = _parse_tex_citations(tex_content)
    bib_db = parse_string(bib_content, bib_format="bibtex")

    for title, meta in papers_meta.items():
        bibtex_id = meta.get("bibtex_id", "")
        if not bibtex_id or bibtex_id not in bib_db.entries:
            logger.warning(
                "BibTeX key %r not found in .bib file — skipping paper %r",
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
            contents=meta.get("contents", ""),
            bibtex_id=bibtex_id,
            bibtex=bibtex_str,
            authors=authors,
        )
