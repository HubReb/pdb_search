"""Bulk import service for paper_sorts.

Parses a LaTeX literature-overview .tex file and a BibTeX .bib file,
yielding PaperCreate DTOs for each successfully matched entry.

Pure function — no I/O side effects, no database interaction.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

from pybtex.database import parse_file as parse_bib_file
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger(__name__)


def _parse_tex(tex_path: Path) -> dict[str, dict[str, str]]:
    """Parse a .tex file and extract title → {bibtex_id, contents} mapping.

    Mirrors the logic of the legacy helpers.get_data() function.

    :param tex_path: Path to the .tex file.
    :returns: Dict mapping title strings to {bibtex_id, contents}.
    """
    raw = tex_path.read_text(encoding="utf-8")
    text_lines = [
        line
        for line in LatexNodes2Text().latex_to_text(raw).split("\n")
        if line.strip()
    ]
    papers: dict[str, dict[str, str]] = defaultdict(dict)
    title: str | None = None
    bibtex_key: str | None = None

    for line in text_lines:
        if "*" in line and "<cit.>" in line:
            # Title line — format: "* <title> <cit.>[:]"
            parts = line.split("<cit.>")
            candidate = parts[1].rstrip(":").strip()
            if not candidate or candidate == ":":
                # Title is before <cit.>, not after (e.g. "* Title <cit.>:")
                before = parts[0]
                star_parts = before.split("*")
                # Take the last non-empty segment after the last *
                candidate = star_parts[-1].strip() if star_parts else ""
            title = candidate if candidate else None

            # Find matching \cite{...} in original source
            for src_line in raw.split("\n"):
                if title is not None and title in src_line:
                    cite_parts = src_line.split(r"\cite{")
                    if len(cite_parts) > 1:
                        if "\\item" in cite_parts[0]:
                            bibtex_key = cite_parts[1].split("}")[0]
                        else:
                            bibtex_key = cite_parts[0].split("}")[0]
                        break
        else:
            description = line.strip()
            if description and title:
                papers[title]["bibtex_id"] = bibtex_key or ""
                papers[title]["contents"] = description
                title = None
                bibtex_key = None

    return papers


def extract_papers_from_tex_bib(tex_path: Path, bib_path: Path) -> Iterator[PaperCreate]:
    """Yield PaperCreate DTOs for entries that appear in both the .tex and .bib files.

    Entries whose BibTeX key has no matching record in the .bib file are
    skipped with a logged warning. Entries with no BibTeX key are skipped
    with a logged warning.

    :param tex_path: Path to the LaTeX literature-overview .tex file.
    :param bib_path: Path to the BibTeX .bib file.
    :yields: :class:`~paper_sorts.db.repositories.PaperCreate` for each matched entry.
    """
    papers = _parse_tex(tex_path)
    bib_graph = parse_bib_file(str(bib_path), bib_format="bibtex")

    for title, meta in papers.items():
        bib_key = meta.get("bibtex_id", "")
        contents = meta.get("contents", "")

        if not bib_key:
            logger.warning("No BibTeX key found for title %r — skipping", title)
            continue

        if bib_key not in bib_graph.entries:
            logger.warning("BibTeX key %r not found in .bib file — skipping", bib_key)
            continue

        entry = bib_graph.entries[bib_key]
        bibtex_str = entry.to_string("bibtex")

        authors: list[str] = []
        try:
            for person in entry.persons.get("author", []):
                last = person.last_names[0] if person.last_names else ""
                first = person.first_names[0] if person.first_names else ""
                if last or first:
                    authors.append(f"{last}, {first}" if first else last)
        except (IndexError, KeyError) as exc:
            logger.warning("Could not parse authors for %r: %s", bib_key, exc)

        yield PaperCreate(
            title=title,
            contents=contents,
            bibtex_id=bib_key,
            bibtex=bibtex_str,
            authors=authors,
        )
