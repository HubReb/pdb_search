"""Bulk import service for paper_sorts.

Parses a LaTeX overview file and a matching BibTeX file, then yields one
:class:`~paper_sorts.db.repositories.PaperCreate` per matched entry.

This module has no I/O, no prompts, and no database calls — it is pure
domain logic, making it independently testable.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger(__name__)


def extract_papers_from_tex_bib(
    tex_path: str,
    bib_path: str,
) -> Iterator[PaperCreate]:
    """Parse *tex_path* and *bib_path* and yield a :class:`PaperCreate` per entry.

    The LaTeX file is expected to have the structure produced by the legacy
    ``get_data.py`` workflow: bullet-list items whose titles contain a
    ``\\cite{key}`` reference, followed by a one-sentence description on the
    next non-blank line.

    Entries whose cite key has no matching record in *bib_path* are skipped
    with a ``WARNING``-level log message.  The caller is responsible for
    inserting the yielded entries into the database.

    :param tex_path: Path to the ``.tex`` file to parse.
    :param bib_path: Path to the ``.bib`` file containing BibTeX records.
    :yields: :class:`~paper_sorts.db.repositories.PaperCreate` for each
        matched entry.
    :raises FileNotFoundError: If either *tex_path* or *bib_path* does not
        exist.
    """
    import os

    from pybtex.database import parse_file  # type: ignore[import-untyped]
    from pylatexenc.latex2text import LatexNodes2Text  # type: ignore[import-untyped]

    if not os.path.exists(tex_path):
        raise FileNotFoundError(f"LaTeX file not found: {tex_path!r}")
    if not os.path.exists(bib_path):
        raise FileNotFoundError(f"BibTeX file not found: {bib_path!r}")

    with open(tex_path, encoding="utf-8") as f:
        tex_raw = f.read()

    # Parse LaTeX → plain text to extract titles and descriptions
    text_lines = [
        line
        for line in LatexNodes2Text().latex_to_text(tex_raw).split("\n")
        if line.strip()
    ]

    # Extract (title, bibtex_key, description) triples from plain-text lines
    papers_meta: dict[str, dict[str, str]] = defaultdict(dict)
    title: str | None = None
    bibtex_key: str | None = None

    for line in text_lines:
        if "*" in line and "<cit.>" in line:
            # Title line: "* Some Title <cit.>:" or "* <cit.> Some Title:"
            parts = line.split("<cit.>")
            candidate = parts[1].rstrip(":").strip() if len(parts) > 1 else ""
            if candidate and candidate != ":":
                title = candidate
            else:
                title = parts[0].split("*")[-1].strip()

            # Find the \\cite{key} in the raw LaTeX for this title
            bibtex_key = _find_cite_key(tex_raw, title)
        else:
            description = line.strip()
            if description and title and bibtex_key:
                papers_meta[bibtex_key]["title"] = title
                papers_meta[bibtex_key]["contents"] = description
                title = None
                bibtex_key = None

    # Parse BibTeX file
    bib_data = parse_file(bib_path, bib_format="bibtex")

    for key, meta in papers_meta.items():
        if key not in bib_data.entries:
            logger.warning("BibTeX key %r has no matching record in %r — skipping", key, bib_path)
            continue

        entry = bib_data.entries[key]
        try:
            authors = [
                f"{a.last_names[0]}, {a.first_names[0]}"
                for a in entry.persons.get("author", [])
            ]
            bibtex_str = entry.to_string("bibtex")
        except Exception as exc:
            logger.warning("Could not parse entry %r: %s — skipping", key, exc)
            continue

        yield PaperCreate(
            title=meta["title"],
            contents=meta.get("contents", ""),
            bibtex_id=key,
            bibtex=bibtex_str,
            authors=authors if authors else ["Unknown"],
        )


def _find_cite_key(tex_raw: str, title: str) -> str | None:
    r"""Find the ``\cite{key}`` cite key for *title* in the raw LaTeX source.

    :param tex_raw: Full raw LaTeX source.
    :param title: Paper title to look for.
    :returns: The cite key string, or ``None`` if not found.
    """
    for latex_line in tex_raw.split("\n"):
        if title not in latex_line:
            continue
        parts = latex_line.split(r"\cite{")
        if len(parts) < 2:
            continue
        key = parts[1].split("}")[0].strip()
        return key
    return None
