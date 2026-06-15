"""Bulk import service for paper_sorts.

Extracts paper metadata from a LaTeX literature-overview .tex file and a
corresponding .bib file. Yields PaperCreate DTOs one at a time so the caller
can commit each paper individually (constitution Principle IV — per-paper commit
so a partial failure leaves already-committed rows intact).

Logic ported from legacy helpers.py / get_data.py using the same pylatexenc +
pybtex libraries to preserve parsing behaviour.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

from pybtex.database import parse_file as pybtex_parse_file  # type: ignore[import-untyped]
from pylatexenc.latex2text import LatexNodes2Text  # type: ignore[import-untyped]

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger(__name__)


def _parse_tex(tex_path: Path) -> dict[str, dict[str, str]]:
    """Parse a LaTeX literature-overview file into a title→metadata dict.

    Looks for lines containing both '*' and '<cit.>' to identify cited paper
    entries (the pattern used in the legacy `get_data.py`).

    :param tex_path: Path to the .tex file.
    :return: Dict mapping title → {'bibtex_id': str, 'contents': str}.
    """
    with open(tex_path, encoding="utf-8") as fh:
        data = fh.read()

    text = LatexNodes2Text().latex_to_text(data).split("\n")
    text = [line for line in text if line]

    papers_dict: dict[str, dict[str, str]] = defaultdict(lambda: defaultdict(str))
    title: str | None = None
    bibtex: str | None = None

    for line in text:
        if "*" in line and "<cit.>" in line:
            # Title line
            title = line.split("<cit.>")[1].rstrip(":")
            if title == ":":
                title = line.split("<cit.>")[0].split("*")[1]
            title = title.strip()
            for latex_line in data.split("\n"):
                if title in latex_line:
                    parts = latex_line.split(r"\cite{")
                    if len(parts) > 1:
                        if "\\item" in parts[0]:
                            bibtex = parts[1].split("}")[0]
                        else:
                            bibtex = parts[0].split("}")[0]
                    else:
                        bibtex = None
                    break
            description = None
        else:
            description = line.strip()

        if description and title:
            papers_dict[title]["bibtex_id"] = bibtex or ""
            papers_dict[title]["contents"] = description
            title = None
            bibtex = None

    return papers_dict


def _enrich_with_bib(
    papers_dict: dict[str, dict[str, str]],
    bib_path: Path,
) -> dict[str, dict[str, str]]:
    """Add BibTeX source text and author lists from the .bib file.

    Entries in papers_dict whose bibtex_id has no matching entry in the .bib
    file are left with empty 'bibtex' and 'authors' fields so the caller can
    skip them cleanly.

    :param papers_dict: Title→metadata dict from _parse_tex.
    :param bib_path: Path to the .bib file.
    :return: Enriched papers_dict with 'bibtex' and 'authors' fields added.
    """
    try:
        bib_graph = pybtex_parse_file(str(bib_path), bib_format="bibtex")
    except Exception as exc:
        logger.error("Failed to parse .bib file '%s': %s", bib_path, exc)
        return papers_dict

    bib_by_key: dict[str, object] = {k: bib_graph.entries[k] for k in bib_graph.entries}

    for title, values in papers_dict.items():
        key = values.get("bibtex_id", "")
        if not key or key not in bib_by_key:
            continue
        entry = bib_graph.entries[key]
        try:
            authors = [
                f"{a.last_names[0]}, {a.first_names[0]}"
                for a in entry.persons.get("author", [])
            ]
        except (IndexError, AttributeError):
            authors = []
        papers_dict[title]["bibtex"] = entry.to_string("bibtex")
        papers_dict[title]["authors"] = ",".join(authors)  # join for storage; split on yield

    return papers_dict


def extract_papers_from_tex_bib(
    tex_path: Path,
    bib_path: Path,
) -> Iterator[PaperCreate]:
    """Parse a .tex + .bib pair and yield one PaperCreate per valid entry.

    Entries with no matching bib record are skipped with a logged warning
    rather than failing the whole import (spec US5 / FR-002).

    :param tex_path: Path to the LaTeX .tex file with cited entries.
    :param bib_path: Path to the BibTeX .bib file.
    :yields: PaperCreate DTO for each valid entry found.
    """
    papers_dict = _parse_tex(tex_path)
    papers_dict = _enrich_with_bib(papers_dict, bib_path)

    for title, values in papers_dict.items():
        bibtex_id = values.get("bibtex_id", "")
        bibtex = values.get("bibtex", "")
        contents = values.get("contents", "")

        if not bibtex_id:
            logger.warning("Skipping '%s': no BibTeX key found.", title)
            continue
        if not bibtex:
            logger.warning("Skipping '%s' (%s): no matching bib record.", title, bibtex_id)
            continue

        authors_raw = values.get("authors", "")
        authors = [a.strip() for a in authors_raw.split(",") if a.strip()] if authors_raw else []

        yield PaperCreate(
            title=title,
            contents=contents,
            bibtex_id=bibtex_id,
            bibtex=bibtex,
            authors=authors,
        )
