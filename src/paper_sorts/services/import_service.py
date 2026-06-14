"""Bulk import service for paper_sorts.

Extracts paper metadata from a LaTeX literature overview (.tex) and matching
BibTeX file (.bib), yielding PaperCreate DTOs for each matched entry.

The expected .tex format (mirroring the original literature_overview.tex):
  \\item * \\cite{BibKey} Paper Title:
  One-sentence description of the paper.

Entries in the .tex file with no matching .bib record are skipped with a warning.
No database operations here — pure extraction, yielding one PaperCreate per entry.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from pathlib import Path

from pybtex.database import parse_file
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger("paper_sorts.services.import_service")


def _parse_tex_to_entries(tex_path: Path) -> list[dict[str, str]]:
    """Parse a LaTeX file and extract paper entries.

    Each entry in the returned list is a dict with keys:
    - 'bibtex_id': citation key extracted from \\cite{...}
    - 'title': paper title (text after <cit.> in converted text)
    - 'contents': description line immediately following the title line

    Expected LaTeX format:
        \\item * \\cite{BibKey} Paper Title:
        One-sentence description.

    :param tex_path: path to the .tex file
    :type tex_path: Path
    :return: list of parsed entry dicts
    :rtype: list[dict[str, str]]
    """
    raw = tex_path.read_text(encoding="utf-8")
    converted = LatexNodes2Text().latex_to_text(raw)
    converted_lines = [line for line in converted.split("\n") if line.strip()]

    # Build a mapping from position in converted to (bibtex_id, title)
    # by matching the raw \\cite{key} lines to converted lines containing <cit.>
    raw_lines = raw.split("\n")

    # Extract all \cite{key} from raw LaTeX lines that also have an \item
    cite_pattern = re.compile(r"\\cite\{([^}]+)\}")
    item_cite_map: list[tuple[str, str]] = []  # (bibtex_id, raw_line)
    for raw_line in raw_lines:
        if "\\item" in raw_line and "\\cite{" in raw_line:
            match = cite_pattern.search(raw_line)
            if match:
                bibtex_id = match.group(1).strip()
                item_cite_map.append((bibtex_id, raw_line))

    # Now walk converted lines to pair title/description with cite keys
    entries: list[dict[str, str]] = []
    cite_iter = iter(item_cite_map)
    current_cite: tuple[str, str] | None = None
    current_title: str | None = None
    current_description: str | None = None

    for line in converted_lines:
        stripped = line.strip()
        if "* <cit.>" in stripped or "* * <cit.>" in stripped:
            # This is a paper item line; advance to next cite entry
            try:
                current_cite = next(cite_iter)
            except StopIteration:
                current_cite = None
                continue

            # Extract title: text after <cit.>, stripped of trailing ':'
            # Handles both "* <cit.> Title:" and "* * <cit.> Title:"
            after_cit = re.split(r"<cit\.>", stripped, maxsplit=1)
            if len(after_cit) > 1:
                title_raw = after_cit[1].strip().rstrip(":")
                current_title = title_raw if title_raw else None
            else:
                current_title = None
            current_description = None  # reset

        elif current_title is not None and current_cite is not None:
            # This should be the description line immediately after the title
            if stripped and not stripped.startswith("*"):
                current_description = stripped
                entries.append(
                    {
                        "bibtex_id": current_cite[0],
                        "title": current_title,
                        "contents": current_description,
                    }
                )
                current_title = None
                current_cite = None
                current_description = None

    return entries


def extract_papers_from_tex_bib(
    tex_path: Path, bib_path: Path
) -> Iterator[PaperCreate]:
    """Extract papers from a LaTeX + BibTeX file pair, yielding PaperCreate DTOs.

    For each paper found in the .tex file:
    - If a matching BibTeX entry exists: yield a PaperCreate DTO
    - If no matching BibTeX entry: log a warning and skip (do not fail entire import)

    :param tex_path: path to the .tex file containing literature overview
    :type tex_path: Path
    :param bib_path: path to the .bib file containing BibTeX entries
    :type bib_path: Path
    :yields: PaperCreate DTOs for matched entries
    :raises FileNotFoundError: if tex_path or bib_path do not exist
    """
    if not tex_path.exists():
        raise FileNotFoundError(f"LaTeX file not found: {tex_path}")
    if not bib_path.exists():
        raise FileNotFoundError(f"BibTeX file not found: {bib_path}")

    # Parse TeX file
    entries = _parse_tex_to_entries(tex_path)
    logger.info("Parsed %d paper entries from %s", len(entries), tex_path.name)

    # Parse BibTeX file
    bib_db = parse_file(str(bib_path), bib_format="bibtex")
    logger.info("Loaded %d BibTeX entries from %s", len(bib_db.entries), bib_path.name)

    for entry_meta in entries:
        bibtex_id = entry_meta.get("bibtex_id", "")
        title = entry_meta.get("title", "")
        contents = entry_meta.get("contents", "")

        if not bibtex_id:
            logger.warning("Skipping '%s': no BibTeX key found in .tex file", title)
            continue

        if bibtex_id not in bib_db.entries:
            logger.warning(
                "Skipping '%s' (key=%r): no matching entry in .bib file", title, bibtex_id
            )
            continue

        bib_entry = bib_db.entries[bibtex_id]

        # Extract authors
        authors: list[str] = []
        try:
            for person in bib_entry.persons.get("author", []):
                last = " ".join(person.last_names) if person.last_names else ""
                first = " ".join(person.first_names) if person.first_names else ""
                if last and first:
                    authors.append(f"{last}, {first}")
                elif last:
                    authors.append(last)
        except Exception as exc:
            logger.warning("Could not parse authors for '%s': %s", title, exc)

        # Extract full BibTeX string
        bibtex_str = bib_entry.to_string("bibtex")

        yield PaperCreate(
            title=title,
            contents=contents,
            bibtex_id=bibtex_id,
            bibtex=bibtex_str,
            authors=authors,
        )
