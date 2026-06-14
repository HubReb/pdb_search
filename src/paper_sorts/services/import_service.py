"""Bulk import service for paper_sorts.

Extracts paper metadata from a LaTeX literature overview (.tex) and matching
BibTeX file (.bib), yielding PaperCreate DTOs for each matched entry.

Entries in the .tex file with no matching .bib record are skipped with a warning.
No database operations here — pure extraction, yielding one PaperCreate per entry.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path
from typing import Iterator

from pybtex.database import parse_file  # type: ignore[import]
from pylatexenc.latex2text import LatexNodes2Text  # type: ignore[import]

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger("paper_sorts.services.import_service")


def _parse_tex_file(tex_path: Path) -> dict[str, str]:
    """Parse a LaTeX file and extract {title: bibtex_key} mappings.

    Looks for lines containing both '*' and '<cit.>' (as rendered by pylatexenc)
    to identify paper items and their citation keys.

    :param tex_path: path to the .tex file
    :type tex_path: Path
    :return: dict mapping paper title strings to BibTeX citation keys
    :rtype: dict[str, str]
    """
    raw_text = tex_path.read_text(encoding="utf-8")
    converted = LatexNodes2Text().latex_to_text(raw_text)
    lines = [line for line in converted.split("\n") if line.strip()]

    title_to_key: dict[str, str] = {}
    current_title: str | None = None
    current_key: str | None = None

    for line in lines:
        if "*" in line and "<cit.>" in line:
            # This is a paper item line: extract title and bibtex key
            parts = line.split("<cit.>")
            title_candidate = parts[1].rstrip(":").strip() if len(parts) > 1 else ""
            if title_candidate == ":":
                title_candidate = parts[0].split("*")[1].strip() if "*" in parts[0] else ""

            # Find bibtex key in the raw LaTeX
            for raw_line in raw_text.split("\n"):
                if title_candidate and title_candidate in raw_line and r"\cite{" in raw_line:
                    cite_parts = raw_line.split(r"\cite{")
                    if len(cite_parts) > 1:
                        key_raw = cite_parts[1].split("}")[0]
                        current_title = title_candidate
                        current_key = key_raw
                        break

    # Re-parse properly: iterate line pairs
    title_to_key = {}
    current_title = None
    current_key = None
    for line in lines:
        if "*" in line and "<cit.>" in line:
            parts = line.split("<cit.>")
            title_candidate = parts[1].rstrip(":").strip() if len(parts) > 1 else ""
            if not title_candidate or title_candidate == ":":
                title_candidate = (
                    parts[0].split("*")[1].strip() if len(parts) > 0 and "*" in parts[0] else ""
                )
            if not title_candidate:
                continue
            # Find BibTeX key in raw LaTeX
            for raw_line in raw_text.split("\n"):
                if title_candidate in raw_line and r"\cite{" in raw_line:
                    cite_parts = raw_line.split(r"\cite{")
                    if len(cite_parts) > 1:
                        key = cite_parts[1].split("}")[0].strip()
                        title_to_key[title_candidate] = key
                        break
        else:
            # description line — associate with previous title
            pass

    return title_to_key


def _parse_tex_to_dict(tex_path: Path) -> dict[str, dict[str, str]]:
    """Parse a LaTeX file and return {title: {bibtex_id: key, contents: description}}.

    Mirrors the logic from the legacy helpers.get_data function.

    :param tex_path: path to the .tex file
    :type tex_path: Path
    :return: dict of title → {bibtex_id, contents} mappings
    :rtype: dict[str, dict[str, str]]
    """
    raw = tex_path.read_text(encoding="utf-8")
    converted = LatexNodes2Text().latex_to_text(raw)
    lines = [line for line in converted.split("\n") if line.strip()]

    papers_dict: dict[str, dict[str, str]] = defaultdict(dict)
    current_title: str | None = None
    current_key: str | None = None

    for line in lines:
        if "*" in line and "<cit.>" in line:
            # Title line
            parts = line.split("<cit.>")
            title = parts[1].rstrip(":").strip() if len(parts) > 1 else ""
            if title == ":" or not title:
                title = parts[0].split("*")[1].strip() if "*" in parts[0] else ""
            title = title.strip()

            # Extract BibTeX key from raw LaTeX
            bib_key = None
            for raw_line in raw.split("\n"):
                if title and title in raw_line and r"\cite{" in raw_line:
                    cite_split = raw_line.split(r"\cite{")
                    if len(cite_split) > 1:
                        bib_key = cite_split[1].split("}")[0].strip()
                        break

            current_title = title if title else None
            current_key = bib_key
        else:
            # Description line
            description = line.strip()
            if description and current_title:
                papers_dict[current_title]["bibtex_id"] = current_key or ""
                papers_dict[current_title]["contents"] = description
                current_title = None
                current_key = None

    return dict(papers_dict)


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
    papers_dict = _parse_tex_to_dict(tex_path)
    logger.info("Parsed %d paper entries from %s", len(papers_dict), tex_path.name)

    # Parse BibTeX file
    bib_db = parse_file(str(bib_path), bib_format="bibtex")
    logger.info("Loaded %d BibTeX entries from %s", len(bib_db.entries), bib_path.name)

    for title, meta in papers_dict.items():
        bibtex_id = meta.get("bibtex_id", "")
        contents = meta.get("contents", "")

        if not bibtex_id:
            logger.warning("Skipping '%s': no BibTeX key found in .tex file", title)
            continue

        if bibtex_id not in bib_db.entries:
            logger.warning(
                "Skipping '%s' (key=%r): no matching entry in .bib file", title, bibtex_id
            )
            continue

        entry = bib_db.entries[bibtex_id]

        # Extract authors
        authors: list[str] = []
        try:
            for person in entry.persons.get("author", []):
                last = " ".join(person.last_names) if person.last_names else ""
                first = " ".join(person.first_names) if person.first_names else ""
                if last and first:
                    authors.append(f"{last}, {first}")
                elif last:
                    authors.append(last)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not parse authors for '%s': %s", title, exc)

        # Extract full BibTeX string
        bibtex_str = entry.to_string("bibtex")

        yield PaperCreate(
            title=title,
            contents=contents,
            bibtex_id=bibtex_id,
            bibtex=bibtex_str,
            authors=authors,
        )
