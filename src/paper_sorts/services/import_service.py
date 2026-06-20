"""Bulk import service for paper_sorts.

Extracts paper records from a LaTeX literature-overview file + BibTeX file
pair and yields PaperCreate DTOs for each successfully resolved entry.

Uses:
- pylatexenc.latex2text.LatexNodes2Text for .tex parsing
- pybtex.database.parse_file for .bib parsing

LaTeX accents in author names (e.g. ``\\"o`` → ``ö``) are preserved as-is
in the raw BibTeX string (so round-trips are lossless); pybtex handles them
correctly when rendering author names.
"""

import logging
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

from pybtex.database import parse_file
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger(__name__)


def extract_papers_from_tex_bib(
    tex_path: Path,
    bib_path: Path,
) -> Iterator[PaperCreate]:
    """Parse a .tex + .bib pair and yield one PaperCreate per resolved entry.

    Entries in the .tex file whose citation key has no matching record in
    the .bib file are skipped with a WARNING log message (not an error).
    Per-entry exceptions are caught and logged, allowing partial success.

    :param tex_path: Path to the LaTeX literature-overview file.
    :param bib_path: Path to the BibTeX file matching the .tex citations.
    :yields: :class:`~paper_sorts.db.repositories.PaperCreate` for each
        successfully resolved entry.
    :raises OSError: If either file cannot be read.
    """
    # Step 1: Parse .tex → {title: bibtex_id, contents}
    papers_dict = _parse_tex(tex_path)
    if not papers_dict:
        logger.warning("No entries found in .tex file %s", tex_path)
        return

    # Step 2: Parse .bib → enrich with bibtex string + authors
    bib_graph = parse_file(str(bib_path), bib_format="bibtex")
    bib_keys = set(bib_graph.entries.keys())

    for title, meta in papers_dict.items():
        bibtex_id = meta.get("bibtex_id", "")
        if not bibtex_id:
            logger.warning("No bibtex_id for title '%s' — skipping", title)
            continue

        if bibtex_id not in bib_keys:
            logger.warning(
                "Citation key '%s' (title: '%s') not found in %s — skipping",
                bibtex_id,
                title,
                bib_path,
            )
            continue

        try:
            entry = bib_graph.entries[bibtex_id]
            bibtex_str = entry.to_string("bibtex")

            authors_list: list[str] = []
            for person in entry.persons.get("author", []):
                last = person.last_names[0] if person.last_names else ""
                first = person.first_names[0] if person.first_names else ""
                if last and first:
                    authors_list.append(f"{last}, {first}")
                elif last:
                    authors_list.append(last)

            yield PaperCreate(
                title=title,
                contents=meta.get("contents", ""),
                bibtex_id=bibtex_id,
                authors=authors_list,
                bibtex=bibtex_str,
            )

        except Exception as exc:
            logger.warning(
                "Failed to extract entry '%s': %s — skipping", bibtex_id, exc
            )


def _parse_tex(tex_path: Path) -> dict[str, dict[str, str]]:
    """Parse a LaTeX literature-overview .tex file.

    Extracts pairs of (title, bibtex_id) and (title, one-sentence summary)
    from the processed plain-text rendering of the file.

    Expected format (from the legacy get_data.py):
    - Lines with ``*`` and ``<cit.>`` are title lines containing a citation key.
    - The next non-empty line after a title line is the summary.

    :param tex_path: Path to the .tex file.
    :returns: Dict of the form ``{title: {bibtex_id: str, contents: str}}``.
    """
    raw = tex_path.read_text(encoding="utf-8")
    text_lines = LatexNodes2Text().latex_to_text(raw).split("\n")
    text_lines = [line for line in text_lines if line.strip()]

    papers_dict: dict[str, dict[str, str]] = defaultdict(dict)
    title: str | None = None
    bibtex_id: str | None = None

    for line in text_lines:
        if "*" in line and "<cit.>" in line:
            # Title line — extract title and citation key
            title = line.split("<cit.>")[1].rstrip(":").strip()
            if not title:
                # Title is before <cit.> — extract it from the left side
                title = line.split("<cit.>")[0].split("*")[-1].strip()

            # Find the citation key in the raw LaTeX source
            bibtex_id = None
            for latex_line in raw.split("\n"):
                if title in latex_line:
                    parts = latex_line.split(r"\cite{")
                    if len(parts) > 1:
                        candidate = parts[1].split("}")[0].strip()
                        bibtex_id = candidate
                        break
        elif title and bibtex_id:
            # First non-empty line after title is the summary
            papers_dict[title]["bibtex_id"] = bibtex_id
            papers_dict[title]["contents"] = line.strip()
            title = None
            bibtex_id = None

    return papers_dict
