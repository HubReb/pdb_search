"""Bulk import service for paper_sorts.

Extracts paper records from a .tex + .bib pair and yields PaperCreate DTOs.
Pure domain logic: no SQL, no rich, no I/O (file reading is the caller's job).

Used by the `pdbsearch import` subcommand.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

from pybtex.database import parse_bytes  # type: ignore[import-untyped]
from pylatexenc.latex2text import LatexNodes2Text  # type: ignore[import-untyped]

from paper_sorts.db.repositories import PaperCreate

logger = logging.getLogger(__name__)


def _parse_bib(bib_content: str) -> dict[str, tuple[str, list[str], str]]:
    """Parse BibTeX content and return a map of key → (bibtex_string, authors, title).

    :param bib_content: full content of a .bib file as a string
    :return: dict mapping bibtex key to (full_bibtex_string, author_list, title)
    """
    bib_data: dict[str, tuple[str, list[str], str]] = {}
    try:
        db = parse_bytes(bib_content.encode(), bib_format="bibtex")
    except Exception as exc:
        logger.warning("Failed to parse .bib content: %s", exc)
        return bib_data

    for key, entry in db.entries.items():
        try:
            bibtex_str = entry.to_string("bibtex")
            authors: list[str] = []
            for person in entry.persons.get("author", []):
                last = " ".join(person.last_names) if person.last_names else ""
                first = " ".join(person.first_names) if person.first_names else ""
                authors.append(f"{last}, {first}".strip(", "))
            title = entry.fields.get("title", "")
            bib_data[key] = (bibtex_str, authors, title)
        except Exception as exc:
            logger.warning("Skipping .bib entry '%s': %s", key, exc)
    return bib_data


def _parse_tex_citations(tex_content: str) -> list[tuple[str, str, str]]:
    r"""Extract cited bibtex keys and their associated title/description from a .tex file.

    Parses lines of the form:
      \item ...title_text... \cite{key} description

    :param tex_content: full content of a .tex file as a string
    :return: list of (bibtex_key, title, description) tuples
    """
    converter = LatexNodes2Text()
    try:
        plain = converter.latex_to_text(tex_content)
    except Exception as exc:
        logger.warning("LatexNodes2Text failed, falling back to raw .tex: %s", exc)
        plain = tex_content

    results: list[tuple[str, str, str]] = []
    lines = [line.strip() for line in plain.split("\n") if line.strip()]
    title_text: str | None = None
    bibtex_key: str | None = None

    for line in lines:
        if "*" in line and "<cit.>" in line:
            # This is a title line
            parts = line.split("<cit.>")
            if len(parts) >= 2:
                title_candidate = parts[1].rstrip(":").strip()
                if not title_candidate:
                    title_candidate = parts[0].split("*")[-1].strip()
                title_text = title_candidate
                # Extract the cite key from the raw .tex source
                bibtex_key = _extract_cite_key(tex_content, title_text)
        elif title_text is not None:
            # Description line follows the title
            description = line.strip()
            if description and bibtex_key:
                results.append((bibtex_key, title_text, description))
            title_text = None
            bibtex_key = None

    return results


def _extract_cite_key(tex_content: str, title_fragment: str) -> str | None:
    r"""Find the \cite{key} associated with a title fragment in raw .tex.

    :param tex_content: raw .tex source
    :param title_fragment: title text fragment to search for
    :return: BibTeX key string, or None if not found
    """
    for line in tex_content.split("\n"):
        if title_fragment and title_fragment[:20] in line and r"\cite{" in line:
            start = line.find(r"\cite{") + len(r"\cite{")
            end = line.find("}", start)
            if end > start:
                return line[start:end].strip()
    return None


def extract_papers_from_tex_bib(
    tex_content: str, bib_content: str
) -> Iterator[PaperCreate]:
    """Yield PaperCreate DTOs for each paper cited in .tex and found in .bib.

    Papers cited in .tex but not in .bib are skipped with a logged warning.

    :param tex_content: content of a .tex file (LaTeX source)
    :param bib_content: content of a .bib file (BibTeX source)
    :yields: PaperCreate DTO for each matched paper
    """
    bib_map = _parse_bib(bib_content)
    citations = _parse_tex_citations(tex_content)

    if not citations:
        # Fallback: import all entries from .bib if tex parsing found nothing
        logger.info("No citations found in .tex; importing all .bib entries")
        for key, (bibtex_str, authors, title) in bib_map.items():
            yield PaperCreate(
                title=title,
                contents="",
                bibtex_id=key,
                bibtex=bibtex_str,
                authors=authors,
            )
        return

    for bibtex_key, title, description in citations:
        if bibtex_key not in bib_map:
            logger.warning(
                "Cited key '%s' (title: '%s') not found in .bib — skipping", bibtex_key, title
            )
            continue
        bibtex_str, authors, bib_title = bib_map[bibtex_key]
        # Prefer the title from .bib if available, else use what tex gave us
        effective_title = bib_title if bib_title else title
        yield PaperCreate(
            title=effective_title,
            contents=description,
            bibtex_id=bibtex_key,
            bibtex=bibtex_str,
            authors=authors,
        )
