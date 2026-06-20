"""Bulk-import service: extract papers from a LaTeX + BibTeX pair.

Parsing stays with the dedicated libraries (pybtex + pylatexenc). The extractor
yields one :class:`PaperCreate` per cited entry that has a matching ``.bib``
record; citation keys with no match are skipped (the caller logs a warning).
"""

from __future__ import annotations

from collections.abc import Iterator

from pybtex.database import parse_string
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate


def _parse_tex(tex: str) -> dict[str, tuple[str, str]]:
    """Map each citation key to its ``(title, summary)`` from the .tex.

    Mirrors the legacy ``get_data`` heuristic: a bullet line containing ``*`` and
    ``<cit.>`` carries the title; the matching ``\\cite{key}`` in the raw LaTeX
    gives the key; the following non-empty rendered line is the one-sentence
    summary.

    :param tex: contents of the LaTeX literature overview.
    :returns: ``{citation_key: (title, summary)}`` for every parsed entry.
    """
    rendered = [line for line in LatexNodes2Text().latex_to_text(tex).split("\n") if line != ""]
    result: dict[str, tuple[str, str]] = {}
    title: str | None = None
    bibtex_key: str | None = None
    for line in rendered:
        if "<cit.>" in line:
            # The title is whatever precedes the citation marker; the legacy
            # overleaf export rendered it after a "*" bullet, but the marker
            # itself is the reliable signal.
            before, _, after = line.partition("<cit.>")
            candidate = before.strip().lstrip("*").strip()
            if not candidate:
                candidate = after.strip().lstrip(":").strip()
            title = candidate.strip()
            bibtex_key = _find_key_for_title(tex, title)
        elif title is not None and bibtex_key:
            result[bibtex_key] = (title, line.strip())
            title, bibtex_key = None, None
    return result


def _find_key_for_title(tex: str, title: str) -> str | None:
    """Return the ``\\cite{...}`` key on the raw LaTeX line bearing ``title``.

    The key is always the token immediately after ``\\cite{`` on the line that
    contains the title.
    """
    for raw in tex.split("\n"):
        if title in raw and r"\cite{" in raw:
            return raw.split(r"\cite{", 1)[1].split("}")[0]
    return None


def extract_papers_from_tex_bib(tex: str, bib: str) -> Iterator[PaperCreate]:
    """Yield a :class:`PaperCreate` for each cited entry with a matching bib record.

    :param tex: contents of the LaTeX literature overview.
    :param bib: contents of the BibTeX file.
    :yields: one ``PaperCreate`` per matched citation key (unmatched keys skipped).
    """
    key_to_meta = _parse_tex(tex)
    bib_db = parse_string(bib, bib_format="bibtex")
    for key, (title, summary) in key_to_meta.items():
        if key not in bib_db.entries:
            continue
        entry = bib_db.entries[key]
        authors = [
            f"{person.last_names[0]}, {person.first_names[0]}"
            for person in entry.persons.get("author", [])
        ]
        yield PaperCreate(
            title=title or entry.fields.get("title", key),
            summary=summary,
            bibtex_id=key,
            bibtex=entry.to_string("bibtex"),
            authors=authors,
        )
