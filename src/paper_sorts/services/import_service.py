r"""Bulk-import service: extract papers from a ``.tex`` + ``.bib`` pair.

:func:`extract_papers_from_tex_bib` parses the BibTeX database, finds every
citation key referenced in the LaTeX source, and yields one
:class:`~paper_sorts.db.repositories.PaperCreate` per cited key that has a
matching ``.bib`` record. Keys with no matching record are skipped with a logged
warning so the caller can commit per paper.

LaTeX accents in titles are decoded to text via ``pylatexenc`` so they round-trip
cleanly; the BibTeX source itself is preserved verbatim via ``pybtex``.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator

from pybtex.database import Entry, parse_string
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate

_logger = logging.getLogger(__name__)

#: Matches ``\cite``/``\citep``/``\citet``/... with an optional ``[..]`` option
#: and captures the comma-separated key list inside the braces.
_CITE_RE = re.compile(r"\\cite[a-zA-Z]*\*?(?:\[[^\]]*\])*\{([^}]*)\}")


def _cited_keys(tex: str) -> list[str]:
    """Return the citation keys referenced in a LaTeX document, in order.

    :param tex: the LaTeX source.
    :return: the cited keys, de-duplicated, preserving first-seen order.
    """
    seen: dict[str, None] = {}
    for match in _CITE_RE.finditer(tex):
        for key in match.group(1).split(","):
            key = key.strip()
            if key:
                seen.setdefault(key, None)
    return list(seen)


def _decode(value: str) -> str:
    """Decode LaTeX escapes/accents in a field value to plain text.

    :param value: the raw LaTeX field value.
    :return: the decoded text.
    """
    return str(LatexNodes2Text().latex_to_text(value)).strip()


def _to_paper_create(key: str, entry: Entry) -> PaperCreate:
    """Build a :class:`PaperCreate` from a parsed BibTeX entry.

    :param key: the BibTeX key.
    :param entry: the parsed pybtex entry.
    :return: the corresponding write DTO.
    """
    title = _decode(entry.fields.get("title", ""))
    abstract = entry.fields.get("abstract") or entry.fields.get("annote") or ""
    contents = _decode(abstract) if abstract else title
    authors = [str(person) for person in entry.persons.get("author", [])]
    return PaperCreate(
        title=title,
        contents=contents,
        bibtex_id=key,
        bibtex=entry.to_string("bibtex").strip(),
        authors=authors,
    )


def extract_papers_from_tex_bib(tex: str, bib: str) -> Iterator[PaperCreate]:
    """Yield a :class:`PaperCreate` for each cited key with a matching ``.bib``.

    Keys cited in ``tex`` but absent from ``bib`` are skipped with a logged
    warning rather than failing the import.

    :param tex: the LaTeX source containing ``\\cite`` commands.
    :param bib: the BibTeX database source.
    :return: an iterator of write DTOs, one per matched cited key.
    """
    database = parse_string(bib, "bibtex")
    for key in _cited_keys(tex):
        entry = database.entries.get(key)
        if entry is None:
            _logger.warning("Citation key %r has no matching .bib record — skipped", key)
            continue
        yield _to_paper_create(key, entry)
