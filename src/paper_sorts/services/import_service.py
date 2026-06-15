"""Bulk-import service: extract papers from a LaTeX overview + BibTeX pair.

Mirrors the legacy ``get_data.py`` flow: parse the ``.tex`` literature overview
for cited titles and their citation keys, join against the ``.bib`` for authors
and source, and yield one :class:`PaperCreate` per cited entry that has a
matching BibTeX record. Cited keys with no ``.bib`` match are skipped (the
caller logs a warning). The CLI driver commits per paper so a partial failure
leaves earlier papers persisted (constitution Principle IV / FR-005).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator

from pybtex.database import parse_string
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate


def _extract_tex_entries(tex: str) -> dict[str, str]:
    r"""Extract ``{bibtex_key: summary}`` from a LaTeX overview.

    Reproduces the legacy ``get_data`` heuristic: a title line carries a
    ``\\cite{key}`` and a ``<cit.>`` marker (after LaTeX→text conversion); the
    following non-empty line is the one-sentence summary.

    :param tex: the raw LaTeX overview source.
    :returns: a mapping of BibTeX key to summary text.
    """
    rendered = LatexNodes2Text().latex_to_text(tex).split("\n")
    lines = [line for line in rendered if line.strip()]
    result: dict[str, str] = {}
    pending_key: str | None = None
    for line in lines:
        if "*" in line and "<cit.>" in line:
            pending_key = _find_cite_key(tex, line)
            continue
        if pending_key is not None:
            result[pending_key] = line.strip()
            pending_key = None
    return result


def _find_cite_key(tex: str, rendered_title_line: str) -> str | None:
    r"""Find the ``\cite{...}`` key for a rendered title line.

    :param tex: the raw LaTeX source (searched for the matching ``\\cite``).
    :param rendered_title_line: the title line after LaTeX→text rendering.
    :returns: the citation key, or ``None`` if not found.
    """
    title = rendered_title_line.split("<cit.>")[0].split("*")[-1].strip().rstrip(":")
    for raw_line in tex.split("\n"):
        if title and title in raw_line and r"\cite{" in raw_line:
            return raw_line.split(r"\cite{")[1].split("}")[0]
    return None


def extract_papers_from_tex_bib(tex: str, bib: str) -> Iterator[PaperCreate]:
    """Yield a :class:`PaperCreate` per cited entry with a matching bib record.

    :param tex: the LaTeX overview source.
    :param bib: the matching BibTeX source.
    :yields: one paper per matched citation key; unmatched keys are skipped.
    """
    summaries = _extract_tex_entries(tex)
    bib_db = parse_string(bib, bib_format="bibtex")
    by_key: dict[str, str] = defaultdict(str)
    for key in bib_db.entries:
        by_key[key] = key
    for key, summary in summaries.items():
        if key not in bib_db.entries:
            continue
        entry = bib_db.entries[key]
        authors = [
            f"{person.last_names[0]}, {person.first_names[0]}"
            for person in entry.persons.get("author", [])
        ]
        title = entry.fields.get("title", "")
        yield PaperCreate(
            title=title,
            summary=summary,
            authors=authors,
            bibtex_id=key,
            bibtex=entry.to_string("bibtex"),
        )
