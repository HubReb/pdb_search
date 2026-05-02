r"""Bulk-import service — port of the legacy ``get_data`` + ``get_bibtex_information``.

Exposes :func:`extract_papers_from_tex_bib`: a per-paper :class:`PaperCreate`
iterator over a ``.tex`` overview + ``.bib`` source pair. Per-paper yielding
preserves the per-paper-commit semantic mandated by constitution Principle IV
(v1.3.0): the CLI caller wraps each yielded record in its own
:func:`with_session` transaction, so a partial failure leaves the database
in a state recoverable by re-running the same command (idempotent on the
documented "skip BibTeX keys already present" rule, see
``contracts/cli-commands.md`` § "Subcommand: import").

The ``.tex`` parser is a faithful port of legacy
``paper_sorts/get_data.py:get_data`` — same ``*<cit.>`` title detection,
same single-description-per-title assumption, same fragile ``\\cite{...}``
extraction (split on ``\\cite{``, branch on whether the prefix contains
``\\item``). Spec FR-002 forbids changing the parsing semantics; the only
deviation is structural — the legacy ``defaultdict`` of dicts is replaced
with a per-paper ``Iterator[PaperCreate]`` so the caller never holds the
whole import in memory.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from pybtex.database import parse_file
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate


@dataclass(frozen=True)
class _TexCitation:
    """One title + description + bibtex key triple harvested from the ``.tex`` file."""

    title: str
    contents: str
    bibtex_id: str


def extract_papers_from_tex_bib(
    tex_path: Path, bib_path: Path
) -> Iterator[PaperCreate]:
    r"""Yield one :class:`PaperCreate` per ``.tex`` cite that resolves in the ``.bib``.

    Args:
        tex_path: Path to the ``.tex`` literature overview. Each paper is
            expected on a ``\\item`` line referencing a ``\\cite{KEY}`` and a
            human title surrounded by ``*...*``, followed by a one-line
            description (the legacy format).
        bib_path: Path to the BibTeX source file holding the entries the
            ``.tex`` cites.

    Yields:
        :class:`PaperCreate` records ready to hand to
        :meth:`PaperService.add_paper`. The caller is responsible for the
        per-paper transaction and for catching
        :class:`DuplicateBibtexIdError` to skip BibTeX keys already in the
        database (logged INFO by the CLI command, per the import contract).

    Logs a WARNING and skips when a ``.tex`` cite has no matching ``.bib``
    record; the import continues with the next entry (FR-002: the legacy
    behaviour is to skip silently — modernization adds the WARNING log
    rather than the silent drop, justified by the import contract).
    """
    bib_graph = parse_file(str(bib_path), bib_format="bibtex")

    for cite in _parse_tex_citations(tex_path):
        if cite.bibtex_id not in bib_graph.entries:
            logging.getLogger(__name__).warning(
                "No bib entry for cite %r (title=%r) in %s; skipping.",
                cite.bibtex_id,
                cite.title,
                bib_path,
            )
            continue

        entry = bib_graph.entries[cite.bibtex_id]
        persons = entry.persons.get("author", [])
        authors = tuple(
            f"{p.last_names[0]}, {p.first_names[0]}"
            for p in persons
            if p.last_names and p.first_names
        )
        if not authors:
            logging.getLogger(__name__).warning(
                "Bib entry %r has no parseable authors; skipping.",
                cite.bibtex_id,
            )
            continue

        yield PaperCreate(
            title=cite.title,
            contents=cite.contents,
            bibtex_id=cite.bibtex_id,
            bibtex=entry.to_string("bibtex"),
            authors=authors,
        )


def _parse_tex_citations(tex_path: Path) -> Iterator[_TexCitation]:
    r"""Yield ``(title, contents, bibtex_id)`` triples from a legacy-format ``.tex``.

    Faithful port of the ``get_data`` loop: ``LatexNodes2Text`` converts
    the source to readable text, then a state machine walks the lines —
    a line containing both ``*`` and ``<cit.>`` is a title, the next
    non-empty line is the description, and the ``\\cite{KEY}`` on the
    matching raw-source line gives the bibtex key.
    """
    raw = tex_path.read_text(encoding="utf-8")
    rendered_lines = [
        line for line in LatexNodes2Text().latex_to_text(raw).split("\n") if line
    ]
    raw_lines = raw.split("\n")

    title: str | None = None
    bibtex_id: str | None = None
    for line in rendered_lines:
        if "*" in line and "<cit.>" in line:
            title = _extract_title(line)
            bibtex_id = _extract_bibtex_id(title, raw_lines) if title else None
            continue

        contents = line.strip()
        if contents and title and bibtex_id:
            yield _TexCitation(title=title, contents=contents, bibtex_id=bibtex_id)
            title = None
            bibtex_id = None


def _extract_title(line: str) -> str:
    """Pull the title out of a ``*Title*<cit.>:`` or ``...<cit.>: Title`` line."""
    after_cit = line.split("<cit.>")[1].rstrip(":")
    if after_cit.strip() == "":
        before_cit = line.split("<cit.>")[0]
        return before_cit.split("*")[1].strip()
    return after_cit.strip()


def _extract_bibtex_id(title: str, raw_lines: list[str]) -> str | None:
    r"""Find the ``\cite{KEY}`` on the raw-source line that names ``title``."""
    for raw_line in raw_lines:
        if title not in raw_line:
            continue
        parts = raw_line.split(r"\cite{")
        if len(parts) < 2:
            return None
        # Legacy quirk: when the line opens with ``\item``, the cite key is
        # in parts[1]; otherwise the original code reads parts[0], which
        # mis-parses lines that don't start with ``\item``. Preserve the
        # ``\item``-prefixed branch (the documented happy path) and bail
        # on the other shape rather than emitting garbage.
        if "\\item" not in parts[0]:
            return None
        return parts[1].split("}")[0]
    return None
