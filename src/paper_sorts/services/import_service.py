"""Bulk-import extraction from a LaTeX overview + matching BibTeX file.

Ports the legacy ``get_data.py`` walk into a pure extractor that yields a
:class:`~paper_sorts.db.repositories.PaperCreate` per cited entry that has a
matching ``.bib`` record. Citation keys without a match are skipped (the caller
logs a warning). No database access happens here.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator

from pybtex.database import parse_file
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate


def _parse_tex(tex_path: str) -> dict[str, dict[str, str]]:
    """Extract ``{title: {bibtex_id, contents}}`` from a LaTeX overview file.

    :param tex_path: path to the ``.tex`` literature overview.
    :returns: a mapping of paper title to its citation key and summary.
    """
    with open(tex_path, encoding="utf-8") as handle:
        data = handle.read()
    lines = [line for line in LatexNodes2Text().latex_to_text(data).split("\n") if line != ""]
    papers: dict[str, dict[str, str]] = defaultdict(lambda: defaultdict(str))
    title: str | None = None
    bibtex_key: str | None = None
    for line in lines:
        if "*" in line and "<cit.>" in line:
            title = line.split("<cit.>")[1].rstrip(":").strip()
            if not title:
                # The title precedes the citation marker (e.g. "* Title <cit.>:").
                title = line.split("<cit.>")[0].split("*", 1)[1]
            title = title.strip()
            bibtex_key = _find_citation_key(data, title)
            description = None
        else:
            description = line.strip()
        if description and title:
            papers[title]["bibtex_id"] = bibtex_key or ""
            papers[title]["contents"] = description
            title, bibtex_key = None, None
    return papers


def _find_citation_key(data: str, title: str) -> str | None:
    """Locate the ``\\cite{...}`` key associated with a title in the raw LaTeX.

    :param data: the raw LaTeX source.
    :param title: the paper title to locate.
    :returns: the citation key, or ``None`` if not found.
    """
    for latex_line in data.split("\n"):
        if title in latex_line:
            split = latex_line.split(r"\cite{")
            if len(split) < 2:
                return None
            fragment = split[1] if "\\item" in split[0] else split[0]
            return fragment.split("}")[0]
    return None


def extract_papers_from_tex_bib(tex_path: str, bib_path: str) -> Iterator[PaperCreate]:
    """Yield a :class:`PaperCreate` for each cited entry with a matching bib record.

    :param tex_path: path to the ``.tex`` literature overview.
    :param bib_path: path to the matching ``.bib`` file.
    :yields: one :class:`PaperCreate` per matched paper (unmatched keys skipped).
    """
    papers = _parse_tex(tex_path)
    bib_graph = parse_file(bib_path, bib_format="bibtex")
    by_key = {
        meta["bibtex_id"]: (title, meta["contents"])
        for title, meta in papers.items()
        if meta.get("bibtex_id")
    }
    for key in bib_graph.entries:
        if key not in by_key:
            continue
        title, contents = by_key[key]
        entry = bib_graph.entries[key]
        authors = [
            f"{author.last_names[0]}, {author.first_names[0]}"
            for author in entry.persons.get("author", [])
        ]
        yield PaperCreate(
            title=title,
            contents=contents,
            bibtex_id=key,
            bibtex=entry.to_string("bibtex"),
            authors=authors,
        )
