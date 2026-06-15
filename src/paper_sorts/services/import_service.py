"""Bulk import from a LaTeX literature overview plus its BibTeX file.

:func:`extract_papers_from_tex_bib` reproduces the legacy ``get_data.py``
parsing: it reads a ``.tex`` overview to pair each paper's title and summary
with its citation key, then resolves each key against a ``.bib`` file to obtain
the authors and the full BibTeX source. A citation key with no matching BibTeX
record is skipped with a logged warning rather than failing the whole import
(FR-002, US5-2).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator

from pybtex.database import parse_file
from pylatexenc.latex2text import LatexNodes2Text

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.logging_config import get_logger

logger = get_logger(__name__)


def _parse_tex(tex_path: str) -> dict[str, dict[str, str]]:
    """Parse the ``.tex`` overview into ``{title: {bibtex_id, contents}}``.

    :param tex_path: path to the LaTeX literature-overview file.
    :return: a mapping of title to its citation key and summary.
    """
    with open(tex_path, encoding="utf-8") as handle:
        data = handle.read()
    text = LatexNodes2Text().latex_to_text(data).split("\n")
    lines = [line for line in text if line != ""]
    papers: dict[str, dict[str, str]] = defaultdict(lambda: defaultdict(str))
    title: str | None = None
    bibtex: str | None = None
    for line in lines:
        if "*" in line and "<cit.>" in line:  # title line
            before, after = line.split("<cit.>", 1)
            after = after.strip().rstrip(":").strip()
            # The title may sit before or after the citation marker depending on
            # the source layout; prefer the non-empty side.
            title = after if after else before.split("*", 1)[1].strip()
            title = title.strip()
            bibtex = _citation_key_for(data, title)
            description = None
        else:
            description = line.strip()
        if description and title:
            papers[title]["bibtex_id"] = bibtex or ""
            papers[title]["contents"] = description
            title, bibtex = None, None
    return papers


def _citation_key_for(raw_latex: str, title: str) -> str | None:
    """Find the citation key associated with a title in the raw LaTeX source.

    :param raw_latex: the full LaTeX file contents.
    :param title: the resolved (text) title to locate.
    :return: the citation key, or ``None`` if not found.
    """
    for latex_line in raw_latex.split("\n"):
        if title in latex_line:
            split_at_cite = latex_line.split(r"\cite{")
            if len(split_at_cite) < 2:
                continue
            chunk = split_at_cite[1] if "\\item" in split_at_cite[0] else split_at_cite[0]
            if "}" not in chunk:
                chunk = split_at_cite[1]
            return chunk.split("}")[0]
    return None


def extract_papers_from_tex_bib(tex_path: str, bib_path: str) -> Iterator[PaperCreate]:
    """Yield one :class:`PaperCreate` per cited entry with a matching BibTeX record.

    :param tex_path: path to the LaTeX literature-overview file.
    :param bib_path: path to the BibTeX file.
    :return: an iterator of :class:`PaperCreate`, skipping unmatched citation keys.
    """
    papers = _parse_tex(tex_path)
    bib_graph = parse_file(bib_path, bib_format="bibtex")
    for title, values in papers.items():
        key = values.get("bibtex_id", "")
        if not key:
            continue
        if key not in bib_graph.entries:
            logger.warning("citation key %s has no matching .bib record — skipping", key)
            continue
        entry = bib_graph.entries[key]
        authors = [
            f"{person.last_names[0]}, {person.first_names[0]}"
            for person in entry.persons.get("author", [])
        ]
        yield PaperCreate(
            title=title,
            authors=authors,
            summary=values.get("contents", ""),
            bibtex_id=key,
            bibtex=entry.to_string("bibtex"),
        )
