"""Canonical seed dataset for the test suite.

Co-located with the tests per the constitution's testing-standards principle: any integration
test asserting on specific rows references ``SEED_PAPERS`` here, so the coupling is visible at
review time. The dataset deliberately includes:

- a BibTeX entry with LaTeX accents/escapes (``\\"o``, ``\\&``, ``{Pino}``) to exercise the
  round-trip edge case,
- a duplicate-title pair (two distinct papers sharing one title) to exercise the search
  disambiguation flow,
- an author who appears on more than one paper (orphan-cleanup coverage on delete).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SeedPaper:
    """One seeded paper.

    :ivar title: the paper title.
    :ivar summary: the paper summary (stored in ``papers.contents``).
    :ivar bibtex_id: the unique BibTeX key.
    :ivar bibtex: the full BibTeX source string.
    :ivar authors: the ``"Last, First"`` author names.
    """

    title: str
    summary: str
    bibtex_id: str
    bibtex: str
    authors: list[str] = field(default_factory=list)


SEED_PAPERS: list[SeedPaper] = [
    SeedPaper(
        title="Direct speech-to-speech translation with discrete units",
        summary="Translates speech to speech using discrete acoustic units.",
        bibtex_id="Lee2022DirectS2ST",
        bibtex=(
            "@article{Lee2022DirectS2ST,\n"
            "  title = {Direct speech-to-speech translation with discrete units},\n"
            "  author = {Lee, Ann and Chen, Peng-Jen and Pino, J.},\n"
            "  year = {2022},\n"
            "}"
        ),
        authors=["Lee, Ann", "Chen, Peng-Jen", "Pino, J."],
    ),
    SeedPaper(
        title="Large-scale Self- and Semi-Supervised learning for speech translation",
        summary="Self- and semi-supervised learning at scale for speech translation.",
        bibtex_id="Wang2021LargeScaleSA",
        bibtex=(
            "@article{Wang2021LargeScaleSA,\n"
            "  title = {Large-scale Self- and Semi-Supervised learning for speech translation},\n"
            "  author = {Wang, Changhan and Pino, J.},\n"
            "  year = {2021},\n"
            "}"
        ),
        authors=["Wang, Changhan", "Pino, J."],
    ),
    SeedPaper(
        title="Schöne Grüße: accents \\& escapes in BibTeX",
        summary="A paper whose BibTeX carries LaTeX accents and escapes.",
        bibtex_id="Mueller2020Accents",
        bibtex=(
            "@article{Mueller2020Accents,\n"
            '  title = {Sch{\\"o}ne Gr{\\"u}{\\ss}e: accents \\& escapes},\n'
            '  author = {M{\\"u}ller, Hans and {Pino}, J.},\n'
            "  year = {2020},\n"
            "}"
        ),
        authors=["Müller, Hans", "Pino, J."],
    ),
    SeedPaper(
        title="A shared title",
        summary="First paper sharing a title with another.",
        bibtex_id="Smith2019Shared",
        bibtex=(
            "@article{Smith2019Shared,\n"
            "  title = {A shared title},\n"
            "  author = {Smith, John},\n"
            "  year = {2019},\n"
            "}"
        ),
        authors=["Smith, John"],
    ),
    SeedPaper(
        title="A shared title",
        summary="Second paper sharing a title with another.",
        bibtex_id="Doe2018Shared",
        bibtex=(
            "@article{Doe2018Shared,\n"
            "  title = {A shared title},\n"
            "  author = {Doe, Jane},\n"
            "  year = {2018},\n"
            "}"
        ),
        authors=["Doe, Jane"],
    ),
]
