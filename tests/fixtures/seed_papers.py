"""Canonical seed dataset for the integration suite.

Co-located with the tests so that any assertion on a specific title, BibTeX key,
or author name is traceable to its source row (constitution Principle II). Rows
mirror the data the legacy ``tests/test_database_connector.py`` asserted on
(``Pino, J.`` / ``Wang2021LargeScaleSA`` and the multi-author speech-translation
paper), plus a LaTeX-accent edge case to exercise BibTeX round-tripping.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SeedPaper:
    """A single seed paper.

    :ivar title: paper title.
    :ivar contents: one-line summary.
    :ivar bibtex_id: unique BibTeX key.
    :ivar bibtex: full BibTeX source string.
    :ivar authors: author names in ``"Last, First"`` form.
    """

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str] = field(default_factory=list)


SEED_PAPERS: list[SeedPaper] = [
    SeedPaper(
        title="Large-scale Self- an Semi-Supervised learning for speech translation",
        contents="Self- and semi-supervised learning scaled up for speech translation.",
        bibtex_id="Wang2021LargeScaleSA",
        bibtex="@article{Wang2021LargeScaleSA,\n"
        "  title={Large-scale Self- an Semi-Supervised learning for speech translation},\n"
        "  author={Wang, Changhan and Pino, J. and Others, A.},\n"
        "  year={2021}\n}",
        authors=["Wang, Changhan", "Pino, J.", "Others, A."],
    ),
    SeedPaper(
        title="Direct speech-to-speech translation with discrete units",
        contents="Translating speech to speech directly via discrete units.",
        bibtex_id="Lee2022DirectS2ST",
        bibtex="@article{Lee2022DirectS2ST,\n"
        "  title={Direct speech-to-speech translation with discrete units},\n"
        "  author={Lee, Ann and Pino, J.},\n"
        "  year={2022}\n}",
        authors=[
            "Lee, Ann",
            "Chen, Peng-Jen",
            "Wang, Changhan",
            "Gu, Jiatao",
            "Ma, Xutai",
            "Polyak, A.",
            "Adi, Yossi",
            "He, Qing",
            "Tang, Yun",
            "Pino, J.",
            "Hsu, Wei-Ning",
        ],
    ),
    SeedPaper(
        title="On the use of LaTeX accents in metadata",
        contents="A note on round-tripping accents through the parser.",
        bibtex_id="Schroeder2020Accents",
        bibtex="@article{Schroeder2020Accents,\n"
        '  title={On the use of {\\"o} and \\& in metadata},\n'
        "  author={Schr{\\\"o}der, M.},\n"
        "  year={2020}\n}",
        authors=["Schröder, M."],
    ),
]
