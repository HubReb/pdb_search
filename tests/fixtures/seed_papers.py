"""Canonical seed dataset for integration tests.

Tests that assert on specific titles, authors, or BibTeX keys reference these
rows by name, so the coupling between an assertion and the data that produces it
is visible at review time (constitution Principle II).

The dataset deliberately includes:
- a multi-author paper (``discrete_units``),
- a shared-title pair (``shared_a`` / ``shared_b`` share the title "Shared Title")
  to exercise disambiguation,
- a LaTeX-accent bib entry (``accent``) to exercise round-tripping.
"""

from __future__ import annotations

from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperCreate, PaperRepository
from paper_sorts.db.session import with_session

SEED_PAPERS: list[PaperCreate] = [
    PaperCreate(
        title="Direct speech-to-speech translation with discrete units",
        summary="Translates speech directly into speech via discrete units.",
        bibtex_id="Lee2022DirectSpeech",
        bibtex="@article{Lee2022DirectSpeech,\n  title={Direct speech-to-speech translation with discrete units},\n  author={Lee, Ann and Chen, Peng-Jen and Pino, J.},\n}",
        authors=["Lee, Ann", "Chen, Peng-Jen", "Pino, J."],
    ),
    PaperCreate(
        title="Large-scale Self- an Semi-Supervised learning for speech translation",
        summary="Self- and semi-supervised learning at scale for speech translation.",
        bibtex_id="Wang2021LargeScaleSA",
        bibtex="@article{Wang2021LargeScaleSA,\n  title={Large-scale Self- an Semi-Supervised learning for speech translation},\n  author={Wang, Changhan and Pino, J.},\n}",
        authors=["Wang, Changhan", "Pino, J."],
    ),
    PaperCreate(
        title="Shared Title",
        summary="First paper sharing a title.",
        bibtex_id="Shared2020A",
        bibtex="@article{Shared2020A,\n  title={Shared Title},\n  author={Alpha, Anne},\n}",
        authors=["Alpha, Anne"],
    ),
    PaperCreate(
        title="Shared Title",
        summary="Second paper sharing a title.",
        bibtex_id="Shared2021B",
        bibtex="@article{Shared2021B,\n  title={Shared Title},\n  author={Beta, Bob},\n}",
        authors=["Beta, Bob"],
    ),
    PaperCreate(
        title="On accents and escapes",
        summary="Round-trips LaTeX accents.",
        bibtex_id="Mueller2019Accents",
        bibtex='@article{Mueller2019Accents,\n  title={On accents and escapes},\n  author={M{\\"u}ller, Jörg and {Pino}, J.},\n}',
        authors=['M\\"uller, Jörg'],
    ),
]


def load_seed(engine: Engine) -> None:
    """Insert :data:`SEED_PAPERS` into the database behind ``engine``."""
    repo = PaperRepository()
    from paper_sorts.db.repositories import AuthorRepository, BibRepository

    authors = AuthorRepository()
    bib = BibRepository()
    with with_session(engine) as session:
        for paper in SEED_PAPERS:
            bib.add(session, paper.bibtex_id, paper.bibtex)
            paper_id = repo.add(session, paper)
            for author in paper.authors:
                authors.link(session, author, paper_id)
