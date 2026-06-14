"""Seed dataset for paper_sorts tests.

SEED_PAPERS is the canonical dataset used by integration tests.
Each entry corresponds to a PaperCreate DTO that will be inserted
into the ephemeral test database.

Seed data supports these test assertions:
  - search_by_title("Large-scale") → 1 result (Wang2021LargeScaleSA)
  - search_by_title("speech") → 2 results (Wang2021LargeScaleSA, Lee2021Direct)
    (title-collision scenario with common term)
  - search_by_author("Pino") → 2 results (Wang2021 and Lee2021 share author Pino, J.)
  - add_paper duplicate bibtex_id → raises ValueError
  - update_field title for Wang2021LargeScaleSA → verifiable
  - delete_paper Wang2021LargeScaleSA → Smith2022 still exists; Pino still linked to Lee2021
"""

from paper_sorts.db.repositories import PaperCreate

SEED_PAPERS: list[PaperCreate] = [
    PaperCreate(
        title="Large-scale Self- and Semi-Supervised Learning for Speech Translation",
        contents=(
            "We study low-resource speech translation (ST) by leveraging"
            " self-supervised and semi-supervised learning."
        ),
        bibtex_id="Wang2021LargeScaleSA",
        bibtex=(
            "@article{Wang2021LargeScaleSA,\n"
            "  author    = {Wang, Changhan and Pino, J.},\n"
            "  title     = {Large-scale Self- and Semi-Supervised Learning"
            " for Speech Translation},\n"
            "  year      = {2021}\n"
            "}"
        ),
        authors=["Wang, Changhan", "Pino, J."],
    ),
    PaperCreate(
        title="Direct speech-to-speech translation with discrete units",
        contents=(
            "We present a direct speech-to-speech translation system"
            " that produces discrete speech units."
        ),
        bibtex_id="Lee2021Direct",
        bibtex=(
            "@article{Lee2021Direct,\n"
            "  author    = {Lee, Ann and Pino, J.},\n"
            "  title     = {Direct speech-to-speech translation with discrete units},\n"
            "  year      = {2021}\n"
            "}"
        ),
        authors=["Lee, Ann", "Pino, J."],
    ),
    PaperCreate(
        title="A survey of transformer architectures for NLP",
        contents="We survey transformer variants used in natural language processing.",
        bibtex_id="Smith2022Survey",
        bibtex=(
            "@article{Smith2022Survey,\n"
            "  author    = {Smith, John},\n"
            "  title     = {A survey of transformer architectures for NLP},\n"
            "  year      = {2022}\n"
            "}"
        ),
        authors=["Smith, John"],
    ),
]
