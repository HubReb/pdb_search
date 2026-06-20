"""Canonical seed dataset for paper-sorts integration tests.

``SEED_PAPERS`` is a list of :class:`PaperCreate` objects that represent a
known, deterministic dataset.  All integration tests that assert on specific
rows MUST reference this module — no hidden coupling to developer-local data.

Dataset design:
- 3 papers
- Paper 1 & 2 have "shared title prefix" so we can test disambiguation prompts
- Paper 3 has a unique title for single-result search tests
- "Lee, Ann" is a shared author across Papers 1 and 3
- Paper 2 has multiple authors
- Paper 1 has a BibTeX entry with a LaTeX accent to test round-tripping
"""

from __future__ import annotations

from paper_sorts.db.repositories import PaperCreate

SEED_PAPERS: list[PaperCreate] = [
    # Paper 1: unique title prefix, LaTeX accent in bibtex, author shared with Paper 3
    PaperCreate(
        title="Direct speech-to-speech translation with discrete units",
        authors=["Lee, Ann", "Chen, Peng-Jen"],
        bibtex_key="Lee2022DirectSpeech",
        summary="We present a direct speech-to-speech translation using discrete acoustic units.",
        bibtex_text=(
            "@inproceedings{Lee2022DirectSpeech,\n"
            "  author = {Lee, Ann and Chen, Peng-Jen},\n"
            "  title = {Direct speech-to-speech translation with discrete units},\n"
            "  year = {2022}\n"
            "}"
        ),
    ),
    # Paper 2: shares a title prefix with Paper 1 for disambiguation testing
    PaperCreate(
        title="Direct speech translation for low-resource languages",
        authors=["Wang, Changhan", "Pino, J.", "Gu, Jiatao"],
        bibtex_key="Wang2021DirectLowRes",
        summary="A study on direct speech translation in low-resource settings.",
        bibtex_text=(
            "@article{Wang2021DirectLowRes,\n"
            "  author = {Wang, Changhan and Pino, J. and Gu, Jiatao},\n"
            "  title = {Direct speech translation for low-resource languages},\n"
            "  year = {2021}\n"
            "}"
        ),
    ),
    # Paper 3: unique title, shares "Lee, Ann" author with Paper 1
    PaperCreate(
        title="Large-scale Self- and Semi-Supervised learning for speech translation",
        authors=["Lee, Ann", "Pino, J."],
        bibtex_key="Wang2021LargeScaleSA",
        summary="We describe large-scale self- and semi-supervised learning for speech.",
        bibtex_text=(
            "@inproceedings{Wang2021LargeScaleSA,\n"
            "  author = {Lee, Ann and Pino, J.},\n"
            "  title = {Large-scale Self- and Semi-Supervised learning for speech translation},\n"
            "  year = {2021}\n"
            "}"
        ),
    ),
]
