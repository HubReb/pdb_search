"""Canonical seed dataset for paper_sorts tests.

All integration tests that assert on specific rows MUST reference this file
(constitution Principle II — no hidden coupling to developer-local databases).

The seed contains:
  - PAPER_1: single-author paper with simple ASCII author name
  - PAPER_2: multi-author paper (3 authors)
  - PAPER_3: paper with LaTeX accents in BibTeX (round-trip safety check)

Test assertions should use the constants defined here so that changes to
seed data are visible at review time.
"""

from __future__ import annotations

from paper_sorts.db.repositories import PaperCreate

# Paper 1 — single author, simple ASCII
PAPER_1 = PaperCreate(
    title="Direct speech-to-speech translation with discrete units",
    contents="Proposes a direct S2ST model using discrete speech units as target.",
    bibtex_id="Lee2021DirectSpeech",
    bibtex=(
        "@inproceedings{Lee2021DirectSpeech,\n"
        "  author = {Lee, Ann},\n"
        "  title  = {Direct speech-to-speech translation with discrete units},\n"
        "  year   = {2021},\n"
        "}"
    ),
    authors=["Lee, Ann"],
)

# Paper 2 — multiple authors
PAPER_2 = PaperCreate(
    title="Large-scale Self- and Semi-Supervised Learning for Speech Translation",
    contents="Combines large-scale self-supervised and semi-supervised training for ST.",
    bibtex_id="Wang2021LargeScaleSA",
    bibtex=(
        "@inproceedings{Wang2021LargeScaleSA,\n"
        "  author = {Wang, Changhan and Pino, J. and Tang, Yun},\n"
        "  title  = {Large-scale Self- and Semi-Supervised Learning for Speech Translation},\n"
        "  year   = {2021},\n"
        "}"
    ),
    authors=["Wang, Changhan", "Pino, J.", "Tang, Yun"],
)

# Paper 3 — BibTeX with LaTeX accents (round-trip check)
PAPER_3 = PaperCreate(
    title="Unsupervised Cross-lingual Representation Learning",
    contents="Learns cross-lingual representations without parallel data.",
    bibtex_id="Conneau2019XLMR",
    bibtex=(
        "@article{Conneau2019XLMR,\n"
        r'  author = {Conneau, Alexis and Lample, Guillaume},' + "\n"
        r'  title  = {Unsupervised Cross-lingual Representation Learning},' + "\n"
        "  journal = {NeurIPS},\n"
        "  year    = {2019},\n"
        "}"
    ),
    authors=["Conneau, Alexis", "Lample, Guillaume"],
)

# Canonical ordered list — add new papers at the end to avoid breaking test indices
SEED_PAPERS = [PAPER_1, PAPER_2, PAPER_3]
