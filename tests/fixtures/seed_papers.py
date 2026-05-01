"""Seed dataset for integration tests.

This module is the single source of truth for what rows the integration
tests assert on. Per constitution Principle II (v1.3.0), any test that
asserts on specific seeded rows must reference this fixture.

Coverage profile (deliberately small but exercises every edge case
named in the spec):

- 4 papers total
- 2 papers share the title "On Fairness in Machine Translation"
  (disambiguation prompt — spec edge case)
- 3 distinct authors; "Pino, J." appears on 2 papers (multi-paper-per-author
  search path); "Lee, A." appears on 1 paper
- One paper has a single author, one has three authors (tests the
  " and "-joined display path)
- BibTeX entries cover both `@article` and `@inproceedings` types
- One BibTeX value contains a LaTeX accent escape (`\\"o`) to verify
  round-tripping of pylatexenc/pybtex output (spec edge case)
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SeedPaper:
    """One paper plus its bib + author list, ready to insert via the service layer."""

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


SEED_PAPERS: list[SeedPaper] = [
    SeedPaper(
        title="Direct speech-to-speech translation with discrete units",
        contents=(
            "Trains an end-to-end model that emits discrete speech units "
            "without an intermediate text representation."
        ),
        bibtex_id="Lee2022DirectSpeechToSpeech",
        bibtex=(
            "@inproceedings{Lee2022DirectSpeechToSpeech,\n"
            "  author = {Lee, A. and Pino, J. and Wang, C.},\n"
            "  title = {Direct speech-to-speech translation with discrete units},\n"
            "  booktitle = {ACL},\n"
            "  year = {2022}\n"
            "}\n"
        ),
        authors=["Lee, A.", "Pino, J.", "Wang, C."],
    ),
    SeedPaper(
        title="Large-scale Self- and Semi-Supervised learning for speech translation",
        contents=(
            "Combines self-supervised pre-training with semi-supervised "
            "fine-tuning for speech translation at scale."
        ),
        bibtex_id="Wang2021LargeScaleSA",
        bibtex=(
            "@article{Wang2021LargeScaleSA,\n"
            "  author = {Wang, C. and Pino, J.},\n"
            "  title = {Large-scale Self- and Semi-Supervised learning for "
            "speech translation},\n"
            "  journal = {arXiv preprint},\n"
            "  year = {2021}\n"
            "}\n"
        ),
        authors=["Wang, C.", "Pino, J."],
    ),
    SeedPaper(
        title="On Fairness in Machine Translation",
        contents=(
            "Position paper on fairness metrics for MT systems; "
            "the variant by Schöttler et al."
        ),
        bibtex_id="Schoettler2023FairnessMT",
        bibtex=(
            "@article{Schoettler2023FairnessMT,\n"
            "  author = {Sch{\\\"o}ttler, K.},\n"
            "  title = {On Fairness in Machine Translation},\n"
            "  journal = {Proc. WMT},\n"
            "  year = {2023}\n"
            "}\n"
        ),
        authors=["Schöttler, K."],
    ),
    SeedPaper(
        title="On Fairness in Machine Translation",
        contents=(
            "Empirical study on fairness in MT outputs by Lee; "
            "shares the title verbatim with the Schöttler paper to "
            "exercise the disambiguation prompt."
        ),
        bibtex_id="Lee2024FairnessMT",
        bibtex=(
            "@inproceedings{Lee2024FairnessMT,\n"
            "  author = {Lee, A.},\n"
            "  title = {On Fairness in Machine Translation},\n"
            "  booktitle = {EMNLP},\n"
            "  year = {2024}\n"
            "}\n"
        ),
        authors=["Lee, A."],
    ),
]
