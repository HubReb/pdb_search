"""Canonical seed dataset for paper_sorts tests.

All integration tests that assert on specific rows MUST reference this module
rather than embedding hardcoded strings inline.  This makes the coupling
between seed data and assertions visible at review time.
"""

from __future__ import annotations

from paper_sorts.db.repositories import PaperCreate

# ---------------------------------------------------------------------------
# Seed data
# ---------------------------------------------------------------------------

SEED_PAPERS: list[PaperCreate] = [
    PaperCreate(
        title="Direct speech-to-speech translation with discrete units",
        contents=(
            "A study on direct speech-to-speech translation using discrete unit representations."
        ),
        bibtex_id="Lee2022DirectSpeech",
        bibtex=(
            "@inproceedings{Lee2022DirectSpeech,\n"
            '  author = {Lee, Ann and Chen, Peng-Jen},\n'
            '  title = {Direct speech-to-speech translation with discrete units},\n'
            '  year = {2022}\n'
            "}"
        ),
        authors=[
            "Lee, Ann",
            "Chen, Peng-Jen",
            "Wang, Changhan",
        ],
    ),
    PaperCreate(
        title="Large-scale Self- and Semi-Supervised learning for speech translation",
        contents="Large-scale self- and semi-supervised methods for speech translation tasks.",
        bibtex_id="Wang2021LargeScaleSA",
        bibtex=(
            "@inproceedings{Wang2021LargeScaleSA,\n"
            '  author = {Wang, Changhan and Pino, J.},\n'
            '  title = {Large-scale Self- and Semi-Supervised learning for speech translation},\n'
            '  year = {2021}\n'
            "}"
        ),
        # Authors include one with LaTeX accent in name (unicode passthrough)
        authors=["Wang, Changhan", "Pino, J."],
    ),
    PaperCreate(
        title="Shared title paper variant A",
        contents="First paper with a shared title — tests disambiguation.",
        bibtex_id="SharedA2023",
        bibtex=(
            "@article{SharedA2023,\n"
            '  author = {M\\"uller, Hans},\n'
            '  title = {Shared title paper variant A},\n'
            '  year = {2023}\n'
            "}"
        ),
        # Author with LaTeX accent in BibTeX; stored name uses unicode
        authors=["Müller, Hans"],
    ),
    PaperCreate(
        title="Shared title paper variant B",
        contents="Second paper with a shared title — tests disambiguation.",
        bibtex_id="SharedB2023",
        bibtex=(
            "@article{SharedB2023,\n"
            '  author = {M\\"uller, Hans},\n'
            '  title = {Shared title paper variant B},\n'
            '  year = {2023}\n'
            "}"
        ),
        authors=["Müller, Hans"],
    ),
]
