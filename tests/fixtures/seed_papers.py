"""Canonical seed dataset for integration tests.

Any test asserting on a specific title, author, or BibTeX key must trace back
to an entry here (Constitution Principle II — seed data co-located with the
assertions). The dataset deliberately covers:

* a paper with multiple authors (pretty-print "A and B and ..."),
* two papers sharing a title (search-by-title disambiguation),
* an author credited on more than one paper (search-by-author disambiguation),
* a BibTeX entry carrying LaTeX accents/escapes (round-trip fidelity).
"""

from __future__ import annotations

from paper_sorts.db.repositories import PaperCreate

SEED_PAPERS: list[PaperCreate] = [
    PaperCreate(
        title="Direct speech-to-speech translation with discrete units",
        authors=["Lee, Ann", "Chen, Peng-Jen", "Pino, J."],
        summary="A speech-to-speech translation model using discrete units.",
        bibtex_id="Lee2022Direct",
        bibtex=(
            "@article{Lee2022Direct,\n"
            "  title = {Direct speech-to-speech translation with discrete units},\n"
            "  author = {Lee, Ann and Chen, Peng-Jen and Pino, J.},\n"
            "  year = {2022}\n"
            "}"
        ),
    ),
    PaperCreate(
        title="Large-scale Self- and Semi-Supervised learning for speech translation",
        authors=["Wang, Changhan", "Pino, J."],
        summary="Self- and semi-supervised learning at scale for speech translation.",
        bibtex_id="Wang2021LargeScaleSA",
        bibtex=(
            "@article{Wang2021LargeScaleSA,\n"
            "  title = {Large-scale Self- and Semi-Supervised learning for speech "
            "translation},\n"
            "  author = {Wang, Changhan and Pino, J.},\n"
            "  year = {2021}\n"
            "}"
        ),
    ),
    # Shared-title pair (disambiguation by author).
    PaperCreate(
        title="On Calibration",
        authors=['Mueller, J\\"org'],
        summary="First paper sharing the title 'On Calibration'.",
        bibtex_id="Mueller2020Calibration",
        bibtex=(
            "@article{Mueller2020Calibration,\n"
            "  title = {On Calibration},\n"
            '  author = {Mueller, J\\"org},\n'
            "  note = {accents \\& escapes survive},\n"
            "  year = {2020}\n"
            "}"
        ),
    ),
    PaperCreate(
        title="On Calibration",
        authors=["Smith, Jane"],
        summary="Second paper sharing the title 'On Calibration'.",
        bibtex_id="Smith2021Calibration",
        bibtex=(
            "@article{Smith2021Calibration,\n"
            "  title = {On Calibration},\n"
            "  author = {Smith, Jane},\n"
            "  year = {2021}\n"
            "}"
        ),
    ),
]
