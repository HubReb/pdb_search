"""Canonical seed dataset for the test suite.

Co-located with the tests that assert on it (constitution Principle II: any test
that asserts on specific seeded rows must reference the seed that produces them).
The fingerprints here reproduce the rows the legacy integration test relied on —
``Pino, J.`` / ``Wang2021LargeScaleSA`` and the multi-author speech-to-speech
paper — so historical assertions remain meaningful, plus a second paper that
shares its title with another to exercise the disambiguation path.
"""

from __future__ import annotations

from paper_sorts.db.repositories import PaperCreate

SEED_PAPERS: list[PaperCreate] = [
    PaperCreate(
        title="Large-scale Self- an Semi-Supervised learning for speech translation",
        contents="Self- and semi-supervised learning scaled up for speech translation.",
        bibtex_id="Wang2021LargeScaleSA",
        bibtex=(
            "@article{Wang2021LargeScaleSA,\n"
            "  title={Large-scale Self- an Semi-Supervised learning for speech "
            "translation},\n"
            "  author={Wang, Changhan and Pino, J. and Gu, Jiatao},\n"
            "  year={2021}\n"
            "}"
        ),
        authors=["Wang, Changhan", "Pino, J.", "Gu, Jiatao"],
    ),
    PaperCreate(
        title="Direct speech-to-speech translation with discrete units",
        contents="Direct S2ST using discrete acoustic units.",
        bibtex_id="Lee2022DirectS2ST",
        bibtex=(
            "@article{Lee2022DirectS2ST,\n"
            "  title={Direct speech-to-speech translation with discrete units},\n"
            "  author={Lee, Ann and Chen, Peng-Jen and Wang, Changhan and Gu, "
            "Jiatao and Ma, Xutai and Polyak, A. and Adi, Yossi and He, Qing and "
            "Tang, Yun and Pino, J. and Hsu, Wei-Ning},\n"
            "  year={2022}\n"
            "}"
        ),
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
    PaperCreate(
        title="Attention is all you need",
        contents="The Transformer architecture (first of two same-title rows).",
        bibtex_id="Vaswani2017Attention",
        bibtex=(
            "@article{Vaswani2017Attention,\n"
            "  title={Attention is all you need},\n"
            "  author={Vaswani, Ashish and Shazeer, Noam},\n"
            "  year={2017}\n"
            "}"
        ),
        authors=["Vaswani, Ashish", "Shazeer, Noam"],
    ),
    PaperCreate(
        title="Attention is all you need",
        contents="A different paper that happens to share the title.",
        bibtex_id="Doe2020Attention",
        bibtex=(
            "@article{Doe2020Attention,\n"
            "  title={Attention is all you need},\n"
            "  author={Doe, Jane},\n"
            "  year={2020}\n"
            "}"
        ),
        authors=["Doe, Jane"],
    ),
]
