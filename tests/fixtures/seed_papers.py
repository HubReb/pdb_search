"""Canonical seed dataset for paper_sorts integration tests.

All tests that assert on specific rows MUST reference this fixture so that
the relationship between test assertions and seeded data is visible at
review time (constitution Principle II).

The dataset covers:
- A paper with multiple authors including "Pino, J." / key "Wang2021LargeScaleSA"
  (matching the legacy test assertion in test_database_connector.py).
- A paper "Direct speech-to-speech translation with discrete units" with its
  full author list (matching the legacy title-search assertion).
- A minimal test paper with a single author.
"""

from paper_sorts.db.repositories import PaperCreate

SEED_PAPERS: list[PaperCreate] = [
    PaperCreate(
        title="Large-scale Self- and Semi-Supervised Learning for Speech Translation",
        contents=(
            "Self-supervised and semi-supervised learning methods are applied at scale "
            "to improve speech translation performance significantly."
        ),
        bibtex_id="Wang2021LargeScaleSA",
        bibtex=(
            "@article{Wang2021LargeScaleSA,\n"
            "  author = {Wang, Changhan and others},\n"
            "  title = {Large-scale Self- and Semi-Supervised Learning for Speech Translation},\n"
            "  year = {2021}\n"
            "}"
        ),
        authors=["Wang, Changhan", "Pino, J.", "Wu, Anne"],
    ),
    PaperCreate(
        title="Direct speech-to-speech translation with discrete units",
        contents=(
            "Proposes a direct speech-to-speech translation system using discrete "
            "speech units as the target representation."
        ),
        bibtex_id="Lee2021DirectSpeech",
        bibtex=(
            "@article{Lee2021DirectSpeech,\n"
            "  author = {Lee, Ann and others},\n"
            "  title = {Direct speech-to-speech translation with discrete units},\n"
            "  year = {2021}\n"
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
        title="A Minimal Test Paper",
        contents="This is a minimal test paper used by the integration test suite.",
        bibtex_id="MinimalTest2026",
        bibtex=(
            "@misc{MinimalTest2026,\n"
            "  author = {Tester, Unit},\n"
            "  title = {A Minimal Test Paper},\n"
            "  year = {2026}\n"
            "}"
        ),
        authors=["Tester, Unit"],
    ),
]
