"""Canonical seed dataset for paper_sorts integration tests.

``SEED_PAPERS`` is a list of :class:`~paper_sorts.db.repositories.PaperCreate`
DTOs.  Every integration test that asserts on specific rows references this
module directly, making the coupling explicit and visible at review time.

Rows included
-------------
- "Direct speech-to-speech translation with discrete units" — multiple
  authors including "Pino, J." and "Lee, Ann"; exercises multi-author join.
- "Large-scale Self- an Semi-Supervised learning for speech translation" —
  author "Pino, J." and "Wang, Changhan"; validates legacy title from the
  old test suite.
- "Duplicate Title Paper" (appears twice with different bibtex keys) —
  exercises the disambiguation prompt path.
- "Paper with LaTeX Accents" — BibTeX contains LaTeX escape sequences
  (``{\"o}``, ``\\&``) to test round-trip fidelity.
"""

from paper_sorts.db.repositories import PaperCreate

SEED_PAPERS: list[PaperCreate] = [
    PaperCreate(
        title="Direct speech-to-speech translation with discrete units",
        contents="A model for direct speech-to-speech translation using discrete units.",
        bibtex_id="Lee2021DirectSpeech",
        bibtex=(
            "@inproceedings{Lee2021DirectSpeech,\n"
            "  author = {Lee, Ann and Chen, Peng-Jen and Wang, Changhan"
            " and Gu, Jiatao and Ma, Xutai and Polyak, A. and Adi, Yossi"
            " and He, Qing and Tang, Yun and Pino, J. and Hsu, Wei-Ning},\n"
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
        title="Large-scale Self- an Semi-Supervised learning for speech translation",
        contents="Large-scale self- and semi-supervised learning methods for speech translation.",
        bibtex_id="Wang2021LargeScaleSA",
        bibtex=(
            "@inproceedings{Wang2021LargeScaleSA,\n"
            "  author = {Wang, Changhan and Pino, J.},\n"
            "  title = {Large-scale Self- an Semi-Supervised learning for speech translation},\n"
            "  year = {2021}\n"
            "}"
        ),
        authors=["Wang, Changhan", "Pino, J."],
    ),
    PaperCreate(
        title="Duplicate Title Paper",
        contents="First paper with a duplicate title.",
        bibtex_id="Dup2021A",
        bibtex=(
            "@article{Dup2021A,\n"
            "  author = {Smith, John},\n"
            "  title = {Duplicate Title Paper},\n"
            "  year = {2021}\n"
            "}"
        ),
        authors=["Smith, John"],
    ),
    PaperCreate(
        title="Duplicate Title Paper",
        contents="Second paper with a duplicate title.",
        bibtex_id="Dup2021B",
        bibtex=(
            "@article{Dup2021B,\n"
            "  author = {Jones, Mary},\n"
            "  title = {Duplicate Title Paper},\n"
            "  year = {2021}\n"
            "}"
        ),
        authors=["Jones, Mary"],
    ),
    PaperCreate(
        title="Paper with LaTeX Accents",
        contents="Tests BibTeX round-trip for LaTeX accents and escapes.",
        bibtex_id="Accent2021",
        bibtex=(
            "@article{Accent2021,\n"
            "  author = {M{\\\"u}ller, Hans and Gonz{\\'a}lez, Maria},\n"
            "  title = {Paper with LaTeX Accents \\& Escapes},\n"
            "  year = {2021}\n"
            "}"
        ),
        authors=["Muller, Hans", "Gonzalez, Maria"],
    ),
]
