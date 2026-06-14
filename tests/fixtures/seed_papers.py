"""Canonical seed dataset for paper_sorts integration tests.

Each entry in ``SEED_PAPERS`` is a :class:`~paper_sorts.db.repositories.PaperCreate`
DTO.  All integration tests that assert on specific rows MUST use only data
defined here, so the relationship between test assertions and seed rows is
visible at review time (constitution Principle II).

Seed entries cover:
    - Single-author paper (search_by_author tests)
    - Multi-author paper (pretty-print join tests)
    - Pair of papers with the same title (disambiguation tests)
    - Paper used for update tests (title, contents, bibtex, author)
    - Paper used for delete tests
"""

from paper_sorts.db.repositories import PaperCreate

SEED_PAPERS: list[PaperCreate] = [
    # Entry 1: single author, unique title
    PaperCreate(
        title="Direct speech-to-speech translation with discrete units",
        contents="A model for end-to-end speech-to-speech translation without intermediate text.",
        bibtex_id="Lee2021DirectS2S",
        bibtex=(
            "@inproceedings{Lee2021DirectS2S,\n"
            "  author = {Lee, Ann},\n"
            "  title = {Direct speech-to-speech translation with discrete units},\n"
            "  year = {2021}\n"
            "}"
        ),
        authors=["Lee, Ann"],
    ),
    # Entry 2: multi-author paper
    PaperCreate(
        title="Large-scale Self- and Semi-Supervised learning for speech translation",
        contents="Self-supervised pre-training for speech translation at scale.",
        bibtex_id="Wang2021LargeScaleSA",
        bibtex=(
            "@inproceedings{Wang2021LargeScaleSA,\n"
            "  author = {Wang, Changhan and Pino, J.},\n"
            "  title = {Large-scale Self- and Semi-Supervised learning for speech translation},\n"
            "  year = {2021}\n"
            "}"
        ),
        authors=["Wang, Changhan", "Pino, J."],
    ),
    # Entry 3a: first paper with a duplicate title (disambiguation tests)
    PaperCreate(
        title="Attention is all you need",
        contents="The transformer architecture using self-attention.",
        bibtex_id="Vaswani2017Attention",
        bibtex=(
            "@inproceedings{Vaswani2017Attention,\n"
            "  author = {Vaswani, Ashish},\n"
            "  title = {Attention is all you need},\n"
            "  year = {2017}\n"
            "}"
        ),
        authors=["Vaswani, Ashish"],
    ),
    # Entry 3b: second paper with the same title (disambiguation tests)
    PaperCreate(
        title="Attention is all you need",
        contents="A replication study of the transformer paper.",
        bibtex_id="Vaswani2017AttentionRepl",
        bibtex=(
            "@inproceedings{Vaswani2017AttentionRepl,\n"
            "  author = {Doe, Jane},\n"
            "  title = {Attention is all you need},\n"
            "  year = {2017}\n"
            "}"
        ),
        authors=["Doe, Jane"],
    ),
    # Entry 4: paper for update tests
    PaperCreate(
        title="Updateable paper title",
        contents="Original summary before update.",
        bibtex_id="Update2024Test",
        bibtex=(
            "@misc{Update2024Test,\n"
            "  author = {Updater, Alice},\n"
            "  title = {Updateable paper title},\n"
            "  year = {2024}\n"
            "}"
        ),
        authors=["Updater, Alice"],
    ),
    # Entry 5: paper for delete tests
    PaperCreate(
        title="Paper to be deleted",
        contents="This paper will be removed by a delete test.",
        bibtex_id="Delete2024Test",
        bibtex=(
            "@misc{Delete2024Test,\n"
            "  author = {Deleter, Bob},\n"
            "  title = {Paper to be deleted},\n"
            "  year = {2024}\n"
            "}"
        ),
        authors=["Deleter, Bob"],
    ),
]
