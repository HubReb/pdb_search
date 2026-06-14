"""Canonical seed dataset for paper_sorts tests.

SEED_PAPERS is the authoritative fixture dataset used by conftest.py.
All tests that assert on specific titles, authors, or bibtex_ids must
reference this constant so the relationship is visible at review time
(constitution Principle II).

Dataset design:
- Paper 1: unique title, two authors → tests single-result title search
- Paper 2: shared title with Paper 3, one author → tests disambiguation prompt
- Paper 3: shared title with Paper 2, two authors, one shared with Paper 1 → disambiguation + author search
- Paper 4: unique title, one author → tests single-result author search
"""

from paper_sorts.db.repositories import PaperCreate

SEED_PAPERS: list[PaperCreate] = [
    PaperCreate(
        title="Attention Is All You Need",
        contents="Proposes the Transformer architecture based solely on attention mechanisms.",
        bibtex_id="Vaswani2017AttentionIA",
        bibtex=(
            "@inproceedings{Vaswani2017AttentionIA,\n"
            "  title={Attention Is All You Need},\n"
            "  author={Vaswani, Ashish and Shazeer, Noam},\n"
            "  year={2017}\n"
            "}"
        ),
        authors=["Vaswani, Ashish", "Shazeer, Noam"],
    ),
    PaperCreate(
        title="BERT: Pre-training of Deep Bidirectional Transformers",
        contents="Introduces BERT, a language model pre-trained on masked language modelling.",
        bibtex_id="Devlin2019BERT",
        bibtex=(
            "@inproceedings{Devlin2019BERT,\n"
            "  title={BERT: Pre-training of Deep Bidirectional Transformers},\n"
            "  author={Devlin, Jacob},\n"
            "  year={2019}\n"
            "}"
        ),
        authors=["Devlin, Jacob"],
    ),
    PaperCreate(
        title="BERT: Pre-training of Deep Bidirectional Transformers",
        contents="Chinese version of BERT, adapted for Chinese NLP tasks.",
        bibtex_id="Cui2020BERT",
        bibtex=(
            "@inproceedings{Cui2020BERT,\n"
            "  title={BERT: Pre-training of Deep Bidirectional Transformers},\n"
            "  author={Cui, Yiming and Vaswani, Ashish},\n"
            "  year={2020}\n"
            "}"
        ),
        authors=["Cui, Yiming", "Vaswani, Ashish"],
    ),
    PaperCreate(
        title="GPT-3: Language Models are Few-Shot Learners",
        contents="Demonstrates that large language models can perform few-shot learning.",
        bibtex_id="Brown2020GPT3",
        bibtex=(
            "@inproceedings{Brown2020GPT3,\n"
            "  title={GPT-3: Language Models are Few-Shot Learners},\n"
            "  author={Brown, Tom},\n"
            "  year={2020}\n"
            "}"
        ),
        authors=["Brown, Tom"],
    ),
]
