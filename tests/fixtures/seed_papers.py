"""Canonical seed dataset for paper_sorts integration tests.

Any test that asserts on specific titles, BibTeX keys, or author names MUST
reference data defined here (constitution Principle II — no hidden coupling
to developer-local databases).
"""

from paper_sorts.db.repositories import PaperCreate

SEED_PAPERS: list[PaperCreate] = [
    PaperCreate(
        title="Attention Is All You Need",
        contents="Introduces the Transformer architecture based solely on attention mechanisms.",
        bibtex_id="Vaswani2017AttentionIA",
        bibtex=(
            "@article{Vaswani2017AttentionIA,"
            " title={Attention Is All You Need},"
            " author={Vaswani, Ashish and Shazeer, Noam},"
            " year={2017}}"
        ),
        authors=["Vaswani, Ashish", "Shazeer, Noam"],
    ),
    # Second paper with the same title for disambiguation test
    PaperCreate(
        title="Attention Is All You Need",
        contents="Alternate edition with extended appendix.",
        bibtex_id="Vaswani2017AttentionIAv2",
        bibtex=(
            "@article{Vaswani2017AttentionIAv2,"
            " title={Attention Is All You Need (v2)},"
            " author={Vaswani, Ashish},"
            " year={2017}}"
        ),
        authors=["Vaswani, Ashish"],
    ),
    PaperCreate(
        title="Large-Scale Study of Language Models",
        contents="Empirical study of scaling laws for neural language models.",
        bibtex_id="Wang2021LargeScaleSA",
        bibtex=(
            "@article{Wang2021LargeScaleSA,"
            " title={Large-Scale Study of Language Models},"
            " author={Wang, Jason},"
            " year={2021}}"
        ),
        authors=["Wang, Jason"],
    ),
    # Paper with LaTeX-accent author name to test round-trip
    PaperCreate(
        title="Pinot Noir: A Study",
        contents="Study of fermentation processes.",
        bibtex_id="Pino2022PinotNoir",
        bibtex=(
            "@article{Pino2022PinotNoir,"
            r' title={Pinot Noir: A Study},'
            r' author={Pi{\~n}o, Juan},'
            " year={2022}}"
        ),
        authors=["Pino, Juan"],
    ),
    # Multi-author paper
    PaperCreate(
        title="Deep Residual Learning",
        contents="Introduces residual connections for very deep neural networks.",
        bibtex_id="He2016DeepRL",
        bibtex=(
            "@article{He2016DeepRL,"
            " title={Deep Residual Learning for Image Recognition},"
            " author={He, Kaiming and Zhang, Xiangyu},"
            " year={2016}}"
        ),
        authors=["He, Kaiming", "Zhang, Xiangyu"],
    ),
]
