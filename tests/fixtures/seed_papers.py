"""Canonical seed dataset for paper_sorts integration tests.

This replaces the developer-local rows ("Pino, J.", "Wang2021LargeScaleSA")
that the legacy test suite silently depended on.

Coverage:
  - Two papers with the same title prefix (disambiguation test)
  - A paper with multiple authors
  - A BibTeX entry with a LaTeX accent (\"o) for round-trip testing
  - A paper with a single author
"""

SEED_PAPERS: list[dict] = [
    {
        "title": "Attention Is All You Need",
        "contents": "The dominant sequence transduction models are based on complex RNNs.",
        "bibtex_id": "Vaswani2017Attention",
        "bibtex": (
            "@inproceedings{Vaswani2017Attention,\n"
            "  author = {Vaswani, Ashish and Shazeer, Noam and Parmar, Niki},\n"
            "  title = {Attention Is All You Need},\n"
            "  booktitle = {NeurIPS},\n"
            "  year = {2017}\n"
            "}\n"
        ),
        "authors": ["Vaswani, Ashish", "Shazeer, Noam", "Parmar, Niki"],
    },
    {
        "title": "Attention Mechanisms in Neural Networks",
        "contents": "A survey of attention mechanisms in deep learning.",
        "bibtex_id": "Survey2020Attention",
        "bibtex": (
            "@article{Survey2020Attention,\n"
            "  author = {M{\\\"u}ller, Hans},\n"
            "  title = {Attention Mechanisms in Neural Networks},\n"
            "  journal = {Journal of ML Research},\n"
            "  year = {2020}\n"
            "}\n"
        ),
        # Author name with LaTeX accent in BibTeX — the name itself stored as plain text
        "authors": ["Müller, Hans"],  # Müller
    },
    {
        "title": "BERT: Pre-training of Deep Bidirectional Transformers",
        "contents": "We introduce BERT, a new language representation model.",
        "bibtex_id": "Devlin2019BERT",
        "bibtex": (
            "@inproceedings{Devlin2019BERT,\n"
            "  author = {Devlin, Jacob and Chang, Ming-Wei},\n"
            "  title = {BERT: Pre-training of Deep Bidirectional Transformers},\n"
            "  booktitle = {NAACL},\n"
            "  year = {2019}\n"
            "}\n"
        ),
        "authors": ["Devlin, Jacob", "Chang, Ming-Wei"],
    },
    {
        "title": "Large-Scale Study of Long-Document Summarisation",
        "contents": "We study summarisation of long documents at scale.",
        "bibtex_id": "Wang2021LargeScale",
        "bibtex": (
            "@article{Wang2021LargeScale,\n"
            "  author = {Wang, Li},\n"
            "  title = {Large-Scale Study of Long-Document Summarisation},\n"
            "  journal = {ACL},\n"
            "  year = {2021}\n"
            "}\n"
        ),
        "authors": ["Wang, Li"],
    },
]
