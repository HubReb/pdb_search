"""Canonical seed dataset for paper_sorts integration tests.

These entries are used by:
- tests/test_repositories.py  — all search/add/delete/update tests
- tests/test_paper_service.py — service-layer tests

Each entry is a dict matching PaperCreate field names.

Entries deliberately cover:
- Multiple authors (Wang2021 has two authors)
- Author shared across papers (Pino appears in both pino2020 and korakakis2022)
- Diverse BibTeX keys
- LaTeX accent in author name (Müller in wang2021)
"""

from paper_sorts.db.repositories import PaperCreate

SEED_PAPERS: list[PaperCreate] = [
    PaperCreate(
        title="Large-Scale Sentence Alignment with Attention",
        contents="Proposes a scalable attention mechanism for cross-lingual alignment.",
        bibtex_id="Wang2021LargeScaleSA",
        authors=["Wang, Lin", "Müller, Hans"],
        bibtex=(
            "@article{Wang2021LargeScaleSA,\n"
            "  author = {Wang, Lin and M\\\"uller, Hans},\n"
            "  title  = {Large-Scale Sentence Alignment with Attention},\n"
            "  year   = {2021}\n"
            "}"
        ),
    ),
    PaperCreate(
        title="Catastrophic Forgetting in Neural Machine Translation",
        contents="Studies forgetting in NMT and proposes regularisation strategies.",
        bibtex_id="korakakis2022",
        authors=["Korakakis, Michalis", "Pino, J."],
        bibtex=(
            "@article{korakakis2022,\n"
            "  author = {Korakakis, Michalis and Pino, J.},\n"
            "  title  = {Catastrophic Forgetting in Neural Machine Translation},\n"
            "  year   = {2022}\n"
            "}"
        ),
    ),
    PaperCreate(
        title="Elastic Weight Consolidation for NMT",
        contents="Applies EWC to prevent forgetting in incremental NMT training.",
        bibtex_id="pino2020",
        authors=["Pino, J."],
        bibtex=(
            "@article{pino2020,\n"
            "  author = {Pino, J.},\n"
            "  title  = {Elastic Weight Consolidation for NMT},\n"
            "  year   = {2020}\n"
            "}"
        ),
    ),
    PaperCreate(
        title="Survey of Low-Resource NMT",
        contents="Comprehensive survey of techniques for low-resource neural MT.",
        bibtex_id="survey2023",
        authors=["Smith, Alice"],
        bibtex=(
            "@article{survey2023,\n"
            "  author = {Smith, Alice},\n"
            "  title  = {Survey of Low-Resource NMT},\n"
            "  year   = {2023}\n"
            "}"
        ),
    ),
    PaperCreate(
        # Duplicate title — same title as first entry, different bibtex_id.
        # Used to test disambiguation when search_by_title returns multiple rows.
        title="Large-Scale Sentence Alignment with Attention",
        contents="A follow-up study with improved results on WMT data.",
        bibtex_id="Wang2022FollowUp",
        authors=["Wang, Lin"],
        bibtex=(
            "@article{Wang2022FollowUp,\n"
            "  author = {Wang, Lin},\n"
            "  title  = {Large-Scale Sentence Alignment with Attention},\n"
            "  year   = {2022}\n"
            "}"
        ),
    ),
]
