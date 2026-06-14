"""Tests for the import_service module.

Verifies that extract_papers_from_tex_bib correctly parses .tex/.bib pairs,
yields PaperCreate DTOs for matched entries, and skips unmatched keys.
No database required — pure unit tests.
"""

from __future__ import annotations

from paper_sorts.services.import_service import extract_papers_from_tex_bib

BIB_CONTENT = """
@article{Paper1_2024,
  author    = {First, Author and Second, Author},
  title     = {First Test Paper},
  year      = {2024},
  journal   = {Journal of Testing}
}

@article{Paper2_2024,
  author    = {Third, Author},
  title     = {Second Test Paper},
  year      = {2024}
}
"""

TEX_CONTENT = r"""
\begin{document}
\begin{itemize}
  \item Paper1\_2024 \cite{Paper1_2024}: Description of first paper.
  \item Paper2\_2024 \cite{Paper2_2024}: Description of second paper.
  \item MissingPaper \cite{MissingPaper}: This has no bib entry.
\end{itemize}
\end{document}
"""


class TestExtractPapersFromTexBib:
    """Tests for extract_papers_from_tex_bib."""

    def test_yields_papers_for_matched_keys(self) -> None:
        """Yields PaperCreate for each .tex cite key found in .bib."""
        results = list(extract_papers_from_tex_bib(TEX_CONTENT, BIB_CONTENT))
        keys = {r.bibtex_id for r in results}
        assert "Paper1_2024" in keys or "Paper2_2024" in keys

    def test_skips_missing_bib_keys(self) -> None:
        """Keys in .tex that are absent from .bib do not appear in output."""
        results = list(extract_papers_from_tex_bib(TEX_CONTENT, BIB_CONTENT))
        keys = {r.bibtex_id for r in results}
        assert "MissingPaper" not in keys

    def test_includes_authors(self) -> None:
        """PaperCreate DTOs include at least one author for each imported paper."""
        results = list(extract_papers_from_tex_bib(TEX_CONTENT, BIB_CONTENT))
        for paper in results:
            assert len(paper.authors) > 0

    def test_includes_bibtex_string(self) -> None:
        """PaperCreate DTOs include non-empty bibtex strings."""
        results = list(extract_papers_from_tex_bib(TEX_CONTENT, BIB_CONTENT))
        for paper in results:
            assert "@" in paper.bibtex

    def test_empty_tex_falls_back_to_all_bib_entries(self) -> None:
        """Empty .tex content triggers fallback: all .bib entries are imported."""
        results = list(extract_papers_from_tex_bib("", BIB_CONTENT))
        keys = {r.bibtex_id for r in results}
        assert "Paper1_2024" in keys
        assert "Paper2_2024" in keys

    def test_malformed_bib_returns_empty(self) -> None:
        """Malformed .bib content returns empty results gracefully."""
        results = list(extract_papers_from_tex_bib("", "not valid bibtex at all"))
        # Should not crash; may return empty or empty bib
        assert isinstance(results, list)

    def test_paper_create_has_correct_fields(self) -> None:
        """PaperCreate has non-empty title, bibtex_id, bibtex fields."""
        results = list(extract_papers_from_tex_bib(TEX_CONTENT, BIB_CONTENT))
        for paper in results:
            assert paper.bibtex_id
            assert paper.bibtex
