"""Tests for the bulk import service.

Uses fixture files in tests/fixtures/ (literature_overview.tex + bib.bib).

Seed relationship:
    literature_overview.tex has 3 entries:
        Smith2023Survey      -> matched in bib.bib (2 authors)
        Bahdanau2015Attention-> matched in bib.bib (3 authors)
        NotInBib2023         -> NOT in bib.bib (should be skipped with warning)
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service
from paper_sorts.services.import_service import extract_papers_from_tex_bib

TEX_FILE = Path(__file__).parent / "fixtures" / "literature_overview.tex"
BIB_FILE = Path(__file__).parent / "fixtures" / "bib.bib"


def test_extract_yields_matched_entries_only() -> None:
    """extract_papers_from_tex_bib yields only entries with matching .bib records."""
    results = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
    # 3 tex entries, 1 has no matching .bib record -> 2 yielded
    assert len(results) == 2
    bibtex_ids = {r.bibtex_id for r in results}
    assert "Smith2023Survey" in bibtex_ids
    assert "Bahdanau2015Attention" in bibtex_ids
    assert "NotInBib2023" not in bibtex_ids


def test_extract_paper_create_fields() -> None:
    """Each yielded PaperCreate has all required fields populated."""
    results = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
    for paper in results:
        assert isinstance(paper, PaperCreate)
        assert paper.title
        assert paper.bibtex_id
        assert paper.bibtex
        assert paper.contents
        assert len(paper.authors) >= 1


def test_extract_author_count() -> None:
    """Author list length matches the .bib file for each entry."""
    results = {r.bibtex_id: r for r in extract_papers_from_tex_bib(TEX_FILE, BIB_FILE)}
    assert len(results["Smith2023Survey"].authors) == 2
    assert len(results["Bahdanau2015Attention"].authors) == 3


def test_extract_skips_missing_bib_with_warning(caplog: pytest.LogCaptureFixture) -> None:
    """extract_papers_from_tex_bib logs a warning for unmatched cite keys."""
    with caplog.at_level(logging.WARNING, logger="paper_sorts.services.import_service"):
        list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
    # Should have at least one warning about NotInBib2023.
    warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("NotInBib2023" in msg for msg in warning_messages)


def test_import_end_to_end(db_session: object) -> None:
    """End-to-end: extract + add_paper inserts correct rows into the DB."""
    from sqlalchemy.orm import Session

    session: Session = db_session  # type: ignore[assignment]
    for paper in extract_papers_from_tex_bib(TEX_FILE, BIB_FILE):
        paper_service.add_paper(session, paper)
    session.commit()

    from paper_sorts.db.repositories import PaperRepository

    survey = PaperRepository.search_by_title(session, "A Survey of Deep Learning Methods")
    assert len(survey) == 1
    assert set(survey[0].authors) == {"Smith, John", "Doe, Jane"}

    attn = PaperRepository.search_by_title(
        session, "Neural Machine Translation by Jointly Learning to Align and Translate"
    )
    assert len(attn) == 1
    assert len(attn[0].authors) == 3


def test_import_idempotent(postgresql_proc: object) -> None:
    """Re-running import skips already-inserted entries (bibtex_id uniqueness).

    Uses its own session (not the rollback-wrapped db_session fixture) so that
    the first import is truly committed and the second import encounters real
    unique-constraint violations.
    """
    import uuid

    from pytest_postgresql.janitor import DatabaseJanitor
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from paper_sorts.db.models import Base

    proc = postgresql_proc  # type: ignore[attr-defined]
    user: str = proc.user
    host: str = proc.host
    port: int = proc.port
    version = proc.version
    template_dbname = getattr(proc, "template_dbname", None)
    dbname = f"idempotent_test_{uuid.uuid4().hex[:8]}"

    with DatabaseJanitor(
        user=user, host=host, port=port, dbname=dbname,
        version=version, template_dbname=template_dbname,
    ):
        url = f"postgresql+psycopg://{user}@{host}:{port}/{dbname}"
        engine = create_engine(url)
        Base.metadata.create_all(engine)

        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))

        # First import — should succeed.
        with Session(engine) as session:
            for paper in papers:
                paper_service.add_paper(session, paper)
            session.commit()

        # Second import — duplicate entries should raise ValueError.
        skipped = 0
        for paper in papers:
            with Session(engine) as session:
                try:
                    paper_service.add_paper(session, paper)
                    session.commit()
                except ValueError:
                    session.rollback()
                    skipped += 1

        engine.dispose()

    assert skipped == len(papers), "All entries should be skipped on re-import."
