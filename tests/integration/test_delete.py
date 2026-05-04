"""Integration tests for ``PaperService.delete_paper`` and the CLI dialog.

Service-layer tests cover the cascade rules from ``data-model.md``:
``authors_papers`` rows gone, orphan author rows gone, bib row dropped
only when no other paper references it, non-existent id rejected with
a plain-language error.

CLI-flow tests (added for spec 002-ux-polish US3) cover the new
search-then-delete flow: the dialog is driven via the ``db_factory``
fixture and a ``Prompt.ask`` monkey-patch that yields scripted
responses for each numbered prompt. The non-interactive ``--id N``
path is also covered for non-regression.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from paper_sorts.cli import delete as delete_cli
from paper_sorts.db.models import Author, Authorship, BibEntry, Paper
from paper_sorts.db.repositories import PaperCreate, PaperRepository
from paper_sorts.services.paper_service import PaperService


class _FakeCtx:
    """Minimal stand-in for ``typer.Context`` — only ``.obj`` is consulted."""

    def __init__(self, factory: sessionmaker[Session]) -> None:
        self.obj = factory


def _script_prompts(monkeypatch: pytest.MonkeyPatch, responses: list[str]) -> Iterator[str]:
    """Replace ``rich.prompt.Prompt.ask`` with a scripted-response iterator.

    Echoes the prompt to stdout (so ``capsys`` captures it for
    prompt-content assertions) and returns the next scripted response.
    """
    iterator = iter(responses)

    def fake_ask(*args: Any, **_kwargs: Any) -> str:
        prompt = args[0] if args else ""
        print(prompt)
        return next(iterator)

    monkeypatch.setattr("rich.prompt.Prompt.ask", fake_ask)
    return iterator


def _paper_id_by_bibtex(session: Session, bibtex_id: str) -> int:
    return session.execute(select(Paper.id).where(Paper.bibtex_id == bibtex_id)).scalar_one()


def test_delete_drops_paper_and_its_authorship_links(db_session: Session) -> None:
    paper_id = _paper_id_by_bibtex(db_session, "Schoettler2023FairnessMT")
    service = PaperService(db_session)
    service.delete_paper(paper_id)

    paper = db_session.execute(select(Paper).where(Paper.id == paper_id)).scalar_one_or_none()
    assert paper is None

    links = (
        db_session.execute(select(Authorship).where(Authorship.paper_id == paper_id))
        .scalars()
        .all()
    )
    assert links == []


def test_delete_removes_orphan_authors(db_session: Session) -> None:
    """Schöttler appears on exactly one paper; deleting it orphans the author row."""
    paper_id = _paper_id_by_bibtex(db_session, "Schoettler2023FairnessMT")
    schoettler = db_session.execute(
        select(Author).where(Author.name == "Schöttler, K.")
    ).scalar_one()
    schoettler_id = schoettler.id

    PaperService(db_session).delete_paper(paper_id)

    still_there = db_session.execute(
        select(Author).where(Author.id == schoettler_id)
    ).scalar_one_or_none()
    assert still_there is None


def test_delete_keeps_authors_with_other_papers(db_session: Session) -> None:
    """Pino appears on two papers; deleting one keeps the author row alive."""
    paper_id = _paper_id_by_bibtex(db_session, "Lee2022DirectSpeechToSpeech")
    PaperService(db_session).delete_paper(paper_id)

    pino = db_session.execute(select(Author).where(Author.name == "Pino, J.")).scalar_one_or_none()
    assert pino is not None


def test_delete_drops_bib_row_when_no_other_paper_references_it(
    db_session: Session,
) -> None:
    paper_id = _paper_id_by_bibtex(db_session, "Schoettler2023FairnessMT")
    PaperService(db_session).delete_paper(paper_id)

    bib = db_session.execute(
        select(BibEntry).where(BibEntry.bibtex_id == "Schoettler2023FairnessMT")
    ).scalar_one_or_none()
    assert bib is None


def test_delete_keeps_bib_row_when_another_paper_references_it(
    db_session: Session,
) -> None:
    """Two papers sharing a bibtex_id is unusual but the cascade rule covers it."""
    # First add a second paper that points at the same bibtex_id as Wang2021.
    # The bib row already exists (seeded), so we add only paper + authorship —
    # the repository layer would normally reject duplicate bibtex_id at the
    # service boundary, so we drop down to the ORM directly to set up the
    # double-reference state. This bypasses PaperService.add_paper's
    # duplicate-key check intentionally for the test fixture.
    wang_paper = db_session.execute(
        select(Paper).where(Paper.bibtex_id == "Wang2021LargeScaleSA")
    ).scalar_one()
    second_paper = Paper(
        title="Second copy referencing the same bib row",
        contents="Same bibtex_id, different paper row.",
        bibtex_id=wang_paper.bibtex_id,
    )
    db_session.add(second_paper)
    db_session.flush()

    PaperService(db_session).delete_paper(wang_paper.id)

    bib = db_session.execute(
        select(BibEntry).where(BibEntry.bibtex_id == "Wang2021LargeScaleSA")
    ).scalar_one_or_none()
    assert bib is not None  # second_paper still references it


def test_delete_nonexistent_paper_id_rejected(db_session: Session) -> None:
    service = PaperService(db_session)
    with pytest.raises(ValueError, match="No paper with id 99999"):
        service.delete_paper(99999)


def test_delete_inserts_new_paper_then_round_trip(db_session: Session) -> None:
    """Smoke: full add -> delete round-trip exercises both directions of the seam."""
    payload = PaperCreate(
        title="Round Trip",
        contents="One-line summary.",
        bibtex_id="RoundTrip2026",
        bibtex="@misc{RoundTrip2026, year={2026}}",
        authors=("Solo, A.",),
    )
    inserted = PaperRepository(db_session).add(payload)
    db_session.flush()

    PaperService(db_session).delete_paper(inserted.id)

    assert (
        db_session.execute(select(Paper).where(Paper.id == inserted.id)).scalar_one_or_none()
        is None
    )
    assert (
        db_session.execute(
            select(BibEntry).where(BibEntry.bibtex_id == "RoundTrip2026")
        ).scalar_one_or_none()
        is None
    )
    assert (
        db_session.execute(select(Author).where(Author.name == "Solo, A.")).scalar_one_or_none()
        is None
    )


# ---------------------------------------------------------------------------
# CLI-flow tests for US3 — search-then-delete.
# ---------------------------------------------------------------------------


def test_cli_delete_search_then_delete_single_match(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Single-result search auto-picks the row; confirmation echoes title."""
    _script_prompts(
        monkeypatch,
        [
            "2",  # search axis → title
            "Direct speech-to-speech translation with discrete units",  # query
            "y",  # confirm
        ],
    )

    delete_cli.delete(_FakeCtx(db_factory))  # type: ignore[arg-type]

    out = capsys.readouterr().out
    assert "Please enter the paper id to delete" not in out
    assert (
        "DELETE paper id" in out
        and "'Direct speech-to-speech translation with discrete units'" in out
    )

    with db_factory() as session:
        paper = session.execute(
            select(Paper).where(Paper.bibtex_id == "Lee2022DirectSpeechToSpeech")
        ).scalar_one_or_none()
    assert paper is None


def test_cli_delete_search_multi_match_disambig(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multi-result search shows the disambig list; the chosen row is deleted."""
    _script_prompts(
        monkeypatch,
        [
            "2",  # by title
            "On Fairness in Machine Translation",  # query — matches 2 rows
            "1",  # disambig: first row
            "y",  # confirm
        ],
    )

    delete_cli.delete(_FakeCtx(db_factory))  # type: ignore[arg-type]

    with db_factory() as session:
        remaining = (
            session.execute(
                select(Paper).where(Paper.title == "On Fairness in Machine Translation")
            )
            .scalars()
            .all()
        )
    # Exactly one fairness paper remains; the other was deleted.
    assert len(remaining) == 1


def test_cli_delete_id_flag_bypasses_search(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``--id N`` skips search; existing non-interactive path still works."""
    with db_factory() as session:
        wang_id = session.execute(
            select(Paper.id).where(Paper.bibtex_id == "Wang2021LargeScaleSA")
        ).scalar_one()

    _script_prompts(
        monkeypatch,
        [
            "y",  # confirm only
        ],
    )

    delete_cli.delete(_FakeCtx(db_factory), paper_id=wang_id)  # type: ignore[arg-type]

    out = capsys.readouterr().out
    assert "Search interface" not in out
    assert "Please choose a method" not in out
    assert f"DELETE paper id {wang_id}" in out

    with db_factory() as session:
        gone = session.execute(select(Paper).where(Paper.id == wang_id)).scalar_one_or_none()
    assert gone is None


def test_cli_delete_zero_results_aborts_cleanly(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A search query matching no rows shows the not-found message and exits."""
    _script_prompts(
        monkeypatch,
        [
            "2",  # by title
            "This Paper Does Not Exist",  # query
            # No further prompts — flow returns early.
        ],
    )

    delete_cli.delete(_FakeCtx(db_factory))  # type: ignore[arg-type]

    out = capsys.readouterr().out
    assert "Paper was not found in db_connector." in out
    assert "DELETE paper id" not in out  # confirmation never shown

    # Seeded data unchanged.
    with db_factory() as session:
        wang = session.execute(
            select(Paper).where(Paper.bibtex_id == "Wang2021LargeScaleSA")
        ).scalar_one_or_none()
    assert wang is not None


def test_cli_delete_id_flag_unknown_id_errors(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``--id N`` against a missing row prints a plain-language error."""
    _script_prompts(monkeypatch, [])  # no prompts expected

    delete_cli.delete(_FakeCtx(db_factory), paper_id=99999)  # type: ignore[arg-type]

    out = capsys.readouterr().out
    assert "Error: no paper with id 99999" in out


def test_cli_delete_user_declines_at_confirm(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``n`` at the confirmation aborts the delete; the row is unchanged."""
    _script_prompts(
        monkeypatch,
        [
            "2",  # by title
            "Direct speech-to-speech translation with discrete units",  # query
            "n",  # decline
        ],
    )

    delete_cli.delete(_FakeCtx(db_factory))  # type: ignore[arg-type]

    with db_factory() as session:
        paper = session.execute(
            select(Paper).where(Paper.bibtex_id == "Lee2022DirectSpeechToSpeech")
        ).scalar_one_or_none()
    assert paper is not None


def test_cli_delete_search_then_cascade(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cascade behaviour preserved on the CLI search-then-delete path.

    Schöttler appears on exactly one seeded paper (``Schoettler2023FairnessMT``);
    deleting it via search-by-author orphans the author, and the bib row has
    no other paper referencing it so the cascade drops both.
    """
    _script_prompts(
        monkeypatch,
        [
            "1",  # search by author
            "Schöttler, K.",  # unique author → single match
            "y",  # confirm
        ],
    )

    delete_cli.delete(_FakeCtx(db_factory))  # type: ignore[arg-type]

    with db_factory() as session:
        # Paper gone
        paper = session.execute(
            select(Paper).where(Paper.bibtex_id == "Schoettler2023FairnessMT")
        ).scalar_one_or_none()
        assert paper is None
        # Author orphaned and removed
        author = session.execute(
            select(Author).where(Author.name == "Schöttler, K.")
        ).scalar_one_or_none()
        assert author is None
        # Bib row also gone (no other paper references it)
        bib = session.execute(
            select(BibEntry).where(BibEntry.bibtex_id == "Schoettler2023FairnessMT")
        ).scalar_one_or_none()
        assert bib is None
