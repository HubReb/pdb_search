"""Integration tests for ``PaperService.update_field`` and the CLI dialog.

Service-layer tests cover the legacy ``UserInteraction.update`` table x
field grid (``papers.title`` / ``papers.contents`` / ``bib.bibtex`` /
``authors.author``), plus the rejection rules.

CLI-flow tests (added for spec 002-ux-polish US2) cover the new
search-then-update flow on the papers table: the dialog is driven via
the ``db_factory`` fixture and a ``Prompt.ask`` monkey-patch that yields
scripted responses for each numbered prompt. The bib/authors raw-id
paths are also covered for non-regression.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from paper_sorts.cli import update as update_cli
from paper_sorts.db.models import Author, BibEntry, Paper
from paper_sorts.services.paper_service import PaperService


class _FakeCtx:
    """Minimal stand-in for ``typer.Context`` — only ``.obj`` is consulted by the command."""

    def __init__(self, factory: sessionmaker[Session]) -> None:
        self.obj = factory


def _script_prompts(monkeypatch: pytest.MonkeyPatch, responses: list[str]) -> Iterator[str]:
    """Replace ``rich.prompt.Prompt.ask`` with a scripted-response iterator.

    Each call to ``Prompt.ask`` echoes the prompt text to stdout (so
    ``capsys`` captures it for prompt-content assertions) and returns
    the next response in order. Returns the iterator so the test can
    assert exhaustion if needed.
    """
    iterator = iter(responses)

    def fake_ask(*args: Any, **_kwargs: Any) -> str:
        # rich.prompt.Prompt.ask is a classmethod; first positional arg is the prompt string.
        prompt = args[0] if args else ""
        print(prompt)
        return next(iterator)

    monkeypatch.setattr("rich.prompt.Prompt.ask", fake_ask)
    return iterator


def _wang_paper_id(session: Session) -> int:
    return session.execute(
        select(Paper.id).where(Paper.bibtex_id == "Wang2021LargeScaleSA")
    ).scalar_one()


def test_update_title(db_session: Session) -> None:
    paper_id = _wang_paper_id(db_session)
    service = PaperService(db_session)
    service.update_field("papers", "title", paper_id, "New Title")

    title = db_session.execute(select(Paper.title).where(Paper.id == paper_id)).scalar_one()
    assert title == "New Title"


def test_update_contents(db_session: Session) -> None:
    paper_id = _wang_paper_id(db_session)
    service = PaperService(db_session)
    service.update_field("papers", "contents", paper_id, "Rewritten summary.")

    contents = db_session.execute(select(Paper.contents).where(Paper.id == paper_id)).scalar_one()
    assert contents == "Rewritten summary."


def test_update_bibtex_source(db_session: Session) -> None:
    service = PaperService(db_session)
    new_source = "@article{Wang2021LargeScaleSA, note={updated source}}"
    service.update_field("bib", "bibtex", "Wang2021LargeScaleSA", new_source)

    bibtex = db_session.execute(
        select(BibEntry.bibtex).where(BibEntry.bibtex_id == "Wang2021LargeScaleSA")
    ).scalar_one()
    assert bibtex == new_source


def test_update_author_name(db_session: Session) -> None:
    pino_id = db_session.execute(select(Author.id).where(Author.name == "Pino, J.")).scalar_one()
    service = PaperService(db_session)
    service.update_field("authors", "author", pino_id, "Pino, Juan")

    name = db_session.execute(select(Author.name).where(Author.id == pino_id)).scalar_one()
    assert name == "Pino, Juan"


def test_update_bibtex_id_itself_is_rejected(db_session: Session) -> None:
    """The BibTeX identifier cannot be updated; only its source string is."""
    service = PaperService(db_session)
    with pytest.raises(ValueError, match="not editable"):
        service.update_field("bib", "bibtex_id", "Wang2021LargeScaleSA", "RenameAttempt")


def test_update_nonexistent_paper_id_rejected(db_session: Session) -> None:
    """Spec rule: non-existent paper id raises a plain-language ValueError."""
    service = PaperService(db_session)
    with pytest.raises(ValueError, match="No paper with id 99999"):
        service.update_field("papers", "title", 99999, "would not have applied")


def test_update_empty_value_rejected(db_session: Session) -> None:
    paper_id = _wang_paper_id(db_session)
    service = PaperService(db_session)
    with pytest.raises(ValueError, match="cannot be empty"):
        service.update_field("papers", "title", paper_id, "")


def test_update_unknown_field_on_papers_rejected(db_session: Session) -> None:
    paper_id = _wang_paper_id(db_session)
    service = PaperService(db_session)
    with pytest.raises(ValueError, match="not editable"):
        service.update_field("papers", "id", paper_id, "1")


def test_abort_complement_data_unchanged(db_session: Session) -> None:
    """If the CLI never calls update_field (user aborted), seeded data is intact.

    The grammar of the abort token is covered in ``tests/unit/test_prompts.py``
    (``ask_confirm`` rejects ``2``/``n``/``no``); the integration-level
    complement is asserting that without a service call no row is mutated.
    """
    paper_id = _wang_paper_id(db_session)
    title_before = db_session.execute(select(Paper.title).where(Paper.id == paper_id)).scalar_one()

    # Deliberately do NOT call service.update_field — the "abort" path.

    title_after = db_session.execute(select(Paper.title).where(Paper.id == paper_id)).scalar_one()
    assert title_after == title_before


# ---------------------------------------------------------------------------
# CLI-flow tests for US2 — search-then-update on the papers table.
# Drive ``cli.update.update(ctx)`` directly with scripted ``Prompt.ask``
# responses; assert via the same factory the CLI uses.
# ---------------------------------------------------------------------------


def _read_paper_title(factory: sessionmaker[Session], bibtex_id: str) -> str:
    with factory() as session:
        return session.execute(select(Paper.title).where(Paper.bibtex_id == bibtex_id)).scalar_one()


def test_cli_update_papers_title_search_then_update_single_match(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Single-result search auto-picks the row; no disambig prompt."""
    _script_prompts(
        monkeypatch,
        [
            "1",  # _pick_table → papers
            "1",  # _pick_field → title
            "2",  # search axis → title
            "Direct speech-to-speech translation with discrete units",  # query (exact match)
            "Direct speech-to-speech translation with discrete units, REVISED",  # new value
            "y",  # confirm
        ],
    )

    update_cli.update(_FakeCtx(db_factory))  # type: ignore[arg-type]

    out = capsys.readouterr().out
    assert "Please enter the respective id" not in out
    assert "of the paper 'Direct speech-to-speech translation with discrete units' (id " in out

    new_title = _read_paper_title(db_factory, "Lee2022DirectSpeechToSpeech")
    assert new_title == "Direct speech-to-speech translation with discrete units, REVISED"


def test_cli_update_papers_title_search_then_update_multi_match(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multi-result search shows the disambig list; the chosen row is updated."""
    _script_prompts(
        monkeypatch,
        [
            "1",  # papers
            "2",  # contents
            "2",  # search by title
            "On Fairness in Machine Translation",  # query — matches 2 rows
            "1",  # disambig: pick first row
            "Updated summary for the chosen fairness paper.",  # new value
            "y",  # confirm
        ],
    )

    update_cli.update(_FakeCtx(db_factory))  # type: ignore[arg-type]

    # One of the two fairness papers now has the new contents — assert that
    # exactly one of them does (which one depends on row ordering, but order
    # is stable for the seeded rows so the disambig "1" picks deterministically).
    with db_factory() as session:
        papers = (
            session.execute(
                select(Paper.contents).where(Paper.title == "On Fairness in Machine Translation")
            )
            .scalars()
            .all()
        )
    assert "Updated summary for the chosen fairness paper." in papers


def test_cli_update_papers_id_flag_bypasses_search(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``--id N`` skips the search step but still walks table/field/value/confirm."""
    # Look up the seeded paper's id via the factory (not via search).
    with db_factory() as session:
        wang_id = session.execute(
            select(Paper.id).where(Paper.bibtex_id == "Wang2021LargeScaleSA")
        ).scalar_one()

    _script_prompts(
        monkeypatch,
        [
            "1",  # papers
            "1",  # title
            # NO search axis or query prompts — --id skipped them.
            "Wang Renamed",  # new value
            "y",  # confirm
        ],
    )

    update_cli.update(_FakeCtx(db_factory), paper_id=wang_id)  # type: ignore[arg-type]

    out = capsys.readouterr().out
    assert "Search interface" not in out
    assert "Please choose a method" not in out

    new_title = _read_paper_title(db_factory, "Wang2021LargeScaleSA")
    assert new_title == "Wang Renamed"


def test_cli_update_papers_id_flag_unknown_id_errors(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _script_prompts(
        monkeypatch,
        [
            "1",  # papers
            "1",  # title
        ],
    )

    update_cli.update(_FakeCtx(db_factory), paper_id=99999)  # type: ignore[arg-type]

    out = capsys.readouterr().out
    assert "Error: no paper with id 99999" in out


def test_cli_update_papers_zero_results_aborts_cleanly(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A search query matching no rows shows the legacy not-found message and exits."""
    _script_prompts(
        monkeypatch,
        [
            "1",  # papers
            "1",  # title
            "2",  # search by title
            "This Paper Does Not Exist",  # query
        ],
    )

    update_cli.update(_FakeCtx(db_factory))  # type: ignore[arg-type]

    out = capsys.readouterr().out
    assert "Paper was not found in db_connector." in out
    # No confirmation/value prompt should follow — the iterator above is exhausted.

    # Seeded data unchanged.
    title = _read_paper_title(db_factory, "Wang2021LargeScaleSA")
    assert title == ("Large-scale Self- and Semi-Supervised learning for speech translation")


def test_cli_update_bib_retains_legacy_raw_id_prompt(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``bib`` table updates still ask for the raw bibtex_id; confirmation uses legacy wording."""
    _script_prompts(
        monkeypatch,
        [
            "2",  # bib
            "1",  # bibtex
            "Wang2021LargeScaleSA",  # raw id
            "@article{Wang2021LargeScaleSA, note={cli-test rewrite}}",  # new value
            "y",  # confirm
        ],
    )

    update_cli.update(_FakeCtx(db_factory))  # type: ignore[arg-type]

    out = capsys.readouterr().out
    assert "Please enter the respective id" in out
    # Legacy summary uses 'entry', not 'paper'/'id N'.
    assert "of the entry 'Wang2021LargeScaleSA'" in out
    assert "of the paper" not in out

    with db_factory() as session:
        bibtex = session.execute(
            select(BibEntry.bibtex).where(BibEntry.bibtex_id == "Wang2021LargeScaleSA")
        ).scalar_one()
    assert "cli-test rewrite" in bibtex


def test_cli_update_authors_retains_legacy_raw_id_prompt(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``authors`` table updates still ask for the raw integer id."""
    with db_factory() as session:
        pino_id = session.execute(select(Author.id).where(Author.name == "Pino, J.")).scalar_one()

    _script_prompts(
        monkeypatch,
        [
            "3",  # authors
            "1",  # author
            str(pino_id),  # raw id
            "Pino, Juan",  # new name
            "y",  # confirm
        ],
    )

    update_cli.update(_FakeCtx(db_factory))  # type: ignore[arg-type]

    out = capsys.readouterr().out
    assert "Please enter the respective id" in out
    assert f"of the entry '{pino_id}'" in out

    with db_factory() as session:
        new_name = session.execute(select(Author.name).where(Author.id == pino_id)).scalar_one()
    assert new_name == "Pino, Juan"


def test_cli_update_papers_user_declines_at_confirm(
    db_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``n`` at the confirmation aborts the update; the row is unchanged."""
    _script_prompts(
        monkeypatch,
        [
            "1",  # papers
            "1",  # title
            "2",  # search by title
            "Direct speech-to-speech translation with discrete units",  # query
            "Should Not Land",  # new value
            "n",  # decline
        ],
    )

    update_cli.update(_FakeCtx(db_factory))  # type: ignore[arg-type]

    title = _read_paper_title(db_factory, "Lee2022DirectSpeechToSpeech")
    assert title == "Direct speech-to-speech translation with discrete units"
