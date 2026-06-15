"""Unit tests for the prompt helpers (empty/malformed/success paths)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import PaperSummary


def _feed(monkeypatch: pytest.MonkeyPatch, answers: list[str]) -> None:
    """Patch ``Prompt.ask`` to return scripted answers in order."""
    it: Iterator[str] = iter(answers)

    def fake_ask(*_args: object, **_kwargs: object) -> str:
        return next(it)

    monkeypatch.setattr(prompts.Prompt, "ask", staticmethod(fake_ask))


def test_ask_nonempty_reprompts_on_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, ["", "   ", "finally"])
    assert prompts.ask_nonempty("x") == "finally"


def test_ask_choice_returns_zero_based_index(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, ["2"])
    assert prompts.ask_choice("pick", ["a", "b", "c"]) == 1


def test_ask_choice_out_of_range_reprompts(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, ["9", "0", "notanumber", "1"])
    assert prompts.ask_choice("pick", ["a", "b"]) == 0


def test_ask_choice_abort_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, ["abort"])
    assert prompts.ask_choice("pick", ["a", "b"]) is None


def test_ask_choice_abort_by_number(monkeypatch: pytest.MonkeyPatch) -> None:
    # The abort option is len(options)+1.
    _feed(monkeypatch, ["3"])
    assert prompts.ask_choice("pick", ["a", "b"]) is None


@pytest.mark.parametrize("answer", ["1", "y", "yes", "YES"])
def test_ask_confirm_accepts(monkeypatch: pytest.MonkeyPatch, answer: str) -> None:
    _feed(monkeypatch, [answer])
    assert prompts.ask_confirm("change?") is True


@pytest.mark.parametrize("answer", ["2", "n", "no", "No"])
def test_ask_confirm_rejects(monkeypatch: pytest.MonkeyPatch, answer: str) -> None:
    _feed(monkeypatch, [answer])
    assert prompts.ask_confirm("change?") is False


def test_ask_confirm_reprompts_on_garbage(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, ["maybe", "y"])
    assert prompts.ask_confirm("change?") is True


def test_pick_from_selects_paper(monkeypatch: pytest.MonkeyPatch) -> None:
    papers = [
        PaperSummary(
            paper_id=i, title=f"T{i}", authors="A", summary="s", bibtex_id=f"k{i}", bibtex="b"
        )
        for i in range(3)
    ]
    _feed(monkeypatch, ["2"])
    chosen = prompts.pick_from("found", papers)
    assert chosen is not None and chosen.paper_id == 1
