"""Unit tests for the prompt helpers (no DB needed)."""

from __future__ import annotations

import pytest

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import PaperSummary


def _feed(monkeypatch: pytest.MonkeyPatch, answers: list[str]) -> None:
    it = iter(answers)
    monkeypatch.setattr(prompts.Prompt, "ask", staticmethod(lambda *a, **k: next(it)))


def test_ask_text_reprompts_until_nonempty(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, ["", "   ", "hello"])
    assert prompts.ask_text("x") == "hello"


def test_ask_choice_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, ["2"])
    assert prompts.ask_choice("pick", ["a", "b", "c"]) == 1


def test_ask_choice_out_of_range_reprompts(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, ["9", "x", "1"])
    assert prompts.ask_choice("pick", ["a", "b"]) == 0


def test_ask_choice_abort(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, ["abort"])
    assert prompts.ask_choice("pick", ["a", "b"]) is None


def test_ask_choice_abort_by_number(monkeypatch: pytest.MonkeyPatch) -> None:
    # two options -> abort is option 3
    _feed(monkeypatch, ["3"])
    assert prompts.ask_choice("pick", ["a", "b"]) is None


def test_confirm_numeric_and_word(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, ["1"])
    assert prompts.confirm("c") is True
    _feed(monkeypatch, ["no"])
    assert prompts.confirm("c") is False
    _feed(monkeypatch, ["maybe", "y"])
    assert prompts.confirm("c") is True


def test_print_paper(capsys: pytest.CaptureFixture[str]) -> None:
    prompts.print_paper(
        PaperSummary(
            paper_id=1,
            title="T",
            authors=["A, a", "B, b"],
            summary="S",
            bibtex_id="K",
            bibtex="BIB",
        )
    )
    out = capsys.readouterr().out
    assert "title: T" in out
    assert "authors: A, a and B, b" in out
    assert "BIB" in out
