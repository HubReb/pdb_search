"""Unit tests for the prompt helpers: re-prompting, choices, confirmations."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from paper_sorts.cli import prompts


def _feed(monkeypatch: pytest.MonkeyPatch, answers: list[str]) -> None:
    """Patch ``Prompt.ask`` to return successive scripted answers.

    :param monkeypatch: pytest's monkeypatch fixture.
    :param answers: the answers to return in order.
    """
    it: Iterator[str] = iter(answers)

    def fake_ask(*_args: object, **_kwargs: object) -> str:
        return next(it)

    monkeypatch.setattr(prompts.Prompt, "ask", staticmethod(fake_ask))


def test_ask_nonempty_reprompts_until_filled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty/whitespace input re-prompts until non-empty (legacy parity)."""
    _feed(monkeypatch, ["", "   ", "value"])
    assert prompts.ask_nonempty("x") == "value"


def test_ask_choice_returns_zero_based_index(monkeypatch: pytest.MonkeyPatch) -> None:
    """A valid numeric selection maps to a 0-based index."""
    _feed(monkeypatch, ["2"])
    assert prompts.ask_choice("pick", ["a", "b", "c"]) == 1


def test_ask_choice_reprompts_on_out_of_range(monkeypatch: pytest.MonkeyPatch) -> None:
    """An out-of-range or malformed choice re-prompts."""
    _feed(monkeypatch, ["9", "x", "1"])
    assert prompts.ask_choice("pick", ["a", "b"]) == 0


def test_ask_choice_abort_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Selecting the appended abort option returns ``None``."""
    _feed(monkeypatch, ["3"])
    assert prompts.ask_choice("pick", ["a", "b"]) is None


def test_ask_choice_word_abort(monkeypatch: pytest.MonkeyPatch) -> None:
    """The word ``abort`` aborts the choice."""
    _feed(monkeypatch, ["abort"])
    assert prompts.ask_choice("pick", ["a", "b"]) is None


@pytest.mark.parametrize("answer", ["1", "y", "yes", "YES"])
def test_confirm_accepts_yes_forms(monkeypatch: pytest.MonkeyPatch, answer: str) -> None:
    """Confirmation accepts numeric and word yes-forms."""
    _feed(monkeypatch, [answer])
    assert prompts.confirm("change?") is True


@pytest.mark.parametrize("answer", ["2", "n", "no", "garbage"])
def test_confirm_rejects_no_and_bad(monkeypatch: pytest.MonkeyPatch, answer: str) -> None:
    """Confirmation treats no-forms and unparseable input as decline."""
    _feed(monkeypatch, [answer])
    assert prompts.confirm("change?") is False
