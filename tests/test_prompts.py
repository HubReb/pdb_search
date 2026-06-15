"""Unit tests for the prompt grammar in ``cli/prompts.py``.

Covers empty-input re-prompt, 1-indexed menu choice with abort, dual-form (numeric + word)
confirmation, and out-of-range disambiguation re-prompt. ``rich.prompt.Prompt.ask`` is patched
to feed a scripted sequence of answers.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from paper_sorts.cli import prompts


def _script(answers: list[str]) -> Iterator[str]:
    """Return an iterator over scripted answers.

    :param answers: the answers to yield in order.
    :return: an iterator over the answers.
    """
    return iter(answers)


def _patch_ask(monkeypatch: pytest.MonkeyPatch, answers: list[str]) -> None:
    """Patch ``Prompt.ask`` to return scripted answers in sequence.

    :param monkeypatch: the pytest monkeypatch fixture.
    :param answers: the answers to feed, in order.
    """
    it = _script(answers)
    monkeypatch.setattr(prompts.Prompt, "ask", staticmethod(lambda *a, **k: next(it)))


def test_ask_nonempty_reprompts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty / whitespace answers are rejected until a non-empty one is given."""
    _patch_ask(monkeypatch, ["", "   ", "hello"])
    assert prompts.ask_nonempty("x") == "hello"


def test_ask_choice_one_indexed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A valid 1-indexed selection returns the zero-based index."""
    _patch_ask(monkeypatch, ["2"])
    assert prompts.ask_choice("pick", ["a", "b", "c"]) == 1


def test_ask_choice_reprompts_out_of_range(monkeypatch: pytest.MonkeyPatch) -> None:
    """Out-of-range and non-numeric input re-prompt."""
    _patch_ask(monkeypatch, ["0", "9", "foo", "3"])
    assert prompts.ask_choice("pick", ["a", "b", "c"]) == 2


def test_confirm_numeric_and_word(monkeypatch: pytest.MonkeyPatch) -> None:
    """Confirmation accepts both numeric and word forms."""
    _patch_ask(monkeypatch, ["1"])
    assert prompts.confirm("ok?") is True
    _patch_ask(monkeypatch, ["no"])
    assert prompts.confirm("ok?") is False
    _patch_ask(monkeypatch, ["yes"])
    assert prompts.confirm("ok?") is True
    _patch_ask(monkeypatch, ["2"])
    assert prompts.confirm("ok?") is False


def test_confirm_reprompts_on_garbage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unrecognised confirmation input re-prompts."""
    _patch_ask(monkeypatch, ["maybe", "y"])
    assert prompts.confirm("ok?") is True


def test_pick_from_reprompts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disambiguation re-prompts on out-of-range and returns the zero-based index."""
    _patch_ask(monkeypatch, ["5", "1"])
    assert prompts.pick_from("which", ["one", "two"]) == 0
