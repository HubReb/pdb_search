"""Unit tests for ``paper_sorts.cli.prompts`` (T028)."""

from __future__ import annotations

import logging

import pytest

from paper_sorts.cli import prompts


def test_ask_text_reprompts_on_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = iter(["", "", "answer"])
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: next(responses))
    assert prompts.ask_text("?") == "answer"


def test_ask_text_returns_first_truthy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "first")
    assert prompts.ask_text("?") == "first"


def test_ask_choice_returns_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompts.IntPrompt, "ask", lambda *_a, **_kw: 2)
    assert prompts.ask_choice("?", ["a", "b", "c"]) == 2


def test_ask_choice_passes_validated_choices(monkeypatch: pytest.MonkeyPatch) -> None:
    """IntPrompt's ``choices=`` is what rejects 0 and out-of-range — confirm we set it."""
    captured: dict[str, object] = {}

    def fake_ask(_prompt: str, **kwargs: object) -> int:
        captured.update(kwargs)
        return 1

    monkeypatch.setattr(prompts.IntPrompt, "ask", fake_ask)
    prompts.ask_choice("?", ["a", "b", "c"])
    assert captured["choices"] == ["1", "2", "3"]


def test_ask_choice_quit_alias_lowercase(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "q")
    assert prompts.ask_choice("?", ["a", "b", "quit"], quit_alias="q") == 3


def test_ask_choice_quit_alias_uppercase(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "Q")
    assert prompts.ask_choice("?", ["a", "b", "quit"], quit_alias="q") == 3


def test_ask_choice_quit_alias_numeric_still_works(monkeypatch: pytest.MonkeyPatch) -> None:
    """The numbered selection still works when a quit_alias is set."""
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "2")
    assert prompts.ask_choice("?", ["a", "b", "quit"], quit_alias="q") == 2


def test_ask_choice_empty_options_raises() -> None:
    with pytest.raises(ValueError, match="at least one option"):
        prompts.ask_choice("?", [])


@pytest.mark.parametrize("token", ["1", "y", "yes", "Y", "YES", "  yes  "])
def test_ask_confirm_accepts_yes_tokens(
    token: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: token)
    assert prompts.ask_confirm("?") is True


@pytest.mark.parametrize("token", ["2", "n", "no", "N", "No", "  NO "])
def test_ask_confirm_accepts_no_tokens(
    token: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: token)
    assert prompts.ask_confirm("?") is False


def test_ask_confirm_unrecognised_returns_false_and_logs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "maybe")
    with caplog.at_level(logging.WARNING, logger="paper_sorts.cli.prompts"):
        assert prompts.ask_confirm("?") is False
    assert any(
        "Unrecognised confirmation" in record.message for record in caplog.records
    )
