"""Unit tests for paper_sorts.cli.prompts.

Tests all three prompt helpers with mocked input, verifying:
- ask_str: empty input causes re-prompt; non-empty returns value
- ask_choice: out-of-range input causes re-prompt; valid index returned
- ask_confirm: y/n/yes/no/1/2 all handled; invalid re-prompts
"""

from __future__ import annotations

import pytest

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_str


class TestAskStr:
    """Tests for ask_str."""

    def test_returns_non_empty_input(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_str returns the first non-empty input."""
        monkeypatch.setattr("builtins.input", lambda _: "hello")
        result = ask_str("Enter: ")
        assert result == "hello"

    def test_re_prompts_on_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_str re-prompts when input is empty."""
        responses = iter(["", "", "valid"])
        monkeypatch.setattr("builtins.input", lambda _: next(responses))
        result = ask_str("Enter: ")
        assert result == "valid"

    def test_strips_whitespace(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_str strips leading/trailing whitespace from the response."""
        monkeypatch.setattr("builtins.input", lambda _: "  hello  ")
        result = ask_str("Enter: ")
        assert result == "hello"

    def test_whitespace_only_reprompts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_str treats whitespace-only input as empty and re-prompts."""
        responses = iter(["   ", "answer"])
        monkeypatch.setattr("builtins.input", lambda _: next(responses))
        result = ask_str("Enter: ")
        assert result == "answer"


class TestAskChoice:
    """Tests for ask_choice."""

    def test_valid_choice_returns_zero_based_index(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ask_choice returns the 0-based index for a valid 1-based input."""
        monkeypatch.setattr("builtins.input", lambda _: "2")
        idx = ask_choice(["option A", "option B", "option C"])
        assert idx == 1

    def test_re_prompts_on_out_of_range(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """ask_choice re-prompts when the user enters a number out of range."""
        responses = iter(["5", "0", "1"])
        monkeypatch.setattr("builtins.input", lambda _: next(responses))
        idx = ask_choice(["A", "B"])
        assert idx == 0
        captured = capsys.readouterr()
        assert "1 and 2" in captured.out

    def test_re_prompts_on_non_numeric(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """ask_choice re-prompts when input is not numeric."""
        responses = iter(["abc", "1"])
        monkeypatch.setattr("builtins.input", lambda _: next(responses))
        idx = ask_choice(["X"])
        assert idx == 0

    def test_first_option(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_choice returns 0 for input '1'."""
        monkeypatch.setattr("builtins.input", lambda _: "1")
        assert ask_choice(["only option"]) == 0


class TestAskConfirm:
    """Tests for ask_confirm."""

    @pytest.mark.parametrize("response", ["y", "Y", "yes", "YES", "1"])
    def test_affirmative(
        self, response: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ask_confirm returns True for all affirmative inputs."""
        monkeypatch.setattr("builtins.input", lambda _: response)
        assert ask_confirm("Do it?") is True

    @pytest.mark.parametrize("response", ["n", "N", "no", "NO", "2"])
    def test_negative(self, response: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_confirm returns False for all negative inputs."""
        monkeypatch.setattr("builtins.input", lambda _: response)
        assert ask_confirm("Do it?") is False

    def test_invalid_then_valid(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """ask_confirm re-prompts on invalid input until a valid choice is made."""
        responses = iter(["maybe", "sure", "n"])
        monkeypatch.setattr("builtins.input", lambda _: next(responses))
        result = ask_confirm("Do it?")
        assert result is False
        captured = capsys.readouterr()
        assert "1, y, yes" in captured.out
