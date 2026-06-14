"""Unit tests for the paper_sorts CLI prompts module.

Tests cover: empty input re-prompt, out-of-range re-prompt, valid inputs.
These are pure unit tests that monkeypatch input() — no database needed.
"""

from __future__ import annotations

import pathlib

import pytest

from paper_sorts.cli import prompts


class TestAskText:
    """Tests for prompts.ask_text."""

    def test_empty_input_reprompts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_text re-prompts when the first input is empty."""
        inputs = iter(["", "  ", "valid answer"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        result = prompts.ask_text("Enter something: ")
        assert result == "valid answer"

    def test_returns_stripped_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_text strips whitespace from the returned value."""
        monkeypatch.setattr("builtins.input", lambda _: "  hello  ")
        result = prompts.ask_text("Enter: ")
        assert result == "hello"

    def test_valid_first_input(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_text returns immediately on a valid first input."""
        monkeypatch.setattr("builtins.input", lambda _: "first")
        result = prompts.ask_text("Enter: ")
        assert result == "first"


class TestAskChoice:
    """Tests for prompts.ask_choice."""

    def test_valid_choice(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_choice returns zero-based index for a valid 1-indexed input."""
        monkeypatch.setattr("builtins.input", lambda _: "2")
        idx = prompts.ask_choice("Pick:", ["Option A", "Option B", "Option C"])
        assert idx == 1  # 0-based

    def test_out_of_range_reprompts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_choice re-prompts on out-of-range input before accepting valid input."""
        inputs = iter(["0", "5", "1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        idx = prompts.ask_choice("Pick:", ["A", "B"])
        assert idx == 0

    def test_non_numeric_reprompts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_choice re-prompts on non-numeric input."""
        inputs = iter(["abc", "!", "1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        idx = prompts.ask_choice("Pick:", ["X"])
        assert idx == 0


class TestAskConfirm:
    """Tests for prompts.ask_confirm."""

    @pytest.mark.parametrize("answer", ["1", "y", "yes"])
    def test_yes_variants(self, answer: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_confirm returns True for all accepted yes forms."""
        monkeypatch.setattr("builtins.input", lambda _: answer)
        assert prompts.ask_confirm("Confirm?") is True

    @pytest.mark.parametrize("answer", ["2", "n", "no"])
    def test_no_variants(self, answer: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_confirm returns False for all accepted no forms."""
        monkeypatch.setattr("builtins.input", lambda _: answer)
        assert prompts.ask_confirm("Confirm?") is False

    def test_invalid_reprompts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ask_confirm re-prompts on unrecognised input."""
        inputs = iter(["maybe", "3", "y"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        assert prompts.ask_confirm("Confirm?") is True


class TestAskFile:
    """Tests for prompts.ask_file."""

    def test_valid_file_returned(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """ask_file returns the path when the file exists."""

        test_file = tmp_path / "test.bib"
        test_file.write_text("@misc{x}")

        monkeypatch.setattr("builtins.input", lambda _: str(test_file))
        result = prompts.ask_file("Enter file: ")
        assert result == str(test_file)

    def test_nonexistent_reprompts(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """ask_file re-prompts when the file does not exist."""

        real_file = tmp_path / "real.bib"
        real_file.write_text("@misc{x}")

        inputs = iter(["/nonexistent/path.bib", str(real_file)])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        result = prompts.ask_file("Enter file: ")
        assert result == str(real_file)
