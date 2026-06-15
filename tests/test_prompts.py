"""Unit tests for paper_sorts CLI prompt helpers.

Covers: empty input, malformed input, and documented success paths
(constitution Principle II — pure helpers should have unit tests).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_nonempty


class TestAskNonempty:
    """Tests for ask_nonempty."""

    def test_returns_first_nonempty_input(self) -> None:
        """ask_nonempty returns immediately on the first non-empty input."""
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=["hello"]):
            result = ask_nonempty("Enter something")
        assert result == "hello"

    def test_strips_whitespace(self) -> None:
        """ask_nonempty strips leading/trailing whitespace."""
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=["  hello  "]):
            result = ask_nonempty("Enter something")
        assert result == "hello"

    def test_retries_on_empty_input(self) -> None:
        """ask_nonempty retries when the user enters an empty string."""
        with patch(
            "paper_sorts.cli.prompts.Prompt.ask",
            side_effect=["", "", "finally non-empty"],
        ):
            result = ask_nonempty("Enter something")
        assert result == "finally non-empty"


class TestAskChoice:
    """Tests for ask_choice."""

    def test_returns_zero_based_index(self) -> None:
        """ask_choice returns 0-based index for valid selection."""
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=["1"]):
            result = ask_choice("Pick one", ["A", "B", "C"])
        assert result == 0

    def test_last_option(self) -> None:
        """ask_choice handles selecting the last option."""
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=["3"]):
            result = ask_choice("Pick one", ["A", "B", "C"])
        assert result == 2

    def test_retries_on_out_of_range(self) -> None:
        """ask_choice retries when input is out of range."""
        with patch(
            "paper_sorts.cli.prompts.Prompt.ask",
            side_effect=["5", "abc", "2"],  # 5 OOB, abc invalid, 2 valid
        ):
            result = ask_choice("Pick one", ["A", "B", "C"])
        assert result == 1

    def test_empty_options_raises(self) -> None:
        """ask_choice raises ValueError if options list is empty."""
        with pytest.raises(ValueError):
            ask_choice("Pick one", [])


class TestAskConfirm:
    """Tests for ask_confirm."""

    def test_yes_returns_true(self) -> None:
        """'y' input returns True."""
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=["y"]):
            assert ask_confirm("Are you sure?") is True

    def test_no_returns_false(self) -> None:
        """'n' input returns False."""
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=["n"]):
            assert ask_confirm("Are you sure?") is False

    def test_numeric_yes(self) -> None:
        """'1' input returns True."""
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=["1"]):
            assert ask_confirm("Proceed?") is True

    def test_numeric_no(self) -> None:
        """'2' input returns False."""
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=["2"]):
            assert ask_confirm("Proceed?") is False

    def test_yes_word_returns_true(self) -> None:
        """'yes' input returns True."""
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=["yes"]):
            assert ask_confirm("Proceed?") is True

    def test_no_word_returns_false(self) -> None:
        """'no' input returns False."""
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=["no"]):
            assert ask_confirm("Proceed?") is False

    def test_retries_on_invalid(self) -> None:
        """ask_confirm retries on unrecognised input."""
        # First call → "maybe" (invalid), retry prompt call → "" (ignored),
        # second main call → "yes"
        with patch(
            "paper_sorts.cli.prompts.Prompt.ask",
            side_effect=["maybe", "", "yes"],
        ):
            assert ask_confirm("Sure?") is True
