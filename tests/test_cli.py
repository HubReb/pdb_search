"""Unit tests for CLI prompts and smoke tests for subcommands.

Tests cover:
- ask_nonempty: empty input re-prompt, non-empty input accepted
- ask_choice: out-of-range selection re-prompt, valid selection accepted
- ask_confirm: y/yes/1 → True; n/no/2 → False
- pdbsearch --help: runs without error (smoke test via CliRunner)
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from paper_sorts.cli.app import app


class TestAskNonempty:
    """Tests for :func:`paper_sorts.cli.prompts.ask_nonempty`."""

    def test_returns_nonempty_on_first_try(self) -> None:
        """ask_nonempty returns the value when non-empty input is given immediately."""
        from paper_sorts.cli.prompts import ask_nonempty

        with patch("paper_sorts.cli.prompts.Prompt.ask", return_value="hello"):
            result = ask_nonempty("Enter something")
        assert result == "hello"

    def test_reprompts_on_empty_then_accepts(self) -> None:
        """ask_nonempty re-prompts on empty input and returns the next non-empty value."""
        from paper_sorts.cli.prompts import ask_nonempty

        call_count = 0

        def mock_ask(prompt: str, **kwargs: object) -> str:
            nonlocal call_count
            call_count += 1
            return "" if call_count == 1 else "world"

        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=mock_ask):
            result = ask_nonempty("Enter something")
        assert result == "world"
        assert call_count == 2


class TestAskChoice:
    """Tests for :func:`paper_sorts.cli.prompts.ask_choice`."""

    def test_valid_choice_accepted(self) -> None:
        """ask_choice returns the choice when a valid number is entered."""
        from paper_sorts.cli.prompts import ask_choice

        with patch("paper_sorts.cli.prompts.Prompt.ask", return_value="2"):
            result = ask_choice(["Option A", "Option B", "Option C"])
        assert result == 2

    def test_out_of_range_reprompts(self) -> None:
        """ask_choice re-prompts when an out-of-range number is entered."""
        from paper_sorts.cli.prompts import ask_choice

        responses = iter(["5", "0", "1"])
        mock_fn = lambda *_a, **_k: next(responses)  # noqa: E731
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=mock_fn):
            result = ask_choice(["Option A", "Option B"])
        assert result == 1

    def test_non_numeric_reprompts(self) -> None:
        """ask_choice re-prompts when non-numeric input is entered."""
        from paper_sorts.cli.prompts import ask_choice

        responses = iter(["abc", "xyz", "2"])
        mock_fn = lambda *_a, **_k: next(responses)  # noqa: E731
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=mock_fn):
            result = ask_choice(["A", "B", "C"])
        assert result == 2

    def test_empty_options_raises(self) -> None:
        """ask_choice raises ValueError when options list is empty."""
        from paper_sorts.cli.prompts import ask_choice

        with pytest.raises(ValueError, match="non-empty"):
            ask_choice([])


class TestAskConfirm:
    """Tests for :func:`paper_sorts.cli.prompts.ask_confirm`."""

    @pytest.mark.parametrize("yes_input", ["y", "yes", "Y", "YES", "1"])
    def test_confirm_yes_variants(self, yes_input: str) -> None:
        """ask_confirm returns True for y/yes/1 (case-insensitive)."""
        from paper_sorts.cli.prompts import ask_confirm

        with patch("paper_sorts.cli.prompts.Prompt.ask", return_value=yes_input):
            assert ask_confirm("Proceed?") is True

    @pytest.mark.parametrize("no_input", ["n", "no", "N", "NO", "2"])
    def test_confirm_no_variants(self, no_input: str) -> None:
        """ask_confirm returns False for n/no/2 (case-insensitive)."""
        from paper_sorts.cli.prompts import ask_confirm

        with patch("paper_sorts.cli.prompts.Prompt.ask", return_value=no_input):
            assert ask_confirm("Proceed?") is False

    def test_confirm_invalid_then_valid(self) -> None:
        """ask_confirm re-prompts on invalid input, then returns on valid."""
        from paper_sorts.cli.prompts import ask_confirm

        responses = iter(["maybe", "sure", "yes"])
        mock_fn = lambda *_a, **_k: next(responses)  # noqa: E731
        with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=mock_fn):
            result = ask_confirm("Proceed?")
        assert result is True


# ---------------------------------------------------------------------------
# Smoke tests for CLI entry point
# ---------------------------------------------------------------------------


class TestCLISmokeTests:
    """Smoke tests for the Typer CLI entry point."""

    def test_help_runs(self) -> None:
        """pdbsearch --help exits cleanly."""
        runner = CliRunner()
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "pdbsearch" in result.output.lower() or "usage" in result.output.lower()

    def test_search_help(self) -> None:
        """pdbsearch search --help exits cleanly."""
        runner = CliRunner()
        result = runner.invoke(app, ["search", "--help"])
        assert result.exit_code == 0

    def test_add_help(self) -> None:
        """pdbsearch add --help exits cleanly."""
        runner = CliRunner()
        result = runner.invoke(app, ["add", "--help"])
        assert result.exit_code == 0

    def test_update_help(self) -> None:
        """pdbsearch update --help exits cleanly."""
        runner = CliRunner()
        result = runner.invoke(app, ["update", "--help"])
        assert result.exit_code == 0

    def test_delete_help(self) -> None:
        """pdbsearch delete --help exits cleanly."""
        runner = CliRunner()
        result = runner.invoke(app, ["delete", "--help"])
        assert result.exit_code == 0

    def test_migrate_help(self) -> None:
        """pdbsearch migrate --help exits cleanly."""
        runner = CliRunner()
        result = runner.invoke(app, ["migrate", "--help"])
        assert result.exit_code == 0

    def test_import_help(self) -> None:
        """pdbsearch import --help exits cleanly."""
        runner = CliRunner()
        result = runner.invoke(app, ["import", "--help"])
        assert result.exit_code == 0

    def test_no_database_url_exits_error(self) -> None:
        """pdbsearch without --database-url and no env var exits with error."""
        runner = CliRunner()
        result = runner.invoke(
            app,
            [],
            env={"PDBSEARCH_DATABASE_URL": ""},
        )
        # Should exit with code 1 (no DB configured)
        assert result.exit_code != 0
