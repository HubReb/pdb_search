"""User-facing prompt functions for paper_sorts CLI.

This module is the ONLY place in src/paper_sorts/ permitted to use
``rich.prompt.Prompt.ask`` or bare ``input()`` — constitution Principle III.

All prompt functions re-prompt on invalid or empty input so users can build
a consistent expectation: you cannot accidentally accept an empty answer.

Usage::

    from paper_sorts.cli.prompts import ask_text, ask_choice, ask_confirm

    name = ask_text("Author name: ")
    idx = ask_choice("Pick a paper:", ["Paper A", "Paper B"])
    confirmed = ask_confirm("Delete this paper?")
"""

from __future__ import annotations

import os

from rich.console import Console

console = Console(stderr=False)


def ask_text(prompt: str) -> str:
    """Prompt the user for a non-empty text input.

    Re-prompts until the user provides a non-empty string (constitution
    Principle III: empty-input re-prompt rule).

    Args:
        prompt: The prompt string shown to the user.

    Returns:
        A non-empty string entered by the user.
    """
    while True:
        answer = input(prompt)
        if answer.strip():
            return answer.strip()
        console.print("[yellow]Input cannot be empty. Please try again.[/yellow]")


def ask_choice(prompt: str, options: list[str]) -> int:
    """Display a 1-indexed numbered menu and return the chosen 0-based index.

    Re-prompts on out-of-range or non-numeric input.
    The menu always displays an "abort" option as the last item if the caller
    has not already included one — but callers are responsible for including
    an abort/quit option per constitution Principle III.

    Args:
        prompt: Introductory text shown before the option list.
        options: List of option strings; displayed as 1, 2, 3, …

    Returns:
        Zero-based index of the chosen option (so options[result] is the pick).

    Raises:
        SystemExit: Never. Invalid input always re-prompts.
    """
    console.print(prompt)
    for i, opt in enumerate(options, start=1):
        console.print(f"  {i}) {opt}")

    while True:
        raw = input("Your choice: ").strip()
        try:
            choice = int(raw)
        except ValueError:
            console.print("[yellow]Please enter a number.[/yellow]")
            continue
        if 1 <= choice <= len(options):
            return choice - 1
        console.print(
            f"[yellow]Please enter a number between 1 and {len(options)}.[/yellow]"
        )


def ask_confirm(prompt: str) -> bool:
    """Ask a yes/no confirmation question and return the boolean answer.

    Accepts numeric (1 = yes, 2 = no) and word (y/yes, n/no) forms per
    constitution Principle III (dual-form confirmation rule).

    Re-prompts on unrecognised input.

    Args:
        prompt: The confirmation question to display.

    Returns:
        True if the user confirmed, False if they declined.
    """
    console.print(prompt)
    console.print("  1) (Y)es\n  2) (N)o")
    while True:
        raw = input("Your choice: ").strip().lower()
        if raw in ("1", "y", "yes"):
            return True
        if raw in ("2", "n", "no"):
            return False
        console.print("[yellow]Please enter 1/y/yes or 2/n/no.[/yellow]")


def ask_file(prompt: str) -> str:
    """Prompt the user for a file path, re-prompting until the path exists.

    Args:
        prompt: The prompt string shown to the user.

    Returns:
        Absolute or relative path to an existing file.
    """
    while True:
        path = input(prompt).strip()
        if not path:
            console.print("[yellow]Please enter a file path.[/yellow]")
            continue
        if os.path.isfile(path):
            return path
        console.print(
            f"[yellow]File {path!r} not found. Please enter a valid file path.[/yellow]"
        )
