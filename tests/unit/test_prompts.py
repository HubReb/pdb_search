"""Unit tests for ``paper_sorts.cli.prompts``.

Covers the v1.4.0 alias mechanism on ``ask_choice`` (T003) plus the
existing ``ask_text`` and ``ask_confirm`` grammar that pre-dates this
feature.
"""

from __future__ import annotations

import logging

import pytest

from paper_sorts.cli import prompts

# ---------------------------------------------------------------------------
# ask_text
# ---------------------------------------------------------------------------


def test_ask_text_reprompts_on_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = iter(["", "", "answer"])
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: next(responses))
    assert prompts.ask_text("?") == "answer"


def test_ask_text_returns_first_truthy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "first")
    assert prompts.ask_text("?") == "first"


# ---------------------------------------------------------------------------
# ask_choice — digit input (legacy parity)
# ---------------------------------------------------------------------------


def test_ask_choice_returns_selection_by_digit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "2")
    assert prompts.ask_choice("?", ["papers", "bib", "(q)uit"]) == 2


def test_ask_choice_passes_validated_choices(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prompt.ask's ``choices=`` is what rejects 0 and out-of-range — confirm we set it,
    and confirm that both digits and aliases (in both cases) appear in ``choices``."""
    captured: dict[str, object] = {}

    def fake_ask(_prompt: str, **kwargs: object) -> str:
        captured.update(kwargs)
        return "1"

    monkeypatch.setattr(prompts.Prompt, "ask", fake_ask)
    prompts.ask_choice("?", ["papers", "bib", "(q)uit"])
    choices = captured["choices"]
    assert isinstance(choices, list)
    # Digits 1..n
    assert "1" in choices and "2" in choices and "3" in choices
    # Aliases (lower- and upper-case)
    assert "p" in choices and "P" in choices
    assert "b" in choices and "B" in choices
    assert "q" in choices and "Q" in choices


# ---------------------------------------------------------------------------
# ask_choice — alias auto-derivation (v1.4.0)
# ---------------------------------------------------------------------------


def test_ask_choice_alias_first_alpha_char(monkeypatch: pytest.MonkeyPatch) -> None:
    """Plain-str labels with no parens auto-derive alias from first alpha char."""
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "p")
    assert prompts.ask_choice("?", ["papers", "bib"]) == 1


def test_ask_choice_alias_other_first_alpha(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "b")
    assert prompts.ask_choice("?", ["papers", "bib"]) == 2


def test_ask_choice_alias_from_parenthesised_char(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parenthesised single-alpha char wins over leading char (two-step rule)."""
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "q")
    # "(Q)uit" leading char is "(" — first-alpha would give "Q"; parens rule still selects q.
    assert prompts.ask_choice("?", ["(Q)uit"]) == 1


def test_ask_choice_alias_parens_inside_label(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parens-wrapped char *inside* the label (not just leading) is also picked up."""
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "a")
    assert prompts.ask_choice("?", ["Search by (a)uthor", "Search by (t)itle"]) == 1


def test_ask_choice_alias_parens_picks_t(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "t")
    assert prompts.ask_choice("?", ["Search by (a)uthor", "Search by (t)itle"]) == 2


def test_ask_choice_alias_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "Q")
    assert prompts.ask_choice("?", ["papers", "bib", "(Q)uit"]) == 3


def test_ask_choice_alias_case_insensitive_lowercase_label(monkeypatch: pytest.MonkeyPatch) -> None:
    """A label with ``(q)uit`` (lowercase) accepts both ``q`` and ``Q`` as aliases."""
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "Q")
    assert prompts.ask_choice("?", ["papers", "bib", "authors", "(q)uit"]) == 4


# ---------------------------------------------------------------------------
# ask_choice — explicit tuple form
# ---------------------------------------------------------------------------


def test_ask_choice_explicit_alias_tuple(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit ``(label, alias)`` tuple overrides auto-derivation."""
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "x")
    # "alpha" would auto-derive "a"; explicit "x" wins.
    assert prompts.ask_choice("?", [("alpha", "x"), ("beta", "y")]) == 1


def test_ask_choice_none_alias_opt_out(monkeypatch: pytest.MonkeyPatch) -> None:
    """``(label, None)`` opts the option out — the alias char does not match it."""
    captured: dict[str, object] = {}

    def fake_ask(_prompt: str, **kwargs: object) -> str:
        captured.update(kwargs)
        return "1"

    monkeypatch.setattr(prompts.Prompt, "ask", fake_ask)
    prompts.ask_choice("?", [("title: A", None), ("title: B", None), "abort"])
    choices = captured["choices"]
    assert isinstance(choices, list)
    # No 't' alias for the title rows; only 'a' for "abort" and digits 1/2/3.
    assert "t" not in choices and "T" not in choices
    assert "a" in choices and "A" in choices
    assert {"1", "2", "3"} <= set(choices)


def test_ask_choice_none_alias_digit_still_works(monkeypatch: pytest.MonkeyPatch) -> None:
    """Digit input still selects an opt-out option."""
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "2")
    assert prompts.ask_choice("?", [("title: A", None), ("title: B", None), "abort"]) == 2


# ---------------------------------------------------------------------------
# ask_choice — construction-time errors
# ---------------------------------------------------------------------------


def test_ask_choice_empty_options_raises() -> None:
    with pytest.raises(ValueError, match="at least one option"):
        prompts.ask_choice("?", [])


def test_ask_choice_collision_raises() -> None:
    """Two non-None aliases resolving to the same char raise at construction time."""
    with pytest.raises(ValueError, match="alias collision"):
        prompts.ask_choice("?", ["authors", "abort"])


def test_ask_choice_collision_message_names_alias_and_labels() -> None:
    with pytest.raises(ValueError) as excinfo:
        prompts.ask_choice("?", ["authors", "abort"])
    msg = str(excinfo.value)
    assert "'a'" in msg
    assert "authors" in msg
    assert "abort" in msg


def test_ask_choice_collision_case_insensitive() -> None:
    """Case-insensitive collision detection — ``Apple`` and ``apricot`` both resolve to ``a``."""
    with pytest.raises(ValueError, match="alias collision"):
        prompts.ask_choice("?", ["Apple", "apricot"])


def test_ask_choice_alias_length_one_required() -> None:
    """An explicit alias of length != 1 raises at construction time."""
    with pytest.raises(ValueError, match="single character"):
        prompts.ask_choice("?", [("foo", "ab"), "bar"])


def test_ask_choice_alias_empty_string_rejected() -> None:
    """An explicit alias of empty string raises (length != 1)."""
    with pytest.raises(ValueError, match="single character"):
        prompts.ask_choice("?", [("foo", ""), "bar"])


def test_ask_choice_label_with_no_alpha_raises() -> None:
    """A plain-str label with neither parenthesised single-alpha nor any alpha raises."""
    with pytest.raises(ValueError, match="cannot auto-derive alias"):
        prompts.ask_choice("?", ["12345"])


def test_ask_choice_collision_skipped_when_one_alias_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``None`` aliases never collide with each other or with non-None aliases.

    Both title rows opt out of aliasing; ``"abort"`` gets ``a``. No collision
    even though the labels both start with ``t``.
    """
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "1")
    result = prompts.ask_choice("?", [("title: A", None), ("title: B", None), "abort"])
    assert result == 1


# ---------------------------------------------------------------------------
# ask_choice — rendering
# ---------------------------------------------------------------------------


def test_ask_choice_renders_paren_wrapped_label_verbatim(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Labels with parens already in them render as-is — no double-wrap."""
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "1")
    prompts.ask_choice("?", ["(Q)uit"])
    out = capsys.readouterr().out
    assert "1) (Q)uit" in out
    assert "((Q))uit" not in out


def test_ask_choice_renders_alias_inserted_into_plain_label(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Plain labels get the alias char wrapped in parens at its first occurrence."""
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "1")
    prompts.ask_choice("?", ["papers"])
    out = capsys.readouterr().out
    assert "1) (p)apers" in out


def test_ask_choice_renders_none_opt_out_verbatim(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Options with ``alias=None`` render verbatim — no parens inserted."""
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "1")
    prompts.ask_choice("?", [("title: Foo", None), "abort"])
    out = capsys.readouterr().out
    assert "1) title: Foo" in out
    assert "(t)" not in out  # title row stays digit-only — no t-wrap
    assert "2) (a)bort" in out


# ---------------------------------------------------------------------------
# ask_confirm
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("token", ["1", "y", "yes", "Y", "YES", "  yes  "])
def test_ask_confirm_accepts_yes_tokens(token: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: token)
    assert prompts.ask_confirm("?") is True


@pytest.mark.parametrize("token", ["2", "n", "no", "N", "No", "  NO "])
def test_ask_confirm_accepts_no_tokens(token: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: token)
    assert prompts.ask_confirm("?") is False


def test_ask_confirm_unrecognised_returns_false_and_logs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(prompts.Prompt, "ask", lambda *_a, **_kw: "maybe")
    with caplog.at_level(logging.WARNING, logger="paper_sorts.cli.prompts"):
        assert prompts.ask_confirm("?") is False
    assert any("Unrecognised confirmation" in record.message for record in caplog.records)
