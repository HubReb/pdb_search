"""Canonical seed dataset, co-located with the tests that assert on it.

Per constitution Principle II, integration tests that assert on specific rows
reference the seed data here. ``SEED_PAPERS`` covers:

* a multi-author paper (author-join display string),
* two papers sharing a title (disambiguation flow),
* a BibTeX entry with a LaTeX accent (round-trip without corruption).

``SEED_TEX`` / ``SEED_BIB`` are a matching LaTeX-overview + ``.bib`` pair for
the bulk-import tests, including one cited key with no ``.bib`` record (skipped).
"""

from __future__ import annotations

from paper_sorts.db.repositories import PaperCreate

SEED_PAPERS: list[PaperCreate] = [
    PaperCreate(
        title="Direct speech-to-speech translation with discrete units",
        summary="Translates speech directly to speech via discrete units.",
        authors=["Lee, Ann", "Chen, Peng-Jen", "Pino, J."],
        bibtex_id="Lee2021Direct",
        bibtex="@article{Lee2021Direct,\n  title={Direct speech-to-speech translation"
        " with discrete units},\n  author={Lee, Ann and Chen, Peng-Jen and Pino, J.}\n}",
    ),
    PaperCreate(
        title="Large-scale Self- and Semi-Supervised learning for speech translation",
        summary="Self- and semi-supervised learning at scale for speech translation.",
        authors=["Wang, Changhan", "Pino, J."],
        bibtex_id="Wang2021LargeScaleSA",
        bibtex="@article{Wang2021LargeScaleSA,\n  title={Large-scale Self- and"
        " Semi-Supervised learning for speech translation},\n  author={Wang, Changhan"
        " and Pino, J.}\n}",
    ),
    # Duplicate-title pair (disambiguation flow).
    PaperCreate(
        title="A Survey",
        summary="First survey under a shared title.",
        authors=["Müller, Anna"],
        bibtex_id="Mueller2020Survey",
        bibtex='@article{Mueller2020Survey,\n  title={A Survey},\n  author={M{\\"u}ller, Anna}\n}',
    ),
    PaperCreate(
        title="A Survey",
        summary="Second survey under the same shared title.",
        authors=["Smith, John"],
        bibtex_id="Smith2022Survey",
        bibtex="@article{Smith2022Survey,\n  title={A Survey},\n  author={Smith, John}\n}",
    ),
]


SEED_TEX = r"""
\begin{itemize}
\item \textbf{Direct speech-to-speech translation} \cite{Lee2021Direct}:
A direct speech-to-speech model.
\item \textbf{An Unmatched Paper} \cite{NoSuchKey2099}:
This citation key has no matching bib record and must be skipped.
\end{itemize}
"""

SEED_BIB = r"""
@article{Lee2021Direct,
  title = {Direct speech-to-speech translation with discrete units},
  author = {Lee, Ann and Chen, Peng-Jen and Pino, J.},
  year = {2021}
}
"""
