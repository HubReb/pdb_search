# CLI Command Contract

**Feature**: 001-modernize-stack  
**Entry Point**: `pdbsearch` (registered in `pyproject.toml` `[project.scripts]`)  
**Framework**: Typer

---

## Invocation Modes

### Interactive mode (no subcommand)
```
pdbsearch [--database-url URL] [--log-level LEVEL]
```
Drops into the four-option top-level menu:
```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
Your choice:
```

### Subcommand mode
```
pdbsearch <subcommand> [options]
```
Subcommands: `search`, `add`, `update`, `delete`, `import`, `migrate`

---

## Global Options

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--database-url` | `PDBSEARCH_DATABASE_URL` | (from config) | SQLAlchemy DB URL |
| `--log-level` | `PDBSEARCH_LOG_LEVEL` | `INFO` | Python logging level name |
| `--config` | `PDBSEARCH_CONFIG` | — | Path to encrypted INI config file |
| `--key` | `PDBSEARCH_KEY` | — | Path to Fernet key file |

---

## Subcommands

### `pdbsearch search`
Interactive subcommand. Prompts for search type (author / title), then prompts for the search term. Displays results in pretty-print format.

**Prompts**:
1. `Search interface\nPlease choose a method:\n1) Search by author\n2) Search by paper title\n3) abort\nYour choice: `
2. `Please enter the author's name: ` or `Please enter the paper title: `

**Output**: `title: {title}\nauthors: {authors}\nsummary: {summary}\nbib entry: {bibtex}`

---

### `pdbsearch add`
Interactive subcommand. Prompts for paper metadata, optionally reads bibtex from a `.bib` file.

**Prompts**:
1. `Author(s), please provide a , separated list: `
2. `Paper title: `
3. `bibtex key: `
4. `Do you want to enter the bibtex entry via a separate file?\n1) Yes\n2) No\nYour choice: `
5. (if 1) `Enter filename: `
6. (if 2) `bib entry: `
7. `summary of the paper: `

---

### `pdbsearch update`
Interactive subcommand. Prompts for table, column, identifier, and new value. Requires confirmation.

**Prompts**:
1. `Which information do you want to update?\n1) papers\n2) bib\n3) authors\n4) abort\nYour choice: `
2. (for papers) `Which information do you want to update?\n1) title\n2) contents\n3) abort\nYour choice: `
3. `Which entry do you want to update?\nPlease enter the respective id: `
4. `Enter the new information: `
5. `Please verify: You wish to change '{column}' of the entry '{id}' to '{value}'.\nProceed?\n1) (Y)es\n2) (N)o\nYour choice: `

**Confirmation**: Accepts `1`, `y`, `yes` (proceed) or `2`, `n`, `no` (abort).

---

### `pdbsearch delete`
Interactive subcommand. Prompts for search term (by title), displays result, requires confirmation before deleting.

**Prompts**:
1. `Please enter the paper title to delete: `
2. (if multiple) disambiguation list
3. `Are you sure you want to delete '{title}'?\n1) (Y)es\n2) (N)o\nYour choice: `

---

### `pdbsearch import`
Non-interactive (scripted) subcommand. Reads a `.tex` + `.bib` pair and bulk-imports papers.

**Usage**: `pdbsearch import --tex TEXFILE --bib BIBFILE`

| Option | Required | Description |
|--------|----------|-------------|
| `--tex` | Yes | Path to `.tex` literature file |
| `--bib` | Yes | Path to `.bib` bibliography file |

**Output**: Progress log to stderr/log; summary count to stdout.  
**Error handling**: Per-paper commit; skip entries whose bibtex key has no matching `.bib` record (with logged WARNING).

---

### `pdbsearch migrate`
Non-interactive (scripted / admin) subcommand. Applies all Alembic migrations to the target database.

**Usage**: `pdbsearch migrate`

**Behaviour**: Runs `alembic upgrade head` against the configured database URL. Idempotent. Does NOT appear in the interactive top-level menu.

---

## Prompt Grammar Rules (constitution Principle III)

- All prompts route through `paper_sorts.cli.prompts` — no bare `input()` elsewhere.
- Numbered menus are 1-indexed.
- Every menu includes an explicit abort/quit option.
- Empty input (just Enter) re-prompts until non-empty input is provided.
- Confirmation prompts accept both numeric (`1`/`2`) and word (`y`/`n`/`yes`/`no`) forms.
- Destructive operations (update, delete) show a summary of the exact change before applying.
- Failure paths: plain-language message to stdout, technical detail to logger.
