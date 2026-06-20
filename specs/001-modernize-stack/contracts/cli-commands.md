# CLI Command Contract

## Entry Point

`pdbsearch` — installed via `[project.scripts]` in `pyproject.toml`.

## Interactive Mode (no subcommand)

When invoked with no subcommand, drops into a four-option menu:

```
1) Search
2) Add
3) Update
4) Delete
Q) Quit
```

Select by number (1–4) or `Q`/`q`. Invalid input re-prompts. Loops until quit.

## Subcommands

### pdbsearch search

```
pdbsearch search [--database-url URL] [--log-level LEVEL]
```

Presents sub-menu:
```
1) Search by title
2) Search by author
3) Quit
```

### pdbsearch add

```
pdbsearch add [--database-url URL] [--log-level LEVEL]
```

Prompts for: title, author(s), bibtex key, summary. Optionally prompts for `.bib` file path.

### pdbsearch update

```
pdbsearch update [--database-url URL] [--log-level LEVEL] [--id ID]
```

Search for paper, present field menu (title / summary / bibtex / author), prompt new value, confirm `y`/`n`.

### pdbsearch delete

```
pdbsearch delete [--database-url URL] [--log-level LEVEL] [--id ID]
```

Search for paper, display summary, confirm `y`/`n` before deletion.

### pdbsearch import (admin/scripted only)

```
pdbsearch import TEX_FILE BIB_FILE [--database-url URL]
```

Bulk import from `.tex` + `.bib` pair. Not shown in interactive menu.

### pdbsearch migrate (admin/scripted only)

```
pdbsearch migrate [--database-url URL]
```

Runs `alembic upgrade head`. Not shown in interactive menu.

## Global Options

| Option | Env var | Default | Description |
|--------|---------|---------|-------------|
| `--database-url` | `PDBSEARCH_DATABASE_URL` | from config file | PostgreSQL DSN |
| `--log-level` | `PDBSEARCH_LOG_LEVEL` | `INFO` | Logging level |
| `--config` | `PDBSEARCH_CONFIG_FILE` | none | Path to encrypted INI |
| `--key` | `PDBSEARCH_KEY_FILE` | none | Path to Fernet key file |

## Prompt Grammar

- **Menus**: 1-indexed, always include an explicit quit/abort option.
- **Confirmation**: accepts `y`, `yes`, `1` (proceed) and `n`, `no`, `2` (abort).
- **Empty input**: re-prompted until non-empty (current `get_user_input` behaviour preserved).
- **Out-of-range selection**: re-prompted until valid.
- **Errors**: plain-language message to stdout; technical details to log file only.
