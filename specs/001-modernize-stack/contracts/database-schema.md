# Database Schema Contract

**Feature**: 001-modernize-stack
**Component**: `migrations/`
**Audience**: Anyone writing/reviewing Alembic migrations or schema changes for this project.

## Steady-state schema (post-migration head)

Four tables. **Verbatim** in shape to the existing schema produced by `DatabaseConnector.create_tables()` — no NOT NULL clauses (other than those implied by `PRIMARY KEY`), no FK constraints beyond the single one the original declares (`papers.bibtex_id → bib.bibtex_id`). Names, columns, and constraint *absences* are preserved (FR-011) to guarantee that Revision 001 succeeds against any current personal database, including one with rows that the application happens to never produce in practice but the schema permits (e.g. a row with `papers.title IS NULL`, or an `authors_papers` row whose `paper_id` no longer points at a live paper).

```sql
-- bib: BibTeX entries, keyed by user-facing BibTeX id.
CREATE TABLE IF NOT EXISTS bib (
    bibtex_id text PRIMARY KEY,
    bibtex    text,
    UNIQUE (bibtex)
);

-- papers: one row per publication.
CREATE TABLE IF NOT EXISTS papers (
    id        SERIAL PRIMARY KEY,
    title     text,
    contents  text,
    bibtex_id text,
    CONSTRAINT fk_bibtex_id FOREIGN KEY (bibtex_id) REFERENCES bib (bibtex_id)
);

-- authors_id: the author dimension. Column name 'author' (not 'name') is
-- preserved to avoid touching production data more than necessary.
CREATE TABLE IF NOT EXISTS authors_id (
    id     SERIAL PRIMARY KEY,
    author text
);

-- authors_papers: the M:N link. Note: the original schema does NOT declare
-- foreign keys on author_id or paper_id. Modernization preserves that
-- looseness; tightening it would require a data audit (potential dangling
-- links) and is out of scope for this work. A future spec may add the FKs
-- after a data-cleanliness check.
CREATE TABLE IF NOT EXISTS authors_papers (
    id        SERIAL PRIMARY KEY,
    author_id int,
    paper_id  int
);
```

NOT NULL is application-level here, not schema-level: the prompt grammar (constitution Principle III) re-prompts on empty input, and the import service validates via pybtex before insert. Schema integrity is therefore "what the application produces", not "what the column types enforce" — preserving the original behaviour exactly.

Indexes added by modernization: **none**. The current schema has no indexes beyond the primary keys, and constitution Principle IV (v1.2.0) requires a Complexity Tracking entry for any addition. Indexes are deferred until the baseline benchmark (SC-006) shows an actual regression that can be attributed to scan cost — at which point an index becomes a measurement-driven decision and can be added with proper justification.

## Alembic revision plan

### Revision 001 — `initial_schema`

Creates the four tables and two indexes above on a database where none of them yet exist. Uses `op.create_table` / `op.create_index`. Detects existing tables via Alembic's reflection and is a no-op against an already-modern database.

`upgrade()`:
1. `CREATE TABLE bib (...)` if absent
2. `CREATE TABLE papers (...)` if absent
3. `CREATE TABLE authors_id (...)` if absent
4. `CREATE TABLE authors_papers (...)` if absent
`downgrade()`: drops in reverse order. Provided for completeness; not exercised in normal use.

### Revision 002 — `legacy_bibtext_to_bibtex`

Targets databases written by the historical `paper_sorts/add.py`, `paper_sorts/get_data.py`, or `paper_sorts/search.py` scripts, which used the column name `bibtext_id` (sic) on `papers` and `bib`. This migration is idempotent — it detects the legacy state and converges.

`upgrade()` (sketch):

```python
def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)

    papers_cols = {c["name"] for c in insp.get_columns("papers")} if insp.has_table("papers") else set()
    bib_cols = {c["name"] for c in insp.get_columns("bib")} if insp.has_table("bib") else set()

    if "bibtext_id" in papers_cols and "bibtex_id" not in papers_cols:
        op.alter_column("papers", "bibtext_id", new_column_name="bibtex_id")
    if "bibtext_id" in bib_cols and "bibtex_id" not in bib_cols:
        op.alter_column("bib", "bibtext_id", new_column_name="bibtex_id")

    # FK constraint name might differ across legacy variants — drop+recreate by reflection.
    # Body omitted from sketch; see versions/002_legacy_bibtext_to_bibtex.py for full code.
```

`downgrade()`: unimplemented (`raise NotImplementedError("legacy schema is no longer supported")`). Per constitution Principle II ("schema changes MUST update… in the same change"), removing the rename later would require a new migration.

## Migration acceptance criteria (FR-011, US4)

The migration command (`pdbsearch migrate`) MUST satisfy these properties for any input database it accepts:

| Input state | Expected output state | Verification |
|-------------|-----------------------|--------------|
| Empty DB                                  | Schema present at revision 002 | `alembic current` returns `002`. |
| Modern DB (current OO `bibtex_id`)        | Unchanged tables, revision stamped | Row counts identical pre/post; `alembic current` returns `002`. |
| Legacy DB (`bibtext_id` columns)          | Columns renamed in place; FKs intact; revision stamped | Row counts identical pre/post; `\d papers` shows `bibtex_id`, not `bibtext_id`. |
| Mid-migration DB (interrupted run)        | Either fully at 002, or fully at the prior revision — never half-migrated | Re-running `pdbsearch migrate` converges to 002. Verified by an integration test that aborts mid-upgrade with a process kill and reruns. |

## Constraints (constitution Principle IV)

- New tables, denormalised columns, or any index beyond the two named above require an entry in this feature's plan under Complexity Tracking. None are needed today.
- Connection sessions are context-managed (`with Session(engine) as session: ...`). Long-lived sessions are forbidden.
