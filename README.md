# Off-line Paper Database searcher 

A small, bare-bones application to add publication metadata to a postgresql database for later querying in case no online connection
is available to enable querying one of the freely available online resources, e.g. traveling by train.

The database can be searched by either author or publication title.
If the respective entry has previously added to the database, a search returns:
* paper title
* author
* small summary
* bibtex entry


*Note:* This application was created for only personal usage and its construction reflects that. If you enter
any problems in your setup, consult the logs.

## Installation

The project is packaged with [uv](https://docs.astral.sh/uv/). With uv installed, run:

```bash
uv sync --all-extras
```

This installs the runtime dependencies plus the dev tooling (pytest, ruff, mypy). The console script `pdbsearch` is registered automatically. See `specs/001-modernize-stack/quickstart.md` for the full developer setup.

## Interaction

Start the interactive CLI:

```bash
uv run pdbsearch
```

Drops into the top-level menu (search / add / update / quit). The non-interactive subcommands are also available — `pdbsearch search`, `pdbsearch add`, `pdbsearch update`, `pdbsearch delete`, `pdbsearch import`, `pdbsearch migrate`. Run `pdbsearch --help` for the full list.

## Search

The following dialog is presented to you 
```
Welcome! Connecting to the database, one moment...
Connected to the database.
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
```
Press 1 to load the search dialog:
```
Search interface
Please choose a method:
1) Search by author
2) Search by paper title
```
### Search by title

Enter the title name and if a paper of that name exists in the database the relevant information will be presented to you.

```
Please enter the paper_information title:
```
If no paper is found, you will be informed of it.
Note that if several papers with that specific title are present in the database, you will be presented with the list of
the respective authors and asked to choose one author (group).
### Search by author

Enter the author's name. You are then presented with a list of papers that author has (co-)authored and asked
to select one.
The name should have the format ```${last name}, ${first name}```.
```
Please enter the author's name:
```
## Add an entry

The program takes you through the steps to add an entry to the database step by step. Note that you are asked
whether you want to provide a file to read the bib entry from or enter the data by hand.

```
Please enter the necessary information
Author(s), please provide a , separated list: ${list_of_author}
Paper title: Fancy new paper
Bibtex key: new key
Do you want to enter the bibtex entry via a separate file?
1) Yes
2) No
Your choice: 1
Enter filename: bibfile.bib 
summary of the paper_information: [...]
```

## Update an entry

The program walks you through all steps necessary to update a single entry.
The below interaction shows an example of an update dialog. 
```
Which information do you want to update?
1) paper 
2) bib
3) authors
4) abort
Your choice: 1
Which information do you want to update?
1) title
2) contents
3) abort
Your choice: 1 
Which entry do you want to update?
Please enter the respective id: ${paper_id}  
```
In order to change an entry you have to know its id in the database. 
You may use the [search](README.md#search) functionality to access this id.
```
Enter the new information: the new title
```
You are asked to review and verify the information you have requested to change before 
any change is applied:
```
Please verify: You wish to change the 'title' of 'paper_id' to 'the new title'.
Proceed?
1) (Y)es
2) (N)o
Your choice: 1
```

# Config

The database connection can come from any of four sources, in priority order (highest first):

1. **CLI flags** — `--database-url`, `--log-level`, etc.
2. **Environment variables** — `PDBSEARCH_DATABASE_URL`, optionally `PDBSEARCH_LOG_LEVEL`, `PDBSEARCH_LOG_FILE`.
3. **`.env` file** at the project root (same keys as the env vars).
4. **Fernet-encrypted INI** for sensitive deployments — pass `--config <path>` and `--key <path>`. The INI is the same shape as before:

   ```ini
   [postgresql]
   dbname=your_dbname
   user=your_dbuser
   password=your_dbuser_password
   ```

   The key file holds a single Fernet key, generated once and kept in a relatively safe location.

See `specs/001-modernize-stack/quickstart.md` for full setup, including how to seed the database with `pdbsearch migrate`.

