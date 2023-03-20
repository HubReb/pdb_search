#!/usr/bin/env python3

from typing import List
import logging

from psycopg import DatabaseError

from paper_sorts.helpers import iterate_through_papers
from paper_sorts.psycopg_db import PsycopgDB


class DatabaseConnector:
    def __init__(
        self,
        config_parameters: dict,
        logging_level: int = logging.DEBUG,
        logger_name: str = "database_logger",
        log_file: str = "db_connector.log",
    ):
        self.config_parameters = config_parameters
        # mostly taken from https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
        # create logger
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging_level)

        # create console handler and set level to debug
        ch = logging.FileHandler(filename=log_file)
        ch.setLevel(logging_level)

        # create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        self.logger.addHandler(ch)
        self.databaseHandler = PsycopgDB(self.config_parameters)

    def add_data_from_dict(self, data_dict: dict):
        self.create_tables()
        try:
            for title, values in data_dict.items():
                bibtex_key = values["bibtex_id"]
                if not bibtex_key:
                    self.logger.error("bibtex entry %s not found", bibtex_key)
                    continue
                authors = values["author"]
                content = values["contents"]
                bibtex = values["bibtex"]
                self.databaseHandler.store_in_db(
                    "select exists(select * from papers where bibtex_id=%s);",
                    (bibtex_key,),
                )
                database_entries = self.databaseHandler.fetch_from_db(
                    "select exists(select * from papers where bibtex_id=%s);",
                    (bibtex_key,),
                )
                if database_entries[0]:
                    self.logger.info(
                        "bibtex key %s already in database - skipping", bibtex_key
                    )
                    continue
                self.databaseHandler.store_in_db(
                    "insert into bib values (%s, %s);", (bibtex_key, bibtex)
                )
                self.databaseHandler.store_in_db(
                    "INSERT INTO papers (title, contents, bibtex_id) VALUES (%s, %s, %s)",
                    (title, content, bibtex_key),
                )
                for author in authors:
                    self.__add_single_author(author)
                    self.logger.info(
                        f"added author {author} and paper {title} to database"
                    )

        except DatabaseError as database_error:
            self.logger.exception(database_error)

    def create_tables(self):
        try:
            self.databaseHandler.store_in_db(
                "CREATE  TABLE IF NOT EXISTS authors_papers (id SERIAL PRIMARY KEY, author_id INT, "
                "paper_id INT) ; "
            )
            self.databaseHandler.store_in_db(
                "CREATE  TABLE IF NOT EXISTS  authors_id (id SERIAL PRIMARY KEY, author TEXT) ; "
            )
            # if our bibtex identifier is not unique, the entire db_connector is useless
            self.databaseHandler.store_in_db(
                "CREATE  TABLE IF NOT EXISTS  bib (bibtex_id text primary key, bibtex text) ;"
            )
            self.databaseHandler.store_in_db(
                "CREATE  TABLE IF NOT EXISTS   papers (id SERIAL PRIMARY KEY, title TEXT, contents TEXT, "
                "bibtex_id TEXT, "
                "constraint fk_bibtex_id foreign key(bibtex_id) references bib(bibtex_id));"
            )
            self.logger.info("created all tables")

        except DatabaseError as exc:
            self.logger.exception(exc)

    def __add_single_author(self, author: str):
        paper_id = self.databaseHandler.fetch_from_db("select max(id) from papers;")[0]
        author_id = self.databaseHandler.fetch_from_db(
            "select * from authors_id where author=%s;", (author,)
        )
        if author_id:
            self.databaseHandler.store_in_db(
                "insert into authors_papers (author_id, paper_id) values (%s, %s);",
                (author_id[0], paper_id),
            )
        else:
            self.databaseHandler.store_in_db(
                "insert into authors_id (author) values (%s);", (author,)
            )
            author_id = self.databaseHandler.fetch_from_db(
                "select max(id) from authors_id;"
            )[0]
            self.databaseHandler.store_in_db(
                "insert into authors_papers (author_id, paper_id) values (%s, %s);",
                (author_id, paper_id),
            )

    def search_for_bibtex_entry_by_id(self, paper: List) -> List[str] | None:
        if paper:
            return self.databaseHandler.fetch_from_db(
                "select * from bib where bibtex_id=%s;", (paper[3],)
            )
        raise KeyError("Bibtex entry was not found!")

    def search_by_title(self, title: str) -> List[List[str]] | None:
        papers = self.databaseHandler.fetch_from_db(
            "select  authors_id.author, papers.id, papers.title, papers.bibtex_id, papers.contents from papers INNER JOIN "
            "authors_papers papers_authors on papers_authors.paper_id=papers.id "
            "INNER JOIN authors_id on papers_authors.author_id = authors_id.id where papers.title=%s",
            (title,),
        )
        if not papers:
            self.logger.info(
                "Paper with title %s not found in table papers, abort!", title
            )
            raise KeyError("Paper not found!")
        down_papers = []
        if len(set(p[2] for p in papers)) > 1:
            down_papers = iterate_through_papers(papers)
        else:
            authors = ""
            for paper in papers:
                authors += f"{paper[0]} and "
            down_papers.append([authors[:-5]] + list(papers[0][1:]))
        return down_papers

    def search_by_author(self, author: str) -> List[str] | None:
        """Search authors_papers table by author, then search papers table"""
        results = self.databaseHandler.fetch_from_db(
            "select authors_id.id, authors_id.author, paper_id, title, bibtex_id, contents from authors_id INNER JOIN authors_papers on "
            "authors_papers.author_id=authors_id.id INNER JOIN papers on paper_id=papers.id where author=%s;",
            (author,),
        )
        if not results:
            self.logger.info("author not found")
            raise KeyError("Author not found!")
        return results

    def search_for_bibtex_entry_by_paper_title(self, paper: List) -> List[str]:
        author_names = self.databaseHandler.fetch_from_db(
            "select authors_id.author, paper_id from authors_id INNER JOIN "
            "authors_papers on authors_id.id = authors_papers.author_id where paper_id = %s",
            (paper[2],),
        )
        if author_names:
            author_pretty = " and ".join(
                [author_name[0] for author_name in author_names]
            )
            author_pretty = author_pretty[:-5]
            return [author_pretty] + list(paper[2:])
        self.logger.info("entry not found in database")
        return []

    def add_entry_to_db(
        self,
        new_bibtex_entry: str,
        author_names: list,
        bibtex_ident: str,
        title: str,
        content: str,
    ) -> bool:
        """
        Add new entry to db_connector table.

        :param new_bibtex_entry: complete bibtex entry as a string
        :param author_names: name of the authors in a list, one element for each author
        :param bibtex_ident: bibtex identifier of the publication, must be unique
        :param title: title of the publication
        :param content: one sentence summarizing the content of the publication
        :return: -
        """
        try:
            try:
                self.sanity_checks(bibtex_ident)
            except ValueError as exc:
                raise ValueError(
                    "Could not add entry to db_connector. Check logs."
                ) from exc
            self.databaseHandler.store_in_db(
                "insert into bib values (%s, %s);",
                (bibtex_ident, new_bibtex_entry),
            )
            sql_instruction = (
                "INSERT INTO papers (title, contents, bibtex_id) VALUES (%s, %s, %s)"
            )
            self.databaseHandler.store_in_db(
                sql_instruction, (title, content, bibtex_ident)
            )
            paper_id = self.databaseHandler.fetch_from_db(
                "select id from papers where title=%s", (title,)
            )[0][0]
            for author in author_names:
                self.insert_single_author(author, paper_id)
            return True

        except DatabaseError as database_error:
            self.logger.error(database_error)
            return False

    def delete_entry_from_database(
        self,
        new_bibtex_entry: str,
        author_names: list,
        bibtex_ident: str,
        title: str,
        content: str,
    ) -> bool:
        try:
            paper_id = self.databaseHandler.fetch_from_db(
                "select id from papers where title=%s", (title,)
            )[0][0]
        except IndexError:
            self.logger.error("paper %s does not exist in database", title)
            raise ValueError(f"Paper {title} does not exist in database Check logs.")

        for author in author_names:
            author_id = self.databaseHandler.fetch_from_db(
                "select * from authors_id where author=%s;", (author,)
            )[0]
            self.databaseHandler.delete_from_db(
                "delete from authors_papers where (author_id=%s and paper_id=%s);",
                (author_id[0], paper_id),
            )
            self.logger.info(
                f"marking author {author_id[1]} and paper {title} for deletion"
            )
        self.databaseHandler.delete_from_db(
            "delete from papers where (title=%s and contents=%s and bibtex_id=%s)",
            (title, content, bibtex_ident),
        )
        self.logger.info("marking bibtex id %s for deletion in papers", bibtex_ident)
        self.databaseHandler.delete_from_db(
            "delete from bib where (bibtex_id=%s and bibtex=%s);",
            (bibtex_ident, new_bibtex_entry),
        )
        self.logger.info("marking bibtex id %s for deletion", bibtex_ident)
        self.logger.info("successfully deleted data")
        return True

    def sanity_checks(self, bibtex_ident):
        if not self.databaseHandler.fetch_from_db(
            "select relname from pg_class where relname = 'papers';"
        ):
            self.logger.error("Table papers not found in db_connector, abort!")
            raise ValueError("Table papers not found!")

        if self.databaseHandler.fetch_from_db(
            f"select exists(select * from papers where bibtex_id='{bibtex_ident}');"
        )[0][0]:
            self.logger.error("Entry %s already exists in table papers", bibtex_ident)
            raise ValueError("Entry already exists")

    def insert_single_author(self, author, paper_id):
        author_id = self.databaseHandler.fetch_from_db(
            "select * from authors_id where author=%s;", (author,)
        )[0]
        if author_id:
            self.databaseHandler.store_in_db(
                "insert into authors_papers (author_id, paper_id) values (%s, %s);",
                (author_id[0], paper_id),
            )
            self.logger.info("added author %s to table authors_papers", author)
        else:
            self.databaseHandler.store_in_db(
                "insert into authors_id (author) values (%s);", (author,)
            )
            self.logger.info("added author %s to table authors_id", author)
            author_id = self.databaseHandler.fetch_from_db(
                "select id from authors_id where author=author;"
            )[0]
            self.databaseHandler.store_in_db(
                "insert into authors_papers (author_id, paper_id) values (%s, %s);",
                (author_id, paper_id),
            )
            self.logger.info("added author %s to table authors_papers", author)
