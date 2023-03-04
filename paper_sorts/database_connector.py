#!/usr/bin/env python3

from typing import List
import logging

from psycopg import sql, connect, DatabaseError
from psycopg2.extensions import cursor, connection


def cast(user_input: str) -> int:
    """Check if user input is valid and cast to Integer if so"""
    try:
        cast_to_int = int(user_input)
    except ValueError:
        return -1
    return cast_to_int


class DatabaseConnector:
    def __init__(
        self,
        config_parameters: dict,
        logging_level: int =logging.DEBUG,
        logger_name: str ="database_logger",
        log_file: str ="db_connector.log",
    ):
        self.config_parameters = config_parameters
        # mostly taken from https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
        # create logger
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging_level)

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
        logger.addHandler(ch)

        self.logger = logger

    def get_connection_and_cursor(self) -> [connection, cursor]:
        con = connect(**self.config_parameters)
        cur = con.cursor()
        return con, cur

    def add_data_from_dict(self, data_dict: dict):
        self.create_tables()
        sql_instruction = (
            "INSERT INTO {} (title, contents, bibtex_id) VALUES (%s, %s, %s)"
        )
        con = None
        try:
            con, cur = self.get_connection_and_cursor()
            for title, values in data_dict.items():
                bibtex_key = values["bibtex_id"]
                if not bibtex_key:
                    self.logger.error("bibtex entry %s not found", bibtex_key)
                    continue
                authors = values["author"]
                content = values["contents"]
                bibtex = values["bibtex"]
                cur.execute(
                    sql.SQL("select exists(select * from papers where bibtex_id=%s);"),
                    (bibtex_key,),
                )
                if cur.fetchone()[0]:
                    continue
                cur.execute(
                    sql.SQL("insert into bib values (%s, %s);"), (bibtex_key, bibtex)
                )
                cur.execute(
                    sql.SQL(sql_instruction).format(sql.Identifier("papers")),
                    [title, content, bibtex_key],
                )
                for author in authors:
                    self.__add_single_author(author, cur)
                con.commit()
        except DatabaseError as database_error:
            self.logger.exception(database_error)
            if con:
                con.rollback()

        finally:
            if con:
                con.close()

    def create_tables(self):
        con = None
        try:
            con, cur = self.get_connection_and_cursor()
            con.commit()
            cur.execute(
                sql.SQL(
                    "CREATE  TABLE IF NOT EXISTS authors_papers (id SERIAL PRIMARY KEY, author_id INT, "
                    "paper_id INT) ; "
                )
            )
            cur.execute(
                sql.SQL(
                    "CREATE  TABLE IF NOT EXISTS  authors_id (id SERIAL PRIMARY KEY, author TEXT) ; "
                )
            )
            # if our bibtex identifier is not unique, the entire db_connector is useless
            cur.execute(
                sql.SQL(
                    "CREATE  TABLE IF NOT EXISTS  bib (bibtex_id text primary key, bibtex text) ;"
                ).format(sql.Identifier("bib"))
            )
            con.commit()
            cur.execute(
                sql.SQL(
                    "CREATE  TABLE IF NOT EXISTS   papers (id SERIAL PRIMARY KEY, title TEXT, contents TEXT, "
                    "bibtex_id TEXT, "
                    "constraint fk_bibtex_id foreign key(bibtex_id) references bib(bibtex_id));"
                ).format(sql.Identifier("papers"))
            )
            con.commit()

        except DatabaseError as database_error:
            self.logger.exception(database_error)
            if con:
                con.rollback()

        finally:
            if con:
                con.close()

    @staticmethod
    def __add_single_author(author: str, cur: cursor):
        cur.execute(sql.SQL("select max(id) from papers;"))
        paper_id = cur.fetchone()[0]
        cur.execute(sql.SQL("select * from authors_id where author=%s;"), (author,))
        author_id = cur.fetchone()
        if author_id:
            cur.execute(
                sql.SQL(
                    "insert into authors_papers (author_id, paper_id) values (%s, %s);"
                ),
                (author_id[0], paper_id),
            )
        else:
            cur.execute(
                sql.SQL("insert into authors_id (author) values (%s);"), (author,)
            )
            cur.execute(sql.SQL("select max(id) from authors_id;"))
            author_id = cur.fetchone()[0]
            cur.execute(
                sql.SQL(
                    "insert into authors_papers (author_id, paper_id) values (%s, %s);"
                ),
                (author_id, paper_id),
            )

    def search_for_bibtex_entry_by_id(self, paper) -> List[str] | None :
        con = None
        try:
            if paper:
                con, curs = self.get_connection_and_cursor()
                curs.execute(
                    sql.SQL("select * from bib where bibtex_id=%s;"), (paper[3],)
                )
                bibtex_data = curs.fetchone()
                con.close()
                return bibtex_data
            raise KeyError("Bibtex entry was not found!")
        except DatabaseError as database_error:
            self.logger.exception(database_error)
            if con:
                con.rollback()
                con.close()
            return None


    def search_by_title(self, title) -> List[List[str]] | None:
        con = None
        try:
            con, curs = self.get_connection_and_cursor()
            curs.execute(
                sql.SQL(
                    "select  authors_id.author, papers.id, papers.title, papers.bibtex_id, papers.contents from papers INNER JOIN "
                    "authors_papers papers_authors on papers_authors.paper_id=papers.id "
                    "INNER JOIN authors_id on papers_authors.author_id = authors_id.id where papers.title=%s"
                ),
                (title,),
            )
            papers = curs.fetchall()
            if not papers:
                self.logger.error(
                    "Paper with title %s not found in table papers, abort!", title
                )
                con.close()
                raise KeyError("Paper not found!")
            down_papers = []
            if len(set(p[2] for p in papers)) > 1:
                down_papers = self.__iterate_through_papers(papers)
            else:
                authors = ""
                for paper in papers:
                    authors += f"{paper[0]} and "
                down_papers.append([authors[:-5]] + list(papers[0][1:]))
                con.close()
            return down_papers

        except DatabaseError as database_error:
            self.logger.exception(database_error)
            if con:
                con.rollback()
                con.close()
            return None

    def search_by_author(self, author: str) -> List[str] | None:
        """Search authors_papers table by author, then search papers table"""
        con = None
        try:
            con, curs = self.get_connection_and_cursor()

            curs.execute(
                sql.SQL(
                    "select authors_id.id, authors_id.author, paper_id, title, bibtex_id, contents from authors_id INNER JOIN authors_papers on "
                    "authors_papers.author_id=authors_id.id INNER JOIN papers on paper_id=papers.id where author=%s;"
                ),
                (author,),
            )
            results = curs.fetchall()
            if not results:
                self.logger.error("author not found")
                con.close()
                raise KeyError("Author not found!")
            con.close()
            return results
        except DatabaseError as database_error:
            self.logger.exception(database_error)
            if con:
                con.rollback()
                con.close()
            return None

    def search_for_bibtex_entry_by_paper_title(self, paper: List) -> List[str]:
        con = None
        try:
            con, curs = self.get_connection_and_cursor()
            curs.execute(
                sql.SQL(
                    "select authors_id.author, paper_id from authors_id INNER JOIN "
                    "authors_papers on authors_id.id = authors_papers.author_id where paper_id = %s"
                ),
                (paper[2],),
            )
            author_pretty = " and ".join(
                [author_name[0] for author_name in curs.fetchall()]
            )
            author_pretty = author_pretty[:-5]
            if con:
                con.close()
            return [author_pretty] + list(paper[2:])

        except DatabaseError as database_error:
            self.logger.exception(database_error)
            if con:
                con.rollback()
                con.close()
            return []

    @staticmethod
    def __iterate_through_papers(papers: List[List[str]]) -> List[List[str]]:
        id_bib = papers[0][3]
        down_papers = []
        authors = ""
        for i, paper in enumerate(papers):
            paper = list(paper)  # papers element are tuples
            if id_bib != paper[3]:
                down_papers.append([authors[:-5]] + list(papers[i - 1])[1:])
                id_bib = paper[3]
            else:
                authors += f"{paper[0]} and "
        return down_papers

    def add_entry_to_db(
        self,
        new_bibtex_entry: str,
        author_names: list,
        bibtex_ident: str,
        title: str,
        content: str,
    ) -> None:
        """
        Add new entry to db_connector table.

        :param new_bibtex_entry: complete bibtex entry a string
        :param author_names: name of the authors in a list, one element for each author
        :param bibtex_ident: bibtex identifier of the publication, must be unique
        :param title: title of the publication
        :param content: one sentence summarizing the content of the publication
        :return: -
        """
        con = None
        try:
            con, curs = self.get_connection_and_cursor()
            try:
                self.sanity_checks(bibtex_ident, curs)
            except ValueError as exc:
                con.close()
                raise ValueError(
                    "Could not add entry to db_connector. Check logs."
                ) from exc
            curs.execute(
                sql.SQL("insert into bib values (%s, %s);"),
                (bibtex_ident, new_bibtex_entry),
            )
            sql_instruction = (
                "INSERT INTO papers (title, contents, bibtex_id) VALUES (%s, %s, %s)"
            )
            curs.execute(sql.SQL(sql_instruction), [title, content, bibtex_ident])
            curs.execute(sql.SQL("select id from papers where title=%s"), (title,))
            paper_id = curs.fetchone()[0]
            for author in author_names:
                self.insert_single_author(author, curs, paper_id)
            con.commit()

        except DatabaseError as database_error:
            self.logger.error(database_error)
            if con:
                con.rollback()

        finally:
            if con:
                con.close()

    def sanity_checks(self, bibtex_ident, curs):
        curs.execute(sql.SQL("select relname from pg_class where relname = 'papers';"))
        if not curs.fetchone():
            self.logger.error("Table papers not found in db_connector, abort!")
            raise ValueError("Table papers not found!")

        curs.execute(
            sql.SQL(
                f"select exists(select * from papers where bibtex_id='{bibtex_ident}');"
            )
        )
        if curs.fetchone()[0]:
            self.logger.error("Entry %s already exists in table papers", bibtex_ident)
            raise ValueError("Entry already exists")

    def insert_single_author(self, author, curs, paper_id):
        curs.execute(sql.SQL("select * from authors_id where author=%s;"), (author,))
        author_id = curs.fetchone()
        if author_id:
            curs.execute(
                sql.SQL(
                    "insert into authors_papers (author_id, paper_id) values (%s, %s);"
                ),
                (author_id[0], paper_id),
            )
            self.logger.info("added author %s to table authors_papers", author)
        else:
            curs.execute(
                sql.SQL("insert into authors_id (author) values (%s);"), (author,)
            )
            self.logger.info("added author %s to table authors_id", author)
            curs.execute(sql.SQL("select id from authors_id where author=author;"))
            author_id = curs.fetchone()[0]
            curs.execute(
                sql.SQL(
                    "insert into authors_papers (author_id, paper_id) values (%s, %s);"
                ),
                (author_id, paper_id),
            )
            self.logger.info("added author %s to table authors_papers", author)
