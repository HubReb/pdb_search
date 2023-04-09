#!/usr/bin/env python3

"""
This contains the :class: `DatabaseConnector` whicn provides the interface for the interaction
with the database and all actions such as adding, deleting, updating and searching.
"""

from typing import List, Optional
import logging

from paper_sorts.helpers import iterate_through_papers, create_logger
from paper_sorts.psycopg_db import PsycopgDB


class DatabaseConnector:
    """
    This class presents the interaction with the database and offers safe methods to change it.

    The higher-level interaction with the database is given via this class. It provides safe methods
    for adding a single or several entries to the database, for searching the database for a
    publication via its title or its author(s), for updating specific information, such as the
    summary of a publication or its bibsonomy, and for deleting a publication from the database,
    while preventing unnecessary duplication in the database.
    The actual instruction are handled by the attribute `database_handler` of the
    :class: `paper_sort.psycopg_db.py, which takes care of the actual interaction with database
    via an external dependency.
    """

    def __init__(
        self,
        config_parameters: dict,
        logging_level: int = logging.DEBUG,
        logger_name: str = "database_logger",
        log_file: str = "db_connector.log",
    ):
        """
        Initialize DatabaseConnector object to interact with the database.

        :param config_parameters: contains the configuration that defines the database interaction
        :type config_parameters: dict
        :param logging_level: specifies the level of the logger, defaults to logging.DEBUG
        :type logging_level: str
        :param logger_name: name of the logger to use, defaults to `database_logger`
        :type logger_name: str
        :param log_file: name of the file the logs are written into, defaults to `db_connector.log`
        :type log_file: str
        """
        self.config_parameters = config_parameters
        self.logger = create_logger(log_file, logger_name, logging_level)
        self.database_handler = PsycopgDB(self.config_parameters)

    def add_data_from_dict(self, data_dict: dict) -> None:
        """
        Add entries in the database as specified in data_dict.

        :param data_dict: dictionary of paper entries of the forms:
            title -
                    bibtex_id - unique identifier to cite paper by
                    author - list of the authors of the paper
                    contents - summary of the paper's contents
        :type data_dict: dict
        :raises RunTimeError: if error occurred in  performing the database actions - check logs
        """
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
                self.database_handler.store_in_db(
                    "select exists(select * from papers where bibtex_id=%s);",
                    (bibtex_key,),
                )
                database_entries = self.database_handler.fetch_from_db(
                    "select exists(select * from papers where bibtex_id=%s);",
                    (bibtex_key,),
                )
                if database_entries[0]:
                    self.logger.info(
                        "bibtex key %s already in database - skipping", bibtex_key
                    )
                    continue
                self.database_handler.store_in_db(
                    "insert into bib values (%s, %s);", (bibtex_key, bibtex)
                )
                self.database_handler.store_in_db(
                    "INSERT INTO papers (title, contents, bibtex_id) VALUES (%s, %s, %s)",
                    (title, content, bibtex_key),
                )
                for author in authors:
                    self.__add_single_author(author)
                    self.logger.info(
                        "added author %s and paper_information %s to database",
                        author,
                        title
                    )

        except ValueError as value_error:
            self.logger.exception(value_error)
            raise RuntimeError("Failed to create tables and populate them - ending application!")

    def create_tables(self) -> None:
        """
        Create the database tables used in this application.

        :raises RunTimeError: if the tables could not be created
        """
        try:
            self.database_handler.store_in_db(
                "CREATE  TABLE IF NOT EXISTS authors_papers (id SERIAL PRIMARY KEY, author_id INT, "
                "paper_id INT) ; "
            )
            self.database_handler.store_in_db(
                "CREATE  TABLE IF NOT EXISTS  authors_id (id SERIAL PRIMARY KEY, author TEXT) ; "
            )
            # if our bibtex author_identification is not unique, the entire db_connector is useless
            self.database_handler.store_in_db(
                "CREATE  TABLE IF NOT EXISTS  bib "
                "(bibtex_id text primary key, bibtex text unique (bibtex));"
            )
            self.database_handler.store_in_db(
                "CREATE  TABLE IF NOT EXISTS   papers "
                "(id SERIAL PRIMARY KEY, title TEXT, contents TEXT, "
                "bibtex_id TEXT, "
                "constraint fk_bibtex_id foreign key(bibtex_id) references bib(bibtex_id));"
            )
            self.logger.info("created all tables")

        except ValueError as exc:
            self.logger.exception(exc)
            raise RuntimeError("Failed to create tables - ending application!")

    def __add_single_author(self, author: str) -> None:
        """
        Add a author to authors_papers and add the author to authors_id if he is not already listed.

        :param author: name of the author
        :type author: str
        """
        paper_id = self.database_handler.fetch_from_db("select max(id) from papers;")[0]
        author_id = self.database_handler.fetch_from_db(
            "select * from authors_id where author=%s;", (author,)
        )
        if author_id:
            self.database_handler.store_in_db(
                "insert into authors_papers (author_id, paper_id) values (%s, %s);",
                (author_id[0], paper_id),
            )
        else:
            self.database_handler.store_in_db(
                "insert into authors_id (author) values (%s);", (author,)
            )
            author_id = self.database_handler.fetch_from_db(
                "select max(id) from authors_id;"
            )[0]
            self.database_handler.store_in_db(
                "insert into authors_papers (author_id, paper_id) values (%s, %s);",
                (author_id, paper_id),
            )

    def search_for_bibtex_entry_by_id(self, paper: List[str]) -> List[str]:
        """
        Search the bib table for the entry specified by  bibtex_id that must be present in paper.

        :param paper: list of meta information for one paper
        :type paper: List[str]
        :return: bib information for the paper
        """
        if paper:
            return self.database_handler.fetch_from_db(
                "select * from bib where bibtex_id=%s;", (paper[3],)
            )
        raise KeyError("Bibtex entry was not found!")

    def search_by_title(self, title: str) -> List[Optional[List[str]]]:
        """
        Search the database for the entire paper information record via the title of the paper.

        :param title: title of the paper to search for
        :type title: str
        :return: all paper's meta information whose title matches the parameter title
        :rtype: List[Optional[List[str]]]
        :raises KeyError: if no paper with the specified title could be found
        """
        papers = self.database_handler.fetch_from_db(
            "select  authors_id.author, papers.id, papers.title, papers.bibtex_id, papers.contents from "
            "papers INNER JOIN "
            "authors_papers on authors_papers.paper_id=papers.id "
            "INNER JOIN authors_id on authors_papers.author_id = authors_id.id where papers.title=%s",
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

    def search_by_author(self, author: str) -> List[str]:
        """Search authors_papers table by author, then search papers table.

        :param author: name of the author to search for
        :type author: str
        :return: information on all paper's the author has worked on that are in the database
        :rtype: List[str]
        """
        results = self.database_handler.fetch_from_db(
            "select authors_id.id, authors_id.author, paper_id, title, bibtex_id, contents from "
            "authors_id INNER JOIN authors_papers on "
            "authors_papers.author_id=authors_id.id INNER JOIN papers on paper_id=papers.id where author=%s;",
            (author,),
        )
        if not results:
            self.logger.info("author not found")
            raise KeyError("Author not found!")
        return results

    def search_for_entry_by_specified_paper_information(
        self, paper_information: List
    ) -> List[Optional[str]]:
        """
        Search the database for the authors of the paper who are specified in paper_information.

        :param paper_information: information of the paper taken from the database
        :type paper_information: List
        :return: names of the author(s) and the information provided in paper_information
        :rtype: List[Optional[str]]
        """
        author_names = self.database_handler.fetch_from_db(
            "select authors_id.author, paper_id from authors_id INNER JOIN "
            "authors_papers on authors_id.id = authors_papers.author_id where paper_id = %s",
            (paper_information[2],),
        )
        if author_names:
            author_pretty = " and ".join(
                [author_name[0] for author_name in author_names]
            )
            author_pretty = author_pretty[:-5]
            return [author_pretty] + list(paper_information[2:])
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
        Add new entry to db_connector database.

        Add a new paper to the database, add a new author to paper entry in the authors_papers
        table, add the bibtex information to the bib table  and - if the author is not yet present
        in the database - add a new entry for the author in the authors_id table.

        :param new_bibtex_entry: complete bibtex entry
        :type new_bibtex_entry: str
        :param author_names: name of the authors in a list, one element for each author
        :type author_names: List[str]
        :param bibtex_ident: bibtex author_identification of the publication, must be unique
        :type bibtex_ident: str
        :param title: title of the publication
        :type title: str
        :param content: one sentence summarizing the content of the publication
        :type content: str
        :return: indicates if the entry was added and all subsequent actions were done successfully
        :rtype: bool
        :raises ValueError: if the sanity checks for the bibtex_ident failed
        :raises ValueError: if the handling of the database failed, ends application to prevent further damage
        """
        try:
            try:
                self.sanity_checks(bibtex_ident)
            except ValueError as exc:
                raise ValueError(
                    "Could not add entry to db_connector. Check logs."
                ) from exc
            self.database_handler.store_in_db(
                "insert into bib values (%s, %s);",
                (bibtex_ident, new_bibtex_entry),
            )
            sql_instruction = (
                "INSERT INTO papers (title, contents, bibtex_id) VALUES (%s, %s, %s)"
            )
            self.database_handler.store_in_db(
                sql_instruction, (title, content, bibtex_ident)
            )
            paper_id = self.database_handler.fetch_from_db(
                "select id from papers where title=%s", (title,)
            )[0][0]
            for author in author_names:
                self.__insert_single_author(author, paper_id)
            return True

        except ValueError as value_error:
            self.logger.error(value_error)
            raise ValueError("Errors occurred in handling of the database - could not add author. End application!")

    def delete_paper_entry_from_database(
        self,
        bibtex_entry: str,
        author_names: list,
        bibtex_ident: str,
        title: str,
        content: str,
    ) -> bool:
        """
        Delete a paper_information and all associated information from the database.

        :param bibtex_entry: bibtex entry to be deleted
        :type bibtex_entry: str
        :param author_names: names of the authors of the paper_information to be deleted
        :type author_names: str
        :param bibtex_ident: author_identification of the bibtex entry
        :type bibtex_ident: str
        :param title: title of the paper_information to be removed from the database
        :type title: str
        :param content: summary of the paper_information
        :type content: str
        :return: Boolean indicating whether deletion of the paper_information was successful
        :rtype: bool
        :raises ValueError: if the paper_information's title was not found in the database
        """
        try:
            paper_id = self.database_handler.fetch_from_db(
                "select id from papers where title=%s", (title,)
            )[0][0]
        except IndexError as exc:
            self.logger.error("paper_information %s does not exist in database", title)
            raise ValueError(
                f"Paper {title} does not exist in database Check logs."
            ) from exc

        for author in author_names:
            author_id = self.database_handler.fetch_from_db(
                "select * from authors_id where author=%s;", (author,)
            )
            if author_id:
                author_id = author_id[0]
                self.database_handler.delete_from_db(
                    "delete from authors_papers where (author_id=%s and paper_id=%s);",
                    (author_id[0], paper_id),
                )
                self.logger.info(
                    "marking author '%s' and paper_information '%s' for deletion",
                    author_id[1],
                    title
                )
                authors = self.database_handler.fetch_from_db(
                    "select * from authors_papers where author_id=%s", (author_id[0],)
                )
                if not authors:
                    self.database_handler.delete_from_db(
                        "delete from authors_id where id=%s;",
                        (author_id[0],),
                    )
                    self.logger.info(
                        "marking author '%s' for deletion in authors_id",
                        author_id[1]
                    )
        self.database_handler.delete_from_db(
            "delete from papers where (title=%s and contents=%s and bibtex_id=%s)",
            (title, content, bibtex_ident),
        )
        self.logger.info("marking bibtex id %s for deletion in papers", bibtex_ident)
        self.database_handler.delete_from_db(
            "delete from bib where (bibtex_id=%s and bibtex=%s);",
            (bibtex_ident, bibtex_entry),
        )
        self.logger.info("marking bibtex id %s for deletion", bibtex_ident)
        self.logger.info("successfully deleted data")
        return True

    def sanity_checks(self, bibtex_ident: str) -> None:
        """
        Check if paper_information table exists and if the bibtex_id already exists

        :param bibtex_ident: unique author_identification of the bibtex entry
        :type bibtex_ident: str
        :raises ValueError: the papers table does not exist in the database
        :raises ValueError: bibtex_identifier already exists in the database
        """
        if not self.database_handler.fetch_from_db(
            "select relname from pg_class where relname = 'papers';"
        ):
            self.logger.error("Table papers not found in db_connector, abort!")
            raise ValueError("Table papers not found!")

        if self.database_handler.fetch_from_db(
            f"select exists(select * from papers where bibtex_id='{bibtex_ident}');"
        )[0][0]:
            self.logger.error("Entry %s already exists in table papers", bibtex_ident)
            raise ValueError("Entry already exists")

    def __insert_single_author(self, author: str, paper_id: str) -> None:
        """
        Insert an author with the paper_information he's (co-) written into database.

        Create an entry for the author in the authors_id table, if the author does not
        have an entry yet, and insert an author to paper relation in the authors_papers
        table.

        :param author: name of the author
        :type author: str
        :param paper_id: unique author_identification of the paper_information
        :type paper_id: str
        """
        author_id = self.database_handler.fetch_from_db(
            "select * from authors_id where author=%s;", (author,)
        )
        if author_id:
            author_id = author_id[0]
            self.database_handler.store_in_db(
                "insert into authors_papers (author_id, paper_id) values (%s, %s);",
                (author_id[0], paper_id),
            )
            self.logger.info("added author %s to table authors_papers", author)
        else:
            self.database_handler.store_in_db(
                "insert into authors_id (author) values (%s);", (author,)
            )
            self.logger.info("added author %s to table authors_id", author)
            author_id = self.database_handler.fetch_from_db(
                "select id from authors_id where author=%s;", (author,)
            )[0][0]
            self.database_handler.store_in_db(
                "insert into authors_papers (author_id, paper_id) values (%s, %s);",
                (author_id, paper_id),
            )
            self.logger.info("added author %s to table authors_papers", author)

    def update_entry(
        self, update_column: str, update_value: str, table: str, identifier: str
    ) -> None:
        """
        Update an already existent entry in the database in the table specified by table parameter.

        :param update_column: column of the table to be updated
        :type update_column: str
        :param update_value: new value to set the entry in update_column in the specified table to
        :type update_value: str
        :param table: table that will  be updated
        :type table: str
        :param identifier: unique key of the row to update - may be paper_id, author_id or bibtex_id
        :type identifier: str
        :raises ValueError: if the update_column is not found in table
        :raises ValueError: if the table authors_papers is attempted to be accessed
        """
        if "_id" in update_column:
            raise ValueError("IDs are unique and must not be changed!")
        match table:
            case "papers":
                match update_column:
                    case "contents":
                        query = "update papers set contents=%s where id=%s;"
                    case "title":
                        query = "update papers set title=%s where id=%s;"
                    case _:
                        raise ValueError(
                            f"Column {update_column} is not present in table papers"
                        )
                self.database_handler.update_db_entry(query, identifier, update_value)
                self.logger.info(
                    "updated column %s wth %s", update_column, update_value
                )
            case "authors_papers":
                self.logger.error("Tried to access table authors_papers!")
                raise ValueError(
                    f"Table authors_papers has no column {update_column} that is changeable!"
                )
            case "authors_id":
                match update_column:
                    case "author":
                        self.__update_author(identifier, update_value)
                    case _:
                        raise ValueError(
                            f"Column {update_column} is not present in table authors_id"
                        )
            case "bib":
                match update_column:
                    case "bibtex":
                        self.__update_bibtex_information(identifier, update_value)
                    case _:
                        raise ValueError(
                            f"Column {update_column} is not present in table bibtex"
                        )

    def __update_bibtex_information(
        self, identifier: str, new_bibtex_information: str
    ) -> None:
        """
        Update the bibtex entry in the bib table.

        :param identifier: unique bibtex_id
        :type identifier: str
        :param new_bibtex_information: new bibtex information (summary, author, publication,...)
        :type new_bibtex_information: str
        :raises ValueError: if the bibtex information provided by new_bibtex_information already
        """
        bibtex_p = self.database_handler.fetch_from_db(
            "select bibtex_id from bib where bibtex=%s;", (new_bibtex_information,)
        )
        if bibtex_p:
            # we already have this entry then - hurts unique constraint
            raise ValueError("bibtex is unique - value already exists!")

        self.database_handler.update_db_entry(
            "update bib set bibtex=%s where bibtex_id=%s",
            identifier,
            new_bibtex_information,
        )
        self.logger.info(
            "updated  bibtex entry for bibtex_id %s to %s",
            identifier, new_bibtex_information
        )

    def __update_author(self, author_identification: str, new_author_name: str) -> None:
        """
        Update the name of an author in authors_id and changes authors_papers if required.

        This updates the name of the author by first checking whether the new_author_name argument
        refers to an author already present in the database and, if so, simply change the author_id
        in authors_papers and check the database for duplicate entries if the author was already
        listed as an author for the paper(s) of the old author. If not, a new entry for the new
        author in authors_id is created, then the author_id in authors_papers is changed
        accordingly.
        If the old author has no other publication in the database, the author is deleted from it.

        :param author_identification: authors_id to identify the author whose name is to be changed
        :type author_identification: str
        :param new_author_name: new name of the author specified by identifier
        :type new_author_name: str
        """
        # check if new author already exists
        author_id = self.database_handler.fetch_from_db(
            "select id from authors_id where author=%s;", (new_author_name,)
        )
        old_author_id = self.database_handler.fetch_from_db(
            "select id from authors_id where author=%s;", (author_identification,)
        )[0][0]
        if author_id:
            self.logger.info("found author %s in table authors_id", new_author_name)
            author_id = author_id[0]
            # new author already exists, we need to update several tables to ensure data validity
            self.database_handler.update_db_entry(
                "update authors_papers set author_id=%s where author_id=%s",
                author_id[0],
                old_author_id,
            )
            self.logger.info(
                "updated author_id %s to  %s in table authors_id",
                old_author_id,
                author_id[0]
            )
            # delete possible duplicate we may have after update
            self.database_handler.delete_from_db(
                "delete from authors_papers a "
                "using authors_papers b "
                "where b.author_id = a.author_id and b.id = a.id and a.id != b.id"
                "; "
            )
            self.logger.info("deleted possible duplicates in authors_papers")
        else:
            self.logger.info(
                "did not find author %s in table authors_id", new_author_name
            )
            # if author doesn't exist, first create him
            self.database_handler.store_in_db(
                "insert into authors_id (author) values (%s);", (new_author_name,)
            )
            self.logger.info("created author %s in table authors_id", new_author_name)
            author_id = self.database_handler.fetch_from_db(
                "select max(id) from authors_id;"
            )[0]
            self.database_handler.update_db_entry(
                "update authors_papers set author_id=%s where author_id=%s",
                old_author_id,
                author_id[0],
            )
            self.logger.info(
                "updated author_id %s to  %s in table authors_id",
                old_author_id,
                author_id[0]
            )
        self.database_handler.delete_from_db(
            "delete from authors_id where id=%s", (old_author_id,)
        )
        self.logger.info("deleted author_id %s from table authors_id", old_author_id)
        authors = self.database_handler.fetch_from_db(
            "select * from authors_papers where author_id=%s", (author_id[0],)
        )
        if not authors:
            self.database_handler.delete_from_db(
                "delete from authors_id where id=%s;",
                (author_id[0],),
            )
            self.logger.info(
                "marking author '%s' for deletion in authors_id", author_id[0]
            )
