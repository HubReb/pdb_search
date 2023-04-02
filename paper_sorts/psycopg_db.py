#!/usr/bin/env python3

from typing import Tuple, List
import logging

from psycopg import sql, connect, DatabaseError
from psycopg2.extensions import cursor, connection


class PsycopgDB:
    def __init__(
        self,
        config_parameters: dict,
        logging_level: int = logging.DEBUG,
        logger_name: str = "psycopg_logger",
        log_file: str = "psycopg_logger.log",
    ):
        """
        Class to handle interaction with the postgresql database via the python package psycopg2.

        :param config_parameters: dictionary containing the configuration that defines the database interaction
        :param logging_level: specifies the level of the logger
        :param logger_name: name of the logger to use
        :param log_file: name of the file the logging information is written to
        """
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

    def create_connection_and_cursor(self) -> [connection, cursor]:
        """
        Initialize connection to postgseql database and create cursor.
        :return: connection to database and the cursor
        """
        con = connect(**self.config_parameters)
        cur = con.cursor()
        return con, cur

    def store_in_db(self, query: str, format_arguments: Tuple[str, ...] = None) -> None:
        """
        Add a new entry in the database.

        :param query: query to perform on the database
        :param format_arguments: arguments to augment the query
        :raises DatabaseError: if interaction with the database failed due to an incorrect query
        """
        con = None
        try:
            con, cur = self.create_connection_and_cursor()
            if not format_arguments:
                cur.execute(sql.SQL(query))
            else:
                cur.execute(sql.SQL(query), format_arguments)
            con.commit()

        except DatabaseError as database_error:
            self.logger.exception(database_error)
            if con:
                con.rollback()
                con.close()
            raise DatabaseError from database_error

        finally:
            if con:
                con.close()

    def fetch_from_db(
        self, query: str, format_arguments: Tuple[str, ...] = None
    ) -> List:
        """
        Search the database for information.

        :param query: query to the database
        :param format_arguments: arguments for the query
        :return: results extracted from the database
        :raises DatabaseError: if interaction with the database failed due to an incorrect query
        """
        con = None
        try:
            con, cur = self.create_connection_and_cursor()
            if not format_arguments:
                cur.execute(sql.SQL(query))
            else:
                cur.execute(sql.SQL(query), format_arguments)

            con.close()
            return cur.fetchall()

        except DatabaseError as database_error:
            self.logger.exception(database_error)
            if con:
                con.close()

    def delete_from_db(
        self, query: str, format_arguments: Tuple[str, ...] = None
    ) -> None:
        """
        Delete information from the database.

        :param query: specification what to delete
        :param format_arguments: string arguments to augment the query
        :raises DatabaseError: if interaction with the database failed due to an incorrect query
        """
        con = None
        try:
            con, cur = self.create_connection_and_cursor()
            if not format_arguments:
                cur.execute(sql.SQL(query))
            else:
                cur.execute(sql.SQL(query), format_arguments)
            con.commit()
            con.close()

        except DatabaseError as database_error:
            self.logger.exception(database_error)
            if con:
                con.rollback()
                con.close()

    def update_db_entry(self, query: str, identifier: str, update_value: str) -> None:
        """
        Update information in the database.

        :param query: specification what to change
        :param identifier: identifier of the table entry to change information in
        :param update_value: new value to set in the table
        """
        con = None
        try:
            con, cur = self.create_connection_and_cursor()
            cur.execute(sql.SQL(query),  (update_value, identifier))
            con.commit()
            con.close()
        except DatabaseError as database_error:
            self.logger.exception(database_error)
            if con:
                con.rollback()
                con.close()
