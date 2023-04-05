#!/usr/bin/env python3

"""
This contains the class PsycopgDB, which enables the interaction with postgreSQL database.

The entire interaction with the postgreSQL database occurs in this module. All calls to the external
package psycopg2 are in this module. If either the database is changed from postgreSQL or the
package for interacting with the database is changed from psycopg2 to something else, only this
module will have to be changed.
"""

from typing import Tuple, List
import logging

from psycopg2 import sql, connect, DatabaseError
from psycopg2.extensions import cursor, connection

from paper_sorts.helpers import create_logger


class PsycopgDB:
    """
    Class to handle interaction with the postgresql database via the python package psycopg2.

    This class functions as an additional layer between the :class: `paper_sorts.DatabaseConnector`
    and the database and thus avoids sprinkling calls to psycopg2 throughout the entire code.
    If the psycopg2 dependency is ever changed or the APIs changes - such as from psycopg to
    psycopg2 in several cases -, all code to be adapted is located in this class.
    """

    def __init__(
        self,
        config_parameters: dict,
        logging_level: int = logging.DEBUG,
        logger_name: str = "psycopg_logger",
        log_file: str = "psycopg_logger.log",
    ):
        """
        Initialize PsycopgDB object from config and initialize logger.

        :param config_parameters: contains the configuration that defines the database interaction
        :type config_parameters: dict
        :param logging_level: specifies the level of the logger, defaults to logging.DEBUG
        :type logging_level: int
        :param logger_name: name of the logger to use, defaults to psycopg_logger
        :type logger_name: str
        :param log_file: name of the file the logs are written into, defaults to psycopg_logger.log
        :type log_file: str
        """

        self.config_parameters = config_parameters
        self.logger = create_logger(log_file, logger_name, logging_level)

    def create_connection_and_cursor(self) -> [connection, cursor]:
        """
        Initialize connection to postgresql database and create cursor.
        :return: connection to database and the cursor
        """
        con = connect(**self.config_parameters)
        cur = con.cursor()
        return con, cur

    def store_in_db(self, query: str, format_arguments: Tuple[str, ...] = None) -> None:
        """
        Add a new entry in the database.

        :param query: query to perform on the database
        :type query: str
        :param format_arguments: arguments to augment the query
        :type format_arguments: Tuple[str, ...]
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
    ) -> List | None:
        """
        Search the database for information.

        :param query: query to the database
        :type query: str
        :param format_arguments: arguments for the query
        :type format_arguments: Tuple[str, ...]
        :raises DatabaseError: if interaction with the database failed due to an incorrect query
        :return: results extracted from the database
        :rtype: list
        """
        con = None
        try:
            con, cur = self.create_connection_and_cursor()
            if not format_arguments:
                cur.execute(sql.SQL(query))
            else:
                cur.execute(sql.SQL(query), format_arguments)
            fetched_information = cur.fetchall()
            con.close()
            return fetched_information

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
        :type query: str
        :raises DatabaseError: if interaction with the database failed due to an incorrect query
        :param format_arguments: string arguments to augment the query
        :type format_arguments: Tuple[str, ...]
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
        :type identifier: str
        :param update_value: new value to set in the table
        :type update_value: str
        """
        con = None
        try:
            con, cur = self.create_connection_and_cursor()
            cur.execute(sql.SQL(query), (update_value, identifier))
            con.commit()
            con.close()
        except DatabaseError as database_error:
            self.logger.exception(database_error)
            if con:
                con.rollback()
                con.close()
