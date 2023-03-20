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

    def get_connection_and_cursor(self) -> [connection, cursor]:
        con = connect(**self.config_parameters)
        cur = con.cursor()
        return con, cur

    def store_in_db(self, query: str, format_arguments: Tuple[str, ...] = None) -> None:
        con = None
        try:
            con, cur = self.get_connection_and_cursor()
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
        con = None
        try:
            con, cur = self.get_connection_and_cursor()
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
        con = None
        try:
            con, cur = self.get_connection_and_cursor()
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
