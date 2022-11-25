#!/usr/bin/env python3


""" Add new entry to the paper database

functions:
    get_single_bibtex_information(bibsomy: str) -> tuple[str, List[str], str, str]
        Take bib information for one publication from a file.
    add_entry_to_db(
        new_bibtex_entry: str,
        author_names: str,
        bibtex_ident: str,
        title: str,
        content: str,
        config_parameters: dict,
        logger
    ):
        Add information in the new entry to papers, bib, authors_id and authors_papers tables in the database.
"""

import logging
from typing import List

from pybtex.database import parse_file
import psycopg
from psycopg import sql

from paper_sorts.get_data import read_config


def get_single_bibtex_information(bibsomy: str) -> tuple[str, List[str], str, str]:
    """
    Read bib information from file

    :param bibsomy: file that contains the bibtex information
    :return: bibtex entry (str), authors (list), bibtex identifier (str)
    """
    bib_graph = parse_file(bibsomy, bib_format="bibtex")
    assert len(bib_graph.entries.keys()) == 1
    for i in bib_graph.entries.keys():
        bibtex_data = bib_graph.entries[i].to_string('bibtex')
        bibtex_ident: str = i
    authors_list = []
    for author in bib_graph.entries[i].persons['author']:
        authors_list.append(f"{author.last_names[0]}, {author.first_names[0]}")
    return bibtex_data, authors_list, bibtex_ident, bib_graph.entries[bibtex_ident].fields['title']


def add_entry_to_db(
        new_bibtex_entry: str,
        author_names: list,
        bibtex_ident: str,
        title: str,
        content: str,
        config_parameters: dict,
        logger: logging.Logger
) -> None:
    """
    Add new entry to database table.

    :param new_bibtex_entry: complete bibtex entry a string
    :param author_names: name of the authors in a list, one element for each author
    :param bibtex_ident: bibtex identifier of the publication, must be unique
    :param title: title of the publication
    :param content: one sentence summarizing the content of the publication
    :param config_parameters: dictionary of database configuration parameters
    :param logger: Logger to log warnings, errors, ...
    :return: -
    """
    con = None
    try:
        con = psycopg.connect(**config_parameters)
        cur = con.cursor()
        cur.execute(sql.SQL(f"select relname from pg_class where relname = 'papers';"))
        if not cur.fetchone():
            logger.exception(
                f"Table papers not found in database {config_parameters['database']}, abort!"
                )
            con.close()
            return
        sql_instruction = "INSERT INTO papers (title, contents, bibtext_id) VALUES (%s, %s, %s)"
        cur.execute(
            sql.SQL(f"select exists(select * from papers where bibtext_id='{bibtex_ident}');")
        )
        if cur.fetchone()[0]:
            logger.exception(f"Entry {bibtex_ident} already exists in table papers")
            con.close()
            return
        cur.execute(sql.SQL("insert into bib values (%s, %s);"), (bibtex_ident, new_bibtex_entry))
        con.commit()
        cur.execute(
            sql.SQL(sql_instruction),
            [title, content, bibtex_ident]
        )
        con.commit()
        cur.execute(sql.SQL("select id from papers where title=%s"), (title,))
        paper_id = cur.fetchone()[0]
        for author in author_names:
            cur.execute(
                sql.SQL("select * from authors_id where author=%s;"), (author,)
            )
            author_id = cur.fetchone()
            if author_id:
                cur.execute(
                    sql.SQL("insert into authors_papers (author_id, paper_id) values (%s, %s);"),
                    (author_id[0], paper_id)
                )
            else:
                cur.execute(
                    sql.SQL("insert into authors_id (author) values (%s);"),
                    (author,)
                )
                cur.execute(sql.SQL("select id from authors_id where author=author;"))
                author_id = cur.fetchone()[0]
                cur.execute(
                    sql.SQL("insert into authors_papers (author_id, paper_id) values (%s, %s);"),
                    (author_id, paper_id)
                )
            con.commit()

    except psycopg.DatabaseError as database_error:
        logger.exception(database_error)
        if con:
            con.rollback()

    finally:
        if con:
            con.close()


if __name__ == "__main__":
    import argparse

    from paper_sorts.get_data import create_logger

    add_logger = create_logger("add.log", "add", logging.DEBUG)
    parser = argparse.ArgumentParser(
        description='transform latex list into postgresql DB',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default="../../database.crypt",
        help="configuration file of the used database"
    )
    parser.add_argument(
        "-s",
        "--summary",
        type=str,
        default="This is a test entry",
        help="one sentence summary of the paper"
    )
    parser.add_argument(
        "--section",
        type=str,
        default="postgresql",
        help="section of the config file to use"
    )
    parser.add_argument(
        "-k",
        "--key",
        type=str,
        default="../../key",
        help="decryption key file"
    )
    parser.add_argument(
        "-b",
        "--bib-file",
        type=str,
        default="../../bib_single.bib",
        help="bib-file"
    )
    args = parser.parse_args()
    config_params = read_config(args.config, args.section, args.key, add_logger)
    bibtex_entry, author_of_p, bibtex_id, paper_title = get_single_bibtex_information(args.bib_file)
    add_entry_to_db(
        bibtex_entry,
        author_of_p,
        bibtex_id,
        paper_title,
        args.summary,
        config_params,
        add_logger
    )
