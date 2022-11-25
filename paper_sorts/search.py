#!/usr/bin/env python3

"""Search a database for an entry.

    functions:
        connect_to_database(config, logger)
            Connect to the database as specified in the config.
        cast(user_input) -> int:
            Cast user_input to int without crashing the program if user_input cannot be cast to
            an integer.
        search_by_title(config_parameters: dict , title: str, logger: logging.Logger) -> List[str]
            Search the tables in the database which can be  connected via the configuration parameters for a
            publication by title.
        search_by_author(config_parameters: dict, , author, logger)
            Search the tables in the database which can be  connected via the configuration parameters by
            author names.
        search(config_parameters: dict, logger, title=None, author=None) -> List[str]
            Search the tables in the database which can be  connected via the configuration parameters either by
            title or author name.
"""

import sys
import logging
from typing import List

import psycopg
from psycopg import sql

from paper_sorts.get_data import read_config


def connect_to_database(config_parameters: dict, logger: logging.Logger):
    """ Connect to the database and return connection"""
    con = None
    try:
        con = psycopg.connect(**config_parameters)
        return con
    except psycopg.DatabaseError as database_error:
        logger.exception(database_error)
        if con:
            con.rollback()
            con.close()
        sys.exit(1)


def cast(user_input: str) -> int:
    """ Check if user input is valid and cast to Integer if so"""
    try:
        cast_to_int = int(user_input)
    except ValueError:
        return -1
    return cast_to_int


def search_by_title(config_parameters: dict, title: str, logger: logging.Logger) -> List[str]:
    """Search papers database by title"""
    con = None
    try:
        con = connect_to_database(config_parameters, logger)
        cur = con.cursor()
        cur.execute(
            sql.SQL("select  authors_id.author, papers.id, papers.title, papers.bibtext_id, papers.contents from papers  INNER JOIN "
                    "authors_papers papers_authors on papers_authors.paper_id=papers.id "
                    "INNER JOIN authors_id on papers_authors.author_id = authors_id.id where papers.title=%s"),
            (title,)

        )
        papers = cur.fetchall()
        if not papers:
            logger.info(f"Paper with title {title} not found in table papers, abort!")
            return []
        down_papers = []
        if len(set(p[2] for p in papers)) > 1:
            id_bib = papers[0][3]
            authors = ""
            for i, paper in enumerate(papers):
                paper = list(paper)  # papers element are tuples
                if id_bib != paper[3]:
                    down_papers.append([authors[:-5]] + list(papers[i - 1])[1:])
                    id_bib = paper[3]
                else:
                    authors += f"{paper[0]} and "
        else:
            authors = ""
            for paper in papers:
                authors += f"{paper[0]} and "
            down_papers.append([ authors[:-5]] + list(papers[0][1:]))
        chosen_paper = -1
        for i, p in enumerate(down_papers):
            print(f"{i+1}) title: {p[2]}\nauthors: {p[0]}")
        while chosen_paper < 0 or chosen_paper >= len(down_papers):
            chosen_paper = cast(input("Choose paper to extract: ")) - 1
            if chosen_paper < 0 or chosen_paper >= len(papers):
                print("Please choose a valid number.")
        return down_papers[chosen_paper]

    except psycopg.DatabaseError as database_error:
        logger.exception(database_error)
        if con:
            con.rollback()
        return []


def search_by_author(config_parameters: dict, logger: logging.Logger, author: str) -> List[str]:
    """Search authors_papers table by author, then search papers table"""
    con = None
    try:
        con = connect_to_database(config_parameters, logger)
        cur = con.cursor()

        cur.execute(
            sql.SQL("select authors_id.id, authors_id.author, paper_id, title, bibtext_id, contents from authors_id INNER JOIN authors_papers on "
                    "authors_papers.author_id=authors_id.id INNER JOIN papers on paper_id=papers.id where author=%s;"), (author,)
        )
        resutls = cur.fetchall()
        if not resutls:
            print("author not found")
            logger.info("author not found")
            return []
        if len(resutls) > 1:
            print("Following papers found: ")
            for i, paper in enumerate(resutls):
                print(f"{i + 1}: title: {paper[3]}")
            chosen_paper = -1
            while chosen_paper < 0 or chosen_paper >= len(resutls):
                chosen_paper = cast(input("Choose paper to extract: ")) - 1
                if chosen_paper < 0 or chosen_paper >= len(resutls):
                    print("Please choose a valid number.")
        else:
            chosen_paper = resutls[0]
        cur.execute(
            sql.SQL(
                "select authors_id.author, paper_id from authors_id INNER JOIN "
                "authors_papers on authors_id.id = authors_papers.author_id where paper_id = %s"),
            (chosen_paper[2],)
        )
        author_names = cur.fetchall()
        author_pretty = ""
        for author_name in author_names:
            author_pretty += f"{author_name[0]} and "
        author_pretty = author_pretty[:-5]
        if con:
            con.close()
        return [author_pretty] + list(resutls[0][2:])

    except psycopg.DatabaseError as database_error:
        logger.exception(database_error)
        if con:
            con.rollback()
            con.close()
        return []


def search(config_parameters: dict, logger, title=None, author=None):
    """Search for a paper in the paper database"""
    con = None
    try:
        con = connect_to_database(config_parameters, logger)
        cur = con.cursor()
        if title:
            paper = search_by_title(config_parameters, title, logger)
            if not paper:
                print("no paper found")
        else:
            paper = search_by_author(config_parameters, logger, author)
            if not paper:
                print("no paper found")
        if paper:
            cur.execute(sql.SQL("select * from bib where bibtext_id=%s;"), (paper[3],))
            bibtex_data = cur.fetchone()
            print(f"title: {paper[2]}\nauthors: {paper[0]}")
            print(f"summary: {paper[4]}\nbib entry: {bibtex_data[1]}")


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
    search_logger = create_logger("search.log", "search", logging.DEBUG)
    parser = argparse.ArgumentParser(
        description='query DB for a paper',
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
        "-a",
        "--authors",
        type=str,
        default="Korakakis, Michalis",
        help="name of the paper authors"
    )
    parser.add_argument(
        "--title",
        type=str,
        #default="Mitigating Catastrophic Forgetting in Scheduled Sampling with Elastic Weight"
         #       " Consolidation in Neural Machine Translation",
        help="title of paper"
    )
    args = parser.parse_args()
    config_params = read_config(args.config, args.section, args.key, search_logger)
    search(config_params, search_logger, title=args.title, author=args.authors)
