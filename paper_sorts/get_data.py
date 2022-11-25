#!/usr/bin/env python3

"""
Simple script to transfer the information in overleaf to a local postgresql database.

functions:
    read_config(filename: str, section: str, key_file: str):
        Read database configuration from the file specified with filename.
     get_data(filename: str) -> dict:
        Extract latex information from .tex-file downloaded from overleaf.
    get_bibtex_information(papers_dict: dict, bibsomy: str) -> dict:
        Extract bibtex information from .bib-file containing the bibtext entries corresponding to
        the ones used in the .tex-file given to get_data.
     load_data_into_db(data: dict, config_parameters: dict, logger) -> None:
        Create tables papers, authors_papers and bib from the data extracted from the .tex and
        .bib-files.
"""
import logging
import sys
from collections import defaultdict
from configparser import ConfigParser

from pylatexenc.latex2text import LatexNodes2Text
import psycopg
from psycopg import sql
from cryptography.fernet import Fernet
from pybtex.database import parse_file


def read_config(filename: str, section: str, key_file: str, logger: logging.Logger) -> dict:
    """ Read database configuration from file

    :param filename: file containing the database specifications
    :param section: which section to take
    :param key_file: file containing the kye to decrypt the config
    :param logger: Logger that logs exception, warning, ...

   :return: db_config (dict): configuration parameters of database
    """
    with open(filename, "rb") as f:
        config = f.read()
    with open(key_file, "rb") as f:
        key = f.read()
    fernet = Fernet(key)
    config = fernet.decrypt(config).decode("utf-8")
    config_parser = ConfigParser()
    config_parser.read_string(config)
    db_config = {}
    if config_parser.has_section(section):
        parameters = config_parser.items(section)
        for key, value in parameters:
            db_config[key] = value
    else:
        logger.exception(f'Section {section} not found in {filename} file')
        sys.exit()
    return db_config


def get_data(filename: str) -> dict:
    """
    Read latex data and construct dictionary of form
    {title : {contents: content in one sentence}, {bibtex: {bibtex identifier}}

    :param filename: name of latex file
    :return: dictionary of above form
    """
    assert filename.endswith("tex")
    with open(filename, encoding="utf-8") as f:
        data = f.read()
    text = LatexNodes2Text().latex_to_text(data).split("\n")
    text = [line for line in text if not line == '']
    papers_dict = defaultdict(lambda: defaultdict(str))
    title, bibtex = None, None
    for line in text:
        if "*" in line and "<cit.>" in line:     # title case
            title = line.split("<cit.>")[1].rstrip(":")
            if title == ":":
                title = line.split("<cit.>")[0].split("*")[1]
            title = title.strip()
            for latex_line in data.split("\n"):
                if title in latex_line:
                    line_split = latex_line.split(r"\cite{")
                    if "\\item" in line_split[0]:
                        bibtex = line_split[1]
                    else:
                        bibtex = line_split[0]
                    bibtex = bibtex.split("}")[0]
                    break
            description = None
        else:
            description = line.strip()
        if description and title:
            papers_dict[title]["bibtex_id"] = bibtex
            papers_dict[title]["contents"] = description
            title, bibtex = None, None
    return papers_dict


def get_bibtex_information(papers_dict: dict, bibsomy: str) -> dict:
    """
    Read bib information from file, replace bibtex key with bibtex information and add authors.

    :param papers_dict: dictionary of the form
        {title: {bibtex: bibtex identifier}, {contents: content description}}
    :param bibsomy: file that contains the bibtex information
    :return: dictionary of the form
        {
            title: {bibtex: bibtex identifier}, {contents: description},
            {author: list of individual author names}
        }
    """
    bib_graph = parse_file(bibsomy, bib_format="bibtex")
    for i in bib_graph.entries:
        for entry in papers_dict.keys():
            if papers_dict[entry]["bibtex_id"] == i:
                authors_list = []
                for author in bib_graph.entries[i].persons['author']:
                    authors_list.append(f"{author.last_names[0]}, {author.first_names[0]}")
                papers_dict[entry]['bibtex'] = bib_graph.entries[i].to_string('bibtex')
                papers_dict[entry]['author'] = authors_list
                break
    return papers_dict


def load_data_into_db(data: dict, config_parameters: dict, logger: logging.Logger) -> None:
    """
    Create table papers, authors_id, bib, authors_papers in database and fill them with the paper information.

    :param data: the dictionary of form
            {title: {bibtex: bibtex identifier}, {contents: description}, {author: author names}}
    :param config_parameters: dictionary of database configuration paramaters
    :param logger: logger to log warning, info, errors, ...
    :return: -
    """
    con = None
    try:
        con = psycopg.connect(**config_parameters)
        cur = con.cursor()
        cur.execute(
            sql.SQL(
                "CREATE  TABLE  IF NOT EXISTS authors_papers (id SERIAL PRIMARY KEY, author_id INT, "
                "paper_id INT) ; "
            )
        )
        cur.execute(
            sql.SQL(
                "CREATE  TABLE  IF NOT EXISTS authors_id (id SERIAL PRIMARY KEY, author TEXT) ; "
            )
        )
        # if our bibtex identifier is not unique, the entire database is useless
        cur.execute(
            sql.SQL(
                "CREATE  TABLE  IF NOT EXISTS bib (bibtext_id text primary key, bibtext text) ;"
            )
            .format(sql.Identifier("bib"))
        )
        con.commit()
        cur.execute(
            sql.SQL(
                "CREATE  TABLE  IF NOT EXISTS papers (id SERIAL PRIMARY KEY, title TEXT, contents TEXT, "
                "bibtext_id TEXT, "
                "constraint fk_bibtex_id foreign key(bibtext_id) references bib(bibtext_id));"
            )
            .format(sql.Identifier("papers"))
        )
        con.commit()

        sql_instruction = "INSERT INTO {} (title, contents, bibtext_id) VALUES (%s, %s, %s)"

        for title, values in data.items():
            content = values["contents"]
            bibtex = values["bibtex"]
            bibtex_key = values["bibtex_id"]
            authors = values["author"]
            if not bibtex_key:
                continue
            cur.execute(
                sql.SQL("select exists(select * from papers where bibtext_id=%s);"), (bibtex_key, )
            )
            if cur.fetchone()[0]:
                continue
            cur.execute(sql.SQL("insert into bib values (%s, %s);"), (bibtex_key, bibtex))
            con.commit()
            cur.execute(
                sql.SQL(sql_instruction).format(sql.Identifier("papers")),
                [title, content, bibtex_key]
            )
            con.commit()
            for author in authors:
                cur.execute(sql.SQL("select max(id) from papers;"))
                paper_id = cur.fetchone()[0]
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
                    cur.execute(sql.SQL("select max(id) from authors_id;"))
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


def create_logger(log_file: str, logger_name: str, logging_level: int) -> logging.Logger:
    """ Create a logger to log in log_file """
    ###
    # mostly taken from https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
    # create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging_level)

    # create console handler and set level to debug
    ch = logging.FileHandler(filename=log_file)
    ch.setLevel(logging_level)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    ###
    return logger


if __name__ == "__main__":
    import argparse

    get_data_logger = create_logger("add_data_batch.log", "add_data_batch", logging.DEBUG)
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
        "-l",
        "--literature_file",
        type=str,
        default="../../literature_overview.tex",
        help="latex file"
    )
    parser.add_argument(
        "-b",
        "--bib-file",
        type=str,
        default="../../bib.bib",
        help="bib-file"
    )
    args = parser.parse_args()
    config_params = read_config(args.config, args.section, args.key, get_data_logger)
    data_dict = get_data(args.literature_file)
    data_dict = get_bibtex_information(data_dict, args.bib_file)
    load_data_into_db(data_dict, config_params, get_data_logger)
