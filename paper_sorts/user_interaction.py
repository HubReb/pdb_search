#!/usr/bin/env python3

import logging

from paper_sorts.database_connector import DatabaseConnector
from paper_sorts.config_reader import ConfigReader
from paper_sorts.helpers import (
    get_user_choice,
    pretty_print_results,
    cast,
    get_user_input,
)


class UserInteraction:
    def __init__(
        self,
        logger_name: str = "user_interaction_logger",
        logging_level: int = logging.DEBUG,
        log_file: str = "interaction.log",
    ):
        """
        Interaction with the user on the command line interface.

        :param logger_name: name of the logger
        :param logging_level: specifies level to log at
        :param log_file: name of the file to write logs to
        """
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

    def search(self, db_connector: DatabaseConnector):
        """
        Search the database for paper information and interact with user at points of uncertainty.

        :param db_connector: object to interact with the database with
        """
        user_input = cast(
            input(
                "Search interface\nPlease choose a method:\n1) Search by author\n2) Search by paper_information title\n"
            )
        )
        while user_input < 1 or user_input > 2:
            user_input = cast(
                input(
                    "Please choose a valid option:\n1) Search by author\n2) Search by paper_information title\n"
                )
            )
        if user_input == 2:
            try:
                paper_title = input("Please enter the paper_information title: ")
                papers = db_connector.search_by_title(paper_title.strip())
                if not papers:
                    print("There was a db_connector error, shutting down.")
                    return
                if len(papers) > 1:
                    chosen_paper = get_user_choice(papers)
                else:
                    chosen_paper = papers[0]
                bibtex_data = db_connector.search_for_bibtex_entry_by_id(chosen_paper)
                pretty_print_results(bibtex_data, chosen_paper)
            except KeyError:
                print("Paper was not found in db_connector.")
                self.logger.error("shutdown")

        else:
            try:
                author_name = input("Please enter the author's name: ")
                papers = db_connector.search_by_author(author_name.strip())
                if not papers:
                    print("There was a db_connector error, shutting down.")
                    return
                chosen_paper = get_user_choice(papers)
                paper = db_connector.search_for_entry_by_specified_paper_information(
                    chosen_paper
                )
                bibtex_data = db_connector.search_for_bibtex_entry_by_id(paper)
                pretty_print_results(bibtex_data, paper)
            except KeyError:
                print("Author was not found in db_connector.")
                self.logger.error("shutdown")

    def add(self, db_connector: DatabaseConnector) -> bool:
        """
        Add a new paper to the database and ask user for all required information either via interaction or by providing
        a file in which the information is stored (only for bibtex information).
        .
        :param db_connector: object to interact with the database with
        :return: boolean indicating whether the paper was successfully added to the database
        """
        author = get_user_input(
            "Please enter the necessary information\nAuthor(s), please provide a , separated list: "
        )
        paper_title = get_user_input("Paper title: ")
        bibtex_key = get_user_input("bibtex key: ")
        bibtex_form = get_user_input("Do you want to enter the bibtex entry via a separate file?\n1) Yes\n2) No\nYour choice: ")
        if cast(bibtex_form) == 1:
            bibtex_information_file= get_user_input("Enter filename: ")
            with open(bibtex_information_file) as f:
                bibtex_information = f.read()
        else:
            bibtex_information = get_user_input("bib entry: ")
        content = get_user_input("summary of the paper_information: ")
        authors = author.split(", ")
        successful = db_connector.add_entry_to_db(
            bibtex_information, authors, bibtex_key, paper_title, content
        )
        authors = ", ".join(authors)
        if successful:
            self.logger.info("added entry %s to database", f"{authors}: {paper_title}")
            return True
        self.logger.info(
            "failed to add entry %s to database - please study logs",
            f"{authors}: {paper_title}",
        )
        return False

    def interact(
        self,
        config_file: str,
        config_section: str,
        key: str,
    ):
        """
        Start dialog with the user and have the user select what to do with the database.

        :param config_file: name of the file that specifies how to connect to the database
        :param config_section: which section of the file to read the configuration from
        :param key: if the configuration file is encrypted, file that contains the key to decrypt it
        """
        print("Welcome! Connecting to the database, one moment...")
        config_reader = ConfigReader(config_file, config_section, key)
        database_connector = DatabaseConnector(
            config_reader.db_config,
            logging.DEBUG,
            "database_tester_logger",
            log_file="db_connector_test.log",
        )
        print("Connected to the database.")
        operation = get_user_input(
            "What do you want to do?\n1) Search the database\n2) Add an entry\n3) (Q)uit\nYour choice: "
        )
        while operation != "q" or cast(operation) == 3:
            match operation:
                case "1":
                    self.search(database_connector)
                case "2":
                    self.add(database_connector)
                case "3" | "q":
                    print("Closing connection...")
                    break
                case _:
                    print("Your input was invalid")
            operation = get_user_input(
                "What do you want to do?\n1) Search the database\n2) Add an entry\n3) (Q)uit\n"
            )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Define parameters for database connection",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default="../../database.crypt",
        help="configuration file of the used db_connector",
    )
    parser.add_argument(
        "--section",
        type=str,
        default="postgresql",
        help="section of the config file to use",
    )
    parser.add_argument(
        "-k", "--key", type=str, default="../../key", help="decryption key file"
    )

    args = parser.parse_args()
    user = UserInteraction()
    user.interact(config_file=args.config, config_section=args.section, key=args.key)