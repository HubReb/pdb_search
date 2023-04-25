#!/usr/bin/env python3

""" Contains the UserInteraction class, which handles all cli-interactions with the user. """

import logging

from paper_sorts.helpers import (
    get_user_choice,
    pretty_print_results,
    cast,
    get_user_input,
    create_logger
)
from paper_sorts.database_connector import DatabaseConnector


class UserInteraction:
    """
    This class handles all interaction with the user.

    This class is all the user interacts with. It contains methods for the user to add, search,
    update and delete data from the database by interacting with an object of the
    :class: `paper_sorts.DatabaseConnector` to transform them into a format to execute them and
    toexecute them and this class presents the user with the results.
    It also provides the user with easy to understand failure messages if the user's chosen
    actions could not be performed on the database.
    """

    def __init__(
            self,
            logger_name: str = "user_interaction_logger",
            logging_level: int = logging.DEBUG,
            log_file: str = "interaction.log",
    ):
        """
        Interaction with the user on the command line interface.

        :param logger_name: name of the logger
        :type logger_name: str
        :param logging_level: specifies level to log at
        :type logging_level: int
        :param log_file: name of the file to write logs to
        :type log_file: str
        """
        self.logger = create_logger(log_file, logger_name, logging_level)

    def search(self, db_connector: DatabaseConnector):
        """
        Search the database for paper information and interact with user at points of uncertainty.

        :param db_connector: object to interact with the database with
        :type db_connector: DatabaseConnector
        """
        user_input = cast(
            input(
                "Search interface\nPlease choose a method:\n"
                "1) Search by author\n"
                "2) Search by paper_information title\n"
            )
        )
        while user_input < 1 or user_input > 2:
            user_input = cast(
                input(
                    "Please choose a valid option:\n"
                    "1) Search by author\n"
                    "2) Search by paper_information title\n"
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
        Add a new paper to the database and ask user for all required information.

        Add a new paper to the database and ask the user to enter the information step
        by step. Bibtex information may by provided via a file in which the information
        is stored.

        :param db_connector: object to interact with the database with
        :type db_connector: DatabaseConnector
        :return: indicates whether the paper was successfully added to the database
        :rtype: bool
        """
        author = get_user_input(
            "Please enter the necessary information\nAuthor(s), please provide a , separated list: "
        )
        paper_title = get_user_input("Paper title: ")
        bibtex_key = get_user_input("bibtex key: ")
        bibtex_form = get_user_input(
            "Do you want to enter the bibtex entry via a separate file?\n"
            "1) Yes\n"
            "2) No\n"
            "Your choice: ")
        if cast(bibtex_form) == 1:
            bibtex_information_file = get_user_input("Enter filename: ")
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
            database_connector: DatabaseConnector
    ):
        """
        Start dialog with the user and have the user select what to do with the database.

        :param database_connector: high-level connector to the database to handle DB interactions
        :type database_connector: DatabaseConnector
        """
        operation = get_user_input(
            "What do you want to do?\n"
            "1) Search the database\n"
            "2) Add an entry\n"
            "3) Update an entry\n"
            "4) (Q)uit\n"
            "Your choice: "
        )
        while operation != "q" or cast(operation) == 3:
            match operation:
                case "1":
                    self.search(database_connector)
                case "2":
                    self.add(database_connector)
                case "3":
                    self.update(database_connector)
                case "4" | "q":
                    print("Closing connection...")
                    break
                case _:
                    print("Your input was invalid")
            operation = get_user_input(
                "What do you want to do?\n1) Search the database\n2) Add an entry\n3) Update an entry\n4) (Q)uit\n"
                "Your choice: "
            )

    def update(
            self,
            database_connector: DatabaseConnector
    ):
        """Update paper information already in the database if it is either author, bib or paper summary.

        :param database_connector: provides connection to database interface
        :type database_connector: DatabaseConnector
        """
        table_to_be_updated = get_user_input(
            "Which information do you want to update?\n1) papers\n2) bib\n3) authors\n4) abort\nYour choice: "
        ).lower()
        match table_to_be_updated:
            case "papers" | "1":
                column_to_be_updated = get_user_input(
                    "Which information do you want to update?\n1) title\n2) contents\n3) abort\nYour choice: "
                ).lower()
                match column_to_be_updated:
                    case "1" | "title":
                        column_to_be_updated = "title"
                    case "2" | "contents":
                        column_to_be_updated = "contents"
                    case "3" | "abort":
                        print("Stopping update process...")
                        return
                    case _:
                        print(f"Column '{column_to_be_updated}' cannot be updated in this manner.")
                        return
            case "bib" | "2":
                print("Only the bibtex can be updated - the bibtex identifier cannot be changed.")
                column_to_be_updated = "bibtex"
            case "authors" | "3":
                print("Only an author name can be updated.")
                column_to_be_updated = "author"
            case "4" | "abort":
                print("Stopping update process...")
                return
            case _:
                print(f"Table '{table_to_be_updated}' cannot be updated in this manner.")
                return
        identifier_of_the_entry_to_update = get_user_input(
            "Which entry do you want to update?\nPlease enter the respective id: ")
        value_to_set = get_user_input("Enter the new information: ")
        proceed_with_change = get_user_input(f"Please verify: "
                                             f"You wish to change '{column_to_be_updated}' of the entry "
                                             f"'{identifier_of_the_entry_to_update}'"
                                             f" to '{value_to_set}.\n Proceed?\n1) (Y)es\n2) ("
                                             f"N)o\n"
                                             f"Your choice: "
                                             ).lower()
        match proceed_with_change:
            case "1" | "y" | "yes":
                verification_given = True
            case "2" | "n" | "no":
                verification_given = False
            case _:
                print("Could not parse your reply. Stopping update process...")
                return
        if not verification_given:
            print("Stopping update process...")
            return
        try:
            database_connector.update_entry(
                column_to_be_updated,
                value_to_set,
                table_to_be_updated,
                identifier_of_the_entry_to_update
            )
        except ValueError as error:
            self.logger.error(error)
            print("Could not update entry - please check logs.")
