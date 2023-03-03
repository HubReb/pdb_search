#!/usr/bin/env python3

import logging

from paper_sorts.database_connector import DatabaseConnector


class UserInteraction:
    def __init__(
        self,
        logger_name="user_interaction_logger",
        logging_level=logging.DEBUG,
        log_file="interaction.log",
    ):
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

    @staticmethod
    def pretty_print_results(bibtex_data, paper_data):
        print(f"title: {paper_data[2]}\nauthors: {paper_data[0]}")
        print(f"summary: {paper_data[4]}\nbib entry: {bibtex_data[1]}")

    def search(self, db_connector: DatabaseConnector):
        user_input = cast(
            input(
                "Search interface\nPlease choose a method:\n1) search by author\n2) search by paper title\n"
            )
        )
        while user_input < 1 or user_input > 2:
            user_input = cast(
                input(
                    "Please choose a valid option:\n1) search by author\n2)search by paper title\n"
                )
            )
        if user_input == 2:
            try:
                paper_title = input("Please enter the paper title: ")
                papers = db_connector.search_by_title(paper_title.strip())
                if not papers:
                    print("There was a db_connector error, shutting down.")
                    return
                if len(papers) > 1:
                    chosen_paper = self.__get_user_choice(papers)
                else:
                    chosen_paper = papers[0]
                bibtex_data = db_connector.search_for_bibtex_entry_by_id(chosen_paper)
                self.pretty_print_results(bibtex_data, chosen_paper)
            except KeyError:
                print("Paper was not found in db_connector.")

        else:
            try:
                author_name = input("Please enter the author's name: ")
                papers = db_connector.search_by_author(author_name.strip())
                if not papers:
                    print("There was a db_connector error, shutting down.")
                    return
                chosen_paper = self.__get_user_choice(papers)
                paper = db_connector.search_for_bibtex_entry_by_paper_title(
                    chosen_paper
                )
                bibtex_data = db_connector.search_for_bibtex_entry_by_id(paper)
                self.pretty_print_results(bibtex_data, paper)
            except KeyError:
                print("Author was not found in db_connector.")

    @staticmethod
    def __get_user_choice(results):
        print("Following papers found: ")
        for i, paper in enumerate(results):
            print(f"{i + 1}: title: {paper[3]}")
        chosen_paper = -1
        while chosen_paper < 0 or chosen_paper >= len(results):
            chosen_paper = cast(input("Choose paper to extract: ")) - 1
            if chosen_paper < 0 or chosen_paper >= len(results):
                print("Please choose a valid number.")
        return results[chosen_paper]


def cast(user_input: str) -> int:
    """Check if user input is valid and cast to Integer if so"""
    try:
        cast_to_int = int(user_input)
    except ValueError:
        return -1
    return cast_to_int


if __name__ == "__main__":
    import argparse
    from paper_sorts.config_reader import ConfigReader

    parser = argparse.ArgumentParser(
        description="transform latex list into postgresql DB",
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
    parser.add_argument(
        "-l",
        "--literature_file",
        type=str,
        default="../../literature_overview.tex",
        help="latex file",
    )
    parser.add_argument(
        "-b", "--bib-file", type=str, default="../../bib.bib", help="bib-file"
    )
    parser.add_argument(
        "-s",
        "--summary",
        type=str,
        default="This is a test entry",
        help="one sentence summary of the paper",
    )
    args = parser.parse_args()
    config_params = ConfigReader(args.config, args.section, args.key)
    config_reader = ConfigReader(args.config, args.section, args.key)
    database = DatabaseConnector(
        config_params.db_config,
        logging.DEBUG,
        "database_logger",
        log_file="db_connector.log",
    )
    user = UserInteraction()
    user.search(database)
