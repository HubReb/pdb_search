#! /usr/bin/env python3

""" This is the module in which the application can be started. """

import logging
import argparse

from paper_sorts.user_interaction import UserInteraction
from paper_sorts.database_connector import DatabaseConnector
from paper_sorts.config_reader import ConfigReader

def run():
    """Start application with either default arguments or cli arguments, if given."""
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
    print("Welcome! Connecting to the database, one moment...")
    config_reader = ConfigReader(args.config, args.section, args.key)
    database_connector = DatabaseConnector(
        config_reader.db_config,
        logging.DEBUG,
        "database_tester_logger",
        log_file="db_connector_test.log",
    )
    print("Connected to the database.")
    user.interact(database_connector)


if __name__ == "__main__":
    run()
