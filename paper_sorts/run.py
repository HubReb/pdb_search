#! /usr/bin/env python3

""" This is the module in which the application can be started. """

import argparse

from paper_sorts.user_interaction import UserInteraction

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
    user.interact(config_file=args.config, config_section=args.section, key=args.key)


if __name__ == "__main__":
    run()
