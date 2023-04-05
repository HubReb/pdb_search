#!/usr/bin/env python3

"""This module contains the :class: `ConfigReader` to load a config from an encrypted file."""

from configparser import ConfigParser

from cryptography.fernet import Fernet


class ConfigReader(ConfigParser):
    """
    Reads the database configuration from a file and stores it as a dict.

    The initilization expects an encrypted file that is decrypted via the
    cryptography package.
    """
    def __init__(self, filename: str, section: str, key_file: str):
        """Read db_connector configuration from file

         :param filename: file containing the db_connector specifications
         :type filename: str
         :param section: which section to take
         :type section: str
         :param key_file: file containing the key to decrypt the config
         :type key_file: str

        :return: db_config (dict): configuration parameters of db_connector
        """
        super().__init__()
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
            raise ValueError(f"Section {section} not found in {filename} file")

        self.db_config = db_config
