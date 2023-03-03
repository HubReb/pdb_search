#!/usr/bin/env python3

from configparser import ConfigParser

from cryptography.fernet import Fernet


class ConfigReader(ConfigParser):
    def __init__(self, filename: str, section: str, key_file: str):
        """Read db_connector configuration from file

         :param filename: file containing the db_connector specifications
         :param section: which section to take
         :param key_file: file containing the kye to decrypt the config
         :param logger: Logger that logs exception, warning, ...

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
