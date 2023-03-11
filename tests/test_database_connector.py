import unittest
import logging

from paper_sorts.database_connector import DatabaseConnector
from paper_sorts.config_reader import ConfigReader


class DataBaseTest(unittest.TestCase):
    def test_search_by_author(self):
        config_reader = ConfigReader("../../database.crypt", "postgresql", "../../key")
        database = DatabaseConnector(
            config_reader.db_config,
            logging.DEBUG,
            "database_tester_logger",
            log_file="db_connector_test.log",
        )
        author_search = database.search_by_author("Pino, J.")
        self.assertEqual(
            author_search[0][3],
            "Large-scale Self- an Semi-Supervised learning for speech translation",
        )
        self.assertEqual(author_search[0][4], "Wang2021LargeScaleSA")
        self.assertRaises(KeyError, database.search_by_author, "blub")

    def test_search_by_title(self):
        config_reader = ConfigReader("../../database.crypt", "postgresql", "../../key")
        database = DatabaseConnector(
            config_reader.db_config,
            logging.DEBUG,
            "database_tester_logger",
            log_file="db_connector_test.log",
        )
        self.assertEqual(
            database.search_by_title(
                "Direct speech-to-speech translation with discrete units"
            )[0][0],
            "Lee, Ann and Chen, Peng-Jen and Wang, Changhan and Gu, Jiatao and Ma, Xutai and Polyak, A. and Adi, Yossi and He, Qing and Tang, Yun and Pino, J. and Hsu, Wei-Ning",
        )
        self.assertRaises(KeyError, database.search_by_title, "blub")

    def test_adding_and_removing(self):
        config_reader = ConfigReader("../../database.crypt", "postgresql", "../../key")
        database = DatabaseConnector(
            config_reader.db_config,
            logging.DEBUG,
            "database_tester_logger",
            log_file="db_connector_test.log",
        )
        self.assertTrue(
            database.add_entry_to_db(
                "test",
                ["list"],
                "x",
                "This is a test",
                "This is a test",
            )
        )
        self.assertRaises(ValueError, database.add_entry_to_db,
                "test",
                ["list"],
                "x",
                "This is a test",
                "This is a test",
            )
        database.delete_entry_from_database(
            "test",
            ["list"],
            "x",
            "This is a test",
            "This is a test",
        )


if __name__ == "__main__":
    unittest.main()
