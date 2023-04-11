#! /usr/bin/env python3

"""
Tests functionality of :class: `paper_sorts.DatabaseConnector`.
"""

import unittest
import logging

from paper_sorts.database_connector import DatabaseConnector
from paper_sorts.config_reader import ConfigReader


class DataBaseTest(unittest.TestCase):
    """
    Tests adding, searching, updating and deleting functionality, but does not offer a significant amount of code coverage.
    """
    def test_search_by_author(self):
        """ Test if a entry know to be in the database can be found if searched for by author name."""
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
        self.assertRaises(KeyError, database.search_by_author, "no author")

    def test_search_by_title(self):
        """ Test if a entry know to be in the database can be found if searched for by publication title."""
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
        self.assertRaises(KeyError, database.search_by_title, "no title")

    def test_adding_and_removing(self):
        """Test whether an entry can be added and removed from the database safely. """
        config_reader = ConfigReader("../../database.crypt", "postgresql", "../../key")
        database = DatabaseConnector(
            config_reader.db_config,
            logging.DEBUG,
            "database_tester_logger",
            log_file="db_connector_test.log",
        )

        self.assertRaises(
            ValueError,
            database.delete_paper_entry_from_database,
            "test",
            ["list"],
            "x",
            "This is a test",
            "This is a test",
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
        self.assertRaises(
            ValueError,
            database.add_entry_to_db,
            "test",
            ["list"],
            "x",
            "This is a test",
            "This is a test",
        )
        self.assertTrue(
            database.delete_paper_entry_from_database(
                "test",
                ["list"],
                "x",
                "This is a test",
                "This is a test",
            )
        )

    def test_update_title(self):
        """Test whether the summary of an entry in the database can be updated safely. """
        config_reader = ConfigReader("../../database.crypt", "postgresql", "../../key")
        database = DatabaseConnector(
            config_reader.db_config,
            logging.DEBUG,
            "database_tester_logger",
            log_file="db_connector_test.log",
        )
        database.add_entry_to_db(
            "test",
            ["list"],
            "x",
            "This is a test",
            "This is a test",
        )
        paper_id = database.database_handler.fetch_from_db(
            "select id from papers where title='This is a test';"
        )[0][0]
        database.update_entry(
            "title",
            "updated title",
            "papers",
            paper_id
        )
        self.assertEqual(
            database.search_by_title(
            "updated title"
            )[0][0],
            "list",
        )
        database.update_entry(
            "contents",
            "updated contents",
            "papers",
            paper_id
        )
        self.assertEqual(
            database.database_handler.fetch_from_db(
                "select contents from papers where id=%s;",
                (paper_id, )
            )[0][0],
            "updated contents"
        )
        database.delete_paper_entry_from_database(
            "test",
            ["list"],
            "x",
            "updated title",
            "updated contents",
        )
        self.assertRaises(
           ValueError,
           database.update_entry,
            "test",
            "should not work",
            "papers",
            "This is a test",
        )
        self.assertRaises(
           ValueError,
           database.update_entry,
            "test",
            "should not work",
            "non-table",
            "This is a test",
        )

    def test_update_authors_papers(self):
        """ Test whether the author-paper relation of an entry cannot be changed."""
        config_reader = ConfigReader("../../database.crypt", "postgresql", "../../key")
        database = DatabaseConnector(
            config_reader.db_config,
            logging.DEBUG,
            "database_tester_logger",
            log_file="db_connector_test.log",
        )
        self.assertRaises(
           ValueError,
           database.update_entry,
            "test",
            "should not work",
            "authors_papers",
            "This is a test",
        )

    def test_update_authors(self):
        """ Test whether the authorship of a paper can be changed as expected.."""
        config_reader = ConfigReader("../../database.crypt", "postgresql", "../../key")
        database = DatabaseConnector(
            config_reader.db_config,
            logging.DEBUG,
            "database_tester_logger",
            log_file="db_connector_test.log",
        )

        database.add_entry_to_db(
            "test",
            ["list"],
            "x",
            "This is a test",
            "This is a test",
        )

        author_id = database.database_handler.fetch_from_db(
                "select id from authors_id where author='list'",
        )[0][0]
        papers = database.database_handler.fetch_from_db(
                "select paper_id from authors_papers where author_id=%s;",
                (author_id, )
        )
        database.update_entry(
            "author",
            "changed_authors",
            "authors_id",
            "list"
        )
        author_id = database.database_handler.fetch_from_db(
                "select id from authors_id where author='changed_authors'",
        )[0][0]

        self.assertEqual(
            database.database_handler.fetch_from_db(
                "select  paper_id from authors_papers where author_id=%s;",
                (author_id, )
            ),
            papers
        )
        self.assertTrue(
            database.delete_paper_entry_from_database(
                "test",
                ["changed_authors"],
                "x",
                "This is a test",
                "This is a test",
            )
        )
        database.add_entry_to_db(
            "test",
            ["list"],
            "x",
            "This is a test",
             "This is a test",
        )
        database.add_entry_to_db(
            "another test",
            ["new list"],
            "u",
            "This is a another test",
            "something",
        )
        database.update_entry(
            "author",
            "new list",
            "authors_id",
            "list"
        )
        self.assertTrue(
            database.delete_paper_entry_from_database(
                "test",
                ["new list"],
                "x",
                "This is a test",
                "This is a test",
            )
        )
        self.assertTrue(
            database.delete_paper_entry_from_database(
                "another test",
                ["new list"],
                "u",
                "This is a another test",
                "something",
            )
        )
        self.assertRaises(
            ValueError,
            database.update_entry,
            "nonexistent column",
            "new list",
            "authors_id",
            "list"
        )

    def test_update_bib(self):
        """ Test whether the content of one bibliography entry can be changed. """
        config_reader = ConfigReader("../../database.crypt", "postgresql", "../../key")
        database = DatabaseConnector(
            config_reader.db_config,
            logging.DEBUG,
            "database_tester_logger",
            log_file="db_connector_test.log",
        )
        database.add_entry_to_db(
            "test",
            ["list"],
            "x",
            "This is a test",
            "This is a test",
        )
        database.update_entry(
            "bibtex",
            "y",
            "bib",
            "x"
        )
        self.assertEqual(
            database.database_handler.fetch_from_db(
            "select  bibtex from bib where bibtex_id='x';"
            )[0][0],
            "y"
        )
        self.assertRaises(
            ValueError,
            database.update_entry,
            "bibtex",
            "y",
            "bib",
            "x"
        )
        self.assertTrue(
            database.delete_paper_entry_from_database(
                "y",
                ["new list"],
                "x",
                "This is a test",
                "This is a test",
            )
        )
        self.assertRaises(
            ValueError,
            database.update_entry,
            "nonexistent",
            "y",
            "bib",
            "x"
        )
if __name__ == "__main__":
    unittest.main()
