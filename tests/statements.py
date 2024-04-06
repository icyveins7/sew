# Load all imports using helper
from ._helpers import *

import unittest
import sqlite3 as sq
import numpy as np

'''
Explicitly test the statement generators separately,
and the FormatSpecifier parsers, since these are linked to the internal
structure we keep.
'''


class TestStatements(unittest.TestCase):

    def test_makeAlterTableStatements(self):
        # Check ADD COLUMN
        stmt = sew.StatementGeneratorMixin._makeAlterTableAddColumnStatement(
            "mytbl", ['col1', 'int']
        )
        self.assertEqual(
            stmt,
            'alter table "mytbl" add column col1 int'
        )

        # Check DROP COLUMN
        stmt = sew.StatementGeneratorMixin._makeAlterTableDropColumnStatement(
            "mytbl", ['col1', 'real'],  # type doesn't matter here
            encloseTableName=False
        )
        self.assertEqual(
            stmt,
            "alter table mytbl drop column col1"
        )

    # Test the column description parser for extracted sql
    # This is one of the most basic extractions of the create table statement
    def test_col_desc_parse(self):
        # Parse a single column description
        desc = ' col1 INTEGER   '  # Add some blanks
        self.assertEqual(
            sew.FormatSpecifier._parseColumnDesc(desc),
            ['col1', 'INTEGER']
        )
        # The same thing but without a type
        desc = ' col1   '
        self.assertEqual(
            sew.FormatSpecifier._parseColumnDesc(desc),
            ['col1', '']
        )
        # Same thing but as INTEGER PRIMARY KEY
        desc = ' col1 INTEGER PRIMARY KEY   '
        self.assertEqual(
            sew.FormatSpecifier._parseColumnDesc(desc),
            ['col1', 'INTEGER PRIMARY KEY']
        )

    # Test the create table splitter

    def test_table_sql_splitter(self):
        # Make something simple
        desc = ' col1 INTEGER, col2, col3 real  '  # With some blanks
        cols, conds, fks = sew.FormatSpecifier._splitColumnsSql(desc)
        self.assertEqual(
            cols,
            [
                ["col1", "INTEGER"],
                ["col2", ""],
                ["col3", "real"]
            ]
        )
        self.assertEqual(
            conds,
            []
        )
        self.assertEqual(
            fks,
            []
        )

        # Make something with some uniques
        desc = ' col1 INTEGER, col2, col3 real, UNIQUE(col1, col2)  '
        cols, conds, fks = sew.FormatSpecifier._splitColumnsSql(desc)
        self.assertEqual(
            cols,
            [
                ["col1", "INTEGER"],
                ["col2", ""],
                ["col3", "real"]
            ]
        )
        self.assertEqual(
            conds,
            ["UNIQUE(col1, col2)"]
        )
        self.assertEqual(
            fks,
            []
        )

        # Make something with uniques and foreign keys
        desc = 'col1 INTEGER, col2, col3 real, UNIQUE(col1, col2), FOREIGN KEY(col1) REFERENCES parent_table(parentcolA)  '
        cols, conds, fks = sew.FormatSpecifier._splitColumnsSql(desc)
        self.assertEqual(
            cols,
            [
                ["col1", "INTEGER"],
                ["col2", ""],
                ["col3", "real"]
            ]
        )
        self.assertEqual(
            conds,
            ["UNIQUE(col1, col2)"]
        )
        self.assertEqual(
            fks,
            [
                ["col1", "parent_table(parentcolA)"]
            ]
        )

        # Use all components more than once, with spaces and newlines as well
        desc = """
            col1 INTEGER,
            col2,
            col3 real,
            col4,
            UNIQUE(col1, col2),
            UNIQUE(col3, col4),
            FOREIGN KEY(col1) REFERENCES parent_table(parentcolA),
            FOREIGN KEY(col3) REFERENCES parent_table(parentcolB)
        """
        cols, conds, fks = sew.FormatSpecifier._splitColumnsSql(desc)
        self.assertEqual(
            cols,
            [
                ["col1", "INTEGER"],
                ["col2", ""],
                ["col3", "real"],
                ["col4", ""]
            ]
        )
        self.assertEqual(
            conds,
            ["UNIQUE(col1, col2)", "UNIQUE(col3, col4)"]
        )
        self.assertEqual(
            fks,
            [
                ["col1", "parent_table(parentcolA)"],
                ["col3", "parent_table(parentcolB)"]
            ]
        )


if __name__ == "__main__":
    unittest.main()
