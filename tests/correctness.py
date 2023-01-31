# Load all imports using helper
from ._helpers import *

import unittest
import sqlite3 as sq

class TestCorrectness(unittest.TestCase):
    def setUp(self):
        self.d = sew.Database(":memory:")
        self.fmtspec = sew.FormatSpecifier(
            [
                ["col1", "real"],
                ["col2", "real"]
            ],
            ["UNIQUE(col1, col2)"]
        )
        self.d.createTable(
            self.fmtspec.generate(),
            "correctness"
        )
        self.d.reloadTables()
        print("Running tests.correctness")

    def tearDown(self):
        print("Completed tests.correctness")

    def test_redirect(self):
        self.assertEqual(
            self.d.cur.execute, self.d.execute,
            "execute redirect has failed")

    def test_makeSelectStatement(self):
        tablename = "tablename"
        columnNames = ["col1", "col2"]
        conditions = ["col1 > ?", "col2 > ?"]
        orderBy = "col1 desc"
        stmt1 = self.d._makeSelectStatement(columnNames, tablename)
        stmt2 = self.d._makeSelectStatement(columnNames, tablename, conditions, orderBy)
        self.assertEqual(
            stmt1, "select col1,col2 from tablename",
            "select statement is incorrect"
        )
        self.assertEqual(
            stmt2, "select col1,col2 from tablename where col1 > ? and col2 > ? order by col1 desc",
            "select statement with conditions and ordering is incorrect"
        )

    def test_uniqueness_throws(self):
        self.d['correctness'].insertMany(
            [(0,1),(0,1)],
            orReplace=False
        )
        # TODO: complete this test

    def test_insert_requires_all_columns(self):
        with self.assertRaises(sq.ProgrammingError):
            self.d['correctness'].insertMany(
                ((i,) for i in range(2))
            )

        with self.assertRaises(sq.ProgrammingError):
            self.d['correctness'].insertOne(
                0.
            )

        