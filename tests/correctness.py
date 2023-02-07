# Load all imports using helper
from ._helpers import *

import unittest
import sqlite3 as sq
import numpy as np

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

    # def test_uniqueness_throws(self):
    #     self.d['correctness'].insertMany(
    #         [(0,1),(0,1)],
    #         orReplace=False
    #     )
    #     # TODO: complete this test

    def test_insert_requires_all_columns(self):
        with self.assertRaises(sq.ProgrammingError):
            self.d['correctness'].insertMany(
                ((i,) for i in range(2))
            )

        with self.assertRaises(sq.ProgrammingError):
            self.d['correctness'].insertOne(
                0.
            )

        
    def test_numpy_plugin(self):
        data_f64 = np.random.randn(100).astype(np.float64)
        data_f32 = np.random.randn(100).astype(np.float32)

        nd = sew.plugins.NumpyDatabase(":memory:")
        npfmtspec = sew.FormatSpecifier(
            [
                ["col1_f64", "real"],
                ["col2_f32", "real"]
            ]
        )

        nd.createTable(
            npfmtspec.generate(),
            "nptable"
        )
        nd.reloadTables()
        # nd['nptable'].insertMany( # float32s need to be cast as float64s or else python's sqlite doesn't parse them properly
        #     ((data_f64[i], data_f32[i].astype(np.float64)) for i in range(data_f64.size)),
        #     commitNow=True
        # )
        _a = nd['nptable']._numpyCast(data_f64,data_f32)
        for i in _a:
            print(i)
        

        nd['nptable'].insertMany(
            data_f64, data_f32, commitNow=True
        )
        stmt = nd['nptable'].select("*")
        print(stmt)
        results = nd['nptable'].fetchAsNumpy()
        print(results)
        # print(results[0])
        # print(results[1])
