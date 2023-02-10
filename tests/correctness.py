# Load all imports using helper
from ._helpers import *

import unittest
import sqlite3 as sq
import numpy as np

#%%
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
        # print("Running tests.correctness")

    # def tearDown(self):
    #     print("Completed tests.correctness")

    def test_insert_simple(self):
        self.d['correctness'].insertMany(
            [(10.0, 20.0), (30.0,40.0)]
        )

    #%%
    def test_create_metadata(self):
        # First check that it throws if tablename or columns are wrong
        metaFmtspec = sew.FormatSpecifier(
            [
                ["data_tblname", "TEXT"],
                ["setting1", "INT"],
                ["setting2", "REAL"]
            ],
            []
        )

        with self.assertRaises(ValueError):
            self.d.createMetaTable(
                metaFmtspec.generate(),
                "wrong_tablename"
            )

        with self.assertRaises(ValueError):
            self.d.createMetaTable(
                self.fmtspec.generate(),
                "tbl_metadata"
            )

        # Now actually create a legitimate one
        self.d.createMetaTable(
            metaFmtspec.generate(),
            "tbl_metadata"
        )
        self.d.reloadTables()

        # Test creating a data table, make sure it throws if the associated meta table doesn't exist
        metadata = [5, 0.2]
        with self.assertRaises(ValueError):
            self.d.createDataTable(
                self.fmtspec.generate(),
                "mydata_table",
                metadata,
                "nonexistent_metadata"
            )

        # Now actually create the data table
        self.d.createDataTable(
            self.fmtspec.generate(),
            "mydata_table",
            metadata,
            "tbl_metadata"
        )
        self.d.reloadTables()

        # Check the list of data tables associated with this metadata table
        datatables = self.d['tbl_metadata'].getDataTables()
        self.assertListEqual(
            datatables,
            ["mydata_table"]
        )

        # Check that the data row exists inside metadata table
        metadataresult = self.d['tbl_metadata'].getMetadataFor(
            'mydata_table'
        )
        self.assertListEqual(
            metadata,
            [i for i in metadataresult]
        )

        # Also check that we can retrieve it from the data table
        metadataresultFromDataTable = self.d['mydata_table'].getMetadata()
        self.assertListEqual(
            metadata,
            [i for i in metadataresultFromDataTable]
        )

        # If this passes then by definition we should be able to see the classes were upgradecd correctly
        self.assertIsInstance(
            self.d['tbl_metadata'],
            sew.MetaTableProxy
        )
        self.assertIsInstance(
            self.d['mydata_table'],
            sew.DataTableProxy
        )


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
        with self.assertRaises(sq.ProgrammingError):
            self.d['correctness'].insertMany(
                [(0,1),(0,1)],
                orReplace=False
            )

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

        nd['nptable'].insertMany(
            data_f64, data_f32, commitNow=True
        )
        stmt = nd['nptable'].select("*")
        
        results = nd['nptable'].fetchAsNumpy()
        
        np.testing.assert_equal(data_f64, results['col1_f64'])
        np.testing.assert_equal(data_f32, results['col2_f32'])

    def test_numpy_insertOne_throws(self):
        data_f64 = np.random.randn(1).astype(np.float64)
        data_f32 = np.random.randn(1).astype(np.float32)

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

        with self.assertRaises(NotImplementedError):
            nd['nptable'].insertOne(
                data_f64, data_f32, commitNow=True
            )
