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
                ["col2", "real"],
                ["col3", "real"]
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

    #%%
    def test_insert_simple(self):
        rows = [(10.0, 20.0, 30.0), (30.0,40.0,50.0)]
        self.d['correctness'].insertMany(
            rows, commitNow=True
        )
        # Check selected values
        self.d['correctness'].select("*")
        results = self.d.fetchall()
        for i, result in enumerate(results):
            for k in range(3):
                self.assertEqual(rows[i][k], result[k])

    #%%
    def test_insert_generators(self):
        data1 = np.array([10.0, 30.0])
        data2 = np.array([30.0, 40.0])
        data3 = np.array([50.0, 60.0])
        self.d['correctness'].insertMany(
            ((data1[i], data2[i], data3[i]) for i in range(len(data1))),
            commitNow=True
        )
        # Check selected values
        self.d['correctness'].select("*")
        results = self.d.fetchall()
        for i, result in enumerate(results):
            self.assertEqual(data1[i], result[0])
            self.assertEqual(data2[i], result[1])
            self.assertEqual(data3[i], result[2])


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

    #%%
    def test_redirect(self):
        self.assertEqual(
            self.d.cur.execute, self.d.execute,
            "execute redirect has failed")

    #%%
    def test_makeSelectStatement(self):
        tablename = "tablename"
        columnNames = ["col1", "col2"]
        conditions = ["col1 > ?", "col2 > ?"]
        orderBy = "col1 desc"
        stmt1 = self.d._makeSelectStatement(columnNames, tablename, encloseTableName=False)
        stmt2 = self.d._makeSelectStatement(columnNames, tablename, conditions, orderBy, encloseTableName=False)
        self.assertEqual(
            stmt1, "select col1,col2 from tablename",
            "select statement is incorrect"
        )
        self.assertEqual(
            stmt2, "select col1,col2 from tablename where col1 > ? and col2 > ? order by col1 desc",
            "select statement with conditions and ordering is incorrect"
        )

    #%%
    def test_uniqueness_throws(self):
        with self.assertRaises(sq.IntegrityError):
            self.d['correctness'].insertMany(
                [(0,1,2),(0,1,2)],
                orReplace=False
            )

    #%%
    def test_insert_requires_all_columns(self):
        with self.assertRaises(sq.ProgrammingError):
            self.d['correctness'].insertMany(
                ((i,) for i in range(2))
            )

        with self.assertRaises(sq.ProgrammingError):
            self.d['correctness'].insertOne(
                0.
            )

    #%%
    def test_insertOne_throws_if_enclosed(self):
        with self.assertRaises(TypeError):
            self.d['correctness'].insertOne((10.0, 20.0, 30.0))

    #%%
    def test_insert_namedcolumns(self):
        stmt = self.d['correctness'].insertOne(
            {
                'col1': 22.0,
                'col3': 44.0
            },
            commitNow=True
        )
        self.d['correctness'].select("*")
        # Ensure that the missing column comes back as None
        result = self.d.fetchone()
        self.assertEqual(result['col1'], 22.0)
        self.assertEqual(result['col2'], None)
        self.assertEqual(result['col3'], 44.0)

    #%%
    def test_makeCaseStatements(self):
        singlecase = self.d._makeCaseSingleConditionVariable(
            'col1', 
            [
                ['5', 'NULL'],
                ['10', '20']
            ],
            '-1'
        )
        checksinglecase = "CASE col1\n" \
            "WHEN 5 THEN NULL\n" \
            "WHEN 10 THEN 20\n" \
            "ELSE -1\n" \
            "END"
        self.assertEqual(singlecase, checksinglecase)

        multiplecase = self.d._makeCaseMultipleConditionVariables(
            [
                ['col1 < 5', 'NULL'],
                ['col2 > 10', '20']
            ],
            "-1"
        )
        checkmultiplecase = "CASE\n" \
            "WHEN col1 < 5 THEN NULL\n" \
            "WHEN col2 > 10 THEN 20\n" \
            "ELSE -1\n" \
            "END"
        self.assertEqual(multiplecase, checkmultiplecase)


    #%%
    def test_insertMany_namedcolumns(self):
        dictlist = [
            {'col1': 22.0, 'col3': 44.0},
            {'col1': 33.0, 'col3': 55.0}
        ]
        stmt = self.d['correctness'].insertManyNamedColumns(
            dictlist,
            commitNow=True
        )
        self.d['correctness'].select("*")
        # Ensure that the missing column comes back as None
        results = self.d.fetchall()
        for i, result in enumerate(results):
            self.assertEqual(result['col1'], dictlist[i]['col1'])
            self.assertEqual(result['col2'], None)
            self.assertEqual(result['col3'], dictlist[i]['col3'])

    #%%
    def test_createView(self):
        # Create a view with renames and amendments within select
        self.d['correctness'].createView(
            ["col1 as A", "(col2+10) as B"], # Warp the column value in the view too
        )
        # Reload to see the new view
        self.d.reloadTables()

        # Insert some data
        data = [(10.0, 20.0, 30.0), (30.0,40.0,50.0)]
        self.d['correctness'].insertMany(
            data
        )

        # Attempt to select from the new view
        self.d['correctness_view'].select("*")
        results = self.d.fetchall()
        for i, result in enumerate(results):
            self.assertIn("A", result.keys(), "A is a view column")
            self.assertIn("B", result.keys(), "B is a view column")
            self.assertEqual(
                result['A'], data[i][0]
            )
            self.assertEqual(
                result['B'], data[i][1] + 10 # Check the warped value
            )

    #%%
    def test_numeric_affinity(self):
        # Create a new table with NUMERIC affinity
        tblfmt = sew.FormatSpecifier(
            [
                ["col1", "NUMERIC"]
            ]
        )
        stmt = self.d.createTable(
            tblfmt.generate(), "tablenumeric")
        self.d.reloadTables()

        # Insert some data
        data = [(10,), (30.0,)]
        stmt = self.d["tablenumeric"].insertMany(
            data,
            commitNow=True
        )

        # Check results
        self.d["tablenumeric"].select("*")
        results = self.d.fetchall()
        for i, result in enumerate(results):
            self.assertEqual(result['col1'], data[i][0])

    #%% ==================================== PLUGINS ==================================== #
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

    #%%
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


if __name__ == "__main__":
    unittest.main()