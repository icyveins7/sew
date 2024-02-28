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
        # self.d.reloadTables()

    #%%
    def test_create_table_insert_drop(self):
        fmtspec = sew.FormatSpecifier()
        fmtspec.addColumn('c1', int)
        fmtspec.addColumn('c2', float)
        self.d.createTable(fmtspec.generate(),
                           'tbl')
        self.assertIn("tbl", self.d.tables)
        tbl = self.d['tbl']
        tbl.insertOne(
            10, 20.0
        )

        row = tbl[0]
        self.assertEqual(row['c1'], 10)
        self.assertEqual(row['c2'], 20.0)

        self.d.dropTable('tbl')
        self.assertNotIn("tbl", self.d.tables)


    #%%
    def test_formatSpecifier_getter(self):
        self.assertEqual(
            self.fmtspec.generate(),
            self.d["correctness"].formatSpecifier
        )

    #%%
    def test_insert_simple_and_delete(self):
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

        # Delete one of the rows
        self.d["correctness"].delete(
            ["col2=20.0"], commitNow=True # This should remove one of the rows
        )
        # Check again
        self.d["correctness"].select("*")
        results = self.d.fetchall()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], rows[1][0])
        self.assertEqual(results[0][1], rows[1][1])
        self.assertEqual(results[0][2], rows[1][2])
        


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

    #%%
    def test_basic_foreignkey(self):
        # Create a parent table
        parentfmt = sew.FormatSpecifier(
            [
                ["id", "INTEGER PRIMARY KEY"],
                ["val", "INTEGER"]
            ]
        )
        self.d.createTable(parentfmt.generate(), "parent")

        # Create a table with a foreign key
        fkfmt = sew.FormatSpecifier(
            [
                ["col1", "INTEGER"],
                ["col2", "INTEGER"]
            ],
            foreign_keys=[
                ["col2", "parent(id)"]
            ]
        )
        self.d.createTable(fkfmt.generate(), "child")

        self.d.reloadTables()

        # Insert into parent
        self.d["parent"].insertMany(
            [(1,2),(2,4)], commitNow=True
        )
        # Show that inserting into child fails if you don't use null
        with self.assertRaises(sq.IntegrityError):
            stmt = self.d["child"].insertOne(
                {"col1": 1, "col2": 10}, commitNow=True
            )
        # Show that inserting null is okay
        stmt = self.d["child"].insertOne(
            {"col1": 1, "col2": None}, commitNow=True
        )
        # Check that it returns as expected
        self.d["child"].select("*")
        result = self.d.fetchone()
        self.assertEqual(result['col1'], 1)
        self.assertEqual(result['col2'], None)

        # Insert one that actually references
        self.d["child"].insertOne(
            {"col1": 2, "col2": 2}, commitNow=True
        )

        # Then now show that removing from parent is not allowed
        with self.assertRaises(sq.IntegrityError):
            self.d["parent"].delete(
                ["id=2"], commitNow=True
            )
        # But removing the other one should be okay
        stmt = self.d["parent"].delete(
            ["id=1"], commitNow=True
        )
        self.d.execute(stmt)
        self.d.commit()

    #%%
    def test_foreignkey_parent_retrieval(self):
        # Create a parent table
        parentfmt = sew.FormatSpecifier(
            [
                ["id", "INTEGER PRIMARY KEY"],
                ["val", "INTEGER"]
            ]
        )
        self.d.createTable(parentfmt.generate(), "parent")

        # Create a table with a foreign key
        fkfmt = sew.FormatSpecifier(
            [
                ["col1", "INTEGER"],
                ["col2", "INTEGER"]
            ],
            foreign_keys=[
                ["col2", "parent(id)"]
            ]
        )
        self.d.createTable(fkfmt.generate(), "child")

        self.d.reloadTables()

        # Insert multiple rows into parent
        self.d["parent"].insertMany(
            [(1,2),(2,3)], commitNow=True
        )

        # Check parent results
        self.d["parent"].select("*")
        results = self.d.fetchall()
        # for result in results:
        #     print(dict(result))

        # Insert into child
        self.d["child"].insertOne(
            1, 1, commitNow=True
        )

        # Retrieve the child row
        self.d["child"].select("*")
        result = self.d.fetchone()
        # Retrieve an associated parent row
        self.d["child"].retrieveParentRow(result)
        parentResults = self.d.fetchall()
        for result in parentResults: # There should be only 1 result anyway
            self.assertEqual(result['id'], 1)
            self.assertEqual(result['val'], 2)

    #%%
    def test_multiple_foreignkeys(self):
         # Create a parent table
        parentfmt = sew.FormatSpecifier(
            [
                ["id", "INTEGER PRIMARY KEY"],
                ["val", "INTEGER"]
            ]
        )
        self.d.createTable(parentfmt.generate(), "parent")

        # Create another parent
        otherparentfmt = sew.FormatSpecifier(
            [
                ["id", "INTEGER PRIMARY KEY"],
                ["val", "INTEGER"]
            ]
        )
        self.d.createTable(otherparentfmt.generate(), "otherparent")

        # Create a table with two separate foreign keys
        fkfmt = sew.FormatSpecifier(
            [
                ["col1", "INTEGER"],
                ["col2", "INTEGER"],
                ["col3", "INTEGER"]
            ],
            foreign_keys=[
                ["col2", "parent(id)"],
                ["col3", "otherparent(id)"]
            ]
        )
        self.d.createTable(fkfmt.generate(), "child")

        self.d.reloadTables()

        # Insert into both parents
        self.d["parent"].insertMany(
            [(1,2),(2,3)], commitNow=True
        )
        self.d["otherparent"].insertMany(
            [(4,8),(5,10)], commitNow=True
        )

        # Then insert into the child
        self.d["child"].insertOne(
            1, 1, 4, commitNow=True
        )

        # Extract from the child
        self.d["child"].select("*")
        childResult = self.d.fetchone()

        # Extract the related parents
        self.d["child"].retrieveParentRow(childResult, "col2")
        parentResults = self.d.fetchall()
        for result in parentResults: # There should be only 1 result anyway
            self.assertEqual(result['id'], 1)
            self.assertEqual(result['val'], 2)

        self.d["child"].retrieveParentRow(childResult, "col3")
        otherparentResults = self.d.fetchall()
        for result in otherparentResults: # There should be only 1 result anyway
            self.assertEqual(result['id'], 4)
            self.assertEqual(result['val'], 8)

    #%%
    def test_foreignkey_relationships(self):
        # Create a bunch of parent tables
        parentfmt = sew.FormatSpecifier(
            [
                ["id", "INTEGER PRIMARY KEY"],
                ["val", "INTEGER"]
            ]
        )
        self.d.createTable(parentfmt.generate(), "parent1")
        self.d.createTable(parentfmt.generate(), "parent2")
        self.d.createTable(parentfmt.generate(), "parent3")

        # Create a bunch of child tables
        childfmt12 = sew.FormatSpecifier(
            [
                ["col1", "INTEGER"],
                ["col2", "INTEGER"],
                ["col3", "INTEGER"]
            ],
            foreign_keys=[
                ["col1", "parent1(id)"],
                ["col2", "parent2(id)"]
            ]
        )
        self.d.createTable(childfmt12.generate(), "child12")

        childfmt23 = sew.FormatSpecifier(
            [
                ["col1", "INTEGER"],
                ["col2", "INTEGER"],
                ["col3", "INTEGER"]
            ],
            foreign_keys=[
                ["col2", "parent2(id)"],
                ["col3", "parent3(id)"],
                ["col1", "parent3(id)"]
            ]
        )
        self.d.createTable(childfmt23.generate(), "child23")

        self.d.reloadTables()

        # Test the relationship dictionary
        families = self.d.relationships

        # Check for parent1
        self.assertTrue(
            ("child12", "col1") in families[("parent1", "id")]
        )

        # Check for parent2 (two children in diff tables to same parent)
        self.assertTrue(
            ("child23", "col2") in families[("parent2", "id")]
        )
        self.assertTrue(
            ("child12", "col2") in families[("parent2", "id")]
        )

        # Check for parent3 (two children in same table to same parent)
        self.assertTrue(
            ("child23", "col3") in families[("parent3", "id")]
        )
        self.assertTrue(
            ("child23", "col1") in families[("parent3", "id")]
        )

    #%%
    def test_table_bracket_access(self):
        self.d['correctness'].insertMany(
            [(i,i+1,i+2) for i in range(10)], commitNow=True
        )

        # Test a single row
        result = self.d['correctness'][3]
        self.assertEqual(result['col1'], 3)
        self.assertEqual(result['col2'], 4)
        self.assertEqual(result['col3'], 5)

        # Test slices
        results = self.d['correctness'][0:5]

        for i, result in enumerate(results):
            self.assertEqual(result['col1'], i)
            self.assertEqual(result['col2'], i+1)
            self.assertEqual(result['col3'], i+2)

        midresults = self.d['correctness'][5:8]
        for i, result in enumerate(midresults):
            self.assertEqual(result['col1'], i+5)
            self.assertEqual(result['col2'], i+6)
            self.assertEqual(result['col3'], i+7)

        # Test error if steps are provided
        with self.assertRaises(ValueError):
            self.d['correctness'][2:7:2]
    
    #%%
    def test_column_proxy_container(self):
        table = self.d['correctness']
        self.assertTrue(
            isinstance(table.columns, dict)
        )
        container = sew.ColumnProxyContainer(table.columns)
        # Check that we can access it like an attribute directly
        # print(container.col1)
        # print(table.columns['col1'])
        # ColumnProxy has an == override, but the object is identical so we use 'is' to compare
        self.assertIs(container.col1, table.columns['col1'])
        self.assertIs(container.col2, table.columns['col2'])
        self.assertIs(container.col3, table.columns['col3'])


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