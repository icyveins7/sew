from ._helpers import *

import unittest
import numpy as np
import os

class TestBlobInterpreter(unittest.TestCase):
    def setUp(self):
        # Make database
        self.d = sew.Database(":memory:")

        self.fmt = sew.FormatSpecifier(
            [
                ["data", "BLOB"]
            ]
        )
        self.tablename = 'table1'
        self.d.createTable(self.fmt.generate(), self.tablename)
        self.d.reloadTables()

        # Insert some test data
        self.data = {
            'p1': np.array([3], np.uint8),
            'p2': np.array([123], np.int64),
            'p3': np.array([1142.2], np.float64)
        }

        toInsert = bytes()
        for k, v in self.data.items():
            toInsert += v.tobytes()
        self.d[self.tablename].insertOne(toInsert, commitNow=True)

    #%%
    def test_interpret_simple(self):
        # Define interpreter directly in code
        p = sew.blobInterpreter.BlobInterpreter(
            [('p1', 'u8'), ('p2', 'i64'), ('p3', 'f64')]
        )

        # Select from the table
        self.d[self.tablename].select("*")
        result = self.d.fetchone()['data']

        # Interpret it
        interpreted = p.interpret(result)

        # Compare
        for k in self.data:
            self.assertEqual(interpreted[k], self.data[k])

    #%%
    def test_interpret_config(self):
        # Construct interpreter from config file
        p = sew.blobInterpreter.BlobInterpreter.fromConfig(
            os.path.join(os.path.dirname(__file__), "blobcfg.ini"), 
            "test")

        # Select from the table
        self.d[self.tablename].select("*")
        result = self.d.fetchone()['data']

        # Interpret it
        interpreted = p.interpret(result)

        # Compare
        for k in self.data:
            self.assertEqual(interpreted[k], self.data[k])

    #%%
    def test_interpret_dictionary(self):
        # Construct interpreter from a dictionary
        structure = {
            'p1': 'u8',
            'p2': 'i64',
            'p3': 'f64'
        }
        p = sew.blobInterpreter.BlobInterpreter.fromDictionary(structure)

        # Select from the table
        self.d[self.tablename].select("*")
        result = self.d.fetchone()['data']

        # Interpret it
        interpreted = p.interpret(result)

        # Compare
        for k in self.data:
            self.assertEqual(interpreted[k], self.data[k])

    #%%
    def test_interpret_sqlitestatement(self):
        # Construct interpreter from dictionary
        structure = {
            'p1': 'u8',
            'p2': 'i64',
            'p3': 'f64'
        }
        p = sew.blobInterpreter.BlobInterpreter.fromDictionary(structure)

        # Now generate statement fragments that correspond to the different components
        # For now, don't stringify them
        stmtfrags = p.generateSplitStatement("data")
        # print(stmtfrags)

        # Select from the table using the fragments
        stmt = "select %s from %s" % (",".join(stmtfrags), self.tablename)
        # print(stmt)
        self.d.execute(stmt)
        result = self.d.fetchone()

        # Check result
        for key in result.keys(): # Remember, sqlite3.Row default iterator is not the keys, so specify .keys()
            self.assertEqual(
                result[key], 
                self.data[key].tobytes()
        )
        
        # Now let's try the stringified version
        stmtfrags = p.generateSplitStatement("data", hexOutput=True)
        
        # Select again
        stmt = "select %s from %s" % (",".join(stmtfrags), self.tablename)
        # print(stmt)
        self.d.execute(stmt)
        result = self.d.fetchone()

        # Check result, failing
        for key, value in self.data.items():
            self.assertEqual(
                result[key], 
                p.hexifyBlob(value.tobytes())
            )

        

#%%
if __name__ == '__main__':
    unittest.main()