from ._helpers import *

import unittest
import numpy as np
import os

class TestBlobInterpreter(unittest.TestCase):
    def setUp(self):
        # Make database
        self.d = sew.Database(":memory:")

        self.fmt = {
            'cols': [
                ["data", "BLOB"]
            ],
            'conds': []
        }
        self.tablename = 'table1'
        self.d.createTable(self.fmt, self.tablename)
        self.d.reloadTables()

        # Insert some test data
        self.data = {
            'p1': np.array([3], np.uint8),
            'p2': np.array([123 + 456j], np.complex128),
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
            [('p1', 'u8'), ('p2', 'fc64'), ('p3', 'f64')]
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
            'p2': 'fc64',
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
if __name__ == '__main__':
    unittest.main()