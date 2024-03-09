# Load all imports using helper
from ._helpers import *

import unittest
import sqlite3 as sq
import numpy as np


class TestPlugins(unittest.TestCase):
    #%%
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

#%%
if __name__ == "__main__":
    unittest.main()