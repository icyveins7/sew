import unittest
from ._helpers import *
import time
import numpy as np
import itertools

class TestBenchmarks(unittest.TestCase):
    def setUp(self):
        self.d = sew.Database(":memory:")
        self.fmtspec = sew.FormatSpecifier(
            [
                ["col1", "REAL"],
                ["col2", "REAL"]
            ]
        )
        self.d.createTable(
            self.fmtspec.generate(),
            "benchmark"
        )
        self.d.reloadTables()
        # print("Running tests.benchmarks")

    # def tearDown(self):
    #     print("Completed tests.benchmarks")

    # def test_benchmarks_10000(self):
    #     length = 10000
    #     t1 = time.time()
    #     self.d['benchmark'].insertMany(
    #         ((i, i+1) for i in range(length)),
    #         commitNow=True
    #     )
    #     t2 = time.time()
    #     print("%d generator inserts at %f/s." % (length, length/(t2-t1)))

    #     # Don't actually need to assert anything

    def test_benchmarks_1000000(self):
        length = 1000000
        t1 = time.time()
        self.d['benchmark'].insertMany(
            ((i, i+1) for i in range(length)),
            commitNow=True
        )
        t2 = time.time()
        print("%d generator inserts at %f/s." % (length, length/(t2-t1)))
        
        # Don't actually need to assert anything

    def test_benchmarks_arr1000000(self):
        length = 1000000
        data = np.random.rand(2, length)

        # This is signficantly slower, regardless of the method of iteration
        t1 = time.time()
        self.d['benchmark'].insertMany(
            np.nditer([data[0,:],data[1,:]], op_flags=[['readonly'],['readonly']]),
            # zip(data[0,:], data[1,:]),
            # ((data[0,i], data[1,i]) for i in range(length)), # All no significant change
            commitNow=True
        )
        t2 = time.time()
        print("%d array reference inserts at %f/s." % (length, length/(t2-t1)))

        # What if we transpose first? No difference..
        data = np.ascontiguousarray(data.T)
        t1 = time.time()
        
        self.d['benchmark'].insertMany(
            np.nditer([data[:,0], data[:,1]], op_flags=[['readonly'],['readonly']]),
            # data, # nditer offers no speedup
            commitNow=True
        )
        t2 = time.time()
        print("%d array (transposed) reference inserts at %f/s." % (length, length/(t2-t1)))

        # Don't actually need to assert anything