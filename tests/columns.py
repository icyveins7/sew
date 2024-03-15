# Load all imports using helper
from ._helpers import *

import unittest

#%%
class TestColumnProxy(unittest.TestCase):
    def test_columnProxy_properties(self):
        # Create a simple one
        proxy = sew.ColumnProxy("col1", "int", "mytbl")
        self.assertEqual(proxy.name, "col1")
        self.assertEqual(proxy.typehint, "int")
        self.assertEqual(proxy.tablename, "mytbl")
        self.assertEqual(str(proxy), "col1 int")


    def test_columnProxyContainer(self):
        # Just add some and reference
        proxy = sew.ColumnProxy("col1", "int", "mytbl")
        proxy2 = sew.ColumnProxy("col2", "int", "mytbl")
        proxy3 = sew.ColumnProxy("col3", "int", "mytbl")
        proxylist = [proxy, proxy2, proxy3]
        container = sew.ColumnProxyContainer(proxylist)

        self.assertEqual(container.col1, proxy)
        self.assertEqual(container.col2, proxy2)
        self.assertEqual(container.col3, proxy3)

        # Check that they should all belong to same table
        proxylist.append(
            sew.ColumnProxy("col4", "int", "mytbl2")
        )
        with self.assertRaises(ValueError):
            sew.ColumnProxyContainer(proxylist)
