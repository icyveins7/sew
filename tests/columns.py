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
 
