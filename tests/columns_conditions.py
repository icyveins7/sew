# Load all imports using helper
from ._helpers import *

import unittest

#%%
class TestColumnProxyContainer(unittest.TestCase):
    def test_columnProxyContainer_simple(self):
        column1 = sew.ColumnProxy("col1", "int", "mytbl")
        column2 = sew.ColumnProxy("col2", "int", "mytbl")
        container = sew.ColumnProxyContainer([column1, column2])

        # Retrieve by dict-like string
        self.assertEqual(column1, container['col1'])
        self.assertEqual(column2, container['col2'])

        # Retrieve by attribute
        self.assertEqual(column1, container.col1)
        self.assertEqual(column2, container.col2)


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

    def test_column_comparisons(self):
        col1 = sew.ColumnProxy("col1", "int", "mytbl")
        cond = col1 < 10
        self.assertEqual(str(cond), "col1 < 10")

        cond = col1 <= 10
        self.assertEqual(str(cond), "col1 <= 10")

        cond = col1 > 10
        self.assertEqual(str(cond), "col1 > 10")

        cond = col1 >= 10
        self.assertEqual(str(cond), "col1 >= 10")

        cond = col1 == 10
        self.assertEqual(str(cond), "col1 = 10")

        cond = col1 != 10
        self.assertEqual(str(cond), "col1 != 10")

    def test_column_composite_comparisons(self):
        col1 = sew.ColumnProxy("col1", "int", "mytbl")
        col2 = sew.ColumnProxy("col2", "int", "mytbl")

        cond1 = col1 < 10
        cond2 = col2 > 20
        self.assertIsInstance(
            cond1,
            sew.Condition
        )
        self.assertIsInstance(
            cond2,
            sew.Condition
        )

        # Testing AND
        comp = cond1 & cond2
        self.assertIsInstance(
            comp,
            sew.Condition
        )
        self.assertEqual(
            str(comp),
            "col1 < 10 AND col2 > 20"
        )

        # Testing OR
        comp = cond1 | cond2
        self.assertEqual(
            str(comp),
            "col1 < 10 OR col2 > 20"
        )

    def test_column_composite_comparisons_noncommutative(self):
        # Here we test 3 or more comparisons in a chain
        # First, 3 ORs
        cond1 = sew.Condition("col1 < 10")
        cond2 = sew.Condition("col2 < 10")
        cond3 = sew.Condition("col3 < 10")

        comp = cond1 | cond2 | cond3
        self.assertEqual(
            str(comp),
            "(col1 < 10 OR col2 < 10) OR col3 < 10"
        )

        # Now, 3 ANDs
        comp = cond1 & cond2 & cond3
        self.assertEqual(
            str(comp),
            "(col1 < 10 AND col2 < 10) AND col3 < 10"
        )

        # Now, if we mix them up, they should be correctly bracketed
        comp = (cond1 | cond2) & cond3
        self.assertEqual(
            str(comp),
            "(col1 < 10 OR col2 < 10) AND col3 < 10"
        )
        comp = cond1 | (cond2 & cond3)
        self.assertEqual(
            str(comp),
            "col1 < 10 OR (col2 < 10 AND col3 < 10)"
        )
        # Note that the previous one is functionally equivalent to having no brackets, since python evaluates the & first, same as SQLite!
        comp = cond1 | cond2 & cond3
        self.assertEqual(
            str(comp),
            "col1 < 10 OR (col2 < 10 AND col3 < 10)"
        )

        # We should also test them with raw strings
        cond2and3 = "col2 < 10 AND col3 < 10"
        cond1or2 = "col1 < 10 OR col2 < 10"
        comp = sew.Condition(cond1or2) & cond3
        self.assertEqual(
            str(comp),
            "(col1 < 10 OR col2 < 10) AND col3 < 10"
        )

        comp = sew.Condition("col1 < 10") | cond2and3
        self.assertEqual(
            str(comp),
            "col1 < 10 OR (col2 < 10 AND col3 < 10)"
        )

        # Finally test a 4-way condition
        comp = (cond1 | cond2) & (cond3 | "col4 < 100")
        self.assertEqual(
            str(comp),
            "(col1 < 10 OR col2 < 10) AND (col3 < 10 OR col4 < 100)"
        )


