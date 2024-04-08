# Load all imports using helper
from ._helpers import *

import unittest
import sqlite3 as sq
import numpy as np


class TestFormatSpecifier(unittest.TestCase):
    """
    This file tests the class itself directly,
    without involving its use-cases with the database/tables.
    """

    def setUp(self):
        # Cover all possible typehints
        self.fmtspec = sew.FormatSpecifier()
        self.fmtspec.addColumn('col1', int)
        self.fmtspec.addColumn('col2', str)
        self.fmtspec.addColumn('col3', float)
        self.fmtspec.addColumn('col4', bytes)
        self.fmtspec.addColumn('col5')
        # Add some unique conditions
        self.fmtspec.addUniques(['col1', 'col3'])
        self.fmtspec.addUniques(['col2', 'col4'])
        # And some foreignkeys
        self.fmtspec.addForeignKey(['col1', 'parent_table(parentcolA)'])
        self.fmtspec.addForeignKey(['col2', 'parent_table(parentcolB)'])

        # Define what it should be
        self.correctFmt = {
            'cols': [
                ['col1', 'INTEGER'],
                ['col2', 'TEXT'],
                ['col3', 'REAL'],
                ['col4', 'BLOB'],
                ['col5', '']
            ],
            'conds': ['UNIQUE(col1, col3)', 'UNIQUE(col2, col4)'],
            'foreign_keys': [
                ['col1', 'parent_table(parentcolA)'],
                ['col2', 'parent_table(parentcolB)']
            ]
        }

    def test_creation_and_repr_str(self):
        """
        Test the __repr__ and __str__ methods.
        This also implicitly checks that the creation is correct,
        and that the addXYZ() methods all work as expected.
        """
        self.assertEqual(
            repr(self.fmtspec),
            str(self.correctFmt)
        )
        self.assertEqual(
            str(self.fmtspec),
            str(self.correctFmt)
        )

    def test_default_args_init(self):
        # This is to check that default arguments are reset when
        # making new instances;
        # shouldn't be a problem now that we use None as default
        fmtspec2 = sew.FormatSpecifier(
            self.correctFmt['cols'],
            self.correctFmt['conds'],
            self.correctFmt['foreign_keys'])

        self.assertEqual(self.fmtspec, fmtspec2)

    def test_clear(self):
        self.fmtspec.clear()
        self.assertEqual(self.fmtspec.cols, [])
        self.assertEqual(self.fmtspec.conds, [])
        self.assertEqual(self.fmtspec.foreign_keys, [])

    def test_invalid_uniques(self):
        with self.assertRaises(ValueError):
            self.fmtspec.addUniques(['col2', 'colxyz'])
