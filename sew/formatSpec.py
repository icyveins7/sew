# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 16:41:17 2022

@author: seo

TODO list:
- Rework to use internal pragma results
- In order of priority, PRAGMA table_info, PRAGMA foreign_key_list, PRAGMA index_info

table_info typical result:
{'cid': 0, 'name': 'col1', 'type': 'INTEGER', 'notnull': 0, 'dflt_value': None, 'pk': 0}

foreing_key_list typical result:
{'id': 0, 'seq': 0, 'table': 'parent', 'from': 'col2', 'to': 'id', 'on_update': 'NO ACTION', 'on_delete': 'NO ACTION', 'match': 'NONE'}

Probably just use these to specify formats, or at least a watered down version of these?
"""

import re

# %%


class FormatSpecifier:
    """
    Formats are generated as a dictionary of 'cols' and 'conds'
    which specify the columns and conditions respectively.

    Example:
    fmt = {
        'cols': [
            ['col1', 'integer'], # list of lists, with inner list begin column name and sqlite type in that order
            ['col2', 'REAL'] # the type upper/lower-case doesn't matter
        ],
        'conds': [
            "UNIQUE(col1, col2)" # this is just a list of strings, with each one specifying an extra condition
        ]
    }

    Foreign keys are also allowed; specify these as a list of lists, with each inner list specifying the column name
    and then the parent class/table name pair that it references.

    Example:
    foreign_key = [
        ["col_child", "parent_table(col_parent)"]
    ]

    becomes

    "FOREIGN KEY(col_child) REFERENCES parent_table(col_parent)"
    """

    # Constant statics
    sqliteTypes = {
        int: "INTEGER",
        float: "REAL",
        str: "TEXT",
        bytes: "BLOB"
    }
    keywordTypes = [
        "INTEGER", "INT", "REAL", "TEXT", "BLOB",
        "DOUBLE", "FLOAT", "NUMERIC"]  # Non-exhaustive list of keyword types

    # Constructor
    def __init__(self, cols: list = None, conds: list = None, foreign_keys: list = None):
        self.fmt = {
            'cols': cols if cols is not None else [],
            'conds': conds if conds is not None else [],
            'foreign_keys': foreign_keys if foreign_keys is not None else []
        }

    def __repr__(self):
        return str(self.fmt)

    def __str__(self):
        return str(self.fmt)

    def __eq__(self, other):
        return self.fmt == other.fmt

    # Define some convenient getters
    @property
    def cols(self):
        return self.fmt['cols']

    @property
    def conds(self):
        return self.fmt['conds']

    @property
    def foreign_keys(self):
        return self.fmt['foreign_keys']

    def clear(self):
        self.fmt = {'cols': [], 'conds': [], 'foreign_keys': []}

    def _getColumnNames(self):
        return [i[0] for i in self.fmt['cols']]

    def addColumn(self, columnName: str, typehint: type = None):
        self.fmt['cols'].append(
            [columnName,
             self.sqliteTypes[typehint] if typehint is not None else ""]
        )

    def addUniques(self, uniqueColumns: list):
        if not all((i in self._getColumnNames() for i in uniqueColumns)):
            raise ValueError("Invalid column found.")
        self.fmt['conds'].append("UNIQUE(%s)" % (', '.join(uniqueColumns)))

    def addForeignKey(self, childParentPair: list):
        """
        Appends a single foreign key to the list of foreign keys.
        Note, this does not check for existence of the parent table/column.

        Parameters
        ----------
        childParentPair : list
            A list of two strings, the child column name and the parent table/column name.
        """
        if childParentPair[0] not in self._getColumnNames():
            raise ValueError("Invalid child column found.")
        self.fmt['foreign_keys'].append(childParentPair)

    def generate(self):
        return self.fmt

    @staticmethod
    def _parseColumnDesc(desc: str) -> list[str, str]:
        '''
        Helper method to parse the section of the CREATE TABLE statement
        that describes a single column.

        This looks like "col1 TYPENAME" or just "col1".

        Parameters
        ----------
        desc : str
            The column description.

        Returns
        -------
        sdesc : list[str, str] or None
            A list of two strings, the column name and the type.
            Returns None if an empty string is passed in.
        '''
        sdesc = desc.strip()
        if len(sdesc) == 0:  # Empty string
            return None

        sdesc = sdesc.split(" ")
        if len(sdesc) == 1:
            return [sdesc[0], ""]  # Blank for type

        else:
            # This accounts for INTEGER PRIMARY KEY for e.g.
            return [sdesc[0], " ".join(sdesc[1:])]

    @staticmethod
    def _splitColumnsSql(fmtstr: str) -> tuple[list[list[str]], list[str], list[list[str]]]:
        '''
        Helper method to split the extracted SQL from the CREATE TABLE statement into
        3 constituent parts:

        1) Description of the columns
        2) Description of the conditions on the columns e.g. UNIQUE
        3) FOREIGN KEY constraints


        Parameters
        ----------
        fmtstr : str
            The extracted SQL from the CREATE TABLE statement.
            This should be everything in the outermost brackets following the table name.

        Returns
        -------
        cols : list[list[str]]
            List of list of strings, with each inner list being returned from ._parseColumnDesc().

        conds : list[str]
            List of strings, with each one specifying an extra condition.
            Example: UNIQUE(col1, col2).

        foreign_keys :  list[list[str]]
            List of list of strings, with each inner list representing the
            child, parent relationship.
            Example: ["col_child", "parent_table(col_parent)"]
        '''

        # Remove any uniques
        uniques = re.finditer(r"UNIQUE\(.+?\)", fmtstr,
                              flags=re.IGNORECASE)  # Non-greedy regex
        conds = []
        for unique in uniques:
            fmtstr = fmtstr.replace(unique.group(), "")  # Drop the substring
            conds.append(unique.group())

        # Remove any foreign keys
        foreignkeys = re.finditer(
            r"FOREIGN KEY(.+?) REFERENCES (.+?)\)", fmtstr, flags=re.IGNORECASE)
        foreign_keys = []
        for foreign in foreignkeys:
            fmtstr = fmtstr.replace(foreign.group(), "")  # Drop the substring
            # Get the child column name by searching the first brackets
            childCol = re.search(r"\(.+?\)", foreign.group(),
                                 flags=re.IGNORECASE).group()[1:-1]
            # Get the parent table/column name by taking everything after REFERENCES
            parentColStart = re.search(
                r"REFERENCES ", foreign.group(), flags=re.IGNORECASE).span()[1]
            parentCol = foreign.group()[parentColStart:]
            foreign_keys.append([childCol, parentCol])

        # Now parse each remaining column description
        cols = [
            sdesc
            # Just split by the commas, these should be the only ones left
            for i in fmtstr.split(",")
            if (sdesc := FormatSpecifier._parseColumnDesc(i)) is not None
        ]

        return cols, conds, foreign_keys

    @classmethod
    def fromSql(cls, stmt: str):
        '''
        Factory method to create format specifier from a CREATE TABLE statement.

        Parameters
        ----------
        stmt : str
            The create table statement.
        '''
        # Pull out everything after tablename, remove parentheses
        fmtstr = re.search(r"\(.+\)", stmt.replace("\n",
                           "").replace("\r", "")).group()[1:-1]  # Greedy regex

        # Call the two helper methods
        cols, conds, foreign_keys = FormatSpecifier._splitColumnsSql(fmtstr)
        return cls(cols, conds, foreign_keys)

    @classmethod
    def fromPragma(cls, tableInfos: list, foreign_key_list: list):
        """
        Factory method to generate format specifier from the output of a 
        PRAGMA table_info() call.

        Parameters
        ----------
        tableInfos : list
            Results returned from PRAGMA table_info().
        """
        cols = list()
        conds = list()
        foreign_keys = list()
        for info in tableInfos:
            cols.append(
                [info['name'], info['type']]
            )
        for fk in foreign_key_list:
            print(fk)
        # TODO: i am not handling 'notnull', 'dflt_value', and 'pk'
        # TODO: also didn't handle UNIQUE conditions here?
        return cls(cols, conds, foreign_keys)

    @staticmethod
    def dictContainsColumn(fmt: dict, colname: str):
        '''Checks if a generated format dictionary contains a particular column.'''
        check = False
        for i, col in enumerate(fmt['cols']):
            if col[0] == colname:
                check = True
                break
        return check

    @staticmethod
    def getParents(fmt: dict):
        parents = dict()
        for keydesc in fmt['foreign_keys']:
            spl = keydesc[1].split("(")
            tablename = spl[0]
            columnname = spl[1][:-1]
            # For the weird cases where the same parent column
            # is pointed to by two child columns in the same table
            if (tablename, columnname) not in parents:
                parents[(tablename, columnname)] = []
            parents[(tablename, columnname)].append(
                keydesc[0])  # Map parent -> child column
        return parents
