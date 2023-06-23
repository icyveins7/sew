# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 16:41:17 2022

@author: seo
"""

import re

#%%
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
        "DOUBLE", "FLOAT", "NUMERIC"] # Non-exhaustive list of keyword types
    
    # Constructor
    def __init__(self, cols: list=[], conds: list=[], foreign_keys: list=[]):
        self.fmt = {'cols': cols, 'conds': conds, 'foreign_keys': foreign_keys}
        
    def __repr__(self):
        return str(self.fmt)
        
    def clear(self):
        self.fmt = {'cols': [], 'conds': [], 'foreign_keys': []}
        
    def _getColumnNames(self):
        return [i[0] for i in self.fmt['cols']]
        
    def addColumn(self, columnName: str, typehint: type):
        self.fmt['cols'].append([columnName, self.sqliteTypes[typehint]])
        
    def addUniques(self, uniqueColumns: list):
        if not all((i in self._getColumnNames() for i in uniqueColumns)):
            raise ValueError("Invalid column found.")
        self.fmt['conds'].append("UNIQUE(%s)" % (','.join(uniqueColumns)))

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
        fmtstr = re.search(r"\(.+\)", stmt.replace("\n","").replace("\r","")).group()[1:-1] # Greedy regex 
        # Remove any uniques
        uniques = re.finditer(r"UNIQUE\(.+?\)", fmtstr, flags=re.IGNORECASE) # Non-greedy regex
        conds = []
        for unique in uniques:
            fmtstr = fmtstr.replace(unique.group(), "") # Drop the substring
            conds.append(unique.group())
        
        # Remove any foreign keys
        foreignkeys = re.finditer(r"FOREIGN KEY(.+?) REFERENCES (.+?)\)", fmtstr, flags=re.IGNORECASE)
        foreign_keys = []
        for foreign in foreignkeys:
            fmtstr = fmtstr.replace(foreign.group(), "") # Drop the substring
            # Get the child column name by searching the first brackets
            childCol = re.search(r"\(.+?\)", foreign.group(), flags=re.IGNORECASE).group()[1:-1]
            # Get the parent table/column name by taking everything after REFERENCES
            parentColStart = re.search(r"REFERENCES ", foreign.group(), flags=re.IGNORECASE).span()[1]
            parentCol = foreign.group()[parentColStart:]
            foreign_keys.append([childCol, parentCol])

        # There are some problems with the old way of getting the columns
        # To be safe, we use another regex that extracts based on the expected types
        cols = re.finditer(
            r"(\w+)\s(%s)" % "|".join(cls.keywordTypes), fmtstr, flags=re.IGNORECASE
        )
        cols = [i.group().split() for i in cols]

        # Note, due to pythonic default arguments only evaluating at definition time,
        # we must take extra care to input all __init__ arguments here! Or else the default argument
        # will 'remember' the previous value.
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
            parents[(tablename, columnname)].append(keydesc[0]) # Map parent -> child column
        return parents
        

#%%
if __name__ == "__main__":
    fmtspec = FormatSpecifier()
    fmtspec.addColumn('col1', int)
    fmtspec.addColumn('col2', str)
    
    try:
        fmtspec.addUniques(['col2', 'col3'])
    except Exception as e:
        print(e)
        print("Should raise exception for invalid column.")
        
    fmtspec.addUniques(['col1', 'col2'])
    
    # Add more uniques
    fmtspec.addColumn('col3', float)
    fmtspec.addColumn('col4', float)
    fmtspec.addUniques(['col3', 'col4'])

    # Add some foreign keys
    fmtspec.addForeignKey(['col1', 'parent_table(parentcolA)'])
    fmtspec.addForeignKey(['col2', 'parent_table(parentcolB)'])

    print(fmtspec.generate())
    
    #%% Test fromSql
    from ._core import StatementGeneratorMixin
    
    stmt = StatementGeneratorMixin._makeCreateTableStatement(fmtspec.generate(), 'table1')
    print(stmt)
    genFmtspec = FormatSpecifier.fromSql(stmt)
    print("\n\n\n")

    print(genFmtspec.generate())
    # print(genFmtspec.generate()['foreign_keys'])
    # print(id(genFmtspec.fmt))
    # print(fmtspec.generate())
    # print(id(fmtspec.fmt))
    assert(genFmtspec.fmt == fmtspec.fmt)