# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 16:41:17 2022

@author: seo
"""

import re

#%%

# Formats are generated as a dictionary of 'cols' and 'conds'
# which specify the columns and conditions respectively.
# Example:
# fmt = {
#     'cols': [
#         ['col1', 'integer'], # list of lists, with inner list begin column name and sqlite type in that order
#         ['col2', 'REAL'] # the type upper/lower-case doesn't matter
#     ],
#     'conds': [
#         "UNIQUE(col1, col2)" # this is just a list of strings, with each one specifying an extra condition
#     ]
# }
class FormatSpecifier:
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
    def __init__(self, cols: list=[], conds: list=[]):
        self.fmt = {'cols': cols, 'conds': conds}
        
    def __repr__(self):
        return str(self.fmt)
        
    def clear(self):
        self.fmt = {'cols': [], 'conds': []}
        
    def _getColumnNames(self):
        return [i[0] for i in self.fmt['cols']]
        
    def addColumn(self, columnName: str, typehint: type):
        self.fmt['cols'].append([columnName, self.sqliteTypes[typehint]])
        
    def addUniques(self, uniqueColumns: list):
        if not all((i in self._getColumnNames() for i in uniqueColumns)):
            raise ValueError("Invalid column found.")
        self.fmt['conds'].append("UNIQUE(%s)" % (','.join(uniqueColumns)))
        
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
        
        # There are some problems with the old way of getting the columns
        # To be safe, we use another regex that extracts based on the expected types
        cols = re.finditer(
            r"(\w+)\s(%s)" % "|".join(cls.keywordTypes), fmtstr, flags=re.IGNORECASE
        )
        cols = [i.group().split() for i in cols]

        # # Split into each column (but we remove if just whitespace)
        # fmtstrs = [s for s in fmtstr.split(",") if not s.isspace()]
        # # This should just be the columns, so stack them as we expect
        # cols = [s.strip().split(" ") for s in fmtstrs]
        
        return cls(cols, conds)

    @staticmethod
    def dictContainsColumn(fmt: dict, colname: str):
        '''Checks if a generated format dictionary contains a particular column.'''
        check = False
        for i, col in enumerate(fmt['cols']):
            if col[0] == colname:
                check = True
                break
        return check
        

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
    print(fmtspec.generate())
    
    #%% Test fromSql
    from ._core import StatementGeneratorMixin
    
    stmt = StatementGeneratorMixin._makeCreateTableStatement(fmtspec.generate(), 'table1')
    genFmtspec = FormatSpecifier.fromSql(stmt)
    assert(genFmtspec.fmt == fmtspec.fmt)