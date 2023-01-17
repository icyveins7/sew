# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 16:41:17 2022

@author: seo
"""

import re

class FormatSpecifier:
    sqliteTypes = {
        int: "INTEGER",
        float: "REAL",
        str: "TEXT",
        bytes: "BLOB"
    }
    
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
        fmtstr = re.search(r"\(.+\)", stmt).group()[1:-1] # Greedy regex 
        # Remove any uniques
        uniques = re.finditer(r"UNIQUE\(.+?\)", fmtstr, flags=re.IGNORECASE) # Non-greedy regex
        conds = []
        for unique in uniques:
            fmtstr = fmtstr.replace(unique.group(), "") # Drop the substring
            conds.append(unique.group())
        
        # Split into each column (but we remove if just whitespace)
        fmtstrs = [s for s in fmtstr.split(",") if not s.isspace()]
        # This should just be the columns, so stack them as we expect
        cols = [s.strip().split(" ") for s in fmtstrs]
        
        return cls(cols, conds)
        

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
    from _core import StatementGeneratorMixin
    
    stmt = StatementGeneratorMixin._makeCreateTableStatement(fmtspec.generate(), 'table1')
    genFmtspec = FormatSpecifier.fromSql(stmt)
    assert(genFmtspec.fmt == fmtspec.fmt)