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
        str: "TEXT"
    }
    
    def __init__(self, cols: list=[], conds: list=[]):
        self.fmt = {'cols': cols, 'conds': conds}
        
    def clear(self):
        self.fmt = {'cols': [], 'conds': []}
        
    def _getColumnNames(self):
        return [i[0] for i in self.fmt['cols']]
        
    def addColumn(self, columnName: str, typehint: type):
        self.fmt['cols'].append([columnName, self.sqliteTypes[typehint]])
        
    def addUniques(self, uniqueColumns: list):
        if not all((i in self._getColumnNames() for i in uniqueColumns)):
            raise ValueError("Invalid column found.")
        self.fmt['conds'].append("UNIQUE (%s)" % (','.join(uniqueColumns)))
        
    def generate(self):
        return self.fmt
    
    @classmethod
    def fromSql(cls, stmt: str):
        # Pull out everything after tablename, remove parentheses
        fmtstr = re.search("\(.+\)", stmt).group()[1:-1]
        # Remove any uniques
        uniques = re.search("unique\(.+\)", fmtstr, flags=re.IGNORECASE) # TODO: iterate over all unique groups
        fmtstr.replace(uniques.group(), "")
        # Split into each column (may contain blanks)
        fmtstrs = fmtstr.split(",")
        # TODO: complete
        

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
    print(fmtspec.generate())