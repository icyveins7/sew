# -*- coding: utf-8 -*-
"""
Created on Mon Dec  5 17:23:06 2022

@author: lken
"""

import sqlite3 as sq

#%% Basic container, the most barebones
class SqliteContainer:
    def __init__(self, dbpath: str):
        self.dbpath = dbpath
        self.con = sq.connect(dbpath)
        self.cur = self.con.cursor()
        
#%% Mixin to redirect common sqlite methods for brevity in code later
class CommonRedirectMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.close = self.con.close
        self.execute = self.cur.execute
        self.executemany = self.cur.executemany
        self.commit = self.con.commit
        self.fetchone = self.cur.fetchone
        self.fetchall = self.cur.fetchall
        
#%% Mixin that contains the helper methods for statement generation
class StatementGeneratorMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def _makeTableColumns(self, fmt: dict):
        return ', '.join([' '.join(i) for i in fmt['cols']])
    
    def _makeTableConditions(self, fmt: dict):
        return ', '.join(fmt['conds'])
    
    def _makeTableStatement(self, fmt: dict):
        return "%s, %s" % (
                self._makeTableColumns(fmt),
                self._makeTableConditions(fmt)
            )
    
    def _makeQuestionMarks(self, fmt: dict):
        return ','.join(["?"] * len(fmt['cols']))
    
    def _makeNotNullConditionals(self, cols: dict):
        return ' and '.join(("%s is not null" % (i[0]) for i in cols))
    
    def _makeSelectStatement(self,
                             tablename: str,
                             columnNames: list,
                             conditions: list=None):
        columns = ','.join(columnNames) if isinstance(columnNames, list) else columnNames
        conditions = [conditions] if isinstance(conditions, str) else conditions # Turn into a list if supplied as a single string
        conditions = ' where ' + ' and '.join(conditions) if isinstance(conditions, list) else ''
        stmt = "select %s from %s%s" % (
                columns,
                tablename,
                conditions
            )
        return stmt
    
    def _makeInsertStatement(self, tablename: str, fmt: dict, orReplace: bool=False):
        stmt = "insert%s into %s values(%s)" % (
                " or replace" if orReplace else '',
                tablename,
                self._makeQuestionMarks(fmt)
            )
        return stmt
    

#%% Inherited class of all the above
class Database(CommonRedirectMixin, StatementGeneratorMixin, SqliteContainer):
    def __init__(self, dbpath: str):
        super().__init__(dbpath)

    
#%%
if __name__ == "__main__":
    d = Database(":memory:")