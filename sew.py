# -*- coding: utf-8 -*-
"""
Created on Mon Dec  5 17:23:06 2022

@author: lken
"""

import sqlite3 as sq

#%% Basic container, the most barebones
class SqliteContainer:
    def __init__(self, dbpath: str, row_factory: type=sq.Row):
        self.dbpath = dbpath
        self.con = sq.connect(dbpath)
        self.con.row_factory = row_factory
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
        
#%% Mixin that contains the helper methods for statement generation. Note that this builds off the standard format.
class StatementGeneratorMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def _makeTableColumns(self, fmt: dict):
        return ', '.join([' '.join(i) for i in fmt['cols']])
    
    def _makeTableConditions(self, fmt: dict):
        return ', '.join(fmt['conds'])
    
    def _makeCreateTableStatement(self, fmt: dict, tablename: str, ifNotExists: bool=False, encloseTableName: bool=False):
        stmt = "create table%s %s(%s%s)" % (
            " if not exists" if ifNotExists else '',
            '"%s"' % tablename if encloseTableName else tablename,
            self._makeTableColumns(fmt),
            ", %s" % (self._makeTableConditions(fmt)) if len(fmt['conds']) > 0 else ''
        )
        return stmt
    
    def _makeQuestionMarks(self, fmt: dict):
        return ','.join(["?"] * len(fmt['cols']))
    
    def _makeNotNullConditionals(self, cols: dict):
        return ' and '.join(("%s is not null" % (i[0]) for i in cols))
    
    def _stitchConditions(self, conditions: list):
        conditionsList = [conditions] if isinstance(conditions, str) else conditions # Turn into a list if supplied as a single string
        conditionsStr = ' where ' + ' and '.join(conditionsList) if isinstance(conditionsList, list) else ''
        return conditionsStr
    
    def _makeSelectStatement(self,
                             tablename: str,
                             columnNames: list,
                             conditions: list=None,
                             orderBy: list=None):
        # Parse columns into comma separated string
        columns = ','.join(columnNames) if isinstance(columnNames, list) else columnNames
        # Parse conditions with additional where keyword
        conditions = self._stitchConditions(conditions)
        # Parse order by as comma separated string and pad the order by keywords
        orderBy = [orderBy] if isinstance(orderBy, str) else orderBy
        orderBy = ' order by %s' % (','.join(orderBy)) if isinstance(orderBy, list) else ''
        # Create the statement
        stmt = "select %s from %s%s%s" % (
                columns,
                tablename,
                conditions,
                orderBy
            )
        return stmt
    
    def _makeInsertStatement(self, tablename: str, fmt: dict, orReplace: bool=False):
        stmt = "insert%s into %s values(%s)" % (
                " or replace" if orReplace else '',
                tablename,
                self._makeQuestionMarks(fmt)
            )
        return stmt
    
    def _makeDropStatement(self, tablename: str):
        stmt = "drop table %s" % tablename
        return stmt
    
    def _makeDeleteStatement(self, tablename: str, conditions: list=None):
        stmt = "delete from %s%s" % (
            tablename,
            self._stitchConditions(conditions)
        )
        return stmt
    
#%% We will not assume the CommonRedirectMixins here
class CommonMethodMixin(StatementGeneratorMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def createTable(self, fmt: dict, tablename: str, ifNotExists: bool=False, encloseTableName: bool=False, commitNow: bool=True):
        self.cur.execute(self._makeCreateTableStatement(fmt, tablename, ifNotExists, encloseTableName))
        if commitNow:
            self.con.commit()
            
    def dropTable(self, tablename: str, commitNow: bool=False):
        self.cur.execute(self._makeDropStatement(tablename))
        if commitNow:
            self.con.commit()
            
    def getTablenames(self):
        stmt = self._makeSelectStatement("sqlite_master", "name",
                                         conditions=["type='table'"])
        self.cur.execute(stmt)
        results = self.cur.fetchall() # Is a list of length 1 tuples
        results = [i[0] for i in results]
        return results
    

#%% Inherited class of all the above
class Database(CommonRedirectMixin, CommonMethodMixin, SqliteContainer):
    def __init__(self, dbpath: str):
        super().__init__(dbpath)

    
#%%
if __name__ == "__main__":
    d = Database(":memory:")
    tablename = "tablename"
    columnNames = ["col1", "col2"]
    conditions = ["col1 > ?", "col2 > ?"]
    orderBy = "col1 desc"
    
    print(d._makeSelectStatement(tablename, columnNames))
    print(d._makeSelectStatement(tablename, columnNames, conditions, orderBy))
    
    fmt = {
        'cols': [
            ["col1", "INTEGER"],
            ["col2", "REAL"]
        ],
        'conds': [
            'UNIQUE(col1, col2)'    
        ]
    }
    
    # Test making tables
    d.createTable(fmt, 'table1')
    
    # Test with no conditions
    fmt['conds'] = []
    d.createTable(fmt, 'table2')
    print(d.getTablenames())
    
    # Test delete statements
    print(d._makeDeleteStatement('table1'))
    print(d._makeDeleteStatement('table1', ['col1 < 10']))
    
    # Test dropping a table
    d.dropTable('table2', True)
    print(d.getTablenames())
    