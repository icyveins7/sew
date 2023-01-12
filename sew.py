# -*- coding: utf-8 -*-
"""
Created on Mon Dec  5 17:23:06 2022

@author: lken
"""

import sqlite3 as sq
import re

from formatSpec import FormatSpecifier

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
    
    @staticmethod
    def _makeTableColumns(fmt: dict):
        return ', '.join([' '.join(i) for i in fmt['cols']])
    
    @staticmethod
    def _makeTableConditions(fmt: dict):
        return ', '.join(fmt['conds'])
    
    @staticmethod
    def _makeCreateTableStatement(fmt: dict, tablename: str, ifNotExists: bool=False, encloseTableName: bool=False):
        stmt = "create table%s %s(%s%s)" % (
            " if not exists" if ifNotExists else '',
            '"%s"' % tablename if encloseTableName else tablename,
            StatementGeneratorMixin._makeTableColumns(fmt),
            ", %s" % (StatementGeneratorMixin._makeTableConditions(fmt)) if len(fmt['conds']) > 0 else ''
        )
        return stmt
    
    @staticmethod
    def _makeQuestionMarks(fmt: dict):
        return ','.join(["?"] * len(fmt['cols']))
    
    @staticmethod
    def _makeNotNullConditionals(cols: dict):
        return ' and '.join(("%s is not null" % (i[0]) for i in cols))
    
    @staticmethod
    def _stitchConditions(conditions: list):
        conditionsList = [conditions] if isinstance(conditions, str) else conditions # Turn into a list if supplied as a single string
        conditionsStr = ' where ' + ' and '.join(conditionsList) if isinstance(conditionsList, list) else ''
        return conditionsStr
    
    @staticmethod
    def _makeSelectStatement(columnNames: list,
                             tablename: str,
                             conditions: list=None,
                             orderBy: list=None):
        # Parse columns into comma separated string
        columns = ','.join(columnNames) if isinstance(columnNames, list) else columnNames
        # Parse conditions with additional where keyword
        conditions = StatementGeneratorMixin._stitchConditions(conditions)
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
    
    @staticmethod
    def _makeInsertStatement(tablename: str, fmt: dict, orReplace: bool=False):
        stmt = "insert%s into %s values(%s)" % (
                " or replace" if orReplace else '',
                tablename,
                StatementGeneratorMixin._makeQuestionMarks(fmt)
            )
        return stmt
    
    @staticmethod
    def _makeDropStatement(tablename: str):
        stmt = "drop table %s" % tablename
        return stmt
    
    @staticmethod
    def _makeDeleteStatement(tablename: str, conditions: list=None):
        stmt = "delete from %s%s" % (
            tablename,
            StatementGeneratorMixin._stitchConditions(conditions)
        )
        return stmt
    
#%% We will not assume the CommonRedirectMixins here
class CommonMethodMixin(StatementGeneratorMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self._tables = dict()
        self.reloadTables()
        
    def createTable(self, fmt: dict, tablename: str, ifNotExists: bool=False, encloseTableName: bool=False, commitNow: bool=True):
        self.cur.execute(self._makeCreateTableStatement(fmt, tablename, ifNotExists, encloseTableName))
        if commitNow:
            self.con.commit()
            
    def dropTable(self, tablename: str, commitNow: bool=False):
        self.cur.execute(self._makeDropStatement(tablename))
        if commitNow:
            self.con.commit()
            
    def reloadTables(self):
        stmt = self._makeSelectStatement(["name","sql"], "sqlite_master",
                                         conditions=["type='table'"])
        self.cur.execute(stmt)
        results = self.cur.fetchall()
        self._tables.clear()
        for result in results:
            self._tables[result[0]] = TableProxy(self, result[0],
                                                      FormatSpecifier.fromSql(result[1]).generate())
            
        return results
    
    ### These are useful methods to direct calls to a table or query tables
    def __getitem__(self, tablename: str):
        return self._tables[tablename]
            
    @property
    def tablenames(self):
        return list(self._tables.keys())
    
    @property
    def tables(self):
        return self._tables
    
#%% Akin to configparser, we create a class for tables
class TableProxy(StatementGeneratorMixin):
    def __init__(self, parent: SqliteContainer, tbl: str, fmt: dict):
        self._parent = parent # We redirect calls to the parent
        self._tbl = tbl # The tablename
        self._fmt = fmt
        self._cols = self._populateColumns()
        
    def _populateColumns(self):
        cols = dict()
        # typehints = 
        for col in self._fmt['cols']:
            colname = col[0]
            # Parse the type (note that we cannot determine the upper/lowercase)
            if re.match(r"int", col[1], flags=re.IGNORECASE): # All the versions have the substring 'int', so this works
                cols[colname] = ColumnProxy(colname, int)
            elif re.match(r"text", col[1], flags=re.IGNORECASE) or re.match(r"char", col[1], flags=re.IGNORECASE):
                cols[colname] = ColumnProxy(colname, str)
            elif re.match(r"real", col[1], flags=re.IGNORECASE) or re.match(r"double", col[1], flags=re.IGNORECASE) or re.match(r"float", col[1], flags=re.IGNORECASE):
                cols[colname] = ColumnProxy(colname, float)
            else:
                raise NotImplementedError("Unknown parse for sql type %s" % col[1])
            
        return cols
    
    def __getitem__(self, col: str):
        return self._cols[col]
    
    @property
    def columns(self):
        return self._cols
    
    @property
    def columnNames(self):
        return list(self._cols.keys())
 
    ### These are the actual user methods
    def select(self,
               columnNames: list,
               conditions: list=None,
               orderBy: list=None):
        
        stmt = self._makeSelectStatement(
            columnNames,
            self._tbl,
            conditions,
            orderBy
        )
        self._parent.cur.execute(stmt)
        return stmt
    
    def insertOne(self, *args, orReplace: bool=False, commitNow: bool=False):
        '''Note that this assumes all columns are inserted, and in order.'''
        stmt = self._makeInsertStatement(
            self._tbl, self._fmt, orReplace
        )
        self._parent.cur.execute(stmt, (args))
        if commitNow:
            self.con.commit()
        return stmt
        
    def insertMany(self, *args, orReplace: bool=False, commitNow: bool=False):
        '''Note that this assumes all columns are inserted, and in order.'''
        stmt = self._makeInsertStatement(
            self._tbl, self._fmt, orReplace
        )
        # Handle a special common case where a generator is passed
        if hasattr(args[0], "__next__"): # We use this to test if its a generator, without any other imports (may not be the best solution but works for majority of cases)
            self._parent.cur.executemany(stmt, args[0])
        else: # Otherwise its just plain old data
            self._parent.cur.executemany(stmt, args)
        if commitNow:
            self.con.commit()
        return stmt

#%% And also a class for columns
### TODO: Intention for this is to build it into a way to automatically generate conditions in select statements..
class ColumnProxy:
    def __init__(self, name: str, typehint: type):
        self.name = name
        self.typehint = typehint
        
    def _requireType(self, x):
        if not isinstance(x, self.typehint):
            raise TypeError("Compared value must be of type %s" % str(self.typehint))
        
    def __lt__(self, x):
        self._requireType(x)
        return "%s < %s" % (self.name, str(x))

    def __le__(self, x):
        self._requireType(x)
        return "%s <= %s" % (self.name, str(x))
    
    def __gt__(self, x):
        self._requireType(x)
        return "%s > %s" % (self.name, str(x))

    def __ge__(self, x):
        self._requireType(x)
        return "%s >= %s" % (self.name, str(x))
    
    def __eq__(self, x):
        self._requireType(x)
        return "%s = %s" % (self.name, str(x))
    
    def __ne__(self, x):
        self._requireType(x)
        return "%s != %s" % (self.name, str(x))


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
    
    print(d._makeSelectStatement(columnNames, tablename))
    print(d._makeSelectStatement(columnNames, tablename, conditions, orderBy))
    
    fmt = {
        'cols': [
            ["col1", "integer"],
            ["col2", "DOUBLE"]
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
    # Reload tables to populate internal dict then query
    d.reloadTables()
    print(d.tablenames)
    
    # Test delete statements
    print(d._makeDeleteStatement('table1'))
    print(d._makeDeleteStatement('table1', ['col1 < 10']))
    
    # Test dropping a table
    d.dropTable('table2', True)
    d.reloadTables()
    print(d.tablenames)
    
    # Test inserting into table with dict-like access
    print(d['table1'].insertMany((i, float(i+1)) for i in range(10)))
    # Then check our results
    print(d['table1'].select("*"))
    results = d.fetchall()
    for result in results:
        print(result[0], result[1])
    