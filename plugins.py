#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 15 22:14:34 2023

@author: seoxubuntu
"""

from sew import *
from formatSpec import FormatSpecifier
import pandas as pd

#%% Pandas plugins
class PandasCommonMethodMixin(CommonMethodMixin):
    def reloadTables(self):
        '''
        Loads and parses the details of all tables from sqlite_master.
        Pandas plugin version. Caches pandas-enabled tables internally.
        
        Returns
        -------
        results : 
            Sqlite results from fetchall(). This is usually used for debugging.
        '''
        stmt = self._makeSelectStatement(["name","sql"], "sqlite_master",
                                         conditions=["type='table'"])
        self.cur.execute(stmt)
        results = self.cur.fetchall()
        self._tables.clear()
        for result in results:
            self._tables[result[0]] = PandasTableProxy(self, result[0],
                                                       FormatSpecifier.fromSql(result[1]).generate())
            
        return results

class PandasTableProxy(TableProxy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pdresults = None # Additional cache for pandas dataframe results
        
    @property
    def pdresults(self):
        '''Getter for pandas dataframe results.'''
        return self._pdresults
    
    def select(self,
               columnNames: list,
               conditions: list=None,
               orderBy: list=None):
        '''
        Performs a select on the current table.
        Pandas plugin version. Essentialyl forwards query to pd.read_sql().

        Parameters
        ----------
        columnNames : list
            List of columns to extract. A single column may be specified as a string.
            If all columns are desired, the string "*" may be specified.
            Examples:
                ["col1", "col3"]
                "*"
                "justThisColumn"
                
        conditions : list, optional
            The filter conditions placed after "where".
            A single condition may be specified as a string.
            The default is None, which will place no conditions.
            Examples:
                ["col1 < 10", "col2 = 5"]
                "justThisColumn >= 8"
                
        orderBy : list, optional
            The ordering conditions placed after "order by".
            A single condition may be specified as a string.
            The default is None, which will place no ordering on the results.
            Examples:
                ["col1 desc", "col2 asc"]
                "justThisColumn asc"

        Returns
        -------
        stmt : str
            The actual sqlite statement that was executed.
        '''
        
        stmt = self._makeSelectStatement(
            columnNames,
            self._tbl,
            conditions,
            orderBy
        )
        self._pdresults = pd.read_sql(stmt, self._parent.con) # Store into internals
        return stmt
    
    # TODO: complete insert variations for pandas, using the df.to_sql() command
        
class PandasDatabase(CommonRedirectMixin, PandasCommonMethodMixin, SqliteContainer):
    pass



#%%
if __name__ == "__main__":
    d = PandasDatabase(":memory:")
    
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
    d.reloadTables()
    
    # Test inserting into table with dict-like access
    data = ((i, float(i+1)) for i in range(10)) # Generator expression
    print(d['table1'].insertMany(data))
    # Then check our results
    print(d['table1'].select("*"))
    # Retrieve by property and see a dataframe
    df = d['table1'].pdresults
    print(df)