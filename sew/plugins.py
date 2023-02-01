#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 15 22:14:34 2023

@author: seoxubuntu
"""

from ._core import *
from .formatSpec import FormatSpecifier
import pandas as pd
import numpy as np

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
        Pandas plugin version. Essentially forwards query to pd.read_sql().

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

#%% Numpy plugins %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
class NumpyCommonMethodMixin(CommonMethodMixin):
    '''
    Design goals for Numpy plugin:
        1) Assume columns are marked with data type as a suffix.
        Example:
            "u64" -> np.uint64
            "f32" -> np.float32
            "fc128" -> np.complex128

        2) On selects, convert column into numpy arrays of the associated data type if specified.
        This means that the default behaviour is to return as columns as opposed to rows.

        3) On inserts, check for data type if column has suffix specified.
        
        4) Provide automatic dereferencing of array->column if inserts are done with multiple arrays.
    '''

    def reloadTables(self):
        '''
        Loads and parses the details of all tables from sqlite_master.
        Numpy plugin version.
        
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
            self._tables[result[0]] = NumpyTableProxy(self, result[0],
                                                       FormatSpecifier.fromSql(result[1]).generate())
            
        return results

class NumpyTableProxy(TableProxy):
    numpyColumnSuffixes = {
        "u64": np.uint64,
        "u32": np.uint32,
        "u16": np.uint16,
        "u8": np.uint8,
        "i64": np.int64,
        "i32": np.int32,
        "i16": np.int16,
        "i8": np.int8,
        "f64": np.float64,
        "f32": np.float32,
        "f16": np.float16
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._npresults = None # Additional cache for numpy results
        
    def _getNumpyTypeFromSuffix(self, key: str):
        for suffix in self.numpyColumnSuffixes:
            if key[-len(suffix):] == suffix:
                return self.numpyColumnSuffixes[suffix]

        # Return None if no suffix
        return None

    def fetchAsNumpy(self, firstLength: int=100):
        '''
        Calls fetch and parses the data into numpy arrays for each column if suffixes are defined;
        if no appropriate suffix is found, it is returned as a row.

        Parameters
        ----------
        firstLength : int
            The first length of the arrays to preallocate.
            Due to the nature of sqlite, the number of rows returned is not known until 
            the fetch is complete. Hence, to minimise re-allocations, try to assign a
            number larger than the expected number of rows to this.
        '''

        # Require use of sqlite.Row
        if self._parent.con.row_factory is not sq.Row:
            raise ValueError("Cannot use fetchAsNumpy() unless sqlite3.Row is set as row_factory.")

        # We will return a dictionary of column names
        r = dict()
        # Iterate while fetching
        inited = False
        i = 0 # Our counter
        while True:
            # Fetch a row
            row = self._parent.cur.fetchone()
            if row is None:
                break # No more rows to fetch
            
            # On the first iteration fill the column names
            if not inited:
                for key in row.keys():
                    numpytype = self._getNumpyTypeFromSuffix(key)
                    if numpytype is not None:
                        # Instantiate a numpy array of appropriate type
                        r[key] = np.zeros(firstLength, dtype=numpytype)
                    else:
                        # Otherwise just make a list
                        r[key] = list()

                # Make sure we don't keep initing
                inited = True

            # Now iterate over the columns and write to respective arrays/lists
            for key in row.keys():
                # Check that our array has room left
                if i >= r[key].size:
                    # Otherwise replace with double the size
                    tmp = np.zeros(r[key].size * 2, dtype=r[key].dtype)
                    tmp[:r[key].size] = r[key][:]
                    r[key] = tmp
                
                # Write to our array
                r[key][i] = row[key]

            # Increment counter
            i += 1

        return r

class NumpyDatabase(CommonRedirectMixin, NumpyCommonMethodMixin, SqliteContainer):
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
    
    # What if i select only one column?
    print(d['table1'].select("col1"))
    print(d['table1'].pdresults) # Also works!