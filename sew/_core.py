# -*- coding: utf-8 -*-
"""
Created on Mon Dec  5 17:23:06 2022

@author: icyveins7 
"""

import sqlite3 as sq
import re

from .formatSpec import FormatSpecifier

#%% Basic container, the most barebones
class SqliteContainer:
    def __init__(self, dbpath: str, row_factory: type=sq.Row, pragma_foreign_keys: bool=True):
        '''
        Instantiates an sqlite database container.

        Parameters
        ----------
        dbpath : str
            The database to connect to (either a filepath or ":memory:").
            See sqlite3.connect() for more information.
        row_factory : type, optional
            The row factory for the sqlite3 connection. The default is the in-built sqlite3.Row.
        pragma_foreign_keys : bool, optional
            Turns on PRAGMA FOREIGN_KEYS. The default is True.
        '''
        self.dbpath = dbpath
        self.con = sq.connect(dbpath)
        self.con.row_factory = row_factory
        self.cur = self.con.cursor()

        if pragma_foreign_keys:
            self.cur.execute("PRAGMA foreign_keys=ON")
        
#%% Mixin to redirect common sqlite methods for brevity in code later
class CommonRedirectMixin:
    def __init__(self, *args, **kwargs):
        '''
        Provides several redirects to common methods, just to have shorter code.
        '''
        super().__init__(*args, **kwargs)
        
        self.close = self.con.close
        self.execute = self.cur.execute
        self.executemany = self.cur.executemany
        self.commit = self.con.commit
        self.fetchone = self.cur.fetchone
        self.fetchall = self.cur.fetchall
        self.fetchmany = self.cur.fetchmany
        
#%% Mixin that contains the helper methods for statement generation. Note that this builds off the standard format.
class StatementGeneratorMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    @staticmethod
    def _encloseTableName(tablename: str):
        return '"%s"' % tablename

    @staticmethod
    def _makeTableColumns(fmt: dict):
        return ', '.join([' '.join(i) for i in fmt['cols']])
    
    @staticmethod
    def _makeTableConditions(fmt: dict):
        return ', '.join(fmt['conds'])
    
    @staticmethod
    def _makeTableForeignKeys(fmt: dict):
        return ', '.join(
            ["FOREIGN KEY(%s) REFERENCES %s" % (i[0], i[1]) for i in fmt['foreign_keys']])
    
    @staticmethod
    def _makeCreateTableStatement(
        fmt: dict, tablename: str, ifNotExists: bool=False, encloseTableName: bool=True
    ):
        stmt = "create table%s %s(%s%s%s)" % (
            " if not exists" if ifNotExists else '',
            StatementGeneratorMixin._encloseTableName(tablename) if encloseTableName else tablename,
            StatementGeneratorMixin._makeTableColumns(fmt),
            ", %s" % (StatementGeneratorMixin._makeTableConditions(fmt)) if len(fmt['conds']) > 0 else '',
            ", %s" % (StatementGeneratorMixin._makeTableForeignKeys(fmt)) if fmt.get('foreign_keys') is not None and len(fmt['foreign_keys']) > 0 else ''
        )
        return stmt
    
    @staticmethod
    def _makeCreateViewStatemnt(
        selectStmt: str, viewtablename: str, ifNotExists: bool=False, encloseTableName: bool=True
    ):
        stmt = "create view%s %s as %s" % (
            " if not exists" if ifNotExists else '',
            StatementGeneratorMixin._encloseTableName(viewtablename) if encloseTableName else viewtablename,
            selectStmt
        )
        return stmt

    @staticmethod
    def _makeQuestionMarks(n: int): # fmt: dict):
        return ','.join(["?"] * n)
    
    @staticmethod
    def _makeNotNullConditionals(cols: dict):
        return ' and '.join(("%s is not null" % (i[0]) for i in cols))
    
    @staticmethod
    def _stitchConditions(conditions: list):
        conditionsList = [conditions] if isinstance(conditions, str) else conditions # Turn into a list if supplied as a single string
        conditionsStr = ' where ' + ' and '.join(conditionsList) if isinstance(conditionsList, list) else ''
        return conditionsStr
    
    @staticmethod
    def _makeCaseSingleConditionVariable(conditionVariable: str, whenthens: list, finalElse: str):
        """
        Used to generate a case statement for a single condition variable.
        By SQLite definitions, this can only test for multiple equality conditions, comparing the condition variable.

        This is usually paired with an "AS somenewcolumn".

        Example output:
            CASE colA
                WHEN 1 THEN 10
                WHEN 2 THEN 20
                ELSE -1
            END

        Parameters
        ----------
        conditionVariable : str
            Single string specifying the condition variable (does not need to be a column by itself).

        whenthens : list
            List of sublists of length 2: the first contains the WHEN substring
            and the second contains the THEN substring i.e.
            the first substring evaluates the equality of the condition variable,
            and the second substring will be the result if the equality evaluates to true.
            For this method, the first substring should only be a simple value for comparison.

        finalElse : str
            The final ELSE statement i.e. the value of the output column if no WHEN/THEN
            conditions evaluate to true.
        """
        # Stitch together the when/then sub statements
        whenthensStr = "\n".join(["WHEN %s THEN %s" % (i[0], i[1]) for i in whenthens])
        # Then combine with the final else statement and condition
        s = "CASE %s\n%s\nELSE %s\nEND" % (conditionVariable, whenthensStr, finalElse)

        return s
        
    @staticmethod
    def _makeCaseMultipleConditionVariables(whenthens: list, finalElse: str):
        """
        Used to generate a case statement for multiple condition variables.
        In this structure, every condition variable can be unique, and more general comparisons can
        be done like <, > etc.

        This is usually paired with an "AS somenewcolumn".

        Example output:
            CASE
                WHEN colA = 1 THEN 10
                WHEN colB > 2 THEN 20
                WHEN colC < 3 THEN 30
                ELSE -1
            END

        Parameters
        ----------
        whenthens : list
            List of sublists of length 2: the first contains the WHEN substring
            and the second contains the THEN substring i.e.
            the first substring evaluates the equality of the condition variable,
            and the second substring will be the result if the equality evaluates to true.
            For this method, the first substring can have more complicated expressions,
            which may involve not just equality comparisons and also involve multiple columns.

        finalElse : str
            The final ELSE statement i.e. the value of the output column if no WHEN/THEN
            conditions evaluate to true.
        """
        # Stitch together the when/then sub statements
        whenthensStr = "\n".join(["WHEN %s THEN %s" % (i[0], i[1]) for i in whenthens])
        # Then combine with the final else statement and condition
        s = "CASE\n%s\nELSE %s\nEND" % (whenthensStr, finalElse)

        return s
    
    @staticmethod
    def _makeSelectStatement(columnNames: list,
                             tablename: str,
                             conditions: list=None,
                             orderBy: list=None,
                             encloseTableName: bool=True):
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
                StatementGeneratorMixin._encloseTableName(tablename) if encloseTableName else tablename,
                conditions,
                orderBy
            )
        return stmt
    
    @staticmethod
    def _makeInsertStatement(
        tablename: str, fmt: dict, orReplace: bool=False, encloseTableName: bool=True
    ):
        stmt = "insert%s into %s values(%s)" % (
                " or replace" if orReplace else '',
                StatementGeneratorMixin._encloseTableName(tablename) if encloseTableName else tablename,
                StatementGeneratorMixin._makeQuestionMarks(len(fmt['cols']))
            )
        return stmt
    
    @staticmethod
    def _makeInsertStatementWithNamedColumns(
        tablename: str, insertedColumns: list, orReplace: bool=False, encloseTableName: bool=True
    ):
        stmt = "insert%s into %s(%s) values(%s)" % (
            " or replace" if orReplace else '',
            StatementGeneratorMixin._encloseTableName(tablename) if encloseTableName else tablename,
            ','.join(insertedColumns),
            StatementGeneratorMixin._makeQuestionMarks(len(insertedColumns))
        )
        return stmt
    
    @staticmethod
    def _makeDropStatement(tablename: str):
        stmt = "drop table %s" % tablename
        return stmt
    
    @staticmethod
    def _makeDeleteStatement(
        tablename: str, conditions: list=None, encloseTableName: bool=True):
        stmt = "delete from %s%s" % (
            StatementGeneratorMixin._encloseTableName(tablename) if encloseTableName else tablename,
            StatementGeneratorMixin._stitchConditions(conditions)
        )
        return stmt
    
#%% We will not assume the CommonRedirectMixins here
class CommonMethodMixin(StatementGeneratorMixin):
    def __init__(self, *args, **kwargs):
        '''
        Includes common methods to create and drop tables, and provides
        dictionary-like access to all tables currently in the database.
        Also includes internal methods for statement generation.
        '''
        super().__init__(*args, **kwargs)
        
        self._tables = dict()
        self._relations = dict() # Establish in-memory parent->child mappings
        self.reloadTables()

    def _parseTable(
        self,
        table_name: str,
        table_sql: str,
        table_type: str
    ) -> dict:
        """
        Parses the parameters of a table and appropriately
        populates an internal dictionary with a particular
        table subclass object.

        This only handles TableProxy, MetaTableProxy and ViewProxy.
        For DataTableProxy, see _parseDataTable().

        Parameters
        ----------
        table_name : str
            Table name.
        table_sql : str
            SQLite statement used in generation of the table.
        table_type : str
            Table type, either 'table' or 'view'.

        Returns
        -------
        dataToMeta: dict
            An additional dictionary containing a lookup from
            DataTable: key -> MetaTable: value. Formed from querying
            the MetaTable object.
        """
        dataToMeta = dict()
        # Special case for view
        if table_type == 'view':
            self._tables[table_name] = ViewProxy(self, table_name)

        # Special cases for metadata tables
        elif table_name.endswith(MetaTableProxy.requiredTableSuffix):
            self._tables[table_name] = MetaTableProxy(
                self, table_name, 
                FormatSpecifier.fromSql(table_sql).generate())
            # We also retrieve all associated data table names
            assocDataTables = self._tables[table_name].getDataTables()
            for dt in assocDataTables:
                dataToMeta[dt] = table_name
        else:
            self._tables[table_name] = TableProxy(
                self, table_name, 
                FormatSpecifier.fromSql(table_sql).generate())

        return dataToMeta


    def _parseDataTable(
        self, 
        table_name: str,
        metatable_name: str
    ):
        """
        Parses a data table by re-instantiating it
        as a DataTable in the internal structure.

        Assumes it has already been created as a normal TableProxy.

        Parameters
        ----------
        table_name : str
            Name of the data table.
        metatable_name : str
            Name of its associated metadata table.
        """
        # Recreate the object as a DataTable instead
        self._tables[table_name] = DataTableProxy(
            self._tables[table_name]._parent,
            self._tables[table_name]._tbl,
            self._tables[table_name]._fmt,
            metatable_name
        )


    def _parseRelationship(
        self,
        tablename: str
    ):
        """
        Parses a table's parent-child relationships
        i.e. any foreign key relationships.

        Assumes the table is already present in the internal
        structure via ._parseTable().

        Parameters
        ----------
        tablename : str
            Target tablename.
        """
        if not isinstance(self._tables[tablename], ViewProxy):
            parents = FormatSpecifier.getParents(
                self._tables[tablename]._fmt)
            # Append to the relationship dict
            for parent, child_cols in parents.items():
                if parent not in self._relations.keys():
                    self._relations[parent] = list()
                for child_col in child_cols:
                    self._relations[parent].append((tablename,
                                                    child_col))


    def reloadTables(self):
        '''
        Loads and parses the details of all tables from sqlite_master.
        
        Returns
        -------
        results : 
            Sqlite results from fetchall(). This is usually used for debugging.
        '''
        stmt = self._makeSelectStatement(["name","sql","type"], "sqlite_master",
                                         conditions=["type='table' or type='view'"])
        self.cur.execute(stmt)
        results = self.cur.fetchall()
        self._tables.clear()

        dataToMeta = dict()
        for result in results:
            newDtm = self._parseTable(
                result['name'], result['sql'], result['type']
            )
            # Merge into dataToMeta
            dataToMeta.update(newDtm)

        # Iterate over all the tables to upgrade them to data tables if they exist
        for table in self._tables:
            if table in dataToMeta.keys():
                self._parseDataTable(
                    table,
                    dataToMeta[table]
                )

            # While doing so, check if it has foreign keys
            # But only if its not a view
            self._parseRelationship(table)
           
        return results
        
    def createTable(self, 
                    fmt: dict, 
                    tablename: str, 
                    ifNotExists: bool=False, 
                    encloseTableName: bool=True, 
                    commitNow: bool=False):
        '''
        Creates a new table.

        Parameters
        ----------
        fmt : dict
            Dictionary of column names/types and special conditions that characterises the table.
            The easiest way to generate this is to instantiate a FormatSpecifier object and then use
            generate() to create this.
        tablename : str
            The table name.
        ifNotExists : bool, optional
            Prevents creation if the table already exists. The default is False.
        encloseTableName : bool, optional
            Encloses the table name in quotes to allow for certain table names which may fail;
            for example, this is necessary if the table name starts with digits.
            The default is True.
        commitNow : bool, optional
            Calls commit on the database connection after the transaction if True. 
            The default is False.
        '''
        stmt = self._makeCreateTableStatement(fmt, tablename, ifNotExists, encloseTableName)
        self.cur.execute(stmt)
        if commitNow:
            self.con.commit()

        # Update the internal structure
        self._parseTable(tablename, stmt, 'table')
        return stmt

    def createMetaTable(self, 
                        fmt: dict, 
                        tablename: str, 
                        ifNotExists: bool=False, 
                        encloseTableName: bool=True, 
                        commitNow: bool=False):
        '''
        Creates a new meta table.

        Parameters
        ----------
        fmt : dict
            Dictionary of column names/types and special conditions that characterises the table.
            The easiest way to generate this is to instantiate a FormatSpecifier object and then use
            generate() to create this.
            Metadata tables are expected to have the first column designated as 'data_tblname TEXT'.
        tablename : str
            The table name.
        ifNotExists : bool, optional
            Prevents creation if the table already exists. The default is False.
        encloseTableName : bool, optional
            Encloses the table name in quotes to allow for certain table names which may fail;
            for example, this is necessary if the table name starts with digits.
            The default is True.
        commitNow : bool, optional
            Calls commit on the database connection after the transaction if True. 
            The default is False.
        '''

        # Check if the format and table names are valid
        if not tablename.endswith(MetaTableProxy.requiredTableSuffix):
            raise ValueError("Metadata table %s must end with %s" % (tablename, MetaTableProxy.requiredTableSuffix))

        if not FormatSpecifier.dictContainsColumn(fmt, MetaTableProxy.requiredColumn):
            raise ValueError("Metadata table %s must contain the column %s" % (tablename, MetaTableProxy.requiredColumn))

        # Otherwise, everything else is the same
        stmt = self._makeCreateTableStatement(fmt, tablename, ifNotExists, encloseTableName)
        self.cur.execute(stmt)
        if commitNow:
            self.con.commit()

        # Populate internal structure
        self._parseTable(tablename, stmt, 'table')

    def createDataTable(self, 
                        fmt: dict, 
                        tablename: str, 
                        metadata: list, 
                        metatablename: str,
                        metaOrReplace: bool=False,
                        ifNotExists: bool=False, 
                        encloseTableName: bool=True, 
                        commitNow: bool=False):
        '''
        Creates a new data table. This table will be intrinsically linked to a row in the associated metadata table.

        Parameters
        ----------
        fmt : dict
            Dictionary of column names/types and special conditions that characterises the table.
            The easiest way to generate this is to instantiate a FormatSpecifier object and then use
            generate() to create this.
        tablename : str
            The table name.
        metadata : list
            The metadata to insert into the metadata table associated with this data table.
        metatablename : str
            The metadata table name.
        metaOrReplace : bool, optional
            Whether to use orReplace when inserting into the metadata table.
            Useful when the metadata table has UNIQUE constraints.
        ifNotExists : bool, optional
            Prevents creation if the table already exists. The default is False.
        encloseTableName : bool, optional
            Encloses the table name in quotes to allow for certain table names which may fail;
            for example, this is necessary if the table name starts with digits.
            The default is True.
        commitNow : bool, optional
            Calls commit on the database connection after the transaction if True. 
            The default is False.
        '''

        # Check if the meta table exists
        if not metatablename in self._tables.keys():
            raise ValueError("Metadata table %s does not exist!" % metatablename)

        # Insert the associated metadata into the metadata table
        metastmt = self._makeInsertStatement(
            metatablename,
            self._tables[metatablename]._fmt,
            orReplace=metaOrReplace,
            encloseTableName=encloseTableName)
        metadata.insert(0, tablename) # The first argument is the name of the data table
        self.cur.execute(metastmt, metadata)

        # Otherwise, everything else is the same
        stmt = self._makeCreateTableStatement(fmt, tablename, ifNotExists, encloseTableName)
        self.cur.execute(stmt)
        if commitNow:
            self.con.commit()

        # Create the table internally
        self._parseTable(tablename, stmt, 'table')
        # Update it as a special DataTable
        self._parseDataTable(tablename, metatablename)



            
    def dropTable(self, tablename: str, commitNow: bool=False):
        '''
        Drops a table.

        Parameters
        ----------
        tablename : str
            The table name.
        commitNow : bool, optional
            Calls commit on the database connection after the transaction if True. 
            The default is False.
        '''
        self.cur.execute(self._makeDropStatement(tablename))
        if commitNow:
            self.con.commit()

        # Remove from internal structure
        self._tables.pop(tablename) # TODO: handle meta/data table complications?
    
    ### These are useful methods to direct calls to a table or query tables
    def __getitem__(self, tablename: str):
        return self._tables[tablename]
            
    @property
    def relationships(self):
        """
        Returns a dictionary of parent (key) to child (value) relationships.
        Each key is a 2-tuple of the form tablename,columnname.
        Values are returned as a list of 2-tuples, as multiple children may
        point to the same parent, even from within the same child table.

        Returns
        -------
        dict
            Parent to child relationship dictionary.
        """
        return self._relations

    @property
    def tablenames(self):
        '''
        List of names of current tables in the database.
        You may need to call reloadTables() if something is missing.
        '''
        return list(self._tables.keys())
    
    @property
    def tables(self):
        '''
        Dictionary of TableProxy objects that can be used individually to do table-specific actions.
        You may need to call reloadTables() if something is missing.
        '''
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
            elif re.match(r"blob", col[1], flags=re.IGNORECASE):
                cols[colname] = ColumnProxy(colname, bytes)
            elif re.match(r"numeric", col[1], flags=re.IGNORECASE):
                cols[colname] = ColumnProxy(colname, (int, float))
            else:
                # cols[colname] = ColumnProxy(colname, object)
                raise NotImplementedError("Unknown parse for sql type %s" % col[1])
            
        return cols
    

    def __getitem__(self, i: slice):
        # For now, we don't have a built-in generator for limits and offsets
        # So we must build the statement ourselves
        if isinstance(i, int):
            self._parent.cur.execute(
                "SELECT * FROM %s LIMIT %d OFFSET %d" % (
                    self._tbl, 1, i
                )
            )
            results = self._parent.cur.fetchone()
        elif isinstance(i, slice):
            if i.step is not None and i.step != 1:
                raise ValueError("Cannot use a slice with a step")
            self._parent.cur.execute(
                "SELECT * FROM %s LIMIT %d OFFSET %d" % (
                    self._tbl, i.stop - i.start, i.start
                )
            )
            results = self._parent.cur.fetchall()
        return results

    @property
    def formatSpecifier(self):
        '''
        The format specifier for this table.
        Should effectively match the format specifier used during creation.
        '''
        return self._fmt
    
    @property
    def columns(self):
        '''
        Dictionary of ColumnProxy objects based on the table columns.
        '''
        return self._cols
    
    @property
    def columnNames(self):
        '''
        List of column names of the current table.
        '''
        return list(self._cols.keys())
 
    ### These are the actual user methods
    def select(self,
               columnNames: list,
               conditions: list=None,
               orderBy: list=None,
               encloseTableName: bool=True):
        '''
        Performs a select on the current table.

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

        encloseTableName : bool, optional
            Encloses the table name in quotes to allow for certain table names which may fail;
            for example, this is necessary if the table name starts with digits.
            The default is True.

        Returns
        -------
        stmt : str
            The actual sqlite statement that was executed.
        '''
        
        stmt = self._makeSelectStatement(
            columnNames,
            self._tbl,
            conditions,
            orderBy,
            encloseTableName
        )
        self._parent.cur.execute(stmt)
        return stmt
    
    def delete(self, 
        conditions:list, 
        commitNow: bool=False, 
        encloseTableName: bool=True
    ):
        """
        Performs a delete statement on the current table.
        This does not have a LIMIT so all rows that fulfill the condition will be deleted.

        Parameters
        ----------
        conditions : list
            The filter conditions placed after "where".
            A single condition may be specified as a string.

        commitNow : bool, optional
            Calls commit on the database connection after the transaction if True. 
            The default is False.

        encloseTableName : bool, optional
            Encloses the table name in quotes to allow for certain table names which may fail;
            for example, this is necessary if the table name starts with digits.
            The default is True.
        """
        
        stmt = self._makeDeleteStatement(
            self._tbl, conditions, encloseTableName)
        self._parent.cur.execute(stmt)
        if commitNow:
            self._parent.con.commit()
        return stmt

    
    def insertOne(self, 
                  *args, 
                  orReplace: bool=False, 
                  commitNow: bool=False, 
                  encloseTableName: bool=True):
        '''
        Performs an insert statement for just one row of data.
        Note that this method assumes that a full insert is being performed
        i.e. all columns will have a value inserted.

        Parameters
        ----------
        *args : iterable or dict
            An iterable of the data for the row to be inserted.
            No need to place the arguments in a tuple,
            simply place them one after another before the keyword args.
            In this mode, all columns must have a value inserted 
            (see dict insertion below if you have missing values).
            Example:
                Two REAL columns
                insertOne(10.0, 20.0)

            Can also use a dictionary to used the named column format for insertion.
            Missing columns will have NULLs inserted as per sqlite's norm.
            Example:
                # Columns are ['col1','col2','col3']
                row = {
                    'col1': 11,
                    'col3': 22
                }
                insertOne(row)
                # After selection, the corresponding row will be
                # {'col1': 11, 'col2': None, 'col3': 22}
                
        orReplace : bool, optional
            Overwrites the same data if True, otherwise a new row is created.
            The default is False.
            
        commitNow : bool, optional
            Calls commit on the database connection after the transaction if True. 
            The default is False.

        encloseTableName : bool, optional
            Encloses the table name in quotes to allow for certain table names which may fail;
            for example, this is necessary if the table name starts with digits.
            The default is True.

        Returns
        -------
        stmt : str
            The actual sqlite statement that was executed.
        '''
        if isinstance(args[0], tuple) or isinstance(args[0], list):
            raise TypeError("Do not enclose the arguments in a list/tuple yourself!")
        
        if isinstance(args[0], dict):
            keys = list(args[0].keys())
            stmt = self._makeInsertStatementWithNamedColumns(
                self._tbl, keys, orReplace, encloseTableName
            )
            self._parent.cur.execute(stmt, [args[0][k] for k in keys])
    
        else:
            stmt = self._makeInsertStatement(
                self._tbl, self._fmt, orReplace, encloseTableName
            )
            self._parent.cur.execute(stmt, (args))

        if commitNow:
            self._parent.con.commit()
        return stmt
        
    def insertMany(self, 
                   rows: list, 
                   orReplace: bool=False, 
                   commitNow: bool=False, 
                   encloseTableName: bool=True):
        '''
        Performs an insert statement for multiple rows of data.
        Note that this method assumes that a full insert is being performed
        i.e. all columns will have a value inserted.

        Parameters
        ----------
        rows : iterable or generator expression
            An iterable or generator expression of the data of multiple rows. 
            It is assumed implicitly that every column has a value inserted i.e.
            no missing columns. 
            
            Dictionary mode insertion like in insertOne() is not supported as the
            statement would mutate on every row. See insertManyNamedColumns() for such use-cases.
            See sqlite3.executemany() for more information.

            Example with list of tuples/lists:
                Two REAL columns
                insertMany([(10.0, 20.0),(30.0, 40.0)])
            Example with generator:
                data1 = np.array([...])
                data2 = np.array([...])
                insertMany(
                    ((data1[i], data2[i]) for i in range(data1.size))
                )
            
        orReplace : bool, optional
            Overwrites the same data if True, otherwise a new row is created for every clash.
            The default is False.
            
        commitNow : bool, optional
            Calls commit on the database connection after the transaction if True. 
            The default is False.

        encloseTableName : bool, optional
            Encloses the table name in quotes to allow for certain table names which may fail;
            for example, this is necessary if the table name starts with digits.
            The default is True.

        Returns
        -------
        stmt : str
            The actual sqlite statement that was executed.
        '''
        stmt = self._makeInsertStatement(
            self._tbl, self._fmt, orReplace, encloseTableName
        )

        self._parent.cur.executemany(stmt, rows)

        if commitNow:
            self._parent.con.commit()
        return stmt
    
    def insertManyNamedColumns(self, 
                               dictlist: list, 
                               orReplace: bool=False, 
                               commitNow: bool=False,
                               encloseTableName: bool=True):
        '''
        Performs an insert statement for multiple rows of data.
        This method assumes that every row inserts the same set of named columns.

        Parameters
        ----------
        dictlist : list of dictionaries
            List of dictionaries where the keys are the columns being inserted.
            Each row corresponds to a dictionary. Missing columns will have NULLs inserted as per sqlite's norm.
            Example:
                # Rows are ['A','B','C']
                dictlist = [
                    {'A': 1, 'B': 2},
                    {'A': 4, 'B': 5}
                ]
                insertManyNamedColumns(dictlist)
            
        orReplace : bool, optional
            Overwrites the same data if True, otherwise a new row is created for every clash.
            The default is False.
            
        commitNow : bool, optional
            Calls commit on the database connection after the transaction if True.
            The default is False.

        encloseTableName : bool, optional
            Encloses the table name in quotes to allow for certain table names which may fail;
            for example, this is necessary if the table name starts with digits.
            The default is True.

        Returns
        -------
        stmt : str
            The actual sqlite statement that was executed.
        '''
        keys = list(dictlist[0].keys())
        stmt = self._makeInsertStatementWithNamedColumns(
            self._tbl, keys, orReplace, encloseTableName
        )
        # Create a generator for the list of dictionaries
        g = (
            [dictlist[i][k] for k in keys]
            for i in range(len(dictlist))
        )
        self._parent.cur.executemany(stmt, g)

        if commitNow:
            self._parent.con.commit()

        return stmt
    
    def createView(
        self,
        columnNames: list,
        conditions: list=None,
        orderBy: list=None,
        viewtbl_name: str=None,
        ifNotExists: bool=False,
        encloseTableName: bool=True,
        commitNow: bool=False
    ):
        '''
        Creates a view based on the current table.
        A view is essentially a pre-defined select statement, and is useful
        for looking at data without writing to an actual table, or pre-pending
        large select statements before other select statements.

        Parameters
        ----------
        columnNames : list
            Columns to extract as part of the select. See select().
        conditions : list, optional
            Conditions of the select. See select(). By default None.
        orderBy : list, optional
            Ordering of the select. See select(). By default None.
        viewtbl_name : str, optional
            The view's name, akin to a table name. Defaults to the current table with '_view' appended.
        ifNotExists : bool, optional
            Prevents creation if the view already exists. The default is False.
        encloseTableName : bool, optional
            Encloses the view name in quotes to allow for certain view names which may fail;
            for example, this is necessary if the view name starts with digits.
            The default is True.
        commitNow : bool, optional
            Calls commit on the database connection after the transaction if True.
            The default is False.
        
        Returns
        -------
        stmt : str
            The executed create view statement.
        '''
        # Default view name simply appends _view
        if viewtbl_name is None:
            viewtbl_name = self._tbl + "_view"

        # Generate select statement
        selectStmt = self._makeSelectStatement(
            columnNames,
            self._tbl,
            conditions,
            orderBy,
            encloseTableName
        )

        # Generate create view statement and execute
        stmt = self._makeCreateViewStatemnt(
            selectStmt,
            viewtbl_name,
            ifNotExists,
            encloseTableName
        )
        self._parent.cur.execute(stmt)
        if commitNow:
            self._parent.con.commit()

        return stmt
    
    ### Foreign-key specific methods
    def retrieveParentRow(self, row: sq.Row, foreignKey: str=None):
        """
        Performs a select on the associated parent table and row specified by the foreign key
        in the current child table. You are expected to call the fetch() flavours yourself afterwards.

        This is equivalent to performing

        "SELECT * FROM PARENT WHERE PARENT_KEY=SOME_VALUE"

        Parameters
        ----------
        row : sq.Row
            A row result returned from a select query on this current (child) table.
        foreignKey : str, optional
            The foreign key (column name) whose parent you seek, by default None,
            which just uses the first foreign key in the schema.

        Raises
        ------
        KeyError
            If foreign key is specified and does not exist.
        """
        # If no foreign key specified, assume the first foreign key in the schema
        if foreignKey is None:
            fk = self._fmt['foreign_keys'][0]
        else:
            fk = None
            for tfk in self._fmt['foreign_keys']:
                if tfk[0] == foreignKey:
                    fk = tfk
                    break
            if fk is None:
                raise KeyError(
                    f"The foreign key {foreignKey} does not exist in the table {self._tbl}"
                )
            
        # Extract the child column
        child_col = fk[0]

        # Extract the parent
        parent = fk[1]
        parentspl = parent.split("(")
        parent_table = parentspl[0]
        parent_col = parentspl[1][:-1]

        # Now perform a select on the parent table
        stmt = self._parent[parent_table].select("*", ["%s=%s" % (parent_col, str(row[child_col]))])
        return stmt

        
#%%
class ViewProxy(TableProxy):
    def __init__(self, parent: SqliteContainer, tbl: str):
        super().__init__(parent, tbl, None)

    def _populateColumns(self):
        return None
    
    def __getitem__(self, col: str):
        raise NotImplementedError("Invalid for view.")
    
    @property
    def columns(self):
        '''
        Dictionary of ColumnProxy objects based on the table columns.
        Not implemented for 
        '''
        raise NotImplementedError("Invalid for view.")
    
    @property
    def columnNames(self):
        '''
        List of column names of the current table.
        '''
        raise NotImplementedError("Invalid for view.")
    

#%% We have a special subclass for tables that are treated as metadata for other tables
# These tables contain a data_tblname column, and then all other columns are treated as metadata for it.
# This is especially useful if a table has a bunch of constant columns, but across tables these columns may have different values
# i.e. something like a table of processing results for a particular run, but each table used a different config file.
class MetaTableProxy(TableProxy):
    requiredColumn = 'data_tblname'
    requiredTableSuffix = "_metadata"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Metadata tables MUST end with "_metadata"
        if not self._tbl.endswith(self.requiredTableSuffix): # We throw if either created wrongly by user or accidentally internally
            raise ValueError("Metadata tables must end with '_metadata' but %s did not!" % self._tbl)

        # Format must contain 'data_tblname', and all other columns are treated as the actual metadata
        if not FormatSpecifier.dictContainsColumn(self._fmt, self.requiredColumn):
            raise ValueError("Format must contain 'data_tblname' as the primary key, but %s did not!" % self._fmt)

    def getMetadataFor(self, data_tblname: str):
        '''
        Returns the metadata for a particular data_tblname.

        Parameters
        ----------
        data_tblname : str
            The name of the data table to get the metadata for.

        Returns
        -------
        metadata : sqlite3.Row
            The metadata for the data_tblname.
        '''
        stmt = self._makeSelectStatement("*", self._tbl, ["data_tblname='%s'" % data_tblname])
        self._parent.cur.execute(stmt)
        metadata = self._parent.cur.fetchone()
        if metadata is None:
            raise ValueError("No metadata found for %s!" % data_tblname)
        return metadata

    def getDataTables(self):
        '''
        Returns a list of all the data tables associated to this metadata table.

        Returns
        -------
        data_tblnames : list
            A list of all the data tables in the database.
        '''
        stmt = self._makeSelectStatement("data_tblname", self._tbl)
        self._parent.cur.execute(stmt)
        data_tblnames = [row[0] for row in self._parent.cur.fetchall()]
        return data_tblnames

#%% Data tables act exactly like any other table, but keep track of their metadatatable internally
class DataTableProxy(TableProxy):
    def __init__(self, parent: SqliteContainer, tbl: str, fmt: dict, metadatatable: str=None):
        super().__init__(parent, tbl, fmt)
        self._metadatatable = metadatatable # Keeps track of metadatatable

    def setMetadataTable(self, metadatatable: str):
        '''Sets the metadata table name for this data table.'''
        self._metadatatable = metadatatable

    @property
    def metadataTablename(self):
        '''Returns the metadata tablename.'''
        return self._metadatatable

    def getMetadata(self):
        '''
        Returns the metadata for this data table by accessing the associated
        metadata table and extracting the relevant row.

        Returns
        -------
        metadata : sqlite3.Row
            Sqlite row result for the metadata.
            This will usually contain the current table's name as the first column.
        '''
        # We access the metadata table through the parent container
        return self._parent[self._metadatatable].getMetadataFor(self._tbl)
    
        

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


class ColumnProxyContainer:
    def __init__(self, cols: dict[ColumnProxy]):
        self._cols = cols
        # Write a setattr for each column name
        for colProxy in self._cols:
            setattr(self, colProxy, self._cols[colProxy])



#%% Inherited class of all the above
class Database(CommonRedirectMixin, CommonMethodMixin, SqliteContainer):
    def __init__(self, dbpath: str, row_factory: type=sq.Row, pragma_foreign_keys: bool=True):
        '''
        Instantiates an sqlite database container with all extra functionality included.
        This enables:
            CommonRedirectMixin
                Provides several redirects to common methods, just to have shorter code.
            CommonMethodMixin
                Includes common methods to create and drop tables, and provides
                dictionary-like access to all tables currently in the database.
                Also includes internal methods for statement generation.
            
        Parameters
        ----------
        dbpath : str
            The database to connect to (either a filepath or ":memory:").
            See sqlite3.connect() for more information.
        row_factory : type, optional
            The row factory for the sqlite3 connection. The default is the in-built sqlite3.Row.
        pragma_foreign_keys : bool, optional
            Turns on PRAGMA FOREIGN_KEYS. The default is True.
        '''
        super().__init__(dbpath, row_factory, pragma_foreign_keys)

    
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
    data = ((i, float(i+1)) for i in range(10)) # Generator expression
    print(d['table1'].insertMany(data))
    # Then check our results
    print(d['table1'].select("*"))
    results = d.fetchall()
    for result in results:
        print(result[0], result[1])
    