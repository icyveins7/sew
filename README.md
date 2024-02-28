# sew ![GitHub Workflow Status (with event)](https://img.shields.io/github/actions/workflow/status/icyveins7/sew/run-unit-tests.yml)

Sqlite Extensions &amp; Wrappers for Python.

Tired of writing create/insert/select statements yourself? Or maybe you'd just like to access your tables with keys like a Pythonic dictionary? Then just subclass this.

# Motivation

Many processes for data manipulation with sqlite databases can be achieved with big-data libraries like Pandas. Sometimes, however, you might want some fine-grained control over the database creation and modification.

For example, Pandas currently does not support replacement inserts on a per-row basis. The .to_sql() method will only replace the entire table if specified.

A general ethos for this framework is to reduce the developer's code typed; if the sqlite statement you require is complicated then it may well be more efficient to use a custom statement yourself. Fundamentally, ```sew``` is still meant to only function as a wrapper around the default-included sqlite for Python.

However, for simple CRUD operations it should be easier to use methods this framework provides!

# Installation
As this is currently not a wheel, simply clone and then install in editable mode.

```
git clone https://github.com/icyveins7/sew
cd sew
pip install -e .
```

You should then be able to use it from anywhere. Updates just require a ```git pull``` command on your repository folder.

There are no additional requirements to install to use the base functionality, as this is just a wrapper around the default-included sqlite for Python. However, if you want to use the extra plugins, you can do

```
pip install -r plugin_requirements.txt
```

# Usage
The most common use-case is to initialise a ```Database``` object, just like you would with sqlite3.

```python
import sew
d = sew.Database(":memory:")
```

This contains the sqlite3.Connection as well as the sqlite3.Cursor objects, referenced with ```.con``` and ```.cur``` respectively.

Some common commands have shortcuts, if that's all you want to do:

```python
d.execute() # Same as d.cur.execute
d.executemany() # Same as d.cur.executemany
d.commit() # Same as d.con.commit
```

But the main benefit is in shortened requirements for database changes. Docstrings are available for most common methods.

As an example, instead of writing the whole statements, you can do the following:

## Create Tables

```python
d.createTable(fmt, "mytablename")
# You can now access this table via dict-like methods like d['mytablename']
# See below for uses
```

## Format Specifiers
This framework operates using a pre-defined dictionary structure specifying each table's format. Fundamentally, this is just a dictionary that looks like this:

```python
fmt = {
    'cols': [
        ['col1', 'integer'], # list of lists, with inner list begin column name and sqlite type in that order
        ['col2', 'REAL'] # the type upper/lower-case doesn't matter
    ],
    'conds': [
        "UNIQUE(col1, col2)" # this is just a list of strings, with each one specifying an extra condition
    ],
    'foreign_keys': [
        ["col1", "parent_table(parent_col)"] # This specifies a foreign key relationship
    ]
}
```

You can specify this directly yourself, if you're familiar with sqlite3, or you can use the ```FormatSpecifier``` object to generate one like so:

```python
fmtspec = sew.FormatSpecifier()
fmtspec.addColumn('col1', int) # Yes, it accepts the pythonic type directly
fmtspec.addColumn('col2', float)
fmtspec.addUniques(['col1','col2'])
fmtspec.addForeignKey(["col1", "parent_table(parent_col)"])
fmt = fmtspec.generate() # Then use this wherever you need, like the createTable() call
```

Example of how the framework uses this in the create table example:

```python
stmt = d.createTable(fmt, 'tbl2')
>>> 'create table "tbl2"(col1 INTEGER, col2 REAL, UNIQUE(col1,col2), FOREIGN KEY(col1) REFERENCES parent_table(parent_col))'
```

You can also reference a format specifier from the table; this should be identical to what was used during creation.
But for cases where you need a python-parsable object that reflects the table schema:

```python
fmt = d['tbl2'].formatSpecifier

>>> {'cols': [['col1', 'INTEGER'], ['col2', 'REAL']],
     'conds': ['UNIQUE(col1,col2)'],
     'foreign_keys': [['col1', 'parent_table(parent_col)']]}
```

## Selects

```python
stmt = d["mytablename"].select(["thisColumn", "thatColumn"], ["otherColumn < 10"])
results = d.fetchall()
print(stmt) # Most commands return the statement generated, so you can check if needed
```

## Inserts

Insertions of complete rows (which is probably the most common scenario):

```python
d["mytablename"].insertOne(
    1, 2.0, # For insertOne, just place the arguments one after another; no need to pack into a list/tuple/dict
    orReplace=True # use a replace instead of insert
)

data = ((i, float(i+1)) for i in range(10)) # Generator expression
d["mytablename"].insertMany(
    data # Anything that works with executemany should work here
) 
```

You can also insert specific columns:

```python
# Columns are col1, col2, col3
d["mytablename"].insertOne(
    {'col1': 1, 'col2': 2}, # Only insert 2 column values, leaving col3 NULL
    orReplace=True # use a replace instead of insert
)
```

## Hard Refresh of Internal Tables Structure

```python
d.reloadTables()
```

This is generally only required to be performed when commits of table changes are made from another source - outside the current interpreter. This will force ```sew``` to re-execute a select statement from ```sqlite_master``` and re-populate the internal tables.
It is automatically called when first instantiating the ```sew.Database``` object.

## Other Examples

Many of the examples are encoded into actual tests! Take a look at the ```tests``` folder to see examples of everything from standard selects and inserts to the use of the plugins.


# Plugins

## Pandas
You can return Pandas dataframes using the ```PandasDatabase```, which simply forwards selects to ```read_sql```, but provides the same automatic statement generation behaviour and dictionary-like table access as before. The difference is that results are now extracted via the property ```.pdresults```.

```python
import sew.plugins

d = sew.plugins.PandasDatabase(...)
d['mytable'].select(...)
dataframe = d['mytable'].pdresults
```

## NumPy
If you'd like direct NumPy array support (rather than converting to and from Pandas dataframes), you can use the ```NumpyDatabase```, which performs inserts by assigning each array to a column. Select-statement behaviour returns arrays as a named dictionary, with keys as the column names.

```python
import sew.plugins

d = sew.plugins.NumpyDatabase(...)

data_f64 = np.array([...], dtype=np.float64)
data_f32 = np.array([...], dtype=np.float32)

# Table is created with "col1_f64 REAL, col2_f32 REAL"
d['mytable'].insertMany(data_f64, data_f32)
```

All datatypes should be preserved where possible; that means if you inserted an np.uint8 array, it should automatically return as an np.uint8 array. This is achieved via explicit encoding of the datatypes into the column names as suffixes. You can view these as the class global variable ```NummpyTableProxy.numpyColumnSuffixes```.


# Running Unit Tests and Benchmarks
Unit tests in the ```tests``` folder are a good way to look at some examples. To run them all, simply run the following command from the main repository ```sew``` folder (where the ```tests``` folder is visible):

```bash
python -m tests
```

Individual tests can be run by simply doing:

```bash
python -m tests.correctness
```

Benchmarks are similarly run:

```bash
python -m benchmarks
```