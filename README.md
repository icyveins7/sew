# sew
Sqlite Extensions &amp; Wrappers for Python.

Tired of writing create/insert/select statements yourself? Then just subclass this.

# Installation
As this is currently not a wheel, simply clone and then install in editable mode.

```
git clone https://github.com/icyveins7/sew
cd sew
pip install -e .
```

You should then be able to use it from anywhere.

# Usage
The most common use-case is to initialise a ```Database``` object, just like you would with sqlite3.

```python
import sew
d = Database(":memory:")
```

This contains the sqlite3.Connection as well as the sqlite3.Cursor objects, referenced with ```.con``` and ```.cur``` respectively.

Some common commands have shortcuts, if that's all you want to do:

```python
d.execute() # Same as d.cur.execute
d.executemany() # Same as d.cur.executemany()
d.commit() # Same as d.con.commit()
```

But the main benefit is in shortened requirements for database changes. Instead of writing the whole statements, you can do the following:

## Create Tables

```python
d.createTable(fmt, "mytablename")
d.reloadTables()
```

## Selects

```python
d["mytablename"].select(["thisColumn", "thatColumn"], ["otherColumn < 10"])
```
