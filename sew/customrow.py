import sqlite3 as sq

class CustomRow(sq.Row):
    """
    Small extension to the inbuilt sqlite3.Row object.

    - Defines __repr__ by simply converting to a dict when invoked.
    """
    def __repr__(self):
        return dict(self).__repr__()
