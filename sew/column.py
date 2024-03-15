

#%% And also a class for columns
### TODO: Intention for this is to build it into a way to automatically generate conditions in select statements..
class ColumnProxy:
    """
    Class to represent a column in a table.
    Contains information about the column name, and its type hint if specified.

    Useful in producing conditions and references in more complicated statements.
    """

    def __init__(self, name: str, typehint: type, tblname: str):
        self._name = name
        self._typehint = typehint
        self._tblname = tblname

    @property
    def name(self):
        return self._name

    @property
    def typehint(self):
        return self._typehint

    @property
    def tablename(self):
        return self._tblname

    def __repr__(self):
        return "ColumnProxy(%s, %s)" % (self._name, self._typehint)

    def __str__(self):
        return "%s %s" % (self._name, self._typehint)

    def _requireColumnProxy(self, x):
        if not isinstance(x, self.__class__):
            raise TypeError("Comparisons are only allowed between ColumnProxy objects.")

    # def _requireType(self, x):
    #     if not isinstance(x, self.typehint):
    #         raise TypeError("Compared value must be of type %s" % str(self.typehint))

    def __lt__(self, x):
        self._requireColumnProxy(x)
        return "%s < %s" % (self._name, x.name)

    def __le__(self, x):
        self._requireColumnProxy(x)
        return "%s <= %s" % (self._name, x.name)

    def __gt__(self, x):
        self._requireColumnProxy(x)
        return "%s > %s" % (self._name, x.name)

    def __ge__(self, x):
        self._requireColumnProxy(x)
        return "%s >= %s" % (self._name, x.name)

    def __eq__(self, x):
        self._requireColumnProxy(x)
        return "%s = %s" % (self._name, x.name)

    def __ne__(self, x):
        self._requireColumnProxy(x)
        return "%s != %s" % (self._name, x.name)

#%%
class ColumnProxyContainer:
    """
    Container of ColumnProxy objects, useful to access each ColumnProxy by name
    as an attribute.

    This makes the code easier to type as opposed to a dictionary; e.g.
    mytable.cols.mycolname
    """
    def __init__(self, columnProxies: list[ColumnProxy]):
        lastTablename = None
        for col in columnProxies:
            if lastTablename is not None and lastTablename != col.tablename:
                raise ValueError("ColumnProxyContainer can only contain columns from the same table.")
            lastTablename = col.tablename
            # Here is where we set the attribute directly
            setattr(self, col.name, col)



