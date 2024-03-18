# Import this for type hints with classes
from __future__ import annotations
import re

class Condition:
    """
    Helper class to generate SQLite condition substrings.

    SQLite SELECT statements often have conditions. This class attempts to
    generate the condition substrings for any number of conditions, by chaining them together
    using pythonic operators.

    This works by always instantiating a Condition object, and then using standard comparison operators
    like ==, !=, >, <, >= and <=. At its heart, the class simply contains an extending string, and mutates this
    internally after every operator.

    Examples:

    Condition("col1") == '5'
    >> col1 = 5

    Condition("col1") > '5'
    >> col1 > 5

    You can also then chain multiple conditions together by doing it step by step:

    Example:
    c = Condition("col1") == '5'
    c & 'col2'
    c > '10'
    >> col1 = 5 AND col2 > 10

    Usually, the operators are easy to insert directly into the string yourself, so you can just use the
    &, | operators to splice multiple conditions together (this is the recommendation, as it clearly demarcates
    the different conditions and whether to AND or OR them together).:

    Example:
    c = Condition("col1 = 5")
    c & "col2 > 10"
    c | "col3 < 20"
    >> col1 = 5 AND col2 > 10 OR col3 < 20

    Note that due to the nature of Python's built-in operators, you can't place them all in one line unless you wrap each step in parentheses.

    Example:

    c = Condition("col1") == '5' & 'col2' > '10' # This does not work because Python will compare 'col2' > '10' separately which will give an error.
    c = (Condition("col1") == '5') & (Condition("col2") > '10') # This is a possible workaround, but is slightly verbose.
    c = ((Condition("col1") == '5') & "col2") > '10' # This is another workaround, which is less verbose but will require parentheses after every operator.

    c = Condition("col1 = 5") & Condition("col2 = 10") & Condition("col3 = 6") # No parentheses needed if objects completely wrap each substring, but again very verbose.
    c = (Condition("col1 = 5") & "col2 = 10") & "col3 = 6" # Some parentheses needed, but much less verbose.
    """
    def __init__(self, cond: str):
        if not isinstance(cond, str):
            raise TypeError("Condition must be a string.")
        self._cond = cond
        # A composite condition is one that is made up of several different conditions,
        # stitched via AND or OR keywords, so we search for these
        if re.search("(and|or)", self._cond, re.IGNORECASE) is not None:
            self._isComposite = True
        else:
            self._isComposite = False


    def __str__(self) -> str:
        return self._cond

    def __repr__(self) -> str:
        return self._cond

    # There are a few SQLite conditions that don't really have an 'operator'
    def LIKE(self, other: Condition) -> Condition:
        # May be a string, in which case just attach it to the current condition
        if isinstance(other, str):
            condstr = "%s LIKE %s" % (self._cond, other)

        # 
        elif isinstance(other, Condition):
            condstr = "%s LIKE %s" % (self._cond, other._cond)

        else:
            raise TypeError("Condition must be a string or Condition.")

        return Condition(condstr)

    def IN(self, other: Condition) -> Condition:
        # May be a tuple or list of strings
        if isinstance(other, list) or isinstance(other, tuple):
            condstr = "%s IN (%s)" % (self._cond, ",".join(other))

        # 
        elif isinstance(other, Condition):
            condstr = "%s IN (%s)" % (self._cond, ",".join(other._cond))

        else:
            raise TypeError("Condition must be a list/tuple or Condition.")

        return Condition(condstr)

    # TODO: ALL, ANY, BETWEEN, EXISTS

    def _compositeBracket(self):
        """
        Helper method to surround the condition with brackets if it is a composite condition
        e.g. is conditionA AND conditionB.

        You need this in non-commutative conditions e.g.
        A AND B OR C is not the same as A AND (B OR C).
        """
        if self._isComposite:
            return "(" + self._cond + ")"
        else:
            return self._cond


    ##### Composite Operator overloads
    def __and__(self, other: Condition) -> Condition:
        # If it's a string, convert to a Condition and then use the convenience operator
        if isinstance(other, str):
            condstr = "%s AND %s" % (
                self._compositeBracket(),
                other._compositeBracket()
            )

        elif isinstance(other, Condition):
            condstr = "%s AND %s" % (
                self._compositeBracket(),
                other._compositeBracket()
            )

        else:
            raise TypeError("Condition must be a string or Condition.")

        return Condition(condstr)



    def __or__(self, other: Condition) -> Condition:
        # If it's a string, convert to a Condition and then use the convenience operator
        if isinstance(other, str):
            condstr = "%s OR %s" % (
                self._compositeBracket(), 
                Condition(other)._compositeBracket()
            )

        elif isinstance(other, Condition):
            condstr = "%s OR %s" % (
                self._compositeBracket(),
                other._compositeBracket()
            )

        else:
            raise TypeError("Condition must be a string or Condition.")

        return Condition(condstr)


    ##### Simple comparison operator overloads
    def __eq__(self, other: Condition) -> Condition:
        # If it's a string, convert to a Condition and then use the convenience operator
        if isinstance(other, str):
            condstr = "%s = %s" % (self._cond, other)

        elif isinstance(other, Condition):
            condstr = "%s = %s" % (self._cond, other._cond)

        else:
            raise TypeError("Condition must be a string or Condition.")

        return Condition(condstr)

    def __ne__(self, other: Condition) -> Condition:
        # May be a string, in which case just attach it to the current condition
        if isinstance(other, str):
            condstr = "%s != %s" % (self._cond, other)

        # 
        elif isinstance(other, Condition):
            condstr = "%s != %s" % (self._cond, other._cond)

        else:
            raise TypeError("Condition must be a string or Condition.")

        return Condition(condstr)

    def __gt__(self, other: Condition) -> Condition:
        # May be a string, in which case just attach it to the current condition
        if isinstance(other, str):
            condstr = "%s > %s" % (self._cond, other)

        # 
        elif isinstance(other, Condition):
            condstr = "%s > %s" % (self._cond, other._cond)

        else:
            raise TypeError("Condition must be a string or Condition.")

        return Condition(condstr)

    def __ge__(self, other: Condition) -> Condition:
        # May be a string, in which case just attach it to the current condition
        if isinstance(other, str):
            condstr = "%s >= %s" % (self._cond, other)

        # 
        elif isinstance(other, Condition):
            condstr = "%s >= %s" % (self._cond, other._cond)

        else:
            raise TypeError("Condition must be a string or Condition.")

        return Condition(condstr)

    def __lt__(self, other: Condition) -> Condition:
        # May be a string, in which case just attach it to the current condition
        if isinstance(other, str):
            condstr = "%s < %s" % (self._cond, other)

        # 
        elif isinstance(other, Condition):
            condstr = "%s < %s" % (self._cond, other._cond)

        else:
            raise TypeError("Condition must be a string or Condition.")

        return Condition(condstr)

    def __le__(self, other: Condition) -> Condition:
        # May be a string, in which case just attach it to the current condition
        if isinstance(other, str):
            condstr = "%s <= %s" % (self._cond, other)

        # 
        elif isinstance(other, Condition):
            condstr = "%s <= %s" % (self._cond, other._cond)

        else:
            raise TypeError("Condition must be a string or Condition.")

        return Condition(condstr)

