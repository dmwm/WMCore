#! /usr/bin/env python
from __future__ import division, print_function
import time


def makeList(stringList):
    """
    _makeList_

    Make a python list out of a comma separated list of strings,
    throws a ValueError if the input is not well formed.
    If the stringList is already of type list, then return it untouched.
    """
    if isinstance(stringList, list):
        return stringList
    if isinstance(stringList, str):
        toks = stringList.lstrip(' [').rstrip(' ]').split(',')
        if toks == ['']:
            return []
        return [str(tok.strip(' \'"')) for tok in toks]
    raise ValueError("Can't convert to list %s" % stringList)


def makeNonEmptyList(stringList):
    """
    _makeNonEmptyList_

    Given a string or a list of strings, return a non empty list of strings.
    Throws an exception in case the final list is empty or input data is not
    a string or a python list
    """
    finalList = makeList(stringList)
    if not finalList:
        raise ValueError("Input data cannot be an empty list %s" % stringList)
    return finalList


def strToBool(string):
    """
    _strToBool_

    Try to convert a string to boolean. i.e. "True" to python True
    """
    if string in [False, True]:
        return string
    elif string in ["True", "true", "TRUE"]:
        return True
    elif string in ["False", "false", "FALSE"]:
        return False
    raise ValueError("Can't convert to bool: %s" % string)


def safeStr(string):
    """
    _safeStr_

    Cast simple data (int, float, basestring) to string.
    """
    if not isinstance(string, (tuple, list, set, dict)):
        return str(string)
    raise ValueError("We're not supposed to convert %s to string." % string)


def timeit(func):
    """
    source: https://www.andreas-jung.com/contents/a-python-decorator-for-measuring-the-execution-time-of-methods

    Decorator function to measure how long a method/function takes to run
    It returns a tuple with:
      * wall clock time spent
      * returned result of the function
      * the function name
    """
    def wrapper(*arg, **kw):
        t1 = time.time()
        res = func(*arg, **kw)
        t2 = time.time()
        return (t2 - t1), res, func.__name__

    return wrapper