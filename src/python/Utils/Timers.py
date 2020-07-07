#!/bin/env python
"""
Utilities related to timing and performance testing
"""

from __future__ import print_function, division, absolute_import

from builtins import object
import time


def timeFunction(func):
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
        return round((t2 - t1), 4), res, func.__name__

    return wrapper


class CodeTimer(object):
    """
    A context manager for timing function calls.
    Adapted from https://www.blog.pythonlibrary.org/2016/05/24/python-101-an-intro-to-benchmarking-your-code/

    Use like

    with CodeTimer(label='Doing something'):
        do_something()
    """

    def __init__(self, label='The function'):
        self.start = time.time()
        self.label = label

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end = time.time()
        runtime = end - self.start
        msg = '{label} took {time} seconds to complete'
        print(msg.format(label=self.label, time=runtime))
