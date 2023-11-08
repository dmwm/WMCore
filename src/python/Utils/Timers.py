#!/bin/env python
"""
Utilities related to timing and performance testing
"""

from builtins import object
import logging
import time
import calendar
from datetime import tzinfo, timedelta


def gmtimeSeconds():
    """
    Return GMT time in seconds
    """
    return int(time.mktime(time.gmtime()))


def encodeTimestamp(secs):
    """
    Encode second since epoch to a string GMT timezone representation
    :param secs: input timestamp value (either int or float) in seconds since epoch

    :return: time string in GMT timezone representation
    """
    if not isinstance(secs, (int, float)):
        raise Exception("Wrong input, should be seconds since epoch either int or float value")
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(secs)))


def decodeTimestamp(timeString):
    """
    Decode timestamps in provided document
    :param timeString: timestamp string represention in GMT timezone, see encodeTimestamp

    :return: seconds since ecouch in GMT timezone
    """
    if not isinstance(timeString, str):
        raise Exception("Wrong input, should be time string in GMT timezone representation")
    return calendar.timegm(time.strptime(timeString, "%Y-%m-%dT%H:%M:%SZ"))


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

    def __init__(self, label='The function', logger=None):
        self.start = time.time()
        self.label = label
        self.logger = logger or logging.getLogger()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end = time.time()
        runtime = round((end - self.start), 3)
        self.logger.info(f"{self.label} took {runtime} seconds to complete")


class LocalTimezone(tzinfo):
    """
    A required python 2 class to determine current timezone for formatting rfc3339 timestamps
    Required for sending alerts to the MONIT AlertManager
    Can be removed once WMCore starts using python3

    Details of class can be found at: https://docs.python.org/2/library/datetime.html#tzinfo-objects
    """

    def __init__(self):
        super(LocalTimezone, self).__init__()
        self.ZERO = timedelta(0)
        self.STDOFFSET = timedelta(seconds=-time.timezone)
        if time.daylight:
            self.DSTOFFSET = timedelta(seconds=-time.altzone)
        else:
            self.DSTOFFSET = self.STDOFFSET

        self.DSTDIFF = self.DSTOFFSET - self.STDOFFSET

    def utcoffset(self, dt):
        if self._isdst(dt):
            return self.DSTOFFSET
        else:
            return self.STDOFFSET

    def dst(self, dt):
        if self._isdst(dt):
            return self.DSTDIFF
        else:
            return self.ZERO

    def tzname(self, dt):
        return time.tzname[self._isdst(dt)]

    def _isdst(self, dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = time.mktime(tt)
        tt = time.localtime(stamp)
        return tt.tm_isdst > 0
