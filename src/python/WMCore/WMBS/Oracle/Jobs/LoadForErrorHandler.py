#!/usr/bin/env python
"""
_LoadForErrHandler_

Oracle implementation of Jobs.LoadForErrorHandler.
"""

from WMCore.WMBS.MySQL.Jobs.LoadForErrorHandler import LoadForErrorHandler as MySQLLoadForErrorHandler


class LoadForErrorHandler(MySQLLoadForErrorHandler):
    """
    _LoadForErrorHandler_

    If it's not the same as MySQL, I don't want to know.
    """
    pass