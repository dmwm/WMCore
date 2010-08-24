#!/usr/bin/env python
"""
_BulkNewReturn_

SQLite implementation of Fileset.BulkNewReturn
"""

__all__ = []



from WMCore.WMBS.MySQL.Fileset.BulkNewReturn import BulkNewReturn as MySQLBulkNewReturn

class BulkNewReturn(MySQLBulkNewReturn):
    """
    Does a bulk commit of Fileset, followed by returning their IDs

    """
