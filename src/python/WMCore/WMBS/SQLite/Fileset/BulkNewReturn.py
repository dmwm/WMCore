#!/usr/bin/env python
"""
_BulkNewReturn_

SQLite implementation of Fileset.BulkNewReturn
"""

__all__ = []
__revision__ = "$Id: BulkNewReturn.py,v 1.1 2010/02/25 21:47:25 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Fileset.BulkNewReturn import BulkNewReturn as MySQLBulkNewReturn

class BulkNewReturn(MySQLBulkNewReturn):
    """
    Does a bulk commit of Fileset, followed by returning their IDs

    """
