#!/usr/bin/env python
"""
_BulkNewReturn_

SQLite implementation of JobGroup.BulkNewReturn
"""

__all__ = []
__revision__ = "$Id: BulkNewReturn.py,v 1.1 2010/02/25 21:48:17 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.BulkNewReturn import BulkNewReturn as MySQLBulkNewReturn

class BulkNewReturn(MySQLBulkNewReturn):
    """
    Does a bulk commit of jobGroups, followed by returning their IDs and UIDs

    """
