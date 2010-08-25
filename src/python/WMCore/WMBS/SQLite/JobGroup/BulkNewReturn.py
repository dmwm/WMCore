#!/usr/bin/env python
"""
_BulkNewReturn_

SQLite implementation of JobGroup.BulkNewReturn
"""

__revision__ = "$Id: BulkNewReturn.py,v 1.2 2010/03/08 16:31:14 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.JobGroup.BulkNewReturn import BulkNewReturn as MySQLBulkNewReturn

class BulkNewReturn(MySQLBulkNewReturn):
    pass
