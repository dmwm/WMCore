#!/usr/bin/env python
"""
_FailOrphanFiles_

SQLite implementation of Subscription.FailOrphanFiles
"""

__revision__ = "$Id: FailOrphanFiles.py,v 1.1 2010/08/13 21:20:09 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.FailOrphanFiles import FailOrphanFiles as MySQLFailOrphanFiles

class FailOrphanFiles(MySQLFailOrphanFiles):
    pass
