#!/usr/bin/env python
"""
_FailOrphanFiles_

SQLite implementation of Subscription.FailOrphanFiles
"""




from WMCore.WMBS.MySQL.Subscriptions.FailOrphanFiles import FailOrphanFiles as MySQLFailOrphanFiles

class FailOrphanFiles(MySQLFailOrphanFiles):
    pass
