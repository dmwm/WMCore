#!/usr/bin/env python
"""
_AcquireFilesByRun_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFilesByRun.py,v 1.1 2009/05/01 19:42:49 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesByRun \
    import GetAvailableFilesByRun as GetAvailableFilesByRunMySQL

class GetAvailableFilesByRun(GetAvailableFilesByRunMySQL):
    def getSQL(self, subscription, conn, transaction):
        return GetAvailableFilesByRunMySQL.getSQL(self, subscription, 
                                                  conn, transaction)