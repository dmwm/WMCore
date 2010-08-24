#!/usr/bin/env python
"""
_AcquireFilesByRun_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesByRun \
    import GetAvailableFilesByRun as GetAvailableFilesByRunMySQL

class GetAvailableFilesByRun(GetAvailableFilesByRunMySQL):
    def getSQL(self, subscription, conn, transaction):
        return GetAvailableFilesByRunMySQL.getSQL(self, subscription, 
                                                  conn, transaction)