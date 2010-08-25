#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles \
     as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL):
    def getSQLAndBinds(self, subscription, conn, transaction):
        return GetAvailableFilesMySQL.getSQLAndBinds(self, subscription,
                                                     conn, transaction)
