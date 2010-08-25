#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFiles.py,v 1.9 2009/03/18 13:21:59 sfoulkes Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles \
     as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL):
    def getSQLAndBinds(self, subscription, conn, transaction):
        return GetAvailableFilesMySQL.getSQLAndBinds(self, subscription,
                                                     conn, transaction)
