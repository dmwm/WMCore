#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFiles.py,v 1.7 2008/12/05 21:06:58 sryu Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles \
     as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL):
    def getSQLAndBinds(self, subscription, conn, transaction):
        return GetAvailableFilesMySQL.getSQLAndBinds(self, subscription, conn,
                                                     transaction)
