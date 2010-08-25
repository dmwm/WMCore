#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFiles.py,v 1.8 2009/03/16 16:58:38 sfoulkes Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles \
     as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL):
    def getSQLAndBinds(self, subscription, maxFiles, conn, transaction):
        return GetAvailableFilesMySQL.getSQLAndBinds(self, subscription,
                                                     maxFiles, conn,
                                                     transaction)
