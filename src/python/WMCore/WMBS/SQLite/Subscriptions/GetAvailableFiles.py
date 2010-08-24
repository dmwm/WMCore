#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFiles.py,v 1.4 2008/11/11 14:03:06 metson Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL, SQLiteBase):
    def getSQLAndBinds(self, subscription):
        return GetAvailableFilesMySQL.getSQLAndBinds(self, subscription)