#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFiles.py,v 1.1 2008/10/08 14:30:10 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL, SQLiteBase):
    sql = GetAvailableFilesMySQL.sql