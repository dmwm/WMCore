#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAcquiredFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetAcquiredFiles.py,v 1.1 2008/10/08 14:30:11 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.GetAcquiredFiles import GetAcquiredFiles as GetAcquiredFilesMySQL

class GetAcquiredFiles(GetAcquiredFilesMySQL, SQLiteBase):
    sql = GetAcquiredFilesMySQL.sql

