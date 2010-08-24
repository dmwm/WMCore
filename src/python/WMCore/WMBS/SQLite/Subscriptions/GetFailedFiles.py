#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.GetFailedFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetFailedFiles.py,v 1.2 2008/07/21 14:27:06 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import GetFailedFiles as GetFailedFilesMySQL

class GetFailedFiles(GetFailedFilesMySQL, SQLiteBase):
    sql = GetFailedFilesMySQL.sql