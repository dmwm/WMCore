#!/usr/bin/env python
"""
_GetCompletedFiles_

SQLite implementation of Subscription.GetCompletedFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetCompletedFiles.py,v 1.1 2008/10/08 14:30:10 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFiles import GetCompletedFiles as GetCompletedFilesMySQL

class GetCompletedFiles(GetCompletedFilesMySQL, SQLiteBase):
    sql = GetCompletedFilesMySQL.sql

