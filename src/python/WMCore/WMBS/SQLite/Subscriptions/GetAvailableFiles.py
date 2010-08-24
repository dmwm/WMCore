#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFiles.py,v 1.2 2008/07/21 14:27:06 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL, SQLiteBase):
    sql = """select file from wmbs_fileset_files where
        fileset = (select fileset from wmbs_subscription where id=:subscription)
        and file not in 
            (select file from wmbs_sub_files_acquired where subscription=:subscription)
        and file not in 
            (select file from wmbs_sub_files_failed where subscription=:subscription)
        and file not in 
            (select file from wmbs_sub_files_complete where subscription=:subscription)"""