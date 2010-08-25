#!/usr/bin/env python
"""
_FailFiles_

SQLite implementation of Subscription.FailFiles
"""
__all__ = []
__revision__ = "$Id: FailFiles.py,v 1.4 2009/03/20 14:29:16 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.FailFiles import FailFiles as FailFilesMySQL

class FailFiles(FailFilesMySQL):
    sql = """INSERT INTO wmbs_sub_files_failed (subscription, file)
               SELECT :subscription, :fileid WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_failed WHERE file = :fileid)"""
