#!/usr/bin/env python
"""
_CompleteFiles_

SQLite implementation of Subscription.CompleteFiles
"""
__all__ = []
__revision__ = "$Id: CompleteFiles.py,v 1.6 2009/03/23 19:05:10 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.MySQL.Subscriptions.CompleteFiles import CompleteFiles \
     as CompleteFilesMySQL

class CompleteFiles(CompleteFilesMySQL):
    sql = """INSERT INTO wmbs_sub_files_complete (subscription, file)
               SELECT :subscription, :fileid WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_complete
                    WHERE file = :fileid AND subscription = :subscription)"""
