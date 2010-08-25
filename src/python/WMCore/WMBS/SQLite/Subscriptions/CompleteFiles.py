#!/usr/bin/env python
"""
_CompleteFiles_

SQLite implementation of Subscription.CompleteFiles
"""
__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.CompleteFiles import CompleteFiles \
     as CompleteFilesMySQL

class CompleteFiles(CompleteFilesMySQL):
    sql = """INSERT INTO wmbs_sub_files_complete (subscription, file)
               SELECT :subscription, :fileid WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_complete
                    WHERE file = :fileid AND subscription = :subscription)"""
