#!/usr/bin/env python
"""
_CompleteFiles_

Oracle implementation of Subscription.CompleteFiles
"""

from WMCore.WMBS.MySQL.Subscriptions.CompleteFiles import CompleteFiles \
     as CompleteFilesMySQL

class CompleteFiles(CompleteFilesMySQL):
    sql = """INSERT INTO wmbs_sub_files_complete (subscription, fileid)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT fileid FROM wmbs_sub_files_complete
                    WHERE fileid = :fileid AND subscription = :subscription)"""
