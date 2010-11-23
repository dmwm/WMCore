#!/usr/bin/env python
"""
_FailFiles_

Oracle implementation of Subscription.FailFiles
"""

from WMCore.WMBS.MySQL.Subscriptions.FailFiles import FailFiles \
     as FailFilesMySQL

class FailFiles(FailFilesMySQL):
    sql = """INSERT INTO wmbs_sub_files_failed (subscription, fileid)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT fileid FROM wmbs_sub_files_failed
                    WHERE fileid = :fileid AND subscription = :subscription)"""
