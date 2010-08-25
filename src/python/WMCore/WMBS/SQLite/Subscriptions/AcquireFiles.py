#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.AcquireFiles
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles \
     as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL):
    sql = """INSERT INTO wmbs_sub_files_acquired (subscription, file)
               SELECT :subscription, :fileid WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_acquired
                    WHERE file = :fileid AND subscription = :subscription)"""    
