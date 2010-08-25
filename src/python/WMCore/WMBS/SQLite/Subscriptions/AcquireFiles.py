#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.AcquireFiles
"""

__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.6 2009/03/23 19:05:10 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles \
     as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL):
    sql = """INSERT INTO wmbs_sub_files_acquired (subscription, file)
               SELECT :subscription, :fileid WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_acquired
                    WHERE file = :fileid AND subscription = :subscription)"""    
