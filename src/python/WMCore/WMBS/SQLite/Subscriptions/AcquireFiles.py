#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.AcquireFiles
"""

__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.5 2009/03/20 14:29:16 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles \
     as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL):
    sql = """INSERT INTO wmbs_sub_files_acquired (subscription, file)
               SELECT :subscription, :fileid WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_acquired WHERE file = :fileid)"""    
