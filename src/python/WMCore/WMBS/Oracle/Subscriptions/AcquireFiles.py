#!/usr/bin/env python
"""
_AcquireFiles_

Oracle implementation of Subscription.AcquireFiles
"""

__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.5 2009/03/23 19:05:09 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles \
     as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL):
    sql = """INSERT INTO wmbs_sub_files_acquired (subscription, fileid)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT fileid FROM wmbs_sub_files_acquired
                    WHERE fileid = :fileid AND subscription = :subscription)"""
