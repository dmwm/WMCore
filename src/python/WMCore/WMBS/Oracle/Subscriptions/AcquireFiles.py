#!/usr/bin/env python
"""
_AcquireFiles_

Oracle implementation of Subscription.AcquireFiles
"""

__all__ = []
__revision__ = "$Id: AcquireFiles.py,v 1.4 2009/03/20 14:29:17 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.AcquireFiles import AcquireFiles \
     as AcquireFilesMySQL

class AcquireFiles(AcquireFilesMySQL):
    sql = """INSERT INTO wmbs_sub_files_acquired (subscription, fileid)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT fileid FROM wmbs_sub_files_acquired WHERE fileid = :fileid)"""
