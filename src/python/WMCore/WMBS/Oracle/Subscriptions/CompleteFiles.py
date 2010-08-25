#!/usr/bin/env python
"""
_CompleteFiles_

Oracle implementation of Subscription.CompleteFiles
"""

__all__ = []
__revision__ = "$Id: CompleteFiles.py,v 1.4 2009/03/20 14:29:17 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.CompleteFiles import CompleteFiles \
     as CompleteFilesMySQL

class CompleteFiles(CompleteFilesMySQL):
    sql = """INSERT INTO wmbs_sub_files_complete (subscription, fileid)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT fileid FROM wmbs_sub_files_complete WHERE fileid = :fileid)"""    
