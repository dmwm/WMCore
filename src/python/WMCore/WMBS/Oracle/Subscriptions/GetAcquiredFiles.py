#!/usr/bin/env python
"""
_AcquireFiles_

Oracle implementation of Subscription.GetAcquiredFiles
"""

__all__ = []
__revision__ = "$Id: GetAcquiredFiles.py,v 1.5 2009/03/18 13:21:59 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAcquiredFiles import GetAcquiredFiles \
     as GetAcquiredFilesMySQL

class GetAcquiredFiles(GetAcquiredFilesMySQL):
    sql = """SELECT fileid FROM wmbs_sub_files_acquired 
             WHERE subscription = :subscription
             """
