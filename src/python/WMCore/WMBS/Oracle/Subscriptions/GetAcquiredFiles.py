#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAcquiredFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetAcquiredFiles.py,v 1.2 2008/11/24 21:51:47 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAcquiredFiles import GetAcquiredFiles as GetAcquiredFilesMySQL

class GetAcquiredFiles(GetAcquiredFilesMySQL):
    sql = "select fileid from wmbs_sub_files_acquired where subscription=:subscription"

