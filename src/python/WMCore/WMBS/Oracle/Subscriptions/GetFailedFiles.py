#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.GetFailedFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetFailedFiles.py,v 1.2 2008/11/24 21:51:46 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import GetFailedFiles as GetFailedFilesMySQL

class GetFailedFiles(GetFailedFilesMySQL):
    sql = "select fileid from wmbs_sub_files_failed where subscription=:subscription"