#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.GetFailedFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetFailedFiles.py,v 1.3 2008/11/20 21:54:27 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import GetFailedFiles as GetFailedFilesMySQL

class GetFailedFiles(GetFailedFilesMySQL):
    sql = GetFailedFilesMySQL.sql