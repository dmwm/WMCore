#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.GetFailedFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetFailedFiles.py,v 1.4 2008/12/05 21:06:58 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import \
     GetFailedFiles as GetFailedFilesMySQL

class GetFailedFiles(GetFailedFilesMySQL):
    sql = GetFailedFilesMySQL.sql