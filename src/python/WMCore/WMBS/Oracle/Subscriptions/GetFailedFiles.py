#!/usr/bin/env python
"""
_AcquireFiles_

Oracle implementation of Subscription.GetFailedFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetFailedFiles.py,v 1.3 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import GetFailedFiles \
     as GetFailedFilesMySQL

class GetFailedFiles(GetFailedFilesMySQL):
    sql = """ select fileid from wmbs_sub_files_failed 
              where subscription=:subscription """