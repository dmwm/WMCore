#!/usr/bin/env python
"""
_GetCompletedFiles_

Oracle implementation of Subscription.GetCompletedFiles

Return a list of files that are available for processing
"""

__all__ = []
__revision__ = "$Id: GetCompletedFiles.py,v 1.4 2009/01/12 19:26:05 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFiles import \
     GetCompletedFiles as GetCompletedFilesMySQL

class GetCompletedFiles(GetCompletedFilesMySQL):
    sql = """select fileid from wmbs_sub_files_complete 
             where subscription=:subscription"""

