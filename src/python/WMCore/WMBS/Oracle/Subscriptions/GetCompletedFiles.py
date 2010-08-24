#!/usr/bin/env python
"""
_GetCompletedFiles_

Oracle implementation of Subscription.GetCompletedFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetCompletedFiles.py,v 1.3 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFiles import \
     GetCompletedFiles as GetCompletedFilesMySQL

class GetCompletedFiles(GetCompletedFilesMySQL):
    
    sql = """select fileid from wmbs_sub_files_complete 
             where subscription=:subscription"""

