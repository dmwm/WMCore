#!/usr/bin/env python
"""
_GetCompletedByFileList_

Oracle implementation of Subscription.IsFileCompleted
"""

__all__ = []
__revision__ = "$Id: GetCompletedByFileList.py,v 1.1 2009/09/29 18:35:54 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedByFileList import \
     GetCompletedByFileList as GetCompletedByFileListMySQL

class GetCompletedByFileList(GetCompletedByFileListMySQL):
    
    """
    returns list of file ids which are in complete status by given list of files
    If it returns the same list as input, it means all the input list is completed 
    """
    sql = """SELECT fileid FROM wmbs_sub_files_complete 
                  WHERE subscription = :subscription AND fileid = :fileid
           """
