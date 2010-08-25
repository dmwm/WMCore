#!/usr/bin/env python
"""
_GetCompletedByFileList_

Oracle implementation of Subscription.IsFileCompleted
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetCompletedByFileList import \
     GetCompletedByFileList as GetCompletedByFileListMySQL

class GetCompletedByFileList(GetCompletedByFileListMySQL):
    
    """
    returns list of file ids which are in complete status by given list of files
    If it returns the same list as input, it means all the input list is completed 
    """
    sql = GetCompletedByFileListMySQL.sql
