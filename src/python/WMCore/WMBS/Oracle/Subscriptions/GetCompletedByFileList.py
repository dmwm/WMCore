#!/usr/bin/env python
"""
_GetCompletedByFileList_

Oracle implementation of Subscription.IsFileCompleted
"""

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedByFileList import \
     GetCompletedByFileList as GetCompletedByFileListMySQL

class GetCompletedByFileList(GetCompletedByFileListMySQL):
    pass
