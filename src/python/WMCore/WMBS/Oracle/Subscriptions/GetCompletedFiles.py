#!/usr/bin/env python
"""
_GetCompletedFiles_

Oracle implementation of Subscription.GetCompletedFiles
"""

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFiles import \
     GetCompletedFiles as GetCompletedFilesMySQL

class GetCompletedFiles(GetCompletedFilesMySQL):
    pass
