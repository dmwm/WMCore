#!/usr/bin/env python
"""
_GetFailedFiles_

Oracle implementation of Subscription.GetFailedFiles
"""

from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import GetFailedFiles \
     as GetFailedFilesMySQL

class GetFailedFiles(GetFailedFilesMySQL):
    pass
