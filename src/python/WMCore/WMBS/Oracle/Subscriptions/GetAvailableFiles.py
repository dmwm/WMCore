#!/usr/bin/env python
"""
_GetAvailableFiles_

Oracle implementation of Subscription.GetAvailableFiles
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import \
     GetAvailableFiles as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL):
    pass
