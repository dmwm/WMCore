#!/usr/bin/env python
"""
_AcquireFiles_

Oracle implementation of Subscription.GetAcquiredFiles
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAcquiredFiles import GetAcquiredFiles \
     as GetAcquiredFilesMySQL

class GetAcquiredFiles(GetAcquiredFilesMySQL):
    pass
