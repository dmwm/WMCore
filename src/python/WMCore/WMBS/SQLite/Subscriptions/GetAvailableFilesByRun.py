#!/usr/bin/env python
"""
_GetAvailableFilesByRun_

SQLite implementation of Subscription.GetAvailableFilesByRun
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesByRun \
    import GetAvailableFilesByRun as GetAvailableFilesByRunMySQL

class GetAvailableFilesByRun(GetAvailableFilesByRunMySQL):
    pass
