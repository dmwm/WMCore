#!/usr/bin/env python
"""
_GetAvailableFilesByLimit

SQLite implementation of Subscription.GetAvailableFilesByLimit
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesByLimit \
     import GetAvailableFilesByLimit as GetAvailableFilesByLimitMySQL

class GetAvailableFilesByLimit(GetAvailableFilesByLimitMySQL):
    pass
