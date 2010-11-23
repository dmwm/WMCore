#!/usr/bin/env python
"""
_GetAvailableFilesByRun_

Oracle implementation of Subscription.GetAvailableFilesByRun
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesByRun import \
     GetAvailableFilesByRun as GetAvailableFilesByRunMySQL

class GetAvailableFilesByRun(GetAvailableFilesByRunMySQL):
    pass
