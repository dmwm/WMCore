#!/usr/bin/env python
"""
_GetCompletedFilesByRun_

Oracle implementation of Subscription.GetCompletedFilesByRun
"""

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFilesByRun import \
     GetCompletedFilesByRun as GetCompletedFilesByRunMySQL

class GetCompletedFilesByRun(GetCompletedFilesByRunMySQL):
    pass
