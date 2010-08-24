#!/usr/bin/env python
"""
_GetCompletedFilesByRun_

SQLite implementation of Subscription.GetCompletedFilesByRun
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFilesByRun import \
     GetCompletedFilesByRun as GetCompletedFilesByRunMySQL

class GetCompletedFilesByRun(GetCompletedFilesByRunMySQL):
    sql = GetCompletedFilesByRunMySQL.sql
