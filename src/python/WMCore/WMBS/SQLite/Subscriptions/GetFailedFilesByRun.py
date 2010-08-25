#!/usr/bin/env python
"""
_GetFailedFiles_

SQLite implementation of Subscription.GetFailedFiles
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetFailedFilesByRun import \
     GetFailedFilesByRun as GetFailedFilesByRunMySQL

class GetFailedFilesByRun(GetFailedFilesByRunMySQL):
    sql = GetFailedFilesByRunMySQL.sql 