#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAcquiredFiles

Return a list of files that are available for processing
"""
__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetAcquiredFiles import GetAcquiredFiles \
     as GetAcquiredFilesMySQL

class GetAcquiredFiles(GetAcquiredFilesMySQL):
    sql = GetAcquiredFilesMySQL.sql

