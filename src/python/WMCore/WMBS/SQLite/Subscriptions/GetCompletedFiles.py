#!/usr/bin/env python
"""
_GetCompletedFiles_

SQLite implementation of Subscription.GetCompletedFiles

Return a list of files that are available for processing
"""
__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFiles import \
     GetCompletedFiles as GetCompletedFilesMySQL

class GetCompletedFiles(GetCompletedFilesMySQL):
    sql = GetCompletedFilesMySQL.sql

