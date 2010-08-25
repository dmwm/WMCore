#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.GetFailedFiles

Return a list of files that are available for processing
"""
__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import \
     GetFailedFiles as GetFailedFilesMySQL

class GetFailedFiles(GetFailedFilesMySQL):
    sql = GetFailedFilesMySQL.sql