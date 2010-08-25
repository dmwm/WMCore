#!/usr/bin/env python
"""
_GetFilesForMerge_

SQLite implementation of Subscription.GetFilesForMerge
"""




from WMCore.WMBS.MySQL.Subscriptions.GetFilesForMerge import GetFilesForMerge as GetFilesForMergeMySQL

class GetFilesForMerge(GetFilesForMergeMySQL):
    pass
