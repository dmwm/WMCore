#!/usr/bin/env python
"""
_GetFilesForParentlessMerge_

SQLite implementation of Subscription.GetFilesForParentlessMerge
"""




from WMCore.WMBS.MySQL.Subscriptions.GetFilesForParentlessMerge import GetFilesForParentlessMerge as GetFilesForParentlessMySQL

class GetFilesForParentless(GetFilesForParentlessMySQL):
    pass
