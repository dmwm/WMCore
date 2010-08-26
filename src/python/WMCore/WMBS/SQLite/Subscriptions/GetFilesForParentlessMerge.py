#!/usr/bin/env python
"""
_GetFilesForParentlessMerge_

SQLite implementation of Subscription.GetFilesForParentlessMerge
"""

__revision__ = "$Id: GetFilesForParentlessMerge.py,v 1.1 2010/07/13 21:00:09 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFilesForParentlessMerge import GetFilesForParentlessMerge as GetFilesForParentlessMySQL

class GetFilesForParentless(GetFilesForParentlessMySQL):
    pass
