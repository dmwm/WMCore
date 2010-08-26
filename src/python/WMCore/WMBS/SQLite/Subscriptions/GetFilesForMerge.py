#!/usr/bin/env python
"""
_GetFilesForMerge_

SQLite implementation of Subscription.GetFilesForMerge
"""

__revision__ = "$Id: GetFilesForMerge.py,v 1.2 2010/03/08 17:06:09 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFilesForMerge import GetFilesForMerge as GetFilesForMergeMySQL

class GetFilesForMerge(GetFilesForMergeMySQL):
    pass
