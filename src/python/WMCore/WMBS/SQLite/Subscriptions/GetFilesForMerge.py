#!/usr/bin/env python
"""
_GetFilesForMerge_

SQLite implementation of Subscription.GetFilesForMerge
"""

__all__ = []
__revision__ = "$Id: GetFilesForMerge.py,v 1.1 2009/03/09 18:37:00 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFilesForMerge import GetFilesForMerge as GetFilesForMergeMySQL

class GetFilesForMerge(GetFilesForMergeMySQL):
    sql = GetFilesForMergeMySQL.sql
