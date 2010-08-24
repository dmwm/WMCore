#!/usr/bin/env python
"""
_GetAllJobGroups_

SQLite implementation of Subscriptions.GetJobGroups
"""




from WMCore.WMBS.MySQL.Subscriptions.GetAllJobGroups import GetAllJobGroups as MySQLGetAllJobGroups

class GetAllJobGroups(MySQLGetAllJobGroups):
    """
    Identical to MySQL version

    """
