#!/usr/bin/env python
"""
_GetAllJobGroups_

Oracle implementation of Subscriptions.GetJobGroups
"""




from WMCore.WMBS.MySQL.Subscriptions.GetAllJobGroups import GetAllJobGroups as MySQLGetAllJobGroups

class GetAllJobGroups(MySQLGetAllJobGroups):
    """
    Identical to MySQL version

    """
