#!/usr/bin/env python
"""
_GetAvailableFilesID_

Oracle implementation of Subscription.GetAvailableFilesID
"""



from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesID import \
     GetAvailableFilesID as GetAvailableFilesIDMySQL

class GetAvailableFilesID(GetAvailableFilesIDMySQL):
    """
    Identical to MySQL version

    """
