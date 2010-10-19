#!/usr/bin/env python
"""
_GetAvailableFilesNoLocations_

SQLite implementation of Subscription.GetAvailableFilesNoLocations
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesNoLocations \
     import GetAvailableFilesNoLocations as GetAvailableFilesNoLocationsMySQL

class GetAvailableFilesNoLocations(GetAvailableFilesNoLocationsMySQL):
    pass
