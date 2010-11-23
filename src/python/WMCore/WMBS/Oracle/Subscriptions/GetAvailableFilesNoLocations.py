#!/usr/bin/env python
"""
_GetAvailableFilesNoLocations_

Oracle implementation of Subscription.GetAvailableFilesNoLocations
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesNoLocations \
     import GetAvailableFilesNoLocations as GetAvailableFilesNoLocationsMySQL

class GetAvailableFilesNoLocations(GetAvailableFilesNoLocationsMySQL):
    pass
