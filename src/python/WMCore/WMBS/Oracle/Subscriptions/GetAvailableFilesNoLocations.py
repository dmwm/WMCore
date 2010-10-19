#!/usr/bin/env python
"""
_GetAvailableFilesNoLocations_

Oracle implementation of Subscription.GetAvailableFilesNoLocations
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesNoLocations \
     import GetAvailableFilesNoLocations as GetAvailableFilesNoLocationsMySQL

class GetAvailableFilesNoLocations(GetAvailableFilesNoLocationsMySQL):
    sql = """SELECT wmbs_sub_files_available.fileid AS fileid FROM wmbs_sub_files_available
             WHERE wmbs_sub_files_available.subscription = :subscription"""
