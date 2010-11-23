#!/usr/bin/env python
"""
_GetAvailableFilesNoLocations_

Retrieve the IDs of available files without their locations.
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles as GetAvailableFilesMySQL

class GetAvailableFilesNoLocations(GetAvailableFilesMySQL):
    sql = """SELECT wmbs_sub_files_available.fileid AS fileid FROM wmbs_sub_files_available
             WHERE wmbs_sub_files_available.subscription = :subscription"""
