#!/usr/bin/env python
"""
_GetAvailableFiles_

Oracle implementation of Subscription.GetAvailableFiles
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import \
     GetAvailableFiles as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL):
    sql = """SELECT wmbs_sub_files_available.fileid, wmbs_location.se_name
                    FROM wmbs_sub_files_available
               INNER JOIN wmbs_file_location ON
                 wmbs_sub_files_available.fileid = wmbs_file_location.fileid
               INNER JOIN wmbs_location ON
                 wmbs_file_location.location = wmbs_location.id
             WHERE wmbs_sub_files_available.subscription = :subscription"""
