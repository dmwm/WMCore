#!/usr/bin/env python
"""
_GetFailedFiles_

Oracle implementation of Subscription.GetFailedFiles
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import GetFailedFiles \
     as GetFailedFilesMySQL

class GetFailedFiles(GetFailedFilesMySQL):
    sql = """SELECT wmsff.fileid, wl.site_name FROM wmbs_sub_files_failed wmsff
             INNER JOIN wmbs_file_location wfl ON wfl.fileid = wmsff.fileid
             INNER JOIN wmbs_location wl ON wl.id = wfl.location
             WHERE wmsff.subscription = :subscription
             """
