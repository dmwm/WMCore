#!/usr/bin/env python
"""
_GetFailedFiles_

MySQL implementation of Subscription.GetFailedFiles
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles

class GetFailedFiles(GetAvailableFiles):
    sql = """SELECT wmsff.file, wl.site_name FROM wmbs_sub_files_failed wmsff
             INNER JOIN wmbs_file_location wfl ON wfl.file = wmsff.file
             INNER JOIN wmbs_location wl ON wl.id = wfl.location
             WHERE wmsff.subscription = :subscription
             """

    def execute(self, subscription = None, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"subscription": subscription},
                         conn = conn, transaction = transaction)
        return self.formatDict(results)
