#!/usr/bin/env python
"""
_GetAcquiredFiles_

MySQL implementation of Subscription.GetAcquiredFiles
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles

class GetAcquiredFiles(GetAvailableFiles):
    sql = """SELECT wmsfa.file, wl.site_name FROM wmbs_sub_files_acquired wmsfa
             INNER JOIN wmbs_file_location wfl ON wfl.file = wmsfa.file
             INNER JOIN wmbs_location wl ON wl.id = wfl.location
             WHERE wmsfa.subscription = :subscription
             """

        
    def execute(self, subscription = None, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"subscription": subscription},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
