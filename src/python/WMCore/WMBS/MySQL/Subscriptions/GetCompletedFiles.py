#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.GetCompletedFiles
"""

__all__ = []

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles


class GetCompletedFiles(GetAvailableFiles):
    sql = """SELECT wmsfc.fileid, wl.site_name FROM wmbs_sub_files_complete wmsfc
             INNER JOIN wmbs_file_location wfl ON wfl.fileid = wmsfc.fileid
             INNER JOIN wmbs_location_pnns wlpnn ON wlpnn.pnn = wfl.pnn
             INNER JOIN wmbs_location wl ON wl.id = wlpnn.location
             WHERE wmsfc.subscription = :subscription
             """

    def execute(self, subscription=None, conn=None, transaction=False):
        results = self.dbi.processData(self.sql, {"subscription": subscription},
                                       conn=conn, transaction=transaction)
        return self.formatDict(results)
