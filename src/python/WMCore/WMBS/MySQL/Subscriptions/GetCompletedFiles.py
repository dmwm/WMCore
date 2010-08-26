#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.GetCompletedFiles
"""

__all__ = []
__revision__ = "$Id: GetCompletedFiles.py,v 1.9 2009/09/11 19:07:30 mnorman Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles

class GetCompletedFiles(GetAvailableFiles):
    sql = """SELECT wmsfc.file, wl.site_name FROM wmbs_sub_files_complete wmsfc
             INNER JOIN wmbs_file_location wfl ON wfl.file = wmsfc.file
             INNER JOIN wmbs_location wl ON wl.id = wfl.location
             WHERE wmsfc.subscription = :subscription
             """

    def execute(self, subscription = None, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"subscription": subscription},
                         conn = conn, transaction = transaction)
        return self.formatDict(results)
