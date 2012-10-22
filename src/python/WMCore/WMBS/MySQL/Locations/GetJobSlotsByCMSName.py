#!/usr/bin/env python
"""
_GetJobSlotsByCMSName_

MySQL implementation of Locations.GetSiteInfo
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class GetJobSlotsByCMSName(DBFormatter):
    """
    get number of jobSlots by cms name
    """

    sql = "SELECT cms_name, SUM(job_slots) AS job_slots FROM wmbs_location GROUP BY cms_name"


    def execute(self, conn = None, transaction = False):

        results = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        return self.formatDict(results)
