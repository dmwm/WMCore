#!/usr/bin/env python
"""
_SetPendingJobSlotsForSite_

Set the number of pending job slots available at a given site.
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetPendingJobSlotsForSite(DBFormatter):
    sql = "UPDATE wmbs_location SET pending_slots = :slots WHERE site_name = :site"

    def execute(self, siteName, pendingSlots, conn = None, transaction = False):
        self.dbi.processData(self.sql, {"site": siteName, "slots": pendingSlots},
                             conn = conn, transaction = transaction)
        return
