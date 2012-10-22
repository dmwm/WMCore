# -*- coding: utf-8 -*-
"""
_SetRunningJobSlotsForSite_

Set the number of running job slots available at a given site.

Created on Fri Jun 22 14:07:23 2012

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetRunningJobSlotsForSite(DBFormatter):
    sql = "UPDATE wmbs_location SET running_slots = :slots WHERE site_name = :site"

    def execute(self, siteName, runningSlots, conn = None, transaction = False):
        self.dbi.processData(self.sql, {"site": siteName, "slots": runningSlots},
                             conn = conn, transaction = transaction)
        return
