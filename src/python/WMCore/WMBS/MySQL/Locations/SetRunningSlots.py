# -*- coding: utf-8 -*-
"""
_SetRunningSlots_

MySQL implementation of Locations.SetRunningSlots

Created on Mon Jun 18 13:18:57 2012

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetRunningSlots(DBFormatter):
    sql = """UPDATE wmbs_location
             SET running_slots = :slots
             WHERE site_name = :location
             """

    def execute(self, siteName, jobSlots = 0, conn = None, transaction = False):
        binds = {"location": siteName, "slots": jobSlots}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
