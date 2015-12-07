# -*- coding: utf-8 -*-
"""
_SetPendingSlots_

MySQL implementation of Locations.SetPendingSlots

Created on Mon Jun 18 13:16:30 2012

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetPendingSlots(DBFormatter):
    sql = """UPDATE wmbs_location
             SET pending_slots = :slots
             WHERE site_name = :location
             """

    def execute(self, siteName, jobSlots = 0, conn = None, transaction = False):
        binds = {"location": siteName, "slots": jobSlots}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
