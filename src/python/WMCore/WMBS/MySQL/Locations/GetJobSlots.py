# -*- coding: utf-8 -*-
"""
_GetJobSlots_

MySQL implementation of Locations.GetJobSlots
Created on Mon Jun 18 15:26:58 2012

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetJobSlots(DBFormatter):
    sql = """SELECT pending_slots, running_slots FROM  wmbs_location
             WHERE site_name = :location
             """

    def execute(self, siteName, conn = None, transaction = False):
        binds = {"location": siteName}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        formatted = self.formatDict(result)

        return formatted
