# -*- coding: utf-8 -*-
"""
_GetPendingSlots_

MySQL implementation of Locations.GetPendingSlots
Created on Mon Jun 18 12:39:06 2012

@author: dballest
"""

from future.utils import listvalues

from WMCore.Database.DBFormatter import DBFormatter

class GetPendingSlots(DBFormatter):
    sql = """SELECT pending_slots FROM  wmbs_location
             WHERE site_name = :location
             """

    def execute(self, siteName, conn = None, transaction = False):
        binds = {"location": siteName}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return listvalues(result[0].fetchall()[0])[0]
