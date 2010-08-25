#!/usr/bin/env python
"""
_New_

MySQL implementation of Locations.SetJobSlots
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetJobSlots(DBFormatter):
    sql = """UPDATE wmbs_location
             SET job_slots = :slots
             WHERE site_name = :location
             """
    
    def execute(self, siteName, jobSlots = 0, conn = None, transaction = False):
        binds = {"location": siteName, "slots": jobSlots}
        self.dbi.processData(self.sql, binds, conn = conn, 
                             transaction = transaction)
        return
