#!/usr/bin/env python
"""
_New_

MySQL implementation of Locations.New
"""

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_location (site_name, job_slots) 
               SELECT :location AS site_name, :slots AS job_slots FROM DUAL WHERE NOT EXISTS
                (SELECT site_name FROM wmbs_location WHERE site_name = :location)"""
    
    def execute(self, siteName = None, jobSlots = 0, conn = None, transaction = False):
        binds = {"location": siteName, "slots": jobSlots}
        self.dbi.processData(self.sql, binds, conn = conn, 
                             transaction = transaction)
        return
