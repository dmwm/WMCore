#!/usr/bin/env python
"""
_New_

MySQL implementation of Locations.SetJobSlots
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetJobSlots(DBFormatter):
    sql = """SELECT job_slots FROM  wmbs_location
             WHERE site_name = :location
             """
    
    def execute(self, siteName, conn = None, transaction = False):
        binds = {"location": siteName}
        result = self.dbi.processData(self.sql, binds, conn = conn, 
                                      transaction = transaction)

        return result[0].fetchall()[0].values()[0]
