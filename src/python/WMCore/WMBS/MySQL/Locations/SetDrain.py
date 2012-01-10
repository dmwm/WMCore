#!/usr/bin/env python
"""
_New_

MySQL implementation of Locations.SetJobSlots
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetDrain(DBFormatter):
    sql = """UPDATE wmbs_location
             SET drain = :drain
             WHERE site_name = :location
             """

    def execute(self, siteName, drain = True, conn = None, transaction = False):
        binds = {"location": siteName, "drain": drain}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
