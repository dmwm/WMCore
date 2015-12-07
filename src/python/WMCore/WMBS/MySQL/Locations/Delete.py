#!/usr/bin/env python
"""
_Delete_

MySQL implementation of Locations.Delete
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    sql = "delete from wmbs_location where site_name = :location"

    def getBinds(self, location = None):
        return self.dbi.buildbinds(self.dbi.makelist(location), 'location')

    def execute(self, siteName = None, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(location = siteName),
                         conn = conn, transaction = transaction)
        return True
