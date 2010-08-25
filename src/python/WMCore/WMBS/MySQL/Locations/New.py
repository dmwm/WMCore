#!/usr/bin/env python
"""
_New_

MySQL implementation of Locations.New
"""

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_location (site_name) SELECT :location AS site_name
             FROM DUAL WHERE NOT EXISTS (SELECT site_name FROM wmbs_location
             WHERE site_name = :location)"""
    
    def getBinds(self, location = None):
        return self.dbi.buildbinds(self.dbi.makelist(location), 'location')
    
    def execute(self, siteName = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(siteName), 
                         conn = conn, transaction = transaction)
        return
