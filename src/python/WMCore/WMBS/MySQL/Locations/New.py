#!/usr/bin/env python
"""
_New_

MySQL implementation of Locations.New
"""

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_location (se_name) SELECT :location AS se_name
             FROM DUAL WHERE NOT EXISTS (SELECT se_name FROM wmbs_location
             WHERE se_name = :location)"""
    
    def getBinds(self, location = None):
        return self.dbi.buildbinds(self.dbi.makelist(location), 'location')
    
    def format(self, result):
        return True
    
    def execute(self, sename = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(sename), 
                         conn = conn, transaction = transaction)
        return self.format(result)
