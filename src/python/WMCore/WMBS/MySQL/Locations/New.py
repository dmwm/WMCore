#!/usr/bin/env python
"""
_New_

MySQL implementation of Locations.New
"""
from WMCore.WMBS.MySQL.Base import MySQLBase

class New(MySQLBase):
    sql = "insert into wmbs_location (se_name) values (:location)"
    
    def getBinds(self, location = None):
        return self.dbi.buildbinds(self.dbi.makelist(location), 'location')
    
    def execute(self, sename = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(sename), 
                         conn = conn, transaction = transaction)
        return self.format(result)