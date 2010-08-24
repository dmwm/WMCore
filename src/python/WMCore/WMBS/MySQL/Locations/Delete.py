#!/usr/bin/env python
"""
_Delete_

MySQL implementation of Locations.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/06/12 10:02:09 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Delete(MySQLBase):
    sql = "delete from wmbs_location where se_name = :location"
    
    def getBinds(self, location = None):
        return self.dbi.buildbinds(self.dbi.makelist(location), 'location')
        
    def execute(self, sename = None, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(location=sename), 
                         conn = conn, transaction = transaction)
        return True #or raise