#!/usr/bin/env python
"""
_Delete_

MySQL implementation of Locations.Delete

"""
__all__ = []
__revision__ = "$Id: DeleteSQL.py,v 1.1 2008/06/10 11:55:59 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Delete(MySQLBase):
    sql = "delete from wmbs_location where se_name = :se"
    
    def getBinds(self, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(name), 'se')
        
    def execute(self, name = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(name), 
                         conn = conn, transaction = transaction)
        return self.format(result)