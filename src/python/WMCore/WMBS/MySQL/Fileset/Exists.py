#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Fileset.Exists
"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2008/06/12 10:01:59 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Exists(MySQLBase):
    sql = """select count(*) from wmbs_fileset 
            where name = :name"""
    
    def format(self, result):
        result = MySQLBase.format(self, result)
        self.logger.debug( result )
        if result[0][0] > 0:
            return True
        else:
            return False
        
    def getBinds(self, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(name), 'name')
        
    def execute(self, name = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(name), 
                         conn = conn, transaction = transaction)
        return self.format(result)