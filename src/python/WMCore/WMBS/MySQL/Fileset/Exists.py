#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Fileset.Exists
"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2008/11/20 21:52:33 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = """select count(*) from wmbs_fileset 
            where name = :name"""
    
    def format(self, result):
        result = DBFormatter.format(self, result)
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