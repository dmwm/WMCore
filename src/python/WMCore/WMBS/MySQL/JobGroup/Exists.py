#!/usr/bin/env python
"""
_Exists_

MySQL implementation of JobGroup.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.3 2008/11/24 21:47:10 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = "select id from wmbs_jobgroup where uid = :guid"
    
    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) == 0:
            return False
        else:
            return result[0][0]
    
    def getBinds(self, uid=None):
        return self.dbi.buildbinds(self.dbi.makelist(uid), "guid")
        
    def execute(self, uid, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(uid), 
                         conn = conn, transaction = transaction)
        return self.format(result)
