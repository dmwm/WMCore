#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Files.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2009/01/12 23:02:38 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = "select id from dbsbuffer_file where lfn = :lfn"
    
    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) == 0:
            return False
        else:
            return result[0][0]
    
    def getBinds(self, lfn=None):
        return self.dbi.buildbinds(self.dbi.makelist(lfn), "lfn")
        
    def execute(self, lfn=None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(lfn), 
                         conn = conn, transaction = transaction)
        return self.format(result)
