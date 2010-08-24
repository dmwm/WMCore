#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Files.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2008/11/20 16:46:01 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Exists(MySQLBase):
    sql = "select id from wmbs_file_details where lfn = :lfn"
    
    def format(self, result):
        result = MySQLBase.format(self, result)

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
