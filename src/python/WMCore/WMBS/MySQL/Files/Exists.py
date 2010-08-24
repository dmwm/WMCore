#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Files.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2008/10/22 19:08:27 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Exists(MySQLBase):
    sql = "select id from wmbs_file_details where lfn = :lfn"
    
    def format(self, result):
        result = MySQLBase.format(self, result)
        try:
            return result[0][0]
        except Exception, e:
            self.logger.error('Exists Exception: %s' % e)
            self.logger.debug( 'Exists Result: %s' % result )
            return False
    
    def getBinds(self, lfn=None):
        return self.dbi.buildbinds(self.dbi.makelist(lfn), "lfn")
        
    def execute(self, lfn=None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(lfn), 
                         conn = conn, transaction = transaction)
        return self.format(result)
