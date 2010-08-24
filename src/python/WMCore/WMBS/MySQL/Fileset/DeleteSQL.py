#!/usr/bin/env python
"""
_DeleteFileset_

MySQL implementation of DeleteFileset

"""
__all__ = []
__revision__ = "$Id: DeleteSQL.py,v 1.1 2008/06/09 16:30:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Delete(MySQLBase):
    sql = "delete from wmbs_fileset where name = :fileset"
    
    def getBinds(self, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(name), 'fileset')
        
    def execute(self, name = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(name), 
                         conn = conn, transaction = transaction)
        return self.format(result)