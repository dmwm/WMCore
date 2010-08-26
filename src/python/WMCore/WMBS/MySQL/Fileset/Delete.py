#!/usr/bin/env python
"""
_DeleteFileset_

MySQL implementation of DeleteFileset

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2008/11/20 21:52:33 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    sql = "delete from wmbs_fileset where name = :fileset"
    
    def getBinds(self, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(name), 'fileset')
        
    def execute(self, name = None, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(name), 
                         conn = conn, transaction = transaction)
        return True #or raise