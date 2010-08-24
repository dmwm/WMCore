#!/usr/bin/env python
"""
_DeleteFile_

MySQL implementation of DeleteFile

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/06/16 16:03:36 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Delete(MySQLBase):
    sql = "delete from wmbs_file_details where lfn = :file"
    
    def getBinds(self, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(name), 'file')
        
    def execute(self, file = None, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(file), 
                         conn = conn, transaction = transaction)
        return True #or raise