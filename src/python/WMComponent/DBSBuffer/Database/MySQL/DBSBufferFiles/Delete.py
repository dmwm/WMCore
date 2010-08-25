#!/usr/bin/env python
"""
_DeleteFile_

MySQL implementation of DeleteFile

"""

__revision__ = "$Id: Delete.py,v 1.2 2009/07/13 19:55:26 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    sql = "delete from dbsbuffer_file where lfn = :lfn"
    
    def getBinds(self, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(name), 'lfn')
        
    def execute(self, file = None, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(file), 
                         conn = conn, transaction = transaction)
        return
