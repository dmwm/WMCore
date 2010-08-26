#!/usr/bin/env python
"""
_Delete_

MySQL implementation of Jobs.Delete

"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.3 2008/12/10 22:26:15 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    sql = "delete from wmbs_job where id = :id"
    
    def getBinds(self, id = None):
        return self.dbi.buildbinds(self.dbi.makelist(id), "id")
    
    def format(self, result):
        return True
        
    def execute(self, id, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(id), 
                         conn = conn, transaction = transaction)
        return True #or raise
