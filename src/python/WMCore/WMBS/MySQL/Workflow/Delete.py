#!/usr/bin/env python
"""
_DeleteWorkflow_

MySQL implementation of DeleteWorkflow

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.3 2008/06/23 16:01:16 metson Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Delete(MySQLBase):
    """
    Create a workflow ready for subscriptions
    """
    sql = """delete from wmbs_workflow where id = :id"""
    
    def getBinds(self, id=None):
        return self.dbi.buildbinds(self.dbi.makelist(id), 'id')
        
    def execute(self, id = -1, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(id), 
                     conn = conn, transaction = transaction)
        return True #or raise