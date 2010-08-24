#!/usr/bin/env python
"""
_DeleteWorkflow_

MySQL implementation of DeleteWorkflow

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2008/06/23 09:35:29 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Delete(MySQLBase):
    """
    Create a workflow ready for subscriptions
    """
    sql = """delete from wmbs_workflow where id = :id"""
    
    def getBinds(self, id=None):
        return self.dbi.buildbinds(self.dbi.makelist(id), 'id')
        
    def execute(self, id = -1, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(id), 
                     conn = conn, transaction = transaction)
        return True #or raise