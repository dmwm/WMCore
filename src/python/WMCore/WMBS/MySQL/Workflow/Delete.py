#!/usr/bin/env python
"""
_DeleteWorkflow_

MySQL implementation of DeleteWorkflow

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/06/12 10:02:06 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Delete(MySQLBase):
    """
    Create a workflow ready for subscriptions
    """
    sql = """delete from wmbs_workflow where spec = :spec and owner = :owner and name = :name"""
    
    def getBinds(self, spec=None, owner=None, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(owner), 'owner',
                                   self.dbi.buildbinds(self.dbi.makelist(spec), 'spec',
                                   self.dbi.buildbinds(self.dbi.makelist(name), 'name')))
        
    def execute(self, spec=None, owner=None, conn = None, name = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(spec, owner, name), 
                     conn = conn, transaction = transaction)
        return True #or raise