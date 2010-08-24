#!/usr/bin/env python
"""
_NewWorkflow_

MySQL implementation of NewWorkflow

"""
__all__ = []
__revision__ = "$Id: NewSQL.py,v 1.1 2008/06/09 16:23:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class New(MySQLBase):
    """
    Create a workflow ready for subscriptions
    """
    sql = """insert into wmbs_workflow (spec, owner, name)
                values (:spec, :owner, :name)"""
    
    def getBinds(self, spec=None, owner=None, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(owner), 'owner',
                                   self.dbi.buildbinds(self.dbi.makelist(spec), 'spec',
                                   self.dbi.buildbinds(self.dbi.makelist(name), 'name')))
        
    def execute(self, spec=None, owner=None, conn = None, name = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(spec, owner, name), 
                         conn = conn, transaction = transaction)
        return self.format(result)